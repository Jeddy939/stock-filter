import crypto from "node:crypto";
import cors from "cors";
import express, {type NextFunction, type Request, type Response} from "express";
import {GoogleAuth, OAuth2Client} from "google-auth-library";
import {Pool} from "pg";
import {ApiError, requireAdmin, requireAnalyst, requireAppCheck, requireAuth, type UserContext} from "./auth";
import {currentMarket, MARKET_DEFAULTS, rangeDays, VALID_LABELS, yahooUrl} from "./market";

interface JobRow {
  id?: string;
  job_type?: string;
  status?: string;
  stage?: string;
  detail?: string;
  current_count?: number | string;
  total_count?: number | string | null;
  percent?: number | string;
  parameters_json?: unknown;
  result_json?: unknown;
  error?: string | null;
  [key: string]: unknown;
}

interface PriceRow {
  date: string | Date;
  open: number | string | null;
  high: number | string | null;
  low: number | string | null;
  close: number | string | null;
  volume: number | string | null;
}

const apiApp = express();
apiApp.use(cors({origin: true}));
apiApp.use(express.json({limit: "5mb"}));

let pool: Pool | undefined;

function databaseUrl(): string {
  const value = String(process.env.MONEYMAKER_DATABASE_URL ?? process.env.DATABASE_URL ?? "").trim();
  if (!value) throw new ApiError(503, "MONEYMAKER_DATABASE_URL is not configured");
  return value;
}

function db(): Pool {
  if (!pool) {
    pool = new Pool({
      connectionString: databaseUrl(),
      max: Number(process.env.MONEYMAKER_DB_POOL_MAX ?? 5),
      idleTimeoutMillis: 30000
    });
  }
  return pool;
}

function asyncRoute(handler: (req: Request, res: Response) => Promise<void>) {
  return (req: Request, res: Response) => {
    handler(req, res).catch((error) => {
      const status = error instanceof ApiError ? error.status : 500;
      const message = error instanceof Error ? error.message : String(error);
      res.status(status).json({ok: false, error: message, detail: message});
    });
  };
}

function numberOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function dateOnly(value: string | Date): string {
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return String(value).slice(0, 10);
}

function jobPayload(row?: JobRow | null): Record<string, unknown> {
  if (!row) {
    return {running: false, success: null, message: "Idle", stage: "Idle", percent: 0};
  }
  const status = String(row.status ?? "queued");
  return {
    ...row,
    running: status === "queued" || status === "running",
    success: status === "succeeded" ? true : status === "failed" ? false : null,
    message: row.detail ?? status.charAt(0).toUpperCase() + status.slice(1),
    current: row.current_count ?? 0,
    total: row.total_count ?? null,
    summary: row.parameters_json ?? {},
    results: row.result_json ?? []
  };
}

function intervalKey(date: string, interval: string): string {
  const parsed = new Date(`${date}T00:00:00.000Z`);
  if (interval === "monthly") {
    return `${parsed.getUTCFullYear()}-${String(parsed.getUTCMonth() + 1).padStart(2, "0")}-01`;
  }
  if (interval !== "weekly") return date;
  const day = parsed.getUTCDay();
  const daysUntilFriday = (5 - day + 7) % 7;
  parsed.setUTCDate(parsed.getUTCDate() + daysUntilFriday);
  return parsed.toISOString().slice(0, 10);
}

function aggregateRows(rows: PriceRow[], interval: string): PriceRow[] {
  if (interval === "daily") return rows;
  const grouped = new Map<string, PriceRow>();
  for (const row of rows) {
    const key = intervalKey(dateOnly(row.date), interval);
    const current = grouped.get(key);
    if (!current) {
      grouped.set(key, {...row, date: key});
      continue;
    }
    current.high = Math.max(numberOrNull(current.high) ?? -Infinity, numberOrNull(row.high) ?? -Infinity);
    current.low = Math.min(numberOrNull(current.low) ?? Infinity, numberOrNull(row.low) ?? Infinity);
    current.close = row.close;
    current.volume = (numberOrNull(current.volume) ?? 0) + (numberOrNull(row.volume) ?? 0);
  }
  return Array.from(grouped.values()).filter((row) => numberOrNull(row.close) !== null);
}

function movingAverage(values: Array<number | null>, period: number): Array<number | null> {
  let sum = 0;
  const output: Array<number | null> = [];
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index] ?? 0;
    sum += value;
    if (index >= period) sum -= values[index - period] ?? 0;
    output.push(index >= period - 1 ? sum / period : null);
  }
  return output;
}

async function dispatchCloudRunJob(jobType: string, payload: Record<string, unknown>, jobId: string): Promise<string> {
  const project = process.env.GOOGLE_CLOUD_PROJECT ?? process.env.GCLOUD_PROJECT ?? "moneymaker-aedf7";
  const region = process.env.MONEYMAKER_RUN_REGION ?? "australia-southeast1";
  const jobName = process.env[jobType === "fetch" ? "MONEYMAKER_FETCH_JOB" : "MONEYMAKER_FILTER_JOB"] ?? `moneymaker-${jobType}`;
  const url = `https://run.googleapis.com/v2/projects/${project}/locations/${region}/jobs/${jobName}:run`;
  const auth = new GoogleAuth({scopes: ["https://www.googleapis.com/auth/cloud-platform"]});
  const client = await auth.getClient();
  await client.request({
    url,
    method: "POST",
    data: {
      overrides: {
        containerOverrides: [
          {
            env: [
              {name: "MONEYMAKER_JOB_ID", value: jobId},
              {name: "MONEYMAKER_JOB_TYPE", value: jobType},
              {name: "MONEYMAKER_JOB_PAYLOAD", value: JSON.stringify(payload)}
            ]
          }
        ]
      }
    }
  });
  return `projects/${project}/locations/${region}/jobs/${jobName}`;
}

async function createJob(jobType: string, payload: Record<string, unknown>) {
  const jobId = crypto.randomUUID();
  const market = currentMarket(payload.market);
  const started = new Date();
  await db().query(
    `
    INSERT INTO job_runs
      (id, job_type, market, status, stage, detail, started_at_utc, parameters_json)
    VALUES ($1, $2, $3, 'queued', 'Queued', $4, $5, $6::jsonb)
    `,
    [jobId, jobType, market, "Queued Cloud Run Job", started, JSON.stringify(payload)]
  );

  try {
    const cloudRunJob = await dispatchCloudRunJob(jobType, payload, jobId);
    await db().query("UPDATE job_runs SET detail = $1 WHERE id = $2", [`Queued Cloud Run Job ${cloudRunJob}`, jobId]);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await db().query(
      "UPDATE job_runs SET status = 'failed', stage = 'Failed', error = $1, detail = $2 WHERE id = $3",
      [message, "Could not dispatch Cloud Run Job", jobId]
    );
    throw new ApiError(502, `Could not queue ${jobType} job: ${message}`);
  }

  return {
    ok: true,
    job: jobPayload({
      id: jobId,
      job_type: jobType,
      status: "queued",
      stage: "Queued",
      detail: "Cloud Run Job queued",
      current_count: 0,
      total_count: null,
      percent: 0,
      parameters_json: payload,
      result_json: []
    })
  };
}

async function createRefreshTracking(
  payload: Record<string, unknown>,
  user?: UserContext
): Promise<{id: string; totalTickers: number; batchCount: number}> {
  const market = currentMarket(payload.market);
  const provider = String(payload.provider ?? "yfinance");
  const limit = Math.max(Number(payload.limit ?? 0), 0);
  const batchSize = Math.min(Math.max(Number(payload.batch_size ?? payload.history_chunk_size ?? 100), 1), 500);
  const tickerResult = await db().query(
    `
    SELECT ticker
    FROM companies
    WHERE market = $1
    ORDER BY ticker
    ${limit > 0 ? "LIMIT $2" : ""}
    `,
    limit > 0 ? [market, limit] : [market]
  );
  const tickers = tickerResult.rows.map((row) => String(row.ticker).toUpperCase()).filter(Boolean);
  const refreshJobId = crypto.randomUUID();
  const client = await db().connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `
      INSERT INTO refresh_jobs (
        id, market, provider, status, stage, requested_by_uid,
        requested_by_email, total_tickers, parameters_json
      )
      VALUES ($1, $2, $3, 'queued', 'Queued', $4, $5, $6, $7::jsonb)
      `,
      [
        refreshJobId,
        market,
        provider,
        user?.uid ?? null,
        user?.email ?? null,
        tickers.length,
        JSON.stringify(payload)
      ]
    );
    for (let start = 0; start < tickers.length; start += batchSize) {
      await client.query(
        `
        INSERT INTO refresh_batches (
          id, refresh_job_id, market, provider, batch_index, tickers_json, status
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'queued')
        `,
        [
          crypto.randomUUID(),
          refreshJobId,
          market,
          provider,
          Math.floor(start / batchSize) + 1,
          JSON.stringify(tickers.slice(start, start + batchSize))
        ]
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
  return {id: refreshJobId, totalTickers: tickers.length, batchCount: Math.ceil(tickers.length / batchSize)};
}

async function requireScheduler(req: Request): Promise<void> {
  const audience = String(process.env.MONEYMAKER_SCHEDULER_AUDIENCE ?? "").trim();
  const expectedEmail = String(process.env.MONEYMAKER_SCHEDULER_SERVICE_ACCOUNT ?? "").trim();
  const header = String(req.header("authorization") ?? "");
  if (!audience || !header.startsWith("Bearer ")) {
    throw new ApiError(401, "Scheduler authentication required");
  }
  const ticket = await new OAuth2Client().verifyIdToken({idToken: header.slice(7).trim(), audience});
  const payload = ticket.getPayload();
  if (expectedEmail && payload?.email !== expectedEmail) {
    throw new ApiError(401, "Unexpected scheduler service account");
  }
}

async function latestJob(jobType: string, jobId?: string): Promise<JobRow | null> {
  const result = jobId
    ? await db().query("SELECT * FROM job_runs WHERE id = $1 AND job_type = $2", [jobId, jobType])
    : await db().query("SELECT * FROM job_runs WHERE job_type = $1 ORDER BY started_at_utc DESC LIMIT 1", [jobType]);
  return result.rows[0] ?? null;
}

async function overlayUserAppraisals(user: UserContext, scanId: number, results: Array<Record<string, unknown>>) {
  if (!scanId || results.length === 0) return results;
  const tickers = results.map((row) => String(row.ticker ?? "").toUpperCase()).filter(Boolean);
  const appraisalResult = await db().query(
    `
    SELECT up.ticker, up.label, un.note, up.status, up.updated_at_utc AS appraised_at_utc
    FROM user_picks up
    LEFT JOIN user_notes un
      ON un.firebase_uid = up.firebase_uid
     AND un.scan_id = up.scan_id
     AND un.source_id = up.source_id
     AND un.ticker = up.ticker
    WHERE up.firebase_uid = $1 AND up.scan_id = $2 AND up.ticker = ANY($3)
    `,
    [user.uid, scanId, tickers]
  );
  const appraisals = new Map(appraisalResult.rows.map((row) => [String(row.ticker).toUpperCase(), row]));
  return results.map((row) => {
    const appraisal = appraisals.get(String(row.ticker ?? "").toUpperCase());
    return {
      ...row,
      label: appraisal?.label ?? null,
      personal_note: appraisal?.note ?? null,
      personal_status: appraisal?.status ?? null,
      appraised_at_utc: appraisal?.appraised_at_utc ?? null
    };
  });
}

apiApp.get("/api/health", asyncRoute(async (_req, res) => {
  try {
    await db().query("SELECT 1");
    res.json({ok: true, database: "online", cloud: true, functions: true});
  } catch (error) {
    res.json({ok: false, database: "unavailable", error: error instanceof Error ? error.message : String(error)});
  }
}));

apiApp.get("/api/auth-config", asyncRoute(async (_req, res) => {
  res.json({
    ok: true,
    enabled: Boolean(process.env.FIREBASE_API_KEY),
    apiKey: process.env.FIREBASE_API_KEY ?? "",
    authDomain: process.env.FIREBASE_AUTH_DOMAIN ?? "moneymaker-aedf7.firebaseapp.com",
    projectId: process.env.GOOGLE_CLOUD_PROJECT ?? "moneymaker-aedf7",
    storageBucket: process.env.FIREBASE_STORAGE_BUCKET ?? "",
    appId: process.env.FIREBASE_APP_ID ?? "",
    appCheck: {
      enabled: Boolean(process.env.FIREBASE_APPCHECK_SITE_KEY),
      enforce: ["1", "true", "yes"].includes(String(process.env.MONEYMAKER_REQUIRE_APP_CHECK ?? "false").toLowerCase()),
      siteKey: process.env.FIREBASE_APPCHECK_SITE_KEY ?? ""
    }
  });
}));

apiApp.use("/api", (req: Request, res: Response, next: NextFunction) => {
  requireAppCheck(req)
    .then(() => next())
    .catch((error) => {
      const status = error instanceof ApiError ? error.status : 500;
      const message = error instanceof Error ? error.message : String(error);
      res.status(status).json({ok: false, error: message, detail: message});
    });
});

apiApp.get("/api/config", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  res.json({ok: true, config: {}, markets: MARKET_DEFAULTS, cloud: true});
}));

apiApp.get("/api/profile", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  res.json({ok: true, user});
}));

apiApp.get("/api/user/profile", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  res.json({ok: true, user});
}));

apiApp.get("/api/ticker-files", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  const files = ["asx_yfinance_valid_stocks_2026-05-11.txt", "us_tickers_nasdaqtrader.txt"];
  res.json({ok: true, files: files.map((name) => ({name, size_kb: 0}))});
}));

apiApp.get("/api/status", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  let market = currentMarket(req.query.market);
  if (String(req.query.cache_file ?? "").toLowerCase().includes("_us")) market = "us";
  const statusResult = await db().query(
    `
    SELECT COUNT(DISTINCT ticker) AS ticker_count, COUNT(*) AS history_rows,
           MAX(price_date)::text AS latest_date
    FROM price_history WHERE market = $1
    `,
    [market]
  );
  const tickerResult = await db().query(
    "SELECT DISTINCT ticker FROM price_history WHERE market = $1 ORDER BY ticker LIMIT 500",
    [market]
  );
  const status = statusResult.rows[0] ?? {};
  res.json({
    ok: true,
    status: {
      ...status,
      exists: Boolean(Number(status.ticker_count ?? 0)),
      size_mb: 0,
      tickers: tickerResult.rows.map((row) => row.ticker)
    },
    job: jobPayload(await latestJob("fetch"))
  });
}));

apiApp.get("/api/job", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  res.json({ok: true, job: jobPayload(await latestJob("fetch"))});
}));

apiApp.get("/api/filter/job", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  res.json({ok: true, job: jobPayload(await latestJob("filter", String(req.query.job_id ?? "") || undefined))});
}));

apiApp.get("/api/scans", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  const market = currentMarket(req.query.market);
  const result = await db().query(
    `
    SELECT id, source_id, created_at_utc, provider, query, scanned_count,
           result_count, skipped_no_history, config_json
    FROM scan_runs WHERE market = $1 ORDER BY created_at_utc DESC LIMIT 100
    `,
    [market]
  );
  res.json({ok: true, scans: result.rows});
}));

apiApp.get("/api/labels", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const scanId = Number(req.query.scan_id ?? 0);
  const result = await db().query(
    `
    SELECT up.ticker, up.label, un.note, up.status, up.updated_at_utc AS labeled_at_utc
    FROM user_picks up
    LEFT JOIN user_notes un
      ON un.firebase_uid = up.firebase_uid
     AND un.scan_id = up.scan_id
     AND un.source_id = up.source_id
     AND un.ticker = up.ticker
    WHERE up.firebase_uid = $1 AND up.scan_id = $2
    ORDER BY up.ticker
    `,
    [user.uid, scanId]
  );
  res.json({ok: true, labels: result.rows});
}));

apiApp.get("/api/user/picks", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const market = String(req.query.market ?? "").trim().toLowerCase() || null;
  const limit = Math.min(Math.max(Number(req.query.limit ?? 250), 1), 1000);
  const result = await db().query(
    `
    SELECT up.scan_id, up.source_id, up.market, up.ticker, up.label, up.status,
           up.created_at_utc, up.updated_at_utc, un.note,
           sr.rank, sr.signal_date, sr.close_price, sr.market_cap,
           sr.avg_volume, sr.volume_ratio, sr.sector, sr.industry, sr.result_json
    FROM user_picks up
    LEFT JOIN user_notes un
      ON un.firebase_uid = up.firebase_uid
     AND un.scan_id = up.scan_id
     AND un.source_id = up.source_id
     AND un.ticker = up.ticker
    LEFT JOIN scan_results sr
      ON sr.scan_id = up.scan_id
     AND sr.source_id = up.source_id
     AND sr.ticker = up.ticker
    WHERE up.firebase_uid = $1
      AND ($2::text IS NULL OR up.market = $2)
    ORDER BY up.updated_at_utc DESC
    LIMIT $3
    `,
    [user.uid, market, limit]
  );
  res.json({ok: true, picks: result.rows});
}));

apiApp.get("/api/user/notes", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const market = String(req.query.market ?? "").trim().toLowerCase() || null;
  const ticker = String(req.query.ticker ?? "").trim().toUpperCase() || null;
  const limit = Math.min(Math.max(Number(req.query.limit ?? 250), 1), 1000);
  const result = await db().query(
    `
    SELECT id, scan_id, source_id, market, ticker, note, created_at_utc, updated_at_utc
    FROM user_notes
    WHERE firebase_uid = $1
      AND ($2::text IS NULL OR market = $2)
      AND ($3::text IS NULL OR ticker = $3)
    ORDER BY updated_at_utc DESC
    LIMIT $4
    `,
    [user.uid, market, ticker, limit]
  );
  res.json({ok: true, notes: result.rows});
}));

apiApp.get("/api/user/rating-history", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const ticker = String(req.query.ticker ?? "").trim().toUpperCase() || null;
  const limit = Math.min(Math.max(Number(req.query.limit ?? 250), 1), 1000);
  const result = await db().query(
    `
    SELECT id, event_at_utc, action, market, scan_id, ticker, label, note,
           rank, signal_date, close_price, market_cap, avg_volume,
           volume_ratio, sector, industry, yahoo_url
    FROM rating_events
    WHERE firebase_uid = $1
      AND ($2::text IS NULL OR ticker = $2)
    ORDER BY event_at_utc DESC
    LIMIT $3
    `,
    [user.uid, ticker, limit]
  );
  res.json({ok: true, events: result.rows});
}));

apiApp.get("/api/chart", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  const ticker = String(req.query.ticker ?? "").trim().toUpperCase();
  if (!ticker) throw new ApiError(400, "ticker is required");
  const market = currentMarket(req.query.market, ticker);
  const provider = String(req.query.provider ?? "yfinance");
  const interval = String(req.query.interval ?? "daily").toLowerCase();
  if (!["daily", "weekly", "monthly"].includes(interval)) throw new ApiError(400, "Invalid chart interval");
  const range = String(req.query.range ?? "1y").toLowerCase();
  const history = await db().query(
    `
    SELECT price_date::text AS date, open_price AS open, high_price AS high,
           low_price AS low, close_price AS close, volume
    FROM price_history
    WHERE market = $1 AND provider = $2 AND ticker = $3
      AND price_date >= CURRENT_DATE - ($4::int * INTERVAL '1 day')
    ORDER BY price_date
    `,
    [market, provider, ticker, rangeDays(range)]
  );
  const companyResult = await db().query("SELECT info_json FROM companies WHERE market = $1 AND ticker = $2", [market, ticker]);
  const rows = aggregateRows(history.rows as PriceRow[], interval);
  const closes = rows.map((row) => numberOrNull(row.close));
  const maPeriods = String(req.query.ma ?? "")
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0);
  const movingAverages: Record<string, Array<number | null>> = {};
  for (const period of maPeriods) {
    movingAverages[String(period)] = movingAverage(closes, period);
  }
  const candles = rows.map((row) => ({
    date: dateOnly(row.date),
    open: numberOrNull(row.open),
    high: numberOrNull(row.high),
    low: numberOrNull(row.low),
    close: numberOrNull(row.close),
    volume: numberOrNull(row.volume)
  }));
  res.json({
    ok: true,
    ticker,
    market,
    provider,
    interval,
    range,
    company: companyResult.rows[0]?.info_json ?? {},
    candles,
    moving_averages: movingAverages,
    count: candles.length,
    start: candles[0]?.date ?? null,
    end: candles[candles.length - 1]?.date ?? null
  });
}));

apiApp.post("/api/label", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const scanId = Number(req.body.scan_id ?? 0);
  const ticker = String(req.body.ticker ?? "").trim().toUpperCase();
  let label = String(req.body.label ?? "").trim().toLowerCase().replace(/\s+/g, "_");
  const note = String(req.body.note ?? "").trim();
  const status = String(req.body.status ?? "").trim() || null;
  if (!scanId || !ticker) throw new ApiError(400, "scan_id and ticker are required");
  if (["", "clear", "none", "unlabelled", "unlabeled"].includes(label)) {
    label = "";
  } else if (!VALID_LABELS.has(label)) {
    throw new ApiError(400, "Invalid rating label");
  }

  const client = await db().connect();
  try {
    await client.query("BEGIN");
    const scanResult = await client.query("SELECT market FROM scan_runs WHERE id = $1", [scanId]);
    const scan = scanResult.rows[0];
    if (!scan) throw new ApiError(404, "Scan not found");
    const resultSet = await client.query("SELECT * FROM scan_results WHERE scan_id = $1 AND ticker = $2", [scanId, ticker]);
    const result = resultSet.rows[0];
    if (!result) throw new ApiError(404, "Ticker is not in this scan");

    if (!label) {
      await client.query(
        "DELETE FROM user_picks WHERE firebase_uid = $1 AND scan_id = $2 AND source_id = $3 AND ticker = $4",
        [user.uid, scanId, result.source_id, ticker]
      );
      if (!note) {
        await client.query(
          "DELETE FROM user_notes WHERE firebase_uid = $1 AND scan_id = $2 AND source_id = $3 AND ticker = $4",
          [user.uid, scanId, result.source_id, ticker]
        );
      }
      await client.query(
        "DELETE FROM user_appraisals WHERE firebase_uid = $1 AND scan_id = $2 AND source_id = $3 AND ticker = $4",
        [user.uid, scanId, result.source_id, ticker]
      );
    } else {
      await client.query(
        `
        INSERT INTO user_picks
          (firebase_uid, scan_id, source_id, market, ticker, label, status, created_at_utc, updated_at_utc)
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
        ON CONFLICT (firebase_uid, scan_id, source_id, ticker) DO UPDATE SET
          label = EXCLUDED.label,
          status = EXCLUDED.status,
          updated_at_utc = EXCLUDED.updated_at_utc
        `,
        [user.uid, scanId, result.source_id, scan.market, ticker, label, status]
      );
      await client.query(
        `
        INSERT INTO user_appraisals
          (firebase_uid, scan_id, source_id, market, ticker, label, note, status, appraised_at_utc)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (firebase_uid, scan_id, source_id, ticker) DO UPDATE SET
          label = EXCLUDED.label,
          note = EXCLUDED.note,
          status = EXCLUDED.status,
          appraised_at_utc = EXCLUDED.appraised_at_utc
        `,
        [user.uid, scanId, result.source_id, scan.market, ticker, label, note || null, status]
      );
    }
    if (note) {
      await client.query(
        `
        INSERT INTO user_notes
          (firebase_uid, scan_id, source_id, market, ticker, note, created_at_utc, updated_at_utc)
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        ON CONFLICT (firebase_uid, scan_id, source_id, ticker) DO UPDATE SET
          note = EXCLUDED.note,
          updated_at_utc = EXCLUDED.updated_at_utc
        `,
        [user.uid, scanId, result.source_id, scan.market, ticker, note]
      );
    }

    await client.query(
      `
      INSERT INTO rating_events
        (event_at_utc, action, rated_by, market, scan_id, ticker, label,
         note, rank, signal_date, close_price, market_cap, avg_volume,
         volume_ratio, sector, industry, result_json, yahoo_url,
         firebase_uid, user_email)
      VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
              $13, $14, $15, $16::jsonb, $17, $18, $19)
      `,
      [
        label ? "label" : "clear",
        user.email ?? user.display_name ?? req.body.rated_by ?? "anonymous",
        scan.market,
        scanId,
        ticker,
        label || null,
        note || null,
        result.rank,
        result.signal_date,
        result.close_price,
        result.market_cap,
        result.avg_volume,
        result.volume_ratio,
        result.sector,
        result.industry,
        JSON.stringify(result.result_json ?? {}),
        yahooUrl(ticker),
        user.uid,
        user.email
      ]
    );
    await client.query("COMMIT");
    res.json({ok: true, scan_id: scanId, ticker, label: label || null, note: note || null, online: true, user});
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}));

apiApp.post("/api/fetch", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const payload = req.body ?? {};
  const refresh = await createRefreshTracking(payload, user);
  res.json(await createJob("fetch", {...payload, refresh_job_id: refresh.id}));
}));

apiApp.post("/api/admin/refresh-market", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const payload = req.body ?? {};
  const refresh = await createRefreshTracking(payload, user);
  res.json(await createJob("fetch", {...payload, refresh_job_id: refresh.id}));
}));

apiApp.post("/api/admin/import-sqlite", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  throw new ApiError(501, "SQLite imports must be uploaded to Storage and run through the admin migration worker");
}));

apiApp.post("/api/scheduled-fetch", asyncRoute(async (req, res) => {
  await requireScheduler(req);
  const market = currentMarket(req.body?.market);
  const scheduledPayload = {
    market,
    ticker_file: req.body?.ticker_file ?? (market === "us" ? "us_tickers_nasdaqtrader.txt" : "asx_yfinance_valid_stocks_2026-05-11.txt"),
    provider: req.body?.provider ?? "yfinance",
    years: Number(req.body?.years ?? 15),
    workers: Number(req.body?.workers ?? 1),
    info_refresh_days: Number(req.body?.info_refresh_days ?? 30),
    history_refresh_days: Number(req.body?.history_refresh_days ?? 5),
    history_chunk_size: Number(req.body?.history_chunk_size ?? 50),
    history_pause_seconds: Number(req.body?.history_pause_seconds ?? 5),
    info_pause_seconds: Number(req.body?.info_pause_seconds ?? 1),
    rate_limit_pause_seconds: Number(req.body?.rate_limit_pause_seconds ?? 900),
    max_rate_limit_retries: Number(req.body?.max_rate_limit_retries ?? 3),
    stop_on_rate_limit: req.body?.stop_on_rate_limit ?? true
  };
  const refresh = await createRefreshTracking(scheduledPayload);
  res.json(await createJob("fetch", {...scheduledPayload, refresh_job_id: refresh.id}));
}));

apiApp.post("/api/filter/start", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  res.json(await createJob("filter", req.body ?? {}));
}));

apiApp.post("/api/admin/rebuild-default-scans", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  res.json(await createJob("filter", req.body ?? {}));
}));

apiApp.post("/api/filter", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const payload = jobPayload(await latestJob("filter")) as Record<string, unknown>;
  const summary = payload.summary as Record<string, unknown> | undefined;
  const scanId = Number(summary?.scan_id ?? payload.scan_id ?? 0);
  if (scanId && Array.isArray(payload.results)) {
    payload.results = await overlayUserAppraisals(user, scanId, payload.results as Array<Record<string, unknown>>);
  }
  res.json({ok: true, ...payload});
}));

apiApp.get("/api/export/ratings", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const market = String(req.query.market ?? "").trim().toLowerCase() || null;
  const limit = Math.min(Math.max(Number(req.query.limit ?? 5000), 1), 25000);
  const result = await db().query(
    `
    SELECT id, event_at_utc, action, rated_by, user_email, firebase_uid,
           market, scan_id, ticker, label, note, rank, signal_date,
           close_price, market_cap, avg_volume, volume_ratio, sector,
           industry, yahoo_url
    FROM rating_events
    WHERE ($1::text IS NULL OR market = $1)
    ORDER BY event_at_utc DESC
    LIMIT $2
    `,
    [market, limit]
  );
  res.json({ok: true, ratings: result.rows});
}));

apiApp.use((_req, res) => {
  res.status(404).json({ok: false, error: "Not found"});
});

export {apiApp};

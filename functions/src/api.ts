import crypto from "node:crypto";
import cors from "cors";
import express, {type NextFunction, type Request, type Response} from "express";
import {getFunctions} from "firebase-admin/functions";
import {getStorage} from "firebase-admin/storage";
import {OAuth2Client} from "google-auth-library";
import {ApiError, requireAdmin, requireAnalyst, requireAppCheck, requireAuth, type UserContext} from "./auth";
import {dispatchCloudRunJob} from "./cloudrun";
import {db} from "./db";
import {readMarketStatusViaDataConnect, type MarketStatusRow} from "./dataconnect";
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

interface RefreshBatch {
  id: string;
  batchIndex: number;
  tickers: string[];
}

interface RefreshTracking {
  id: string;
  totalTickers: number;
  batchCount: number;
  batches: RefreshBatch[];
}

interface RefreshTickerBatchPayload {
  refreshJobId: string;
  refreshBatchId: string;
  parentJobId: string;
  market: string;
  provider?: string;
  tickers: string[];
  fetchPayload?: Record<string, unknown>;
}

const apiApp = express();
apiApp.use(cors({origin: true}));
apiApp.use(express.json({limit: "5mb"}));

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

async function createJob(jobType: string, payload: Record<string, unknown>) {
  const jobId = crypto.randomUUID();
  const requestedMarket = String(payload.market ?? "").trim().toLowerCase();
  const market = requestedMarket === "all" ? null : currentMarket(payload.market);
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

function storageObjectPath(value: unknown, allowedPrefixes: string[]): string {
  const path = String(value ?? "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
  if (!path || path.includes("..") || path.endsWith("/")) {
    throw new ApiError(400, "A valid Storage object path is required");
  }
  if (!allowedPrefixes.some((prefix) => path.startsWith(prefix))) {
    throw new ApiError(400, `Storage path must start with one of: ${allowedPrefixes.join(", ")}`);
  }
  return path;
}

function optionalStorageObjectPath(value: unknown, allowedPrefixes: string[]): string | undefined {
  if (value === undefined || value === null || String(value).trim() === "") return undefined;
  return storageObjectPath(value, allowedPrefixes);
}

function storageBucketName(): string {
  const bucket = String(
    process.env.MONEYMAKER_STORAGE_BUCKET ?? process.env.FIREBASE_STORAGE_BUCKET ?? ""
  ).trim();
  if (!bucket) throw new ApiError(503, "Firebase Storage is not configured");
  return bucket;
}

function sqliteFileExtension(value: unknown): ".sqlite" | ".sqlite3" | ".db" {
  const name = String(value ?? "").trim().toLowerCase();
  if (name.endsWith(".sqlite")) return ".sqlite";
  if (name.endsWith(".sqlite3")) return ".sqlite3";
  if (name.endsWith(".db")) return ".db";
  throw new ApiError(400, "Choose a SQLite (.sqlite, .sqlite3, or .db) file");
}

function importUploadPath(market: "asx" | "us", uid: string, filename: unknown): string {
  const extension = sqliteFileExtension(filename);
  const safeUid = uid.replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 48) || "user";
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `imports/sqlite/${market}/${timestamp}_${safeUid}_${crypto.randomUUID()}${extension}`;
}

function booleanValue(value: unknown, defaultValue: boolean): boolean {
  if (value === undefined || value === null || value === "") return defaultValue;
  if (typeof value === "boolean") return value;
  return ["1", "true", "yes", "on"].includes(String(value).trim().toLowerCase());
}

function positiveInt(value: unknown, defaultValue: number, min: number, max: number): number {
  const parsed = Number(value ?? defaultValue);
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.min(Math.max(Math.trunc(parsed), min), max);
}

function strictMarket(value: unknown, allowAll = false): "asx" | "us" | "all" {
  const market = String(value ?? "").trim().toLowerCase();
  if (market === "asx" || market === "us") return market;
  if (allowAll && (!market || market === "all")) return "all";
  throw new ApiError(400, allowAll ? "Market must be asx, us, or all" : "Market must be asx or us");
}

function analysisHorizon(value: unknown): number {
  const horizon = Number(value ?? 0);
  if ([0, 30, 90, 180, 360].includes(horizon)) return horizon;
  throw new ApiError(400, "Analysis horizon must be current, 30, 90, 180, or 360 days");
}

async function createQueuedRefreshJob(
  payload: Record<string, unknown>,
  refresh: RefreshTracking
) {
  const jobId = crypto.randomUUID();
  const market = currentMarket(payload.market);
  const started = new Date();
  await db().query(
    `
    INSERT INTO job_runs
      (id, job_type, market, status, stage, detail, started_at_utc, total_count, parameters_json)
    VALUES ($1, 'fetch', $2, 'queued', 'Queued', $3, $4, $5, $6::jsonb)
    `,
    [
      jobId,
      market,
      `Queued ${refresh.batchCount} ticker refresh batches`,
      started,
      refresh.batchCount,
      JSON.stringify({...payload, refresh_job_id: refresh.id, task_queue: true})
    ]
  );

  try {
    const queue = getFunctions().taskQueue<RefreshTickerBatchPayload>(
      "locations/australia-southeast1/functions/refreshTickerBatch"
    );
    await Promise.all(refresh.batches.map((batch) => queue.enqueue({
      refreshJobId: refresh.id,
      refreshBatchId: batch.id,
      parentJobId: jobId,
      market,
      provider: String(payload.provider ?? "yfinance"),
      tickers: batch.tickers,
      fetchPayload: payload
    })));
    await db().query("UPDATE job_runs SET detail = $1 WHERE id = $2", ["Ticker refresh batches queued", jobId]);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await db().query(
      "UPDATE job_runs SET status = 'failed', stage = 'Failed', error = $1, detail = $2 WHERE id = $3",
      [message, "Could not enqueue ticker refresh batches", jobId]
    );
    throw new ApiError(502, `Could not queue refresh batches: ${message}`);
  }

  return {
    ok: true,
    job: jobPayload({
      id: jobId,
      job_type: "fetch",
      status: "queued",
      stage: "Queued",
      detail: "Ticker refresh batches queued",
      current_count: 0,
      total_count: refresh.batchCount,
      percent: 0,
      parameters_json: {...payload, refresh_job_id: refresh.id, task_queue: true},
      result_json: []
    })
  };
}

async function createFetchJob(
  payload: Record<string, unknown>,
  refresh: RefreshTracking
) {
  const useTaskQueue = ["1", "true", "yes"].includes(
    String(process.env.MONEYMAKER_USE_TASK_QUEUE ?? "false").toLowerCase()
  );
  if (useTaskQueue) {
    return createQueuedRefreshJob(payload, refresh);
  }
  return createJob("fetch", {...payload, refresh_job_id: refresh.id});
}

async function createRefreshTracking(
  payload: Record<string, unknown>,
  user?: UserContext
): Promise<RefreshTracking> {
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
  const batches: RefreshBatch[] = [];
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
      const batchId = crypto.randomUUID();
      const batchTickers = tickers.slice(start, start + batchSize);
      const batchIndex = Math.floor(start / batchSize) + 1;
      batches.push({id: batchId, batchIndex, tickers: batchTickers});
      await client.query(
        `
        INSERT INTO refresh_batches (
          id, refresh_job_id, market, provider, batch_index, tickers_json, status
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'queued')
        `,
        [
          batchId,
          refreshJobId,
          market,
          provider,
          batchIndex,
          JSON.stringify(batchTickers)
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
  return {id: refreshJobId, totalTickers: tickers.length, batchCount: batches.length, batches};
}

export function defaultScheduledFetchPayload(marketInput: unknown): Record<string, unknown> {
  const market = currentMarket(marketInput);
  return {
    market,
    ticker_file: market === "us" ? "us_tickers_nasdaqtrader.txt" : "asx_yfinance_valid_stocks_2026-05-11.txt",
    provider: "yfinance",
    years: 15,
    workers: 1,
    info_refresh_days: 30,
    history_refresh_days: 5,
    batch_size: 50,
    history_chunk_size: 25,
    history_pause_seconds: 5,
    info_pause_seconds: 1,
    rate_limit_pause_seconds: 900,
    max_rate_limit_retries: 3,
    stop_on_rate_limit: true,
    scheduled: true
  };
}

export async function startMarketRefresh(
  payload: Record<string, unknown>,
  user?: UserContext
): Promise<Record<string, unknown>> {
  const refresh = await createRefreshTracking(payload, user);
  return createFetchJob(payload, refresh);
}

export async function startScheduledMarketRefresh(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
  const market = currentMarket(payload.market);
  const active = await db().query(
    `
    SELECT id, status, stage, total_tickers, completed_tickers, failed_tickers, started_at_utc
    FROM refresh_jobs
    WHERE market = $1
      AND status IN ('queued', 'running')
      AND started_at_utc > NOW() - INTERVAL '12 hours'
    ORDER BY started_at_utc DESC
    LIMIT 1
    `,
    [market]
  );
  const row = active.rows[0];
  if (row) {
    return {
      skipped: true,
      reason: "refresh_already_running",
      refresh_job_id: row.id,
      market,
      status: row.status,
      stage: row.stage,
      total_tickers: row.total_tickers,
      completed_tickers: row.completed_tickers,
      failed_tickers: row.failed_tickers,
      started_at_utc: row.started_at_utc
    };
  }
  return startMarketRefresh(payload);
}

export function defaultScanPayload(marketInput: unknown): Record<string, unknown> {
  return {
    market: currentMarket(marketInput),
    provider: "yfinance",
    limit: 0,
    query: "",
    volume_multiplier: 2,
    avg_volume_weeks: 52,
    price_avg_weeks: 1,
    lookback_weeks: 1,
    ma_periods: {
      short: 90,
      intermediate: 180,
      medium: 360,
      long: 700
    },
    min_market_cap: 0,
    max_market_cap: 0,
    scheduled: true
  };
}

export async function startFilterJob(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
  return createJob("filter", payload);
}

export async function startRatingOutcomesJob(payload: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
  return createJob("rating-outcomes", payload);
}

export async function reconcileStaleJobs(): Promise<Record<string, number>> {
  const client = await db().connect();
  try {
    await client.query("BEGIN");
    const expiredRefreshes = await client.query(
      `
      WITH expired AS (
        SELECT id
        FROM refresh_jobs
        WHERE status IN ('queued', 'running')
          AND started_at_utc < NOW() - INTERVAL '48 hours'
      ),
      failed_batches AS (
        UPDATE refresh_batches rb
        SET status = 'failed',
            finished_at_utc = COALESCE(finished_at_utc, NOW()),
            error = COALESCE(error, 'Refresh batch expired before completion')
        FROM expired
        WHERE rb.refresh_job_id = expired.id
          AND rb.status IN ('queued', 'running')
        RETURNING rb.id
      ),
      failed_refreshes AS (
        UPDATE refresh_jobs r
        SET status = 'failed',
            stage = 'Expired',
            finished_at_utc = COALESCE(finished_at_utc, NOW()),
            error = COALESCE(error, 'Refresh job expired before completion')
        FROM expired
        WHERE r.id = expired.id
        RETURNING r.id
      )
      SELECT
        (SELECT COUNT(*)::int FROM failed_batches) AS batches,
        (SELECT COUNT(*)::int FROM failed_refreshes) AS refreshes
      `
    );
    const expiredRefreshCounts = expiredRefreshes.rows[0] ?? {};

    const expiredJobRuns = await client.query(
      `
      WITH expired AS (
        SELECT id, job_type, parameters_json
        FROM job_runs
        WHERE status IN ('queued', 'running')
          AND (
            (job_type = 'fetch'
             AND parameters_json ? 'refresh_batch_id'
             AND started_at_utc < NOW() - INTERVAL '5 hours')
            OR (job_type = 'fetch'
                AND NOT (parameters_json ? 'refresh_batch_id')
                AND started_at_utc < NOW() - INTERVAL '48 hours')
            OR (job_type = 'filter'
                AND started_at_utc < NOW() - INTERVAL '2 hours')
            OR (job_type IN ('export-ratings', 'rating-outcomes')
                AND started_at_utc < NOW() - INTERVAL '2 hours')
            OR (job_type = 'import-sqlite'
                AND started_at_utc < NOW() - INTERVAL '6 hours')
            OR (job_type NOT IN ('fetch', 'filter', 'export-ratings', 'rating-outcomes', 'import-sqlite')
                AND started_at_utc < NOW() - INTERVAL '6 hours')
          )
      ),
      failed_jobs AS (
        UPDATE job_runs jr
        SET status = 'failed',
            stage = 'Expired',
            finished_at_utc = COALESCE(finished_at_utc, NOW()),
            error = COALESCE(error, 'Job expired before completion'),
            detail = COALESCE(NULLIF(detail, ''), 'Job expired before completion')
        FROM expired
        WHERE jr.id = expired.id
        RETURNING jr.id, jr.parameters_json
      ),
      failed_batches AS (
        UPDATE refresh_batches rb
        SET status = 'failed',
            finished_at_utc = COALESCE(finished_at_utc, NOW()),
            error = COALESCE(error, 'Child fetch job expired before completion')
        FROM failed_jobs fj
        WHERE rb.id::text = fj.parameters_json ->> 'refresh_batch_id'
          AND rb.refresh_job_id::text = fj.parameters_json ->> 'refresh_job_id'
          AND rb.status IN ('queued', 'running')
        RETURNING rb.id
      )
      SELECT
        (SELECT COUNT(*)::int FROM failed_jobs) AS jobs,
        (SELECT COUNT(*)::int FROM failed_batches) AS batches
      `
    );
    const expiredJobCounts = expiredJobRuns.rows[0] ?? {};

    const finalizedRefreshes = await client.query(
      `
      WITH ready AS (
        SELECT
          r.id,
          COUNT(*)::int AS total_batches,
          COUNT(*) FILTER (WHERE rb.status = 'succeeded')::int AS succeeded_batches,
          COUNT(*) FILTER (WHERE rb.status = 'failed')::int AS failed_batches,
          COALESCE(SUM(CASE WHEN rb.status = 'succeeded' THEN jsonb_array_length(rb.tickers_json) ELSE 0 END), 0)::int AS succeeded_tickers,
          COALESCE(SUM(CASE WHEN rb.status = 'failed' THEN jsonb_array_length(rb.tickers_json) ELSE 0 END), 0)::int AS failed_tickers
        FROM refresh_jobs r
        JOIN refresh_batches rb ON rb.refresh_job_id = r.id
        WHERE r.status IN ('queued', 'running')
        GROUP BY r.id
        HAVING COUNT(*) FILTER (WHERE rb.status IN ('queued', 'running')) = 0
      ),
      updated_refreshes AS (
        UPDATE refresh_jobs r
        SET status = CASE WHEN ready.failed_batches > 0 THEN 'failed' ELSE 'succeeded' END,
            stage = CASE WHEN ready.failed_batches > 0 THEN 'Failed' ELSE 'Complete' END,
            completed_tickers = ready.succeeded_tickers,
            failed_tickers = ready.failed_tickers,
            finished_at_utc = COALESCE(r.finished_at_utc, NOW()),
            error = CASE
              WHEN ready.failed_batches > 0
              THEN COALESCE(r.error, ready.failed_batches || ' refresh batches failed')
              ELSE r.error
            END
        FROM ready
        WHERE r.id = ready.id
        RETURNING r.id, r.status, ready.total_batches, ready.succeeded_batches, ready.failed_batches
      ),
      updated_jobs AS (
        UPDATE job_runs jr
        SET status = ur.status,
            stage = CASE WHEN ur.status = 'failed' THEN 'Failed' ELSE 'Complete' END,
            current_count = ur.succeeded_batches + ur.failed_batches,
            total_count = ur.total_batches,
            percent = 100,
            finished_at_utc = COALESCE(jr.finished_at_utc, NOW()),
            error = CASE
              WHEN ur.status = 'failed'
              THEN COALESCE(jr.error, ur.failed_batches || ' refresh batches failed')
              ELSE jr.error
            END,
            detail = CASE
              WHEN ur.status = 'failed'
              THEN ur.failed_batches || ' refresh batches failed'
              ELSE 'All refresh batches complete'
            END
        FROM updated_refreshes ur
        WHERE jr.job_type = 'fetch'
          AND jr.parameters_json ->> 'refresh_job_id' = ur.id::text
          AND jr.status IN ('queued', 'running')
        RETURNING jr.id
      )
      SELECT
        (SELECT COUNT(*)::int FROM updated_refreshes) AS refreshes,
        (SELECT COUNT(*)::int FROM updated_jobs) AS jobs
      `
    );
    const finalizedCounts = finalizedRefreshes.rows[0] ?? {};
    await client.query("COMMIT");
    return {
      expired_refresh_jobs: Number(expiredRefreshCounts.refreshes ?? 0),
      expired_refresh_batches: Number(expiredRefreshCounts.batches ?? 0) + Number(expiredJobCounts.batches ?? 0),
      expired_job_runs: Number(expiredJobCounts.jobs ?? 0),
      finalized_refresh_jobs: Number(finalizedCounts.refreshes ?? 0),
      finalized_parent_job_runs: Number(finalizedCounts.jobs ?? 0)
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function reconcileStaleJobsSafe(): Promise<void> {
  try {
    await reconcileStaleJobs();
  } catch (error) {
    console.warn("Stale job reconciliation failed", error);
  }
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
  await reconcileStaleJobsSafe();
  const result = jobId
    ? await db().query("SELECT * FROM job_runs WHERE id = $1 AND job_type = $2", [jobId, jobType])
    : await db().query("SELECT * FROM job_runs WHERE job_type = $1 ORDER BY started_at_utc DESC LIMIT 1", [jobType]);
  return result.rows[0] ?? null;
}

function requireJobVisibility(user: UserContext, jobType: string, row: JobRow | null): void {
  if (user.role === "admin") return;
  if (jobType === "filter") {
    requireAnalyst(user);
    return;
  }
  const parameters = row?.parameters_json && typeof row.parameters_json === "object" && !Array.isArray(row.parameters_json)
    ? row.parameters_json as Record<string, unknown>
    : {};
  if (String(parameters.requested_by_uid ?? "") === user.uid) return;
  throw new ApiError(403, "admin access required");
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
  let status: MarketStatusRow | null = null;
  try {
    status = await readMarketStatusViaDataConnect(market);
  } catch (error) {
    console.warn("Data Connect status read failed; falling back to PostgreSQL", error);
  }
  if (!status) {
    const statusResult = await db().query(
      `
      SELECT ticker_count, history_rows, latest_date::text AS latest_date
      FROM market_status
      WHERE market = $1 AND provider = 'yfinance'
      `,
      [market]
    );
    status = statusResult.rows[0] ?? null;
  }
  if (!status) {
    const statusResult = await db().query(
      `
      SELECT COUNT(DISTINCT ticker) AS ticker_count, COUNT(*) AS history_rows,
             MAX(week_date)::text AS latest_date
      FROM weekly_metrics WHERE market = $1 AND provider = 'yfinance'
      `,
      [market]
    );
    status = statusResult.rows[0] ?? {};
  }
  const marketStatus = status ?? {};
  const refreshResult = await db().query(
    `
    WITH latest_refresh AS (
      SELECT *
      FROM refresh_jobs
      WHERE market = $1
      ORDER BY started_at_utc DESC
      LIMIT 1
    ),
    batch_counts AS (
      SELECT
        refresh_job_id,
        COUNT(*)::int AS total_batches,
        COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded_batches,
        COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_batches,
        COUNT(*) FILTER (WHERE status IN ('queued', 'running'))::int AS active_batches
      FROM refresh_batches
      WHERE refresh_job_id = (SELECT id FROM latest_refresh)
      GROUP BY refresh_job_id
    )
    SELECT
      r.id,
      r.market,
      r.status,
      r.stage,
      r.total_tickers,
      r.completed_tickers,
      r.failed_tickers,
      r.started_at_utc,
      r.finished_at_utc,
      r.error,
      COALESCE(b.total_batches, 0) AS total_batches,
      COALESCE(b.succeeded_batches, 0) AS succeeded_batches,
      COALESCE(b.failed_batches, 0) AS failed_batches,
      COALESCE(b.active_batches, 0) AS active_batches
    FROM latest_refresh r
    LEFT JOIN batch_counts b ON b.refresh_job_id = r.id
    `,
    [market]
  );
  const latestRefresh = refreshResult.rows[0] ?? null;
  const refreshPercent = latestRefresh && Number(latestRefresh.total_tickers)
    ? Math.min(100, Math.round((Number(latestRefresh.completed_tickers ?? 0) / Number(latestRefresh.total_tickers)) * 10000) / 100)
    : 0;
  const refreshPayload = latestRefresh ? {
    ...latestRefresh,
    running: ["queued", "running"].includes(String(latestRefresh.status)),
    success: latestRefresh.status === "succeeded" ? true : latestRefresh.status === "failed" ? false : null,
    message: latestRefresh.error || `${String(latestRefresh.market).toUpperCase()} refresh ${latestRefresh.status}`,
    detail: `${latestRefresh.succeeded_batches}/${latestRefresh.total_batches} batches complete, ${latestRefresh.completed_tickers}/${latestRefresh.total_tickers} tickers processed`,
    current: latestRefresh.completed_tickers ?? 0,
    total: latestRefresh.total_tickers ?? null,
    percent: refreshPercent,
    log: `${latestRefresh.succeeded_batches}/${latestRefresh.total_batches} batches complete. ${latestRefresh.active_batches} queued/running, ${latestRefresh.failed_batches} failed.`
  } : null;
  const tickerResult = await db().query(
    "SELECT DISTINCT ticker FROM price_history WHERE market = $1 ORDER BY ticker LIMIT 500",
    [market]
  );
  res.json({
    ok: true,
      status: {
      ...marketStatus,
      exists: Boolean(Number(marketStatus.ticker_count ?? 0)),
      size_mb: 0,
      tickers: tickerResult.rows.map((row) => row.ticker)
    },
    refresh: refreshPayload,
    job: jobPayload(await latestJob("fetch"))
  });
}));

apiApp.get("/api/job", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const jobType = String(req.query.type ?? "fetch").trim().toLowerCase();
  const allowed = new Set(["fetch", "filter", "import-sqlite", "export-ratings", "rating-outcomes"]);
  if (!allowed.has(jobType)) throw new ApiError(400, "Unsupported job type");
  const job = await latestJob(jobType, String(req.query.job_id ?? "") || undefined);
  requireJobVisibility(user, jobType, job);
  res.json({ok: true, job: jobPayload(job)});
}));

apiApp.get("/api/filter/job", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const job = await latestJob("filter", String(req.query.job_id ?? "") || undefined);
  requireJobVisibility(user, "filter", job);
  res.json({ok: true, job: jobPayload(job)});
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

apiApp.get("/api/scan-results", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const market = currentMarket(req.query.market);
  const requestedScanId = Number(req.query.scan_id ?? 0);
  const scanResult = requestedScanId
    ? await db().query(
      `SELECT id, source_id, created_at_utc, provider, query, scanned_count, result_count,
              skipped_no_history, config_json
       FROM scan_runs WHERE id = $1 AND market = $2`,
      [requestedScanId, market]
    )
    : await db().query(
      `SELECT id, source_id, created_at_utc, provider, query, scanned_count, result_count,
              skipped_no_history, config_json
       FROM scan_runs WHERE market = $1 ORDER BY created_at_utc DESC LIMIT 1`,
      [market]
    );
  const scan = scanResult.rows[0];
  if (!scan) throw new ApiError(404, "No shared scan is available for this market yet");

  const rows = await db().query(
    `
    SELECT id, scan_id, source_id, rank, ticker, signal_date, close_price, market_cap,
           avg_volume, volume_ratio, sector, industry, result_json
    FROM scan_results
    WHERE scan_id = $1
    ORDER BY rank ASC
    `,
    [scan.id]
  );
  const results = rows.rows.map((row) => {
    const raw = row.result_json;
    const source = raw && typeof raw === "object" && !Array.isArray(raw)
      ? raw as Record<string, unknown>
      : {};
    return {
      ...source,
      id: row.id,
      scan_id: row.scan_id,
      source_id: row.source_id,
      rank: row.rank,
      ticker: row.ticker,
      date: source.date ?? (row.signal_date ? dateOnly(row.signal_date) : null),
      close_price: row.close_price,
      market_cap: row.market_cap,
      avg_volume: row.avg_volume,
      volume_ratio: row.volume_ratio,
      sector: row.sector,
      industry: row.industry
    };
  });
  res.json({ok: true, scan, results: await overlayUserAppraisals(user, Number(scan.id), results)});
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

apiApp.get("/api/analysis/summary", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const requestedMarket = strictMarket(req.query.market, true);
  const market = requestedMarket === "all" ? null : requestedMarket;
  const horizon = analysisHorizon(req.query.horizon);
  const result = await db().query(
    `
    WITH latest_labels AS (
      SELECT DISTINCT ON (firebase_uid, market, ticker)
        id, firebase_uid, user_email, market, ticker, label, event_at_utc, signal_date,
        close_price AS signal_price
      FROM rating_events
      WHERE action = 'label'
        AND label IS NOT NULL
        AND ($1::text IS NULL OR market = $1)
      ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
    ), performance AS (
      SELECT
        labelled.*,
        CASE WHEN $2::int = 0 THEN latest.close_price ELSE outcome.price_at_horizon END AS latest_price,
        CASE WHEN $2::int = 0 THEN latest.price_date ELSE NULL END AS latest_date,
        CASE
          WHEN $2::int = 0 AND labelled.signal_price > 0 AND latest.close_price IS NOT NULL
          THEN ((latest.close_price - labelled.signal_price) / labelled.signal_price) * 100
          WHEN $2::int <> 0 THEN outcome.return_percent
          ELSE NULL
        END AS return_percent
      FROM latest_labels labelled
      LEFT JOIN LATERAL (
        SELECT close_price, price_date
        FROM price_history
        WHERE market = labelled.market
          AND ticker = labelled.ticker
          AND provider = 'yfinance'
          AND close_price IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 1
      ) latest ON TRUE
      LEFT JOIN rating_outcomes outcome
        ON outcome.rating_event_id = labelled.id
       AND outcome.horizon_days = $2::int
    )
    SELECT
      label,
      COUNT(*)::int AS pick_count,
      COUNT(*) FILTER (WHERE return_percent IS NOT NULL)::int AS priced_count,
      COUNT(*) FILTER (WHERE return_percent > 0)::int AS positive_count,
      ROUND(AVG(return_percent)::numeric, 2) AS average_return_percent,
      ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_percent)::numeric, 2) AS median_return_percent,
      ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - event_at_utc)) / 86400)::numeric, 1) AS average_age_days
    FROM performance
    GROUP BY label
    ORDER BY CASE label
      WHEN 'winner' THEN 1
      WHEN 'potential_winner' THEN 2
      WHEN 'maybe' THEN 3
      WHEN 'bad' THEN 4
      ELSE 5
    END
    `,
    [market, horizon]
  );
  res.json({ok: true, market: requestedMarket, horizon_days: horizon, summary: result.rows});
}));

apiApp.get("/api/analysis/picks", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const requestedMarket = strictMarket(req.query.market, true);
  const market = requestedMarket === "all" ? null : requestedMarket;
  const horizon = analysisHorizon(req.query.horizon);
  const requestedLabel = String(req.query.label ?? "").trim().toLowerCase().replace(/\s+/g, "_");
  if (requestedLabel && !VALID_LABELS.has(requestedLabel)) throw new ApiError(400, "Invalid rating label");
  const limit = Math.min(Math.max(Number(req.query.limit ?? 250), 1), 1000);
  const result = await db().query(
    `
    WITH latest_labels AS (
      SELECT DISTINCT ON (firebase_uid, market, ticker)
        id, firebase_uid, user_email, market, ticker, label, event_at_utc, signal_date,
        close_price AS signal_price
      FROM rating_events
      WHERE action = 'label'
        AND label IS NOT NULL
        AND ($1::text IS NULL OR market = $1)
        AND ($2::text IS NULL OR label = $2)
      ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
    )
    SELECT
      labelled.market, labelled.ticker, labelled.label, labelled.user_email,
      labelled.event_at_utc, labelled.signal_date, labelled.signal_price,
      CASE WHEN $3::int = 0 THEN latest.close_price ELSE outcome.price_at_horizon END AS latest_price,
      CASE WHEN $3::int = 0 THEN latest.price_date ELSE NULL END AS latest_date,
      outcome.measured_at_utc,
      CASE
        WHEN $3::int = 0 AND labelled.signal_price > 0 AND latest.close_price IS NOT NULL
        THEN ROUND((((latest.close_price - labelled.signal_price) / labelled.signal_price) * 100)::numeric, 2)
        WHEN $3::int <> 0 THEN ROUND(outcome.return_percent::numeric, 2)
        ELSE NULL
      END AS return_percent
    FROM latest_labels labelled
    LEFT JOIN LATERAL (
      SELECT close_price, price_date
      FROM price_history
      WHERE market = labelled.market
        AND ticker = labelled.ticker
        AND provider = 'yfinance'
        AND close_price IS NOT NULL
      ORDER BY price_date DESC
      LIMIT 1
    ) latest ON TRUE
    LEFT JOIN rating_outcomes outcome
      ON outcome.rating_event_id = labelled.id
     AND outcome.horizon_days = $3::int
    ORDER BY return_percent ASC NULLS LAST, labelled.event_at_utc DESC
    LIMIT $4
    `,
    [market, requestedLabel || null, horizon, limit]
  );
  res.json({ok: true, market: requestedMarket, horizon_days: horizon, picks: result.rows});
}));

apiApp.get("/api/analysis/insights", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const requestedMarket = strictMarket(req.query.market, true);
  const market = requestedMarket === "all" ? null : requestedMarket;
  const horizon = analysisHorizon(req.query.horizon);
  const underperformerLimit = Math.min(Math.max(Number(req.query.underperformer_limit ?? 25), 1), 100);
  const patternLimit = Math.min(Math.max(Number(req.query.pattern_limit ?? 40), 1), 100);

  const commonCte = `
    WITH latest_labels AS (
      SELECT DISTINCT ON (firebase_uid, market, ticker)
        id, firebase_uid, user_email, market, ticker, label, event_at_utc, signal_date,
        close_price AS signal_price, market_cap, avg_volume, volume_ratio, sector, industry
      FROM rating_events
      WHERE action = 'label'
        AND label IS NOT NULL
        AND ($1::text IS NULL OR market = $1)
      ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
    ), performance AS (
      SELECT
        labelled.*,
        CASE WHEN $2::int = 0 THEN latest.close_price ELSE outcome.price_at_horizon END AS latest_price,
        CASE WHEN $2::int = 0 THEN latest.price_date ELSE NULL END AS latest_date,
        outcome.measured_at_utc,
        CASE
          WHEN $2::int = 0 AND labelled.signal_price > 0 AND latest.close_price IS NOT NULL
          THEN ((latest.close_price - labelled.signal_price) / labelled.signal_price) * 100
          WHEN $2::int <> 0 THEN outcome.return_percent
          ELSE NULL
        END AS return_percent,
        CASE
          WHEN labelled.market_cap IS NULL OR labelled.market_cap <= 0 THEN 'Unknown'
          WHEN labelled.market_cap < 300000000 THEN 'Micro cap'
          WHEN labelled.market_cap < 2000000000 THEN 'Small cap'
          WHEN labelled.market_cap < 10000000000 THEN 'Mid cap'
          ELSE 'Large cap'
        END AS market_cap_bucket
      FROM latest_labels labelled
      LEFT JOIN LATERAL (
        SELECT close_price, price_date
        FROM price_history
        WHERE market = labelled.market
          AND ticker = labelled.ticker
          AND provider = 'yfinance'
          AND close_price IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 1
      ) latest ON TRUE
      LEFT JOIN rating_outcomes outcome
        ON outcome.rating_event_id = labelled.id
       AND outcome.horizon_days = $2::int
    )
  `;

  const underperformers = await db().query(
    `
    ${commonCte}
    SELECT market, ticker, label, user_email, event_at_utc, signal_date,
           signal_price, latest_price, latest_date, measured_at_utc,
           ROUND(return_percent::numeric, 2) AS return_percent,
           sector, industry, market_cap, avg_volume, volume_ratio
    FROM performance
    WHERE label IN ('winner', 'potential_winner')
      AND return_percent < 0
    ORDER BY return_percent ASC, event_at_utc DESC
    LIMIT $3
    `,
    [market, horizon, underperformerLimit]
  );

  const patterns = await db().query(
    `
    ${commonCte},
    grouped AS (
      SELECT 'Sector' AS dimension, COALESCE(NULLIF(sector, ''), 'Unknown') AS group_name, label,
             COUNT(*)::int AS pick_count,
             COUNT(*) FILTER (WHERE return_percent IS NOT NULL)::int AS priced_count,
             COUNT(*) FILTER (WHERE return_percent > 0)::int AS positive_count,
             ROUND(AVG(return_percent)::numeric, 2) AS average_return_percent,
             ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_percent)::numeric, 2) AS median_return_percent,
             ROUND(AVG(market_cap)::numeric, 0) AS average_market_cap,
             ROUND(AVG(volume_ratio)::numeric, 2) AS average_volume_ratio
      FROM performance
      WHERE label IN ('winner', 'potential_winner')
      GROUP BY COALESCE(NULLIF(sector, ''), 'Unknown'), label
      UNION ALL
      SELECT 'Industry' AS dimension, COALESCE(NULLIF(industry, ''), 'Unknown') AS group_name, label,
             COUNT(*)::int AS pick_count,
             COUNT(*) FILTER (WHERE return_percent IS NOT NULL)::int AS priced_count,
             COUNT(*) FILTER (WHERE return_percent > 0)::int AS positive_count,
             ROUND(AVG(return_percent)::numeric, 2) AS average_return_percent,
             ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_percent)::numeric, 2) AS median_return_percent,
             ROUND(AVG(market_cap)::numeric, 0) AS average_market_cap,
             ROUND(AVG(volume_ratio)::numeric, 2) AS average_volume_ratio
      FROM performance
      WHERE label IN ('winner', 'potential_winner')
      GROUP BY COALESCE(NULLIF(industry, ''), 'Unknown'), label
      UNION ALL
      SELECT 'Market Cap' AS dimension, market_cap_bucket AS group_name, label,
             COUNT(*)::int AS pick_count,
             COUNT(*) FILTER (WHERE return_percent IS NOT NULL)::int AS priced_count,
             COUNT(*) FILTER (WHERE return_percent > 0)::int AS positive_count,
             ROUND(AVG(return_percent)::numeric, 2) AS average_return_percent,
             ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_percent)::numeric, 2) AS median_return_percent,
             ROUND(AVG(market_cap)::numeric, 0) AS average_market_cap,
             ROUND(AVG(volume_ratio)::numeric, 2) AS average_volume_ratio
      FROM performance
      WHERE label IN ('winner', 'potential_winner')
      GROUP BY market_cap_bucket, label
    )
    SELECT *,
           CASE WHEN priced_count > 0 THEN ROUND(((positive_count::numeric / priced_count) * 100), 1) ELSE NULL END AS hit_rate_percent
    FROM grouped
    WHERE pick_count > 0
    ORDER BY priced_count DESC, average_return_percent DESC NULLS LAST, pick_count DESC
    LIMIT $3
    `,
    [market, horizon, patternLimit]
  );

  res.json({
    ok: true,
    market: requestedMarket,
    horizon_days: horizon,
    underperformers: underperformers.rows,
    patterns: patterns.rows
  });
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
  res.json(await startMarketRefresh(payload, user));
}));

apiApp.post("/api/admin/refresh-market", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const payload = req.body ?? {};
  res.json(await startMarketRefresh(payload, user));
}));

apiApp.post("/api/admin/import-sqlite", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const body = req.body ?? {};
  const storagePath = storageObjectPath(body.storage_path ?? body.storagePath, ["imports/"]);
  if (!/\.(sqlite|sqlite3|db)$/i.test(storagePath)) {
    throw new ApiError(400, "Storage import must point to a .sqlite, .sqlite3, or .db file");
  }
  const ratingsPath = optionalStorageObjectPath(body.ratings_storage_path ?? body.ratingsStoragePath, ["imports/"]);
  if (ratingsPath && !/\.(sqlite|sqlite3|db)$/i.test(ratingsPath)) {
    throw new ApiError(400, "Ratings import must point to a .sqlite, .sqlite3, or .db file");
  }
  const market = strictMarket(body.market) as "asx" | "us";
  const payload = {
    market,
    storage_path: storagePath,
    ratings_storage_path: ratingsPath,
    chunk_size: positiveInt(body.chunk_size ?? body.chunkSize, 5000, 100, 50000),
    price_since: body.price_since ? String(body.price_since).trim() : undefined,
    full_tickers: booleanValue(body.full_tickers ?? body.fullTickers, true),
    resume: booleanValue(body.resume, true),
    bulk_prices: booleanValue(body.bulk_prices ?? body.bulkPrices, true),
    rebuild_weekly: booleanValue(body.rebuild_weekly ?? body.rebuildWeekly, true),
    requested_by_uid: user.uid,
    requested_by_email: user.email
  };
  res.json(await createJob("import-sqlite", payload));
}));

apiApp.post("/api/admin/import-upload-url", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const body = req.body ?? {};
  const market = strictMarket(body.market) as "asx" | "us";
  const fileName = String(body.file_name ?? body.fileName ?? "").trim();
  sqliteFileExtension(fileName);
  const byteSize = Number(body.size_bytes ?? body.sizeBytes ?? 0);
  if (!Number.isFinite(byteSize) || byteSize < 1 || byteSize > 5 * 1024 * 1024 * 1024) {
    throw new ApiError(400, "SQLite upload must be between 1 byte and 5 GB");
  }

  const storagePath = importUploadPath(market, user.uid, fileName);
  const expiresAt = new Date(Date.now() + 20 * 60 * 1000);
  const [uploadUrl] = await getStorage().bucket(storageBucketName()).file(storagePath).getSignedUrl({
    version: "v4",
    action: "write",
    expires: expiresAt,
    contentType: "application/x-sqlite3"
  });
  res.json({
    ok: true,
    storage_path: storagePath,
    content_type: "application/x-sqlite3",
    expires_at: expiresAt.toISOString(),
    upload: {url: uploadUrl, method: "PUT"}
  });
}));

apiApp.get("/api/admin/storage-download-url", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const storagePath = storageObjectPath(req.query.path, ["exports/"]);
  const expiresAt = new Date(Date.now() + 20 * 60 * 1000);
  const [downloadUrl] = await getStorage().bucket(storageBucketName()).file(storagePath).getSignedUrl({
    version: "v4",
    action: "read",
    expires: expiresAt,
    responseDisposition: `attachment; filename="${storagePath.split("/").at(-1) ?? "moneymaker-export"}"`
  });
  res.json({ok: true, storage_path: storagePath, expires_at: expiresAt.toISOString(), download_url: downloadUrl});
}));

apiApp.post("/api/scheduled-fetch", asyncRoute(async (req, res) => {
  await requireScheduler(req);
  const market = currentMarket(req.body?.market);
  const scheduledPayload = {
    ...defaultScheduledFetchPayload(market),
    ...(req.body ?? {}),
    market
  };
  res.json(await startScheduledMarketRefresh(scheduledPayload));
}));

apiApp.post("/api/filter/start", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  res.json(await startFilterJob(req.body ?? {}));
}));

apiApp.post("/api/admin/rebuild-default-scans", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  res.json(await startFilterJob(req.body ?? {}));
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

apiApp.post("/api/export/ratings", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const body = req.body ?? {};
  const market = strictMarket(body.market, true);
  const payload = {
    market,
    limit: positiveInt(body.limit, 25000, 1, 250000),
    format: String(body.format ?? "csv").trim().toLowerCase() === "json" ? "json" : "csv",
    storage_path: optionalStorageObjectPath(body.storage_path ?? body.storagePath, ["exports/"]),
    requested_by_uid: user.uid,
    requested_by_email: user.email
  };
  res.json(await createJob("export-ratings", payload));
}));

apiApp.post("/api/admin/reconcile-jobs", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  res.json({ok: true, reconciled: await reconcileStaleJobs()});
}));

apiApp.post("/api/admin/recalculate-rating-outcomes", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const body = req.body ?? {};
  const market = strictMarket(body.market, true);
  const payload = {
    market,
    horizons: Array.isArray(body.horizons) ? body.horizons : [30, 90, 180, 360],
    limit: positiveInt(body.limit, 100000, 1, 500000),
    requested_by_uid: user.uid,
    requested_by_email: user.email
  };
  res.json(await startRatingOutcomesJob(payload));
}));

apiApp.use((_req, res) => {
  res.status(404).json({ok: false, error: "Not found"});
});

export {apiApp};

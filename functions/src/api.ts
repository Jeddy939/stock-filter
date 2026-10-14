import crypto from "node:crypto";
import {gunzipSync} from "node:zlib";
import cors from "cors";
import express, {type NextFunction, type Request, type Response} from "express";
import type {PoolClient} from "pg";
import {getAuth, type UserRecord} from "firebase-admin/auth";
import {getFunctions} from "firebase-admin/functions";
import {getStorage} from "firebase-admin/storage";
import {OAuth2Client} from "google-auth-library";
import {ApiError, requireAdmin, requireAnalyst, requireAppCheck, requireAuth, type UserContext} from "./auth";
import {dispatchCloudRunJob} from "./cloudrun";
import {db} from "./db";
import {readMarketStatusViaDataConnect, type MarketStatusRow} from "./dataconnect";
import {normalizeFeedbackInput, normalizeFeedbackStatus} from "./feedback";
import {
  analysisRangeDays,
  currentMarket,
  exclusiveHistoryEndDate,
  MARKET_DEFAULTS,
  normalizeCompanyProfile,
  rangeDays,
  VALID_LABELS,
  yahooUrl
} from "./market";
import {requiresAppCheck} from "./request-policy";
import {ipRateLimit} from "./ip-rate-limit";
import {normalizeScreenConfig, screenConfigHash} from "./screen-config";
import {databaseState, ensureDatabaseRunning, startDatabase, stopDatabase} from "./database-lifecycle";

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
  started_at_utc?: string | Date;
  finished_at_utc?: string | Date | null;
  updated_at_utc?: string | Date;
  config_hash?: string | null;
  dedupe_key?: string | null;
  [key: string]: unknown;
}

export interface PriceRow {
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

// Keep in sync with FEATURE_VERSION (cloud_backend/insights/features.py) and
// OUTCOME_VERSION (firebase/worker.py).
const INSIGHT_FEATURE_VERSION = 3;
const INSIGHT_OUTCOME_VERSION = 2;

const apiApp = express();
apiApp.disable("x-powered-by");
apiApp.use(ipRateLimit);
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
  const startedAt = row.started_at_utc ? new Date(row.started_at_utc) : null;
  const finishedAt = row.finished_at_utc ? new Date(row.finished_at_utc) : null;
  const elapsedSeconds = startedAt && !Number.isNaN(startedAt.getTime())
    ? Math.max(0, Math.round(((finishedAt?.getTime() ?? Date.now()) - startedAt.getTime()) / 1000))
    : null;
  return {
    ...row,
    running: status === "queued" || status === "running",
    success: status === "succeeded" ? true : status === "failed" ? false : null,
    message: row.detail ?? status.charAt(0).toUpperCase() + status.slice(1),
    current: row.current_count ?? 0,
    total: row.total_count ?? null,
    summary: row.parameters_json ?? {},
    results: row.result_json ?? [],
    elapsed_seconds: elapsedSeconds
  };
}

function nextScheduledRefresh(market: "asx" | "us", now = new Date()): string {
  const brisbaneOffsetMs = 10 * 60 * 60 * 1000;
  const localNow = new Date(now.getTime() + brisbaneOffsetMs);
  const allowedDays = market === "asx" ? new Set([1, 2, 3, 4, 5]) : new Set([2]);
  const hour = market === "asx" ? 6 : 7;
  for (let dayOffset = 0; dayOffset < 8; dayOffset += 1) {
    const localCandidate = new Date(Date.UTC(
      localNow.getUTCFullYear(),
      localNow.getUTCMonth(),
      localNow.getUTCDate() + dayOffset,
      hour,
      30
    ));
    if (!allowedDays.has(localCandidate.getUTCDay())) continue;
    const utcCandidate = new Date(localCandidate.getTime() - brisbaneOffsetMs);
    if (utcCandidate.getTime() > now.getTime()) return utcCandidate.toISOString();
  }
  throw new Error(`Could not determine next ${market.toUpperCase()} refresh`);
}

function defaultConfigHash(market: "asx" | "us"): string {
  return screenConfigHash(defaultScanPayload(market));
}

function stageCode(value: unknown): string {
  return String(value ?? "working")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "working";
}

async function recordJobEvent(
  jobId: string,
  stage: string,
  status: string,
  message: string,
  current = 0,
  total: number | null = null,
  percent: number | null = null,
  metadata: Record<string, unknown> = {}
): Promise<void> {
  await db().query(
    `
    INSERT INTO job_events
      (job_id, stage_code, stage, status, message, current_count, total_count, percent, metadata_json)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
    `,
    [jobId, stageCode(stage), stage, status, message, current, total, percent, JSON.stringify(metadata)]
  );
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

export function buildChartSeries(
  historyRows: PriceRow[],
  interval: string,
  range: string,
  periods: number[]
): {
  rows: PriceRow[];
  movingAverages: Record<string, Array<number | null>>;
  availability: Record<string, {available: boolean; required_bars: number; cached_bars: number; initialized_at: string | null}>;
} {
  const allRows = aggregateRows(historyRows, interval)
    .filter((row) => numberOrNull(row.close) !== null);
  const closes = allRows.map((row) => numberOrNull(row.close));
  const uniquePeriods = [...new Set(periods)];
  const completeAverages: Record<string, Array<number | null>> = {};
  for (const period of uniquePeriods) {
    completeAverages[String(period)] = movingAverage(closes, period);
  }

  let visibleStartIndex = 0;
  if (range !== "all" && allRows.length) {
    const latest = new Date(`${dateOnly(allRows[allRows.length - 1].date)}T00:00:00.000Z`);
    latest.setUTCDate(latest.getUTCDate() - rangeDays(range));
    const cutoff = latest.toISOString().slice(0, 10);
    const firstVisible = allRows.findIndex((row) => dateOnly(row.date) >= cutoff);
    visibleStartIndex = firstVisible >= 0 ? firstVisible : allRows.length;
  }

  const rows = allRows.slice(visibleStartIndex);
  const movingAverages: Record<string, Array<number | null>> = {};
  const availability: Record<string, {available: boolean; required_bars: number; cached_bars: number; initialized_at: string | null}> = {};
  for (const period of uniquePeriods) {
    const values = completeAverages[String(period)].slice(visibleStartIndex);
    movingAverages[String(period)] = values;
    availability[String(period)] = {
      available: values.some((value) => Number.isFinite(value)),
      required_bars: period,
      cached_bars: allRows.length,
      initialized_at: allRows.length >= period ? dateOnly(allRows[period - 1].date) : null
    };
  }
  return {rows, movingAverages, availability};
}

async function createJob(jobType: string, payload: Record<string, unknown>) {
  const screenConfig = jobType === "filter" ? normalizeScreenConfig(payload) : null;
  const insightEventIds = jobType === "insight-snapshot-backfill" && Array.isArray(payload.event_ids)
    ? payload.event_ids.map((value) => Number(value)).filter(Number.isSafeInteger).sort((left, right) => left - right)
    : [];
  const insightRunConfig = jobType === "insight-run" ? {
    market: String(payload.market ?? "all").toLowerCase(),
    scope: String(payload.scope ?? "mine").toLowerCase(),
    owner_uid: String(payload.owner_uid ?? ""),
    horizon_days: Number(payload.horizon_days ?? 84),
    timing: String(payload.timing ?? "decision").toLowerCase(),
    feature_version: Number(payload.feature_version ?? INSIGHT_FEATURE_VERSION),
    outcome_version: Number(payload.outcome_version ?? INSIGHT_OUTCOME_VERSION),
    target_percent: Number(payload.target_percent ?? 15),
    stop_percent: Number(payload.stop_percent ?? -12),
    requested_by_uid: String(payload.requested_by_uid ?? "")
  } : null;
  const backgroundConfig = jobType === "insight-snapshot-backfill" ? {
    market: String(payload.market ?? "all"),
    owner_uid: String(payload.owner_uid ?? ""),
    labels: Array.isArray(payload.labels) ? [...payload.labels].map(String).sort() : [],
    event_ids: insightEventIds,
    feature_version: Number(payload.feature_version ?? INSIGHT_FEATURE_VERSION)
  } : jobType === "fundamentals-refresh" ? {
    market: "us",
    tickers: Array.isArray(payload.tickers) ? [...payload.tickers].map(String).sort() : [],
    force: payload.force === true
  } : null;
  const configHash = screenConfig
    ? screenConfigHash(payload)
    : backgroundConfig
      ? crypto.createHash("sha256").update(JSON.stringify(backgroundConfig)).digest("hex")
      : insightRunConfig
        ? crypto.createHash("sha256").update(JSON.stringify(insightRunConfig)).digest("hex")
        : null;
  const effectivePayload = screenConfig
    ? {...payload, ...screenConfig, config_hash: configHash}
    : configHash
      ? {...payload, config_hash: configHash}
      : payload;
  const requestedMarket = String(effectivePayload.market ?? "").trim().toLowerCase();
  const market = requestedMarket === "all" ? null : currentMarket(payload.market);
  const dedupeKey = ["filter", "insight-snapshot-backfill", "insight-run", "fundamentals-refresh"].includes(jobType) && configHash
    ? `${market ?? "all"}:${configHash}`
    : null;
  if (dedupeKey) {
    const active = await db().query(
      `
      SELECT * FROM job_runs
      WHERE job_type = $1 AND market IS NOT DISTINCT FROM $2 AND dedupe_key = $3
        AND status IN ('queued', 'running')
      ORDER BY started_at_utc DESC
      LIMIT 1
      `,
      [jobType, market, dedupeKey]
    );
    if (active.rows[0]) {
      return {ok: true, deduplicated: true, job: jobPayload(active.rows[0])};
    }
  }

  const jobId = crypto.randomUUID();
  const started = new Date();
  try {
    await db().query(
      `
      INSERT INTO job_runs
        (id, job_type, market, status, stage, detail, started_at_utc, updated_at_utc,
         parameters_json, dedupe_key, config_hash)
      VALUES ($1, $2, $3, 'queued', 'Queued', $4, $5, $5, $6::jsonb, $7, $8)
      `,
      [jobId, jobType, market, "Waiting for a worker", started, JSON.stringify(effectivePayload), dedupeKey, configHash]
    );
  } catch (error) {
    if ((error as {code?: string}).code === "23505" && dedupeKey) {
      const active = await db().query(
        `SELECT * FROM job_runs
         WHERE job_type = $1 AND market IS NOT DISTINCT FROM $2 AND dedupe_key = $3
           AND status IN ('queued', 'running')
         ORDER BY started_at_utc DESC LIMIT 1`,
        [jobType, market, dedupeKey]
      );
      if (active.rows[0]) return {ok: true, deduplicated: true, job: jobPayload(active.rows[0])};
    }
    throw error;
  }
  await recordJobEvent(jobId, "Queued", "queued", "Waiting for a worker", 0, null, null, {
    market,
    config_hash: configHash
  });

  try {
    const cloudRunJob = await dispatchCloudRunJob(jobType, effectivePayload, jobId);
    await db().query(
      "UPDATE job_runs SET detail = $1, updated_at_utc = NOW() WHERE id = $2",
      [`Worker ${cloudRunJob} requested`, jobId]
    );
    await recordJobEvent(jobId, "Starting worker", "queued", "Cloud worker requested", 0, null, null, {
      cloud_run_execution: cloudRunJob
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await db().query(
      "UPDATE job_runs SET status = 'failed', stage = 'Failed', error = $1, detail = $2, updated_at_utc = NOW(), finished_at_utc = NOW() WHERE id = $3",
      [message, "Could not dispatch Cloud Run Job", jobId]
    );
    await recordJobEvent(jobId, "Failed", "failed", message);
    throw new ApiError(502, `Could not queue ${jobType} job: ${message}`);
  }

  return {
    ok: true,
    job: jobPayload({
      id: jobId,
      job_type: jobType,
      status: "queued",
      stage: "Queued",
      detail: "Waiting for a worker",
      current_count: 0,
      total_count: null,
      percent: 0,
      parameters_json: effectivePayload,
      result_json: [],
      started_at_utc: started,
      updated_at_utc: started,
      config_hash: configHash,
      dedupe_key: dedupeKey
    })
  };
}

async function insertSnapshotStub(client: PoolClient, ratingEventId: number): Promise<number | null> {
  const result = await client.query(
    `
    WITH target AS (
      SELECT id, firebase_uid, market, ticker, label, event_at_utc
      FROM rating_events
      WHERE id = $1 AND action = 'label' AND label IS NOT NULL
        AND firebase_uid IS NOT NULL AND market IN ('asx', 'us')
    ), origin AS (
      SELECT first_event.id
      FROM target
      JOIN LATERAL (
        SELECT re.id
        FROM rating_events re
        WHERE re.firebase_uid = target.firebase_uid
          AND re.market = target.market
          AND re.ticker = target.ticker
          AND re.action = 'label'
          AND re.label IS NOT NULL
          AND (re.event_at_utc, re.id) <= (target.event_at_utc, target.id)
        ORDER BY re.event_at_utc, re.id
        LIMIT 1
      ) first_event ON TRUE
    )
    INSERT INTO pick_feature_snapshots (
      rating_event_id, origin_event_id, firebase_uid, market, ticker,
      appraisal_label, appraisal_at_utc, feature_version, snapshot_status
    )
    SELECT target.id, origin.id, target.firebase_uid, target.market, target.ticker,
           target.label, target.event_at_utc, $2::int, 'queued'
    FROM target CROSS JOIN origin
    ON CONFLICT (rating_event_id, feature_version) DO NOTHING
    RETURNING id
    `,
    [ratingEventId, INSIGHT_FEATURE_VERSION]
  );
  return result.rows[0] ? Number(result.rows[0].id) : null;
}

async function queueSnapshotBuild(
  ratingEventId: number,
  market: "asx" | "us",
  user: UserContext
): Promise<Record<string, unknown>> {
  try {
    return await createJob("insight-snapshot-backfill", {
      market,
      event_ids: [ratingEventId],
      feature_version: INSIGHT_FEATURE_VERSION,
      requested_by_uid: user.uid,
      requested_by_email: user.email
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Point-in-time snapshot dispatch failed", {ratingEventId, market, message});
    return {ok: false, queued: false, error: message};
  }
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
    await db().query(
      `UPDATE job_runs
       SET status = 'running', stage = 'Fetching batches', detail = $1, updated_at_utc = NOW()
       WHERE id = $2`,
      ["Ticker refresh batches queued", jobId]
    );
    await db().query(
      `UPDATE refresh_jobs
       SET status = 'running', stage = 'Fetching batches', finished_at_utc = NULL, error = NULL
       WHERE id = $1`,
      [refresh.id]
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await db().query(
      "UPDATE job_runs SET status = 'failed', stage = 'Failed', error = $1, detail = $2 WHERE id = $3",
      [message, "Could not enqueue ticker refresh batches", jobId]
    );
    await db().query(
      `UPDATE refresh_jobs
       SET status = 'failed', stage = 'Queue failed', error = $1, finished_at_utc = NOW()
       WHERE id = $2`,
      [message, refresh.id]
    );
    throw new ApiError(502, `Could not queue refresh batches: ${message}`);
  }

  return {
    ok: true,
    job: jobPayload({
      id: jobId,
      job_type: "fetch",
      status: "running",
      stage: "Fetching batches",
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

async function resumeFailedRefreshTracking(
  payload: Record<string, unknown>,
  user?: UserContext
): Promise<RefreshTracking | null> {
  if (!booleanValue(payload.resume, true)) return null;
  const market = currentMarket(payload.market);
  const provider = String(payload.provider ?? "yfinance");
  const historyEndDate = String(payload.history_end_date ?? "");
  const forceFullHistory = booleanValue(payload.force_full_history, false);
  const client = await db().connect();
  try {
    await client.query("BEGIN");
    const candidate = await client.query(
      `
      SELECT r.id
      FROM refresh_jobs r
      WHERE r.market = $1 AND r.provider = $2 AND r.status = 'failed'
        AND COALESCE(r.parameters_json->>'history_end_date', '') = $3
        AND COALESCE((r.parameters_json->>'force_full_history')::boolean, false) = $4
        AND r.started_at_utc > NOW() - INTERVAL '7 days'
        AND EXISTS (
          SELECT 1 FROM refresh_batches failed
          WHERE failed.refresh_job_id = r.id AND failed.status = 'failed'
        )
        AND NOT EXISTS (
          SELECT 1 FROM refresh_batches active
          WHERE active.refresh_job_id = r.id AND active.status IN ('queued', 'running')
        )
      ORDER BY r.started_at_utc DESC
      LIMIT 1
      FOR UPDATE
      `,
      [market, provider, historyEndDate, forceFullHistory]
    );
    const refreshJobId = String(candidate.rows[0]?.id ?? "");
    if (!refreshJobId) {
      await client.query("ROLLBACK");
      return null;
    }
    const failed = await client.query(
      `
      SELECT id, batch_index, tickers_json
      FROM refresh_batches
      WHERE refresh_job_id = $1 AND status = 'failed'
      ORDER BY batch_index
      FOR UPDATE
      `,
      [refreshJobId]
    );
    const batches: RefreshBatch[] = failed.rows.map((row) => ({
      id: String(row.id),
      batchIndex: Number(row.batch_index),
      tickers: Array.isArray(row.tickers_json) ? row.tickers_json.map(String) : []
    }));
    await client.query(
      `
      UPDATE refresh_batches
      SET status = 'queued', attempts = 0, started_at_utc = NULL,
          finished_at_utc = NULL, error = NULL,
          result_json = COALESCE(result_json, '{}'::jsonb) - 'cloud_run_dispatch'
      WHERE refresh_job_id = $1 AND status = 'failed'
      `,
      [refreshJobId]
    );
    const succeeded = await client.query(
      `SELECT
         COALESCE(SUM(
           jsonb_array_length(tickers_json)
           - COALESCE((result_json #>> '{counts,missing_history_count}')::int, 0)
         ), 0)::int AS completed_count,
         COALESCE(SUM(
           COALESCE((result_json #>> '{counts,missing_history_count}')::int, 0)
         ), 0)::int AS failed_count
       FROM refresh_batches WHERE refresh_job_id = $1 AND status = 'succeeded'`,
      [refreshJobId]
    );
    const total = await client.query(
      `SELECT COALESCE(SUM(jsonb_array_length(tickers_json)), 0)::int AS count
       FROM refresh_batches WHERE refresh_job_id = $1`,
      [refreshJobId]
    );
    await client.query(
      `
      UPDATE refresh_jobs
      SET status = 'running', stage = 'Resuming failed batches',
          requested_by_uid = COALESCE($2, requested_by_uid),
          requested_by_email = COALESCE($3, requested_by_email),
          total_tickers = $4, completed_tickers = $5, failed_tickers = $6,
          parameters_json = $7::jsonb, error = NULL, finished_at_utc = NULL
      WHERE id = $1
      `,
      [refreshJobId, user?.uid ?? null, user?.email ?? null, Number(total.rows[0]?.count ?? 0),
        Number(succeeded.rows[0]?.completed_count ?? 0), Number(succeeded.rows[0]?.failed_count ?? 0),
        JSON.stringify({...payload, resumed: true})]
    );
    await client.query("COMMIT");
    return {
      id: refreshJobId,
      totalTickers: batches.reduce((sum, batch) => sum + batch.tickers.length, 0),
      batchCount: batches.length,
      batches
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

function insightHorizon(value: unknown): number {
  const horizon = Number(value ?? 84);
  if ([28, 56, 84, 182].includes(horizon)) return horizon;
  throw new ApiError(400, "Insights horizon must be 28, 56, 84, or 182 days");
}

type RuleClause = {feature: string; operator: "gt" | "gte" | "lt" | "lte" | "eq"; value: number | string | boolean};

function normalizeRuleCondition(value: unknown): {all: RuleClause[]} {
  const source = value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
  const clauses = Array.isArray(source.all) ? source.all : [];
  if (!clauses.length || clauses.length > 12) throw new ApiError(400, "A rule requires between 1 and 12 conditions");
  const allowedOperators = new Set(["gt", "gte", "lt", "lte", "eq"]);
  return {
    all: clauses.map((raw) => {
      const clause = raw && typeof raw === "object" && !Array.isArray(raw) ? raw as Record<string, unknown> : {};
      const feature = String(clause.feature ?? "").trim();
      const operator = String(clause.operator ?? "").trim().toLowerCase();
      const comparison = clause.value;
      if (!/^[a-z][a-z0-9_]{1,100}$/.test(feature)) throw new ApiError(400, "Invalid rule feature");
      if (!allowedOperators.has(operator)) throw new ApiError(400, "Invalid rule operator");
      if (!["number", "string", "boolean"].includes(typeof comparison)) throw new ApiError(400, "Invalid rule comparison value");
      if (typeof comparison === "number" && !Number.isFinite(comparison)) throw new ApiError(400, "Invalid numeric rule value");
      return {feature, operator: operator as RuleClause["operator"], value: comparison as RuleClause["value"]};
    })
  };
}

function ruleMatches(features: Record<string, unknown>, condition: {all: RuleClause[]}): boolean {
  return condition.all.every((clause) => {
    const raw = features[clause.feature];
    if (raw === null || raw === undefined) return false;
    if (clause.operator === "eq") return raw === clause.value || String(raw) === String(clause.value);
    const left = Number(raw);
    const right = Number(clause.value);
    if (!Number.isFinite(left) || !Number.isFinite(right)) return false;
    if (clause.operator === "gt") return left > right;
    if (clause.operator === "gte") return left >= right;
    if (clause.operator === "lt") return left < right;
    return left <= right;
  });
}

function numericPercentile(values: number[], probability: number): number {
  if (!values.length) return 0;
  const ordered = [...values].sort((left, right) => left - right);
  const position = (ordered.length - 1) * probability;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return ordered[lower];
  return ordered[lower] * (upper - position) + ordered[upper] * (position - lower);
}

export function normalizeAnalysisTicker(market: "asx" | "us", value: unknown): string {
  let ticker = String(value ?? "").trim().toUpperCase();
  if (market === "asx" && ticker && !ticker.endsWith(".AX")) ticker = `${ticker}.AX`;
  if (!/^[A-Z0-9^][A-Z0-9.^=-]{0,19}$/.test(ticker)) {
    throw new ApiError(400, "Enter a valid stock ticker");
  }
  return ticker;
}

export function manualRefreshPayload(input: Record<string, unknown>): Record<string, unknown> {
  const requestedMarket = strictMarket(input.market);
  if (requestedMarket === "all") throw new ApiError(400, "Market must be asx or us");
  return {
    ...defaultScheduledFetchPayload(requestedMarket),
    scheduled: false,
    manual: true
  };
}

export async function startMarketRefresh(
  payload: Record<string, unknown>,
  user?: UserContext
): Promise<Record<string, unknown>> {
  const market = currentMarket(payload.market);
  const provider = String(payload.provider ?? "yfinance");
  let forceFullHistory = booleanValue(payload.force_full_history ?? payload.forceFullHistory, false);
  if (provider === "yfinance" && payload.force_full_history === undefined && payload.forceFullHistory === undefined) {
    const basisResult = await db().query(
      "SELECT price_basis FROM market_status WHERE market = $1 AND provider = $2",
      [market, provider]
    );
    forceFullHistory = String(basisResult.rows[0]?.price_basis ?? "legacy_mixed") !== "raw_close_v1";
  }
  const refreshPayload = {
    ...payload,
    market,
    provider,
    force_full_history: forceFullHistory,
    target_price_basis: provider === "yfinance" ? "raw_close_v1" : undefined,
    history_end_date: String(payload.history_end_date ?? exclusiveHistoryEndDate())
  };
  const refresh = await resumeFailedRefreshTracking(refreshPayload, user)
    ?? await createRefreshTracking(refreshPayload, user);
  return createFetchJob(refreshPayload, refresh);
}

export async function startScheduledMarketRefresh(
  payload: Record<string, unknown>,
  user?: UserContext
): Promise<Record<string, unknown>> {
  const market = currentMarket(payload.market);
  const active = await db().query(
    `
    SELECT id, status, stage, total_tickers, completed_tickers, failed_tickers, started_at_utc
    FROM refresh_jobs
    WHERE market = $1
      AND status IN ('queued', 'running')
      AND started_at_utc > NOW() - INTERVAL '48 hours'
    ORDER BY started_at_utc DESC
    LIMIT 1
    `,
    [market]
  );
  const row = active.rows[0];
  if (row) {
    return {
      ok: true,
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
  return startMarketRefresh(payload, user);
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

export async function startSnapshotPublishJob(payload: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
  return createJob("publish-snapshot", payload);
}

export async function startFundamentalsRefreshJob(payload: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
  return createJob("fundamentals-refresh", payload);
}

export async function reconcileStaleJobs(): Promise<Record<string, number>> {
  const client = await db().connect();
  try {
    await client.query("BEGIN");
    const expiredRefreshes = await client.query(
      `
      WITH expired AS (
        SELECT id, status
        FROM refresh_jobs
        WHERE status IN ('queued', 'running')
          AND (
            started_at_utc < NOW() - INTERVAL '48 hours'
            OR (
              status = 'queued'
              AND started_at_utc < NOW() - INTERVAL '6 hours'
              AND NOT EXISTS (
                SELECT 1
                FROM refresh_batches active
                WHERE active.refresh_job_id = refresh_jobs.id
                  AND active.attempts > 0
              )
            )
            OR (
              status = 'running'
              AND started_at_utc < NOW() - INTERVAL '45 minutes'
              AND NOT EXISTS (
                SELECT 1
                FROM refresh_batches started
                WHERE started.refresh_job_id = refresh_jobs.id
                  AND started.attempts > 0
              )
            )
          )
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
            error = COALESCE(
              error,
              CASE
                WHEN expired.status = 'queued'
                THEN 'Refresh remained queued without a started batch'
                ELSE 'Refresh job expired before completion'
              END
            )
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
                AND updated_at_utc < NOW() - INTERVAL '45 minutes')
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
            updated_at_utc = NOW(),
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
      ),
      failed_parent_batches AS (
        UPDATE refresh_batches rb
        SET status = 'failed',
            finished_at_utc = COALESCE(finished_at_utc, NOW()),
            error = COALESCE(error, 'Parent fetch job expired before completion')
        FROM failed_jobs fj
        WHERE NOT (fj.parameters_json ? 'refresh_batch_id')
          AND rb.refresh_job_id::text = fj.parameters_json ->> 'refresh_job_id'
          AND rb.status IN ('queued', 'running')
        RETURNING rb.id, rb.refresh_job_id
      ),
      failed_parent_refreshes AS (
        UPDATE refresh_jobs r
        SET status = 'failed',
            stage = 'Expired',
            finished_at_utc = COALESCE(finished_at_utc, NOW()),
            error = COALESCE(error, 'Parent fetch job expired before completion')
        FROM failed_jobs fj
        WHERE NOT (fj.parameters_json ? 'refresh_batch_id')
          AND r.id::text = fj.parameters_json ->> 'refresh_job_id'
          AND r.status IN ('queued', 'running')
        RETURNING r.id
      )
      SELECT
        (SELECT COUNT(*)::int FROM failed_jobs) AS jobs,
        ((SELECT COUNT(*) FROM failed_batches) +
         (SELECT COUNT(*) FROM failed_parent_batches))::int AS batches,
        (SELECT COUNT(*)::int FROM failed_parent_refreshes) AS refreshes
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
            updated_at_utc = NOW(),
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
    const deletedEvents = await client.query(
      "DELETE FROM job_events WHERE created_at_utc < NOW() - INTERVAL '30 days' RETURNING id"
    );
    await client.query("COMMIT");
    return {
      expired_refresh_jobs: Number(expiredRefreshCounts.refreshes ?? 0) + Number(expiredJobCounts.refreshes ?? 0),
      expired_refresh_batches: Number(expiredRefreshCounts.batches ?? 0) + Number(expiredJobCounts.batches ?? 0),
      expired_job_runs: Number(expiredJobCounts.jobs ?? 0),
      finalized_refresh_jobs: Number(finalizedCounts.refreshes ?? 0),
      finalized_parent_job_runs: Number(finalizedCounts.jobs ?? 0),
      deleted_job_events: deletedEvents.rowCount ?? 0
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

export function applyLatestAppraisals(
  results: Array<Record<string, unknown>>,
  appraisalRows: Array<Record<string, unknown>>
): Array<Record<string, unknown>> {
  const appraisals = new Map(appraisalRows.map((row) => [String(row.ticker ?? "").toUpperCase(), row]));
  return results.map((row) => {
    const latest = appraisals.get(String(row.ticker ?? "").toUpperCase());
    const appraisal = latest?.action === "label" && latest.label ? latest : null;
    return {
      ...row,
      label: appraisal?.label ?? null,
      personal_note: appraisal?.note ?? null,
      personal_status: appraisal?.status ?? null,
      appraised_at_utc: appraisal?.event_at_utc ?? null
    };
  });
}

async function latestUserAppraisals(user: UserContext, market: "asx" | "us", tickers: string[]) {
  if (!tickers.length) return [];
  const result = await db().query(
    `
    SELECT ticker, action, label, note, event_at_utc
    FROM (
      SELECT DISTINCT ON (ticker)
        ticker, action, label, note, event_at_utc, id
      FROM rating_events
      WHERE firebase_uid = $1
        AND market = $2
        AND ticker = ANY($3::text[])
      ORDER BY ticker, event_at_utc DESC, id DESC
    ) latest
    `,
    [user.uid, market, tickers]
  );
  return result.rows as Array<Record<string, unknown>>;
}

async function overlayUserAppraisals(
  user: UserContext,
  market: "asx" | "us",
  results: Array<Record<string, unknown>>
) {
  if (results.length === 0) return results;
  const tickers = results.map((row) => String(row.ticker ?? "").toUpperCase()).filter(Boolean);
  return applyLatestAppraisals(results, await latestUserAppraisals(user, market, tickers));
}

async function marketFreshness(market: "asx" | "us") {
  const configHash = defaultConfigHash(market);
  const [statusResult, latestRefreshResult, successfulRefreshResult, scanResult] = await Promise.all([
    db().query(
      `SELECT market, provider, ticker_count, history_rows, weekly_rows,
              latest_date::text AS latest_bar_date, refreshed_at_utc
       FROM market_status
       WHERE market = $1 AND provider = 'yfinance'`,
      [market]
    ),
    db().query(
      `SELECT id, status, stage, total_tickers, completed_tickers, failed_tickers,
              started_at_utc, finished_at_utc, error
       FROM refresh_jobs
       WHERE market = $1
       ORDER BY started_at_utc DESC
       LIMIT 1`,
      [market]
    ),
    db().query(
      `SELECT id, finished_at_utc
       FROM refresh_jobs
       WHERE market = $1 AND status = 'succeeded'
       ORDER BY finished_at_utc DESC NULLS LAST
       LIMIT 1`,
      [market]
    ),
    db().query(
      `SELECT id, created_at_utc, market_snapshot_date::text AS market_snapshot_date,
              scanned_count, result_count, skipped_no_history, config_hash, config_json
       FROM scan_runs
       WHERE market = $1
         AND (config_hash = $2 OR config_hash IS NULL)
       ORDER BY (config_hash = $2) DESC, created_at_utc DESC
       LIMIT 1`,
      [market, configHash]
    )
  ]);
  const status = statusResult.rows[0] ?? {};
  const latestRefresh = latestRefreshResult.rows[0] ?? null;
  const successfulRefresh = successfulRefreshResult.rows[0] ?? null;
  return {
    market,
    provider: status.provider ?? "yfinance",
    latest_bar_date: status.latest_bar_date ?? null,
    database_refreshed_at_utc: status.refreshed_at_utc ?? null,
    last_successful_refresh_at_utc: successfulRefresh?.finished_at_utc ?? null,
    covered_tickers: Number(status.ticker_count ?? 0),
    history_rows: Number(status.history_rows ?? 0),
    weekly_metric_rows: Number(status.weekly_rows ?? 0),
    next_scheduled_refresh_at_utc: nextScheduledRefresh(market),
    latest_refresh: latestRefresh,
    latest_default_scan: scanResult.rows[0] ?? null,
    default_config_hash: configHash
  };
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

apiApp.get("/api/auth/bootstrap", asyncRoute(async (req, res) => {
  const rawToken = String(req.query.token ?? "").trim().toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(rawToken)) throw new ApiError(410, "This sign-in link is invalid or has expired");
  const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");
  const client = await db().connect();
  try {
    await client.query("BEGIN");
    const tokenResult = await client.query(
      `
      SELECT token.email
      FROM auth_bootstrap_tokens token
      JOIN app_user_invites invite ON invite.email = token.email
      WHERE token.token_hash = $1
        AND token.used_at_utc IS NULL
        AND token.expires_at_utc > NOW()
        AND invite.status = 'active'
      FOR UPDATE OF token
      `,
      [tokenHash]
    );
    const email = String(tokenResult.rows[0]?.email ?? "").trim().toLowerCase();
    if (!email) throw new ApiError(410, "This sign-in link is invalid or has expired");

    let firebaseUser: UserRecord;
    try {
      firebaseUser = await getAuth().getUserByEmail(email);
    } catch (error) {
      const code = error && typeof error === "object" && "code" in error ? String(error.code) : "";
      if (code !== "auth/user-not-found") throw error;
      firebaseUser = await getAuth().createUser({email, emailVerified: false, disabled: false});
    }
    if (firebaseUser.disabled) throw new ApiError(403, "This Firebase account is disabled");
    const customToken = await getAuth().createCustomToken(firebaseUser.uid, {bootstrap: true});
    await client.query(
      "UPDATE auth_bootstrap_tokens SET used_at_utc = NOW(), used_by_uid = $2 WHERE token_hash = $1",
      [tokenHash, firebaseUser.uid]
    );
    await client.query("COMMIT");

    const firebaseConfig = JSON.stringify({
      apiKey: process.env.FIREBASE_API_KEY ?? "",
      authDomain: process.env.FIREBASE_AUTH_DOMAIN ?? "moneymaker-aedf7.firebaseapp.com",
      projectId: process.env.GOOGLE_CLOUD_PROJECT ?? "moneymaker-aedf7",
      appId: process.env.FIREBASE_APP_ID ?? ""
    }).replace(/</g, "\\u003c");
    const serializedToken = JSON.stringify(customToken).replace(/</g, "\\u003c");
    const nonce = crypto.randomBytes(18).toString("base64");
    res.setHeader("Cache-Control", "no-store, max-age=0");
    res.setHeader("Referrer-Policy", "no-referrer");
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("Content-Security-Policy", `default-src 'none'; script-src 'nonce-${nonce}' https://www.gstatic.com; connect-src https://*.googleapis.com; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'`);
    res.status(200).type("html").send(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Signing in to Moneymaker</title><style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0d1014;color:#f1f4f7;font:16px/1.5 system-ui,sans-serif}main{width:min(420px,calc(100% - 40px));padding:22px;border:1px solid #303844;border-radius:6px;background:#15191f}h1{margin:0 0 8px;font-size:21px}p{margin:0;color:#9ca8b5}.bad{color:#df7770}</style></head>
<body><main><h1>Signing in</h1><p id="status">Preparing Brady's secure session...</p></main>
<script type="module" nonce="${nonce}">
import {initializeApp} from "https://www.gstatic.com/firebasejs/10.12.5/firebase-app.js";
import {getAuth, signInWithCustomToken} from "https://www.gstatic.com/firebasejs/10.12.5/firebase-auth.js";
const status = document.getElementById("status");
try {
  const app = initializeApp(${firebaseConfig});
  await signInWithCustomToken(getAuth(app), ${serializedToken});
  status.textContent = "Signed in. Opening Moneymaker...";
  window.location.replace("/");
} catch (error) {
  status.className = "bad";
  status.textContent = error?.message || "Sign-in failed. Ask the owner for a new one-time link.";
}
</script></body></html>`);
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}));

apiApp.use("/api", (req: Request, res: Response, next: NextFunction) => {
  if (!requiresAppCheck(req.path)) {
    next();
    return;
  }
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

apiApp.get("/api/snapshot-object", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  const objectPath = storageObjectPath(req.query.path, ["snapshots/"]);
  let raw: Buffer;
  try {
    [raw] = await getStorage().bucket(storageBucketName()).file(objectPath).download();
  } catch (error) {
    const code = (error as {code?: unknown})?.code;
    if (code === 404 || code === "404") throw new ApiError(404, "Snapshot object not found");
    throw error;
  }
  const content = raw.length >= 2 && raw[0] === 0x1f && raw[1] === 0x8b ? gunzipSync(raw) : raw;
  let snapshot: unknown;
  try {
    snapshot = JSON.parse(content.toString("utf8"));
  } catch (_error) {
    throw new ApiError(502, "Snapshot object is not valid JSON");
  }
  res.setHeader("Cache-Control", "private, max-age=300");
  res.json({ok: true, snapshot});
}));

apiApp.post("/api/feedback", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  let feedback;
  try {
    feedback = normalizeFeedbackInput(req.body);
  } catch (error) {
    throw new ApiError(400, error instanceof Error ? error.message : "Invalid feedback");
  }
  const context = {
    ...feedback.context,
    user_agent: String(req.header("user-agent") ?? "").slice(0, 500)
  };
  const result = await db().query(
    `
    INSERT INTO app_feedback
      (firebase_uid, user_email, category, message, page_path, market, ticker, context_json)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
    RETURNING id, status, created_at_utc
    `,
    [
      user.uid,
      user.email,
      feedback.category,
      feedback.message,
      feedback.pagePath,
      feedback.market,
      feedback.ticker,
      JSON.stringify(context)
    ]
  );
  res.status(201).json({ok: true, feedback: result.rows[0]});
}));

apiApp.get("/api/admin/feedback", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const requestedStatus = String(req.query.status ?? "new").trim().toLowerCase();
  const status = requestedStatus === "all" ? null : (() => {
    try {
      return normalizeFeedbackStatus(requestedStatus);
    } catch (error) {
      throw new ApiError(400, error instanceof Error ? error.message : "Invalid feedback status");
    }
  })();
  const limit = Math.min(Math.max(Number(req.query.limit ?? 100), 1), 500);
  const [items, counts] = await Promise.all([
    db().query(
      `
      SELECT id, user_email, category, message, page_path, market, ticker,
             context_json, status, admin_note, created_at_utc, updated_at_utc
      FROM app_feedback
      WHERE ($1::text IS NULL OR status = $1)
      ORDER BY created_at_utc DESC
      LIMIT $2
      `,
      [status, limit]
    ),
    db().query("SELECT status, COUNT(*)::int AS count FROM app_feedback GROUP BY status")
  ]);
  res.json({ok: true, feedback: items.rows, counts: counts.rows});
}));

apiApp.patch("/api/admin/feedback/:id", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const id = Number(req.params.id);
  if (!Number.isSafeInteger(id) || id <= 0) throw new ApiError(400, "A valid feedback ID is required");
  let status: string;
  try {
    status = normalizeFeedbackStatus(req.body?.status);
  } catch (error) {
    throw new ApiError(400, error instanceof Error ? error.message : "Invalid feedback status");
  }
  const adminNote = String(req.body?.admin_note ?? req.body?.adminNote ?? "").trim().slice(0, 2000) || null;
  const result = await db().query(
    `
    UPDATE app_feedback
    SET status = $1, admin_note = $2, updated_at_utc = NOW()
    WHERE id = $3
    RETURNING id, status, admin_note, updated_at_utc
    `,
    [status, adminNote, id]
  );
  if (!result.rows[0]) throw new ApiError(404, "Feedback item not found");
  res.json({ok: true, feedback: result.rows[0]});
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

apiApp.get("/api/markets/status", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  await reconcileStaleJobsSafe();
  const [asx, us] = await Promise.all([marketFreshness("asx"), marketFreshness("us")]);
  res.json({ok: true, generated_at_utc: new Date().toISOString(), markets: {asx, us}});
}));

apiApp.get("/api/refresh/job", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  const refreshJobId = String(req.query.refresh_job_id ?? "").trim();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(refreshJobId)) {
    throw new ApiError(400, "A valid refresh_job_id is required");
  }

  const [refreshResult, batchResult] = await Promise.all([
    db().query(
      `SELECT id, market, provider, status, stage, total_tickers, completed_tickers,
              failed_tickers, started_at_utc, finished_at_utc, error,
              GREATEST(0, EXTRACT(EPOCH FROM (COALESCE(finished_at_utc, NOW()) - started_at_utc)))::int
                AS elapsed_seconds
       FROM refresh_jobs
       WHERE id = $1`,
      [refreshJobId]
    ),
    db().query(
      `SELECT id, batch_index, status, attempts, started_at_utc, finished_at_utc, error,
              jsonb_array_length(tickers_json)::int AS ticker_count,
              tickers_json ->> 0 AS first_ticker,
              tickers_json ->> (jsonb_array_length(tickers_json) - 1) AS last_ticker,
              result_json #>> '{cloud_run_dispatch,job_id}' AS child_job_id
       FROM refresh_batches
       WHERE refresh_job_id = $1
       ORDER BY batch_index`,
      [refreshJobId]
    )
  ]);
  const refresh = refreshResult.rows[0];
  if (!refresh) throw new ApiError(404, "Refresh job not found");

  const batches = batchResult.rows;
  const completedBatches = batches.filter((batch) => batch.status === "succeeded").length;
  const failedBatches = batches.filter((batch) => batch.status === "failed").length;
  const runningBatches = batches.filter((batch) => batch.status === "running").length;
  const queuedBatches = batches.filter((batch) => batch.status === "queued").length;
  const totalTickers = Number(refresh.total_tickers ?? 0);
  const completedTickers = Number(refresh.completed_tickers ?? 0);
  const status = String(refresh.status ?? "queued");
  const stage = status === "succeeded" ? "Complete" : status === "failed" ? "Failed" : refresh.stage;
  const percent = totalTickers ? Math.min(100, Math.round((completedTickers / totalTickers) * 10000) / 100) : 0;

  res.json({
    ok: true,
    refresh: {
      ...refresh,
      stage,
      running: status === "queued" || status === "running",
      success: status === "succeeded" ? true : status === "failed" ? false : null,
      current: completedTickers,
      total: totalTickers,
      percent,
      detail: `${completedBatches}/${batches.length} batches complete; ${runningBatches} running, ${queuedBatches} queued, ${failedBatches} failed`,
      batch_counts: {
        total: batches.length,
        completed: completedBatches,
        running: runningBatches,
        queued: queuedBatches,
        failed: failedBatches
      }
    },
    batches
  });
}));

apiApp.get("/api/job", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const jobType = String(req.query.type ?? "fetch").trim().toLowerCase();
  const allowed = new Set([
    "fetch", "filter", "import-sqlite", "export-ratings", "rating-outcomes",
    "insight-snapshot-backfill", "insight-run", "fundamentals-refresh"
  ]);
  if (!allowed.has(jobType)) throw new ApiError(400, "Unsupported job type");
  const job = await latestJob(jobType, String(req.query.job_id ?? "") || undefined);
  requireJobVisibility(user, jobType, job);
  res.json({ok: true, job: jobPayload(job)});
}));

apiApp.get("/api/filter/job", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const job = await latestJob("filter", String(req.query.job_id ?? "") || undefined);
  requireJobVisibility(user, "filter", job);
  const afterEventId = Math.max(Number(req.query.after_event_id ?? 0), 0);
  const events = job?.id
    ? await db().query(
      `SELECT id, job_id, stage_code, stage, status, message, current_count,
              total_count, percent, metadata_json, created_at_utc
       FROM job_events
       WHERE job_id = $1 AND id > $2
       ORDER BY id ASC
       LIMIT 200`,
      [job.id, afterEventId]
    )
    : {rows: []};
  const nextEventId = events.rows.length ? Number(events.rows.at(-1)?.id ?? afterEventId) : afterEventId;
  res.json({ok: true, job: jobPayload(job), events: events.rows, next_event_id: nextEventId});
}));

apiApp.get("/api/scans", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  const market = currentMarket(req.query.market);
  const defaultOnly = String(req.query.config ?? "").toLowerCase() === "default";
  const configHash = defaultConfigHash(market);
  const result = await db().query(
    `
    SELECT id, source_id, created_at_utc, provider, query, scanned_count,
           result_count, skipped_no_history, config_json, config_hash,
           market_snapshot_date::text AS market_snapshot_date
    FROM scan_runs
    WHERE market = $1
      AND ($2::boolean = false OR config_hash = $3 OR config_hash IS NULL)
    ORDER BY
      CASE WHEN $2::boolean AND config_hash = $3 THEN 0 ELSE 1 END,
      created_at_utc DESC
    LIMIT 100
    `,
    [market, defaultOnly, configHash]
  );
  res.json({ok: true, scans: result.rows});
}));

apiApp.get("/api/scan-results", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const market = currentMarket(req.query.market);
  const requestedScanId = Number(req.query.scan_id ?? 0);
  const defaultOnly = String(req.query.config ?? "").toLowerCase() === "default";
  const configHash = defaultConfigHash(market);
  const scanResult = requestedScanId
    ? await db().query(
      `SELECT id, source_id, created_at_utc, provider, query, scanned_count, result_count,
               skipped_no_history, config_json, config_hash,
               market_snapshot_date::text AS market_snapshot_date
       FROM scan_runs WHERE id = $1 AND market = $2`,
      [requestedScanId, market]
    )
    : await db().query(
      `SELECT id, source_id, created_at_utc, provider, query, scanned_count, result_count,
               skipped_no_history, config_json, config_hash,
               market_snapshot_date::text AS market_snapshot_date
       FROM scan_runs
       WHERE market = $1
         AND ($2::boolean = false OR config_hash = $3 OR config_hash IS NULL)
       ORDER BY
         CASE WHEN $2::boolean AND config_hash = $3 THEN 0 ELSE 1 END,
         created_at_utc DESC
       LIMIT 1`,
      [market, defaultOnly, configHash]
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
  res.json({ok: true, scan, results: await overlayUserAppraisals(user, market, results)});
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
  await requireAuth(req, db());
  const requestedMarket = strictMarket(req.query.market, true);
  const market = requestedMarket === "all" ? null : requestedMarket;
  const horizon = analysisHorizon(req.query.horizon);
  const result = await db().query(
    `
    WITH latest_events AS (
      SELECT DISTINCT ON (firebase_uid, market, ticker)
        id, firebase_uid, market, ticker, action, label, event_at_utc, signal_date,
        close_price AS signal_price
      FROM rating_events
      WHERE ($1::text IS NULL OR market = $1)
      ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
    ), latest_labels AS (
      SELECT * FROM latest_events WHERE action = 'label' AND label IS NOT NULL
    ), performance AS (
      SELECT
        labelled.*,
        CASE WHEN $2::int = 0 THEN latest.close_price ELSE outcome.price_at_horizon END AS latest_price,
        CASE WHEN $2::int = 0 THEN latest.price_date ELSE NULL END AS latest_date,
        CASE
          WHEN $2::int = 0 AND entry.close_price > 0 AND latest.close_price IS NOT NULL
          THEN ((latest.close_price - entry.close_price) / entry.close_price) * 100
          WHEN $2::int <> 0 THEN outcome.return_percent
          ELSE NULL
        END AS return_percent
      FROM latest_labels labelled
      ${ENTRY_PRICE_LATERAL("labelled")}
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
      WHEN 'needs_confirmation' THEN 2
      WHEN 'maybe' THEN 3
      WHEN 'bad' THEN 4
      ELSE 5
    END
    `,
    [market, horizon]
  );
  res.json({ok: true, market: requestedMarket, horizon_days: horizon, summary: result.rows});
}));

// Entry price for a labelled pick: the last close from price_history in the
// session that had completed when the label was given (same anchor as the
// point-in-time outcomes), so entry and later prices share one price basis.
const ENTRY_PRICE_LATERAL = (alias: string) => `
    LEFT JOIN LATERAL (
      SELECT price_date, close_price
      FROM price_history
      WHERE market = ${alias}.market
        AND ticker = ${alias}.ticker
        AND provider = 'yfinance'
        AND price_date <= appraisal_cutoff_date(${alias}.market, ${alias}.event_at_utc)
        AND close_price > 0
      ORDER BY price_date DESC
      LIMIT 1
    ) entry ON TRUE`;

apiApp.get("/api/analysis/timeseries", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const market = strictMarket(req.query.market);
  const interval = String(req.query.interval ?? "daily").trim().toLowerCase();
  if (interval === "hourly") {
    throw new ApiError(400, "Hourly analysis is unavailable because the database currently stores daily market bars only");
  }
  if (interval !== "daily" && interval !== "weekly") {
    throw new ApiError(400, "Analysis interval must be daily or weekly");
  }
  const range = String(req.query.range ?? "all").trim().toLowerCase();
  const days = analysisRangeDays(range);
  if (days === undefined) throw new ApiError(400, "Analysis range must be 3m, 6m, 1y, 2y, 5y, or all");
  // Range limits how far after each appraisal the curves extend.
  const maxSessions = days === null ? null : Math.round(days * 252 / 365);
  const benchmarkTicker = market === "asx" ? "^AORD" : "SPY";
  const benchmarkName = market === "asx" ? "All Ordinaries" : "S&P 500 (SPY)";

  const result = await db().query(
    `
    WITH latest_events AS (
      SELECT DISTINCT ON (firebase_uid, market, ticker)
        id, firebase_uid, market, ticker, action, label, event_at_utc, signal_date
      FROM rating_events
      WHERE market = $1
        AND firebase_uid = $2
      ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
    ), active_labels AS (
      SELECT * FROM latest_events WHERE action = 'label' AND label IS NOT NULL
    ), entries AS (
      SELECT labelled.id, labelled.label, labelled.market, labelled.ticker,
             entry.price_date AS entry_date, entry.close_price AS entry_price,
             benchmark_entry.close_price AS benchmark_entry_price
      FROM active_labels labelled
      ${ENTRY_PRICE_LATERAL("labelled")}
      LEFT JOIN LATERAL (
        SELECT close_price
        FROM price_history
        WHERE market = labelled.market AND provider = 'yfinance' AND ticker = $3
          AND price_date <= entry.price_date AND close_price > 0
        ORDER BY price_date DESC
        LIMIT 1
      ) benchmark_entry ON TRUE
      WHERE entry.price_date IS NOT NULL
    ), paths AS (
      SELECT entries.id, entries.label,
             ROW_NUMBER() OVER (PARTITION BY entries.id ORDER BY ph.price_date) - 1 AS session,
             ((ph.close_price / entries.entry_price) - 1) * 100 AS return_percent,
             ((benchmark.close_price / entries.benchmark_entry_price) - 1) * 100 AS benchmark_percent
      FROM entries
      JOIN price_history ph
        ON ph.market = entries.market AND ph.provider = 'yfinance' AND ph.ticker = entries.ticker
       AND ph.price_date >= entries.entry_date
       AND ($4::int IS NULL OR ph.price_date <= entries.entry_date + ($4::int * 2))
       AND ph.close_price IS NOT NULL
      LEFT JOIN price_history benchmark
        ON benchmark.market = entries.market AND benchmark.provider = 'yfinance'
       AND benchmark.ticker = $3 AND benchmark.price_date = ph.price_date
    ), sampled AS (
      SELECT id, label,
             CASE WHEN $5 = 'weekly' THEN session / 5 ELSE session END AS step,
             return_percent, return_percent - benchmark_percent AS excess_percent
      FROM paths
      WHERE ($4::int IS NULL OR session <= $4::int)
        AND ($5 <> 'weekly' OR session % 5 = 0)
    ), grouped AS (
      SELECT step, label, return_percent, excess_percent FROM sampled
      UNION ALL
      SELECT step, 'all_picks', return_percent, excess_percent FROM sampled
    )
    SELECT step, label,
           ROUND(AVG(excess_percent)::numeric, 4) AS excess_percent,
           ROUND(AVG(return_percent)::numeric, 4) AS return_percent,
           COUNT(excess_percent)::int AS excess_count,
           COUNT(*)::int AS sample_count
    FROM grouped
    GROUP BY step, label
    ORDER BY label, step
    `,
    [market, user.uid, benchmarkTicker, maxSessions, interval]
  );
  const coverageResult = await db().query(
    `
    WITH latest_events AS (
      SELECT DISTINCT ON (firebase_uid, market, ticker)
        id, market, ticker, action, label, event_at_utc, signal_date
      FROM rating_events
      WHERE market = $1 AND firebase_uid = $2
      ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
    )
    SELECT COUNT(*)::int AS active_pick_count,
           COUNT(DISTINCT ticker)::int AS ticker_count,
           MIN(signal_date)::text AS earliest_signal_date,
           MAX(signal_date)::text AS latest_signal_date,
           COALESCE(jsonb_object_agg(label, label_count) FILTER (WHERE label IS NOT NULL), '{}'::jsonb) AS label_counts
    FROM (
      SELECT *, COUNT(*) OVER (PARTITION BY label) AS label_count
      FROM latest_events WHERE action = 'label' AND label IS NOT NULL
    ) active
    `,
    [market, user.uid]
  );

  const benchmarkAvailable = result.rows.some((row) => Number(row.excess_count) > 0);
  const series = new Map<string, Array<Record<string, unknown>>>();
  for (const row of result.rows) {
    const label = String(row.label);
    const points = series.get(label) ?? [];
    points.push({
      step: Number(row.step),
      excess_percent: numberOrNull(row.excess_percent),
      return_percent: numberOrNull(row.return_percent),
      sample_count: Number(benchmarkAvailable ? row.excess_count : row.sample_count)
    });
    series.set(label, points);
  }
  const categoryOrder = ["winner", "needs_confirmation", "maybe", "bad", "all_picks"];
  res.json({
    ok: true,
    market,
    interval,
    range,
    view: "event_time",
    step_unit: interval === "weekly" ? "week" : "session",
    measure: benchmarkAvailable ? "excess" : "return",
    methodology: "Each pick starts at 0% on the close of the session completed when it was labelled. Lines show the average return relative to the benchmark over the same days, by trading days since labelling.",
    coverage: coverageResult.rows[0] ?? {},
    series: categoryOrder.map((label) => ({label, points: series.get(label) ?? []})),
    benchmark: {
      ticker: benchmarkTicker,
      name: benchmarkName,
      available: benchmarkAvailable,
      unavailable_reason: benchmarkAvailable
        ? null
        : `${benchmarkName} price history has not been loaded into the ${market.toUpperCase()} database yet, so raw returns are shown`
    }
  });
}));

apiApp.get("/api/analysis/picks", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const requestedMarket = strictMarket(req.query.market, true);
  const market = requestedMarket === "all" ? null : requestedMarket;
  const horizon = analysisHorizon(req.query.horizon);
  const requestedLabel = String(req.query.label ?? "").trim().toLowerCase().replace(/\s+/g, "_");
  if (requestedLabel && !VALID_LABELS.has(requestedLabel)) throw new ApiError(400, "Invalid rating label");
  const scope = String(req.query.scope ?? "mine").trim().toLowerCase();
  if (scope !== "mine" && scope !== "team") throw new ApiError(400, "Analysis scope must be mine or team");
  if (scope === "team" && requestedLabel !== "needs_confirmation") {
    throw new ApiError(400, "Team scope is only available for Needs Confirmation");
  }
  const ownerUid = scope === "team" ? null : user.uid;
  const limit = Math.min(Math.max(Number(req.query.limit ?? 250), 1), 5000);
  const result = await db().query(
    `
    WITH latest_events AS (
      SELECT DISTINCT ON (firebase_uid, market, ticker)
        id, firebase_uid, market, ticker, action, label, event_at_utc, signal_date,
        close_price AS signal_price, scan_id, source_id, user_email
      FROM rating_events
      WHERE ($5::text IS NULL OR firebase_uid = $5)
        AND ($1::text IS NULL OR market = $1)
      ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
    ), latest_labels AS (
      SELECT * FROM latest_events
      WHERE action = 'label'
        AND label IS NOT NULL
        AND ($2::text IS NULL OR label = $2)
    )
    SELECT
      labelled.id AS rating_event_id,
      labelled.firebase_uid, COALESCE(labelled.user_email, owner.email) AS owner_email,
      labelled.market, labelled.ticker, labelled.label, labelled.scan_id, labelled.source_id,
      labelled.event_at_utc, labelled.signal_date, labelled.signal_price,
      entry.price_date AS entry_date, entry.close_price AS entry_price,
      CASE WHEN $3::int = 0 THEN latest.close_price ELSE outcome.price_at_horizon END AS latest_price,
      CASE WHEN $3::int = 0 THEN latest.price_date ELSE NULL END AS latest_date,
      outcome.measured_at_utc,
      CASE
        WHEN $3::int = 0 AND entry.close_price > 0 AND latest.close_price IS NOT NULL
        THEN ROUND((((latest.close_price - entry.close_price) / entry.close_price) * 100)::numeric, 2)
        WHEN $3::int <> 0 THEN ROUND(outcome.return_percent::numeric, 2)
        ELSE NULL
      END AS return_percent
    FROM latest_labels labelled
    ${ENTRY_PRICE_LATERAL("labelled")}
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
    LEFT JOIN user_profiles owner ON owner.firebase_uid = labelled.firebase_uid
    ORDER BY return_percent ASC NULLS LAST, labelled.event_at_utc DESC
    LIMIT $4
    `,
    [market, requestedLabel || null, horizon, limit, ownerUid]
  );
  res.json({ok: true, market: requestedMarket, scope, horizon_days: horizon, picks: result.rows});
}));

apiApp.post("/api/analysis/pick", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const market = strictMarket(req.body.market) as "asx" | "us";
  const ticker = normalizeAnalysisTicker(market, req.body.ticker);
  const label = String(req.body.label ?? "").trim().toLowerCase().replace(/\s+/g, "_");
  if (!VALID_LABELS.has(label)) throw new ApiError(400, "Invalid rating label");
  const requestedTargetUid = String(req.body.target_uid ?? "").trim();
  if (requestedTargetUid.length > 128) throw new ApiError(400, "Invalid appraisal owner");
  const targetUid = requestedTargetUid || user.uid;
  if (targetUid !== user.uid && label !== "winner" && label !== "bad") {
    throw new ApiError(403, "Shared review items can only be confirmed as Winner or Bad");
  }

  const client = await db().connect();
  try {
    await client.query("BEGIN");
    const latestEventResult = await client.query(
        `
        SELECT id, action, label, event_at_utc, signal_date, close_price, scan_id, source_id,
               cache_file, provider, query, note, rank, market_cap, avg_volume, volume_ratio,
               sector, industry, result_json, user_email
        FROM rating_events
        WHERE firebase_uid = $1 AND market = $2 AND ticker = $3
        ORDER BY event_at_utc DESC, id DESC
        LIMIT 1
        `,
        [targetUid, market, ticker]
      );
    const latestPriceResult = await client.query(
        `
        SELECT price_date, close_price
        FROM price_history
        WHERE market = $1 AND provider = 'yfinance' AND ticker = $2 AND close_price IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 1
        `,
        [market, ticker]
      );
    const metricResult = await client.query(
        `
        SELECT market_cap, avg_volume_52 AS avg_volume, volume_ratio_52 AS volume_ratio,
               sector, industry
        FROM weekly_metrics
        WHERE market = $1 AND provider = 'yfinance' AND ticker = $2
        ORDER BY week_date DESC
        LIMIT 1
        `,
        [market, ticker]
      );
    const latestPrice = latestPriceResult.rows[0];
    if (!latestPrice) throw new ApiError(404, `${ticker} has no cached ${market.toUpperCase()} price data`);

    const previous = latestEventResult.rows[0];
    const previousIsActive = previous?.action === "label" && previous?.label;
    if (targetUid !== user.uid && (!previousIsActive || previous.label !== "needs_confirmation")) {
      throw new ApiError(409, `${ticker} is no longer waiting for confirmation from that user`);
    }
    const metric = metricResult.rows[0] ?? {};
    const signalDate = previousIsActive && previous.signal_date ? previous.signal_date : latestPrice.price_date;
    const signalPrice = previousIsActive && numberOrNull(previous.close_price) !== null
      ? numberOrNull(previous.close_price)
      : numberOrNull(latestPrice.close_price);
    if (signalPrice === null || signalPrice <= 0) throw new ApiError(409, `${ticker} has no usable cached close price`);

    const scanId = previousIsActive ? numberOrNull(previous.scan_id) : null;
    const sourceId = previousIsActive ? numberOrNull(previous.source_id) : null;
    const previousResult = previousIsActive && previous.result_json && typeof previous.result_json === "object"
      ? previous.result_json
      : {manual_pick: true, added_from: "analysis"};
    const eventResultJson = {
      ...previousResult,
      shared_resolution: targetUid !== user.uid,
      resolved_by_uid: user.uid,
      resolved_by_email: user.email
    };

    if (scanId !== null && sourceId !== null) {
      await client.query(
        `
        INSERT INTO user_picks
          (firebase_uid, scan_id, source_id, market, ticker, label, created_at_utc, updated_at_utc)
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        ON CONFLICT (firebase_uid, scan_id, source_id, ticker) DO UPDATE SET
          label = EXCLUDED.label,
          updated_at_utc = NOW()
        `,
        [targetUid, scanId, sourceId, market, ticker, label]
      );
      await client.query(
        `
        INSERT INTO user_appraisals
          (firebase_uid, scan_id, source_id, market, ticker, label, note, appraised_at_utc)
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ON CONFLICT (firebase_uid, scan_id, source_id, ticker) DO UPDATE SET
          label = EXCLUDED.label,
          note = COALESCE(EXCLUDED.note, user_appraisals.note),
          appraised_at_utc = NOW()
        `,
        [targetUid, scanId, sourceId, market, ticker, label, previous.note ?? null]
      );
    }

    const inserted = await client.query(
      `
      INSERT INTO rating_events
        (event_at_utc, action, rated_by, market, cache_file, scan_id, provider, query,
         ticker, label, note, rank, signal_date, close_price, market_cap, avg_volume,
         volume_ratio, sector, industry, result_json, yahoo_url, source_id,
         firebase_uid, user_email)
      VALUES
        (clock_timestamp(), 'label', $1, $2, $3, $4, $5, $6,
         $7, $8, $9, $10, $11, $12, $13, $14,
         $15, $16, $17, $18::jsonb, $19, $20, $21, $22)
      RETURNING id, event_at_utc
      `,
      [
        user.email ?? user.display_name ?? "anonymous",
        market,
        previous?.cache_file ?? MARKET_DEFAULTS[market].cache_file,
        scanId,
        previous?.provider ?? "yfinance",
        previous?.query ?? null,
        ticker,
        label,
        previous?.note ?? null,
        previous?.rank ?? null,
        signalDate,
        signalPrice,
        previous?.market_cap ?? metric.market_cap ?? null,
        previous?.avg_volume ?? metric.avg_volume ?? null,
        previous?.volume_ratio ?? metric.volume_ratio ?? null,
        previous?.sector ?? metric.sector ?? null,
        previous?.industry ?? metric.industry ?? null,
        JSON.stringify(eventResultJson),
        yahooUrl(ticker),
        sourceId,
        targetUid,
        previous?.user_email ?? (targetUid === user.uid ? user.email : null)
      ]
    );
    const ratingEventId = Number(inserted.rows[0].id);
    const snapshotId = await insertSnapshotStub(client, ratingEventId);
    await client.query("COMMIT");
    const snapshotJob = await queueSnapshotBuild(ratingEventId, market, user);
    res.json({
      ok: true,
      market,
      ticker,
      label,
      owner_uid: targetUid,
      signal_date: dateOnly(signalDate),
      signal_price: signalPrice,
      event: inserted.rows[0],
      feature_snapshot: {id: snapshotId, status: "queued", job: snapshotJob},
      user
    });
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}));

apiApp.get("/api/analysis/insights/overview", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const requestedMarket = strictMarket(req.query.market, true);
  const market = requestedMarket === "all" ? null : requestedMarket;
  const horizon = insightHorizon(req.query.horizon_days);
  const scope = String(req.query.scope ?? "mine").trim().toLowerCase();
  if (scope !== "mine" && scope !== "team") throw new ApiError(400, "Insights scope must be mine or team");
  if (scope === "team" && user.role !== "admin") throw new ApiError(403, "admin access required for team insights");
  const requestedOwnerUid = String(req.query.owner_uid ?? "").trim();
  if (requestedOwnerUid && user.role !== "admin") throw new ApiError(403, "admin access required for another owner");
  const ownerUid = requestedOwnerUid || (scope === "mine" ? user.uid : null);
  const timing = String(req.query.timing ?? "decision").trim().toLowerCase();
  if (timing !== "decision" && timing !== "origin") throw new ApiError(400, "Insights timing must be decision or origin");

  const coverageResult = await db().query(
    `
    WITH winner_events AS (
      SELECT event.id, event.firebase_uid, event.market, event.ticker, event.event_at_utc,
             decision_snapshot.id AS decision_snapshot_id,
             decision_snapshot.origin_event_id
      FROM rating_events event
      LEFT JOIN LATERAL (
        SELECT snapshot.id, snapshot.origin_event_id
        FROM pick_feature_snapshots snapshot
        WHERE snapshot.rating_event_id = event.id
          AND snapshot.feature_version = $5
      ) decision_snapshot ON TRUE
      WHERE event.action = 'label' AND event.label = 'winner'
        AND ($1::text IS NULL OR event.market = $1)
        AND ($2::text IS NULL OR event.firebase_uid = $2)
    ), selected AS (
      SELECT winner.*,
             snapshot.id AS snapshot_id, snapshot.snapshot_status,
             snapshot.rating_event_id AS snapshot_event_id,
             snapshot.fundamental_json, snapshot.feature_as_of_date
      FROM winner_events winner
      LEFT JOIN LATERAL (
        SELECT candidate.*
        FROM pick_feature_snapshots candidate
        WHERE candidate.rating_event_id = CASE
          WHEN $3::text = 'origin' THEN winner.origin_event_id
          ELSE winner.id
        END
          AND candidate.feature_version = $5
      ) snapshot ON TRUE
    )
    SELECT
      COUNT(*)::int AS total_events,
      -- Matches the analysis worker: complete snapshot, current outcome
      -- definition, and one stock per anchor week.
      COUNT(DISTINCT (market, ticker, date_trunc('week', feature_as_of_date))) FILTER (
        WHERE outcome.rating_event_id IS NOT NULL AND snapshot_status = 'complete'
      )::int AS mature_events,
      COUNT(outcome.rating_event_id)::int AS mature_appraisals,
      COUNT(snapshot_id) FILTER (WHERE snapshot_status = 'complete')::int AS complete_snapshots,
      COUNT(snapshot_id) FILTER (WHERE snapshot_status = 'partial')::int AS partial_snapshots,
      COUNT(*) FILTER (WHERE snapshot_id IS NULL)::int AS missing_snapshots,
      ROUND(
        100.0 * COUNT(*) FILTER (WHERE fundamental_json <> '{}'::jsonb)
        / NULLIF(COUNT(*), 0), 1
      ) AS fundamental_coverage_percent,
      MIN(event_at_utc) AS earliest_appraisal,
      MAX(event_at_utc) AS latest_appraisal,
      MIN(feature_as_of_date) AS earliest_snapshot_date,
      MAX(feature_as_of_date) AS latest_snapshot_date
    FROM selected
    LEFT JOIN rating_outcomes outcome
      ON outcome.rating_event_id = selected.snapshot_event_id AND outcome.horizon_days = $4
     AND outcome.outcome_version = $6
     AND outcome.benchmark_excess_return_percent IS NOT NULL
    `,
    [market, ownerUid, timing, horizon, INSIGHT_FEATURE_VERSION, INSIGHT_OUTCOME_VERSION]
  );
  const coverage = coverageResult.rows[0] ?? {};
  const matureEvents = Number(coverage.mature_events ?? 0);
  const latestRun = await db().query(
    `
    SELECT id, status, stage, created_at_utc, finished_at_utc,
           cohort_json, validation_json, performance_json
    FROM insight_runs
    WHERE market = $1 AND horizon_days = $2
      AND requested_by_uid = $3
    ORDER BY created_at_utc DESC LIMIT 1
    `,
    [requestedMarket, horizon, user.uid]
  );
  const minimumSampleMessage = matureEvents < 20
    ? `${20 - matureEvents} more mature winner appraisals are needed before pattern comparisons are shown.`
    : matureEvents < 50
      ? "Descriptive feature comparisons are available; predictive models remain disabled below 50 mature events."
      : matureEvents < 200
        ? "Regularized and shallow-tree models are allowed; boosted models remain disabled below 200 mature events."
        : "All validated analysis tiers are available.";
  res.json({
    ok: true,
    market: requestedMarket,
    scope,
    owner_uid: ownerUid,
    timing,
    horizon_days: horizon,
    coverage,
    latest_run: latestRun.rows[0] ?? null,
    analysis_level: matureEvents < 20 ? "coverage" : matureEvents < 50 ? "descriptive" : matureEvents < 200 ? "regularized" : "boosted",
    can_run_models: matureEvents >= 50,
    minimum_sample_message: minimumSampleMessage
  });
}));

// Screen observations (hits and near-misses) measured from their signal week,
// with how the in-scope users labelled each stock-week. $1 market (null = all),
// $2 owner uid (null = team), $3 horizon, $4 feature version, $5 outcome version.
const SELECTION_COMPARISON_CTE = `
  WITH latest_ratings AS (
    SELECT DISTINCT ON (event.firebase_uid, lower(event.market), event.ticker, event.signal_date)
           event.id, lower(event.market) AS market, event.ticker, event.signal_date,
           event.action, event.label
    FROM rating_events event
    WHERE event.signal_date IS NOT NULL
      AND ($1::text IS NULL OR lower(event.market) = $1)
      AND ($2::text IS NULL OR event.firebase_uid = $2)
    ORDER BY event.firebase_uid, lower(event.market), event.ticker, event.signal_date,
             event.event_at_utc DESC, event.id DESC
  ), current_labels AS (
    SELECT * FROM latest_ratings WHERE action = 'label' AND label IS NOT NULL
  ), measured AS (
    SELECT observation.id, observation.market, observation.ticker, observation.signal_date,
           outcome.benchmark_excess_return_percent AS excess,
           outcome.maximum_drawdown_percent AS drawdown,
           outcome.target_hit,
           hit.hit_count,
           hit.volume_ratio AS hit_volume_ratio,
           miss.failed_rule AS near_miss_rule,
           miss.volume_ratio AS near_miss_volume_ratio
    FROM screen_observations observation
    JOIN screen_observation_outcomes outcome
      ON outcome.observation_id = observation.id
     AND outcome.horizon_days = $3
     AND outcome.outcome_version = $5
     AND outcome.benchmark_excess_return_percent IS NOT NULL
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS hit_count, MAX(result.volume_ratio) AS volume_ratio
      FROM scan_results result
      JOIN scan_runs run ON run.id = result.scan_id
      WHERE lower(run.market) = observation.market AND run.provider = observation.provider
        AND result.ticker = observation.ticker AND result.signal_date = observation.signal_date
    ) hit ON TRUE
    LEFT JOIN LATERAL (
      SELECT near.failed_rule, near.volume_ratio
      FROM scan_near_misses near
      JOIN scan_runs run ON run.id = near.scan_id
      WHERE lower(run.market) = observation.market AND run.provider = observation.provider
        AND near.ticker = observation.ticker AND near.signal_date = observation.signal_date
      ORDER BY near.id DESC LIMIT 1
    ) miss ON TRUE
    WHERE observation.feature_version = $4
      AND ($1::text IS NULL OR observation.market = $1)
  )
`;

apiApp.get("/api/analysis/selection-comparison", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const requestedMarket = strictMarket(req.query.market, true);
  const market = requestedMarket === "all" ? null : requestedMarket;
  const horizon = insightHorizon(req.query.horizon_days);
  const scope = String(req.query.scope ?? "mine").trim().toLowerCase();
  if (scope !== "mine" && scope !== "team") throw new ApiError(400, "Comparison scope must be mine or team");
  if (scope === "team" && user.role !== "admin") throw new ApiError(403, "admin access required for team comparison");
  const ownerUid = scope === "mine" ? user.uid : null;
  const params = [market, ownerUid, horizon, INSIGHT_FEATURE_VERSION, INSIGHT_OUTCOME_VERSION];
  const summaryColumns = `
      COUNT(DISTINCT id)::int AS observation_count,
      ROUND(AVG(excess)::numeric, 2) AS average_excess_percent,
      ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY excess)::numeric, 2) AS median_excess_percent,
      ROUND((100.0 * AVG(CASE WHEN excess > 0 THEN 1 ELSE 0 END))::numeric, 1) AS beat_benchmark_percent,
      ROUND((100.0 * AVG(CASE WHEN target_hit THEN 1 ELSE 0 END))::numeric, 1) AS target_hit_percent,
      ROUND(AVG(drawdown)::numeric, 2) AS average_drawdown_percent`;
  const [groupsResult, volumeResult] = await Promise.all([
    db().query(
      `
      ${SELECTION_COMPARISON_CTE}, grouped AS (
        SELECT 'all_hits'::text AS group_key, measured.*, NULL::double precision AS rating_excess
        FROM measured WHERE hit_count > 0
        UNION ALL
        SELECT 'unrated_hits', measured.*, NULL
        FROM measured
        WHERE hit_count > 0
          AND NOT EXISTS (
            SELECT 1 FROM current_labels label
            WHERE label.market = measured.market AND label.ticker = measured.ticker
              AND label.signal_date = measured.signal_date
          )
        UNION ALL
        SELECT 'label:' || label.label, measured.*, rating_outcome.benchmark_excess_return_percent
        FROM measured
        JOIN current_labels label
          ON label.market = measured.market AND label.ticker = measured.ticker
         AND label.signal_date = measured.signal_date
        LEFT JOIN rating_outcomes rating_outcome
          ON rating_outcome.rating_event_id = label.id
         AND rating_outcome.horizon_days = $3
         AND rating_outcome.outcome_version = $5
        UNION ALL
        SELECT 'near_miss:' || near_miss_rule, measured.*, NULL
        FROM measured WHERE hit_count = 0 AND near_miss_rule IS NOT NULL
      )
      SELECT group_key, ${summaryColumns},
             COUNT(rating_excess)::int AS rating_anchored_count,
             ROUND(AVG(rating_excess)::numeric, 2) AS rating_anchored_average_excess_percent
      FROM grouped
      GROUP BY group_key
      ORDER BY group_key
      `,
      params
    ),
    db().query(
      `
      ${SELECTION_COMPARISON_CTE}, volumes AS (
        SELECT measured.*,
               COALESCE(hit_volume_ratio, near_miss_volume_ratio) AS volume_ratio
        FROM measured
        WHERE hit_count > 0 OR near_miss_rule = 'volume'
      ), bucketed AS (
        SELECT volumes.*,
               CASE
                 WHEN volume_ratio < 1.5 THEN 1 WHEN volume_ratio < 2 THEN 2
                 WHEN volume_ratio < 3 THEN 3 WHEN volume_ratio < 5 THEN 4
                 WHEN volume_ratio < 10 THEN 5 ELSE 6
               END AS bucket_order
        FROM volumes
        WHERE volume_ratio IS NOT NULL
      )
      SELECT bucket_order,
             (ARRAY['Under 1.5x', '1.5-2x', '2-3x', '3-5x', '5-10x', '10x+'])[bucket_order] AS bucket,
             COUNT(DISTINCT id) FILTER (WHERE hit_count > 0)::int AS hit_count,
             COUNT(DISTINCT id) FILTER (WHERE hit_count = 0)::int AS near_miss_count,
             ${summaryColumns}
      FROM bucketed
      GROUP BY bucket_order
      ORDER BY bucket_order
      `,
      params
    )
  ]);
  res.json({
    ok: true,
    market: requestedMarket,
    scope,
    horizon_days: horizon,
    methodology: "Every screen hit and near-miss is measured from its signal-week close against the market benchmark, so labelled, unlabelled and near-miss groups share one starting point. Rating-anchored returns are measured from when the label was given. Team scope counts a stock-week once per label it received.",
    groups: groupsResult.rows,
    volume_buckets: volumeResult.rows
  });
}));

apiApp.get("/api/analysis/insights", asyncRoute(async (req, res) => {
  await requireAuth(req, db());
  const requestedMarket = strictMarket(req.query.market, true);
  const market = requestedMarket === "all" ? null : requestedMarket;
  const horizon = analysisHorizon(req.query.horizon);
  const underperformerLimit = Math.min(Math.max(Number(req.query.underperformer_limit ?? 25), 1), 100);
  const patternLimit = Math.min(Math.max(Number(req.query.pattern_limit ?? 40), 1), 100);

  const commonCte = `
    WITH latest_events AS (
      SELECT DISTINCT ON (firebase_uid, market, ticker)
        id, firebase_uid, market, ticker, action, label, event_at_utc, signal_date,
        close_price AS signal_price, market_cap, avg_volume, volume_ratio, sector, industry
      FROM rating_events
      WHERE ($1::text IS NULL OR market = $1)
      ORDER BY firebase_uid, market, ticker, event_at_utc DESC, id DESC
    ), latest_labels AS (
      SELECT * FROM latest_events WHERE action = 'label' AND label IS NOT NULL
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
    SELECT market, ticker, label, event_at_utc, signal_date,
           signal_price, latest_price, latest_date, measured_at_utc,
           ROUND(return_percent::numeric, 2) AS return_percent,
           sector, industry, market_cap, avg_volume, volume_ratio
    FROM performance
    WHERE label = 'winner'
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
      WHERE label = 'winner'
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
      WHERE label = 'winner'
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
      WHERE label = 'winner'
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

apiApp.post("/api/analysis/insights/run", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const body = req.body ?? {};
  const market = strictMarket(body.market, true);
  const scope = String(body.scope ?? "mine").trim().toLowerCase();
  if (scope !== "mine" && scope !== "team") throw new ApiError(400, "Insights scope must be mine or team");
  if (scope === "team" && user.role !== "admin") throw new ApiError(403, "admin access required for team insights");
  const requestedOwnerUid = String(body.owner_uid ?? "").trim();
  if (requestedOwnerUid && user.role !== "admin") throw new ApiError(403, "admin access required for another owner");
  const timing = String(body.timing ?? "decision").trim().toLowerCase();
  if (timing !== "decision" && timing !== "origin") throw new ApiError(400, "Insights timing must be decision or origin");
  const payload = {
    market,
    scope,
    owner_uid: requestedOwnerUid || (scope === "mine" ? user.uid : null),
    horizon_days: insightHorizon(body.horizon_days),
    timing,
    feature_version: INSIGHT_FEATURE_VERSION,
    outcome_version: INSIGHT_OUTCOME_VERSION,
    target_percent: 15,
    stop_percent: -12,
    requested_by_uid: user.uid,
    requested_by_email: user.email
  };
  res.json(await createJob("insight-run", payload));
}));

apiApp.get("/api/analysis/insights/runs/:runId", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const runId = String(req.params.runId ?? "").trim();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(runId)) {
    throw new ApiError(400, "A valid Insights run ID is required");
  }
  const runResult = await db().query("SELECT * FROM insight_runs WHERE id = $1", [runId]);
  const run = runResult.rows[0];
  if (!run) {
    const job = await latestJob("insight-run", runId);
    if (!job) throw new ApiError(404, "Insights run not found");
    requireJobVisibility(user, "insight-run", job);
    res.json({ok: true, run: null, job: jobPayload(job), findings: []});
    return;
  }
  if (user.role !== "admin" && String(run.requested_by_uid ?? "") !== user.uid) {
    throw new ApiError(403, "This Insights run belongs to another user");
  }
  const limit = Math.min(Math.max(Number(req.query.limit ?? 100), 1), 500);
  const offset = Math.max(Number(req.query.offset ?? 0), 0);
  const findings = await db().query(
    `SELECT * FROM insight_findings WHERE run_id = $1
     ORDER BY adjusted_p_value ASC NULLS LAST, support_count DESC
     LIMIT $2 OFFSET $3`,
    [runId, limit, offset]
  );
  res.json({ok: true, run, job: jobPayload(await latestJob("insight-run", runId)), findings: findings.rows});
}));

apiApp.get("/api/analysis/insights/rules", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const ownerUid = String(req.query.owner_uid ?? "").trim() || user.uid;
  if (ownerUid !== user.uid && user.role !== "admin") throw new ApiError(403, "This rule collection belongs to another user");
  const result = await db().query(
    `SELECT * FROM insight_rule_sets WHERE owner_uid = $1 ORDER BY created_at_utc DESC, version DESC`,
    [ownerUid]
  );
  res.json({ok: true, rules: result.rows});
}));

apiApp.post("/api/analysis/insights/rules", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const name = String(req.body?.name ?? "").trim();
  if (!name || name.length > 120) throw new ApiError(400, "Rule name is required and must be at most 120 characters");
  const condition = normalizeRuleCondition(req.body?.condition);
  const findingIds = Array.isArray(req.body?.source_finding_ids)
    ? req.body.source_finding_ids.map((value: unknown) => String(value).slice(0, 100)).slice(0, 50)
    : [];
  const id = crypto.randomUUID();
  const inserted = await db().query(
    `
    INSERT INTO insight_rule_sets (id, owner_uid, name, version, condition_json, source_finding_ids_json, status)
    SELECT $1, $2, $3, COALESCE(MAX(version), 0) + 1, $4::jsonb, $5::jsonb, 'draft'
    FROM insight_rule_sets WHERE owner_uid = $2 AND name = $3
    RETURNING *
    `,
    [id, user.uid, name, JSON.stringify(condition), JSON.stringify(findingIds)]
  );
  res.status(201).json({ok: true, rule: inserted.rows[0]});
}));

apiApp.patch("/api/analysis/insights/rules/:ruleId", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const ruleId = String(req.params.ruleId ?? "").trim();
  const existing = await db().query("SELECT * FROM insight_rule_sets WHERE id = $1", [ruleId]);
  const rule = existing.rows[0];
  if (!rule) throw new ApiError(404, "Insight rule not found");
  if (String(rule.owner_uid) !== user.uid && user.role !== "admin") throw new ApiError(403, "This rule belongs to another user");
  const requestedStatus = String(req.body?.status ?? rule.status).trim().toLowerCase();
  if (!["draft", "shadow", "approved", "retired"].includes(requestedStatus)) throw new ApiError(400, "Invalid rule status");
  if (requestedStatus === "approved" && user.role !== "admin") throw new ApiError(403, "Only an admin may approve a rule");
  if (rule.status === "approved" && requestedStatus !== "retired" && user.role !== "admin") throw new ApiError(409, "Approved rule versions are immutable");
  const condition = req.body?.condition === undefined ? rule.condition_json : normalizeRuleCondition(req.body.condition);
  const name = String(req.body?.name ?? rule.name).trim();
  const updated = await db().query(
    `UPDATE insight_rule_sets SET name = $1, condition_json = $2::jsonb, status = $3,
       approved_at_utc = CASE WHEN $3 = 'approved' THEN COALESCE(approved_at_utc, now()) ELSE approved_at_utc END
     WHERE id = $4 RETURNING *`,
    [name, JSON.stringify(condition), requestedStatus, ruleId]
  );
  res.json({ok: true, rule: updated.rows[0]});
}));

const RULE_SHADOW_MINIMUM = 10;

export type RuleEvaluationRow = {
  market: string;
  excess: number;
  drawdown: number;
  featureDate: unknown;
  eventAt: number;
  retained: boolean;
};

// Score a rule against appraisals, ranking best and worst within each market
// because ASX and US picks are measured against different benchmarks.
export function summarizeRuleEvaluation(rows: RuleEvaluationRow[]) {
  if (!rows.length) return null;
  const cutoffs = new Map<string, {low: number; high: number}>();
  for (const market of new Set(rows.map((row) => row.market))) {
    const outcomes = rows.filter((row) => row.market === market).map((row) => row.excess);
    cutoffs.set(market, {low: numericPercentile(outcomes, 0.25), high: numericPercentile(outcomes, 0.75)});
  }
  const isHigh = (row: RuleEvaluationRow) => row.excess >= (cutoffs.get(row.market)?.high ?? Infinity);
  const isLow = (row: RuleEvaluationRow) => row.excess <= (cutoffs.get(row.market)?.low ?? -Infinity);
  const average = (values: number[]) => values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
  const retained = rows.filter((row) => row.retained);
  const removed = rows.filter((row) => !row.retained);
  const baselineExcess = average(rows.map((row) => row.excess));
  const retainedExcess = average(retained.map((row) => row.excess));
  const baselineDrawdown = average(rows.map((row) => row.drawdown).filter(Number.isFinite));
  const retainedDrawdown = average(retained.map((row) => row.drawdown).filter(Number.isFinite));
  return {
    evaluated_from: rows[0].featureDate ?? null,
    evaluated_to: rows[rows.length - 1].featureDate ?? null,
    eligible_count: rows.length,
    retained_count: retained.length,
    removed_count: removed.length,
    high_retained_count: retained.filter(isHigh).length,
    high_removed_count: removed.filter(isHigh).length,
    low_retained_count: retained.filter(isLow).length,
    low_removed_count: removed.filter(isLow).length,
    baseline_hit_rate: rows.filter(isHigh).length / rows.length,
    filtered_hit_rate: retained.length ? retained.filter(isHigh).length / retained.length : null,
    baseline_average_excess: baselineExcess,
    retained_average_excess: retainedExcess,
    benchmark_excess_change: retainedExcess === null || baselineExcess === null ? null : retainedExcess - baselineExcess,
    drawdown_change: retainedDrawdown === null || baselineDrawdown === null ? null : retainedDrawdown - baselineDrawdown,
    cutoffs_by_market: Object.fromEntries(cutoffs)
  };
}

apiApp.post("/api/analysis/insights/rules/:ruleId/evaluate", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const ruleId = String(req.params.ruleId ?? "").trim();
  const ruleResult = await db().query("SELECT * FROM insight_rule_sets WHERE id = $1", [ruleId]);
  const rule = ruleResult.rows[0];
  if (!rule) throw new ApiError(404, "Insight rule not found");
  if (String(rule.owner_uid) !== user.uid && user.role !== "admin") throw new ApiError(403, "This rule belongs to another user");
  const condition = normalizeRuleCondition(rule.condition_json);
  const market = strictMarket(req.body?.market, true);
  const marketFilter = market === "all" ? null : market;
  const horizon = insightHorizon(req.body?.horizon_days);
  const rowsResult = await db().query(
    `
    WITH evaluated AS (
      SELECT snapshot.rating_event_id, snapshot.market, snapshot.ticker,
             snapshot.feature_as_of_date, event.event_at_utc,
             outcome.benchmark_excess_return_percent, outcome.maximum_drawdown_percent,
             jsonb_object_agg(definition.feature_name,
               COALESCE(to_jsonb(value.numeric_value), to_jsonb(value.boolean_value), to_jsonb(value.categorical_value))
             ) FILTER (WHERE NOT value.is_missing) AS features,
             -- One row per stock and anchor week, matching the Insights analysis.
             ROW_NUMBER() OVER (
               PARTITION BY snapshot.market, snapshot.ticker, date_trunc('week', snapshot.feature_as_of_date)
               ORDER BY event.event_at_utc, event.id
             ) AS week_rank
      FROM pick_feature_snapshots snapshot
      JOIN rating_events event ON event.id = snapshot.rating_event_id
      JOIN rating_outcomes outcome ON outcome.rating_event_id = event.id AND outcome.horizon_days = $1
       AND outcome.outcome_version = $5
      JOIN pick_feature_values value ON value.snapshot_id = snapshot.id
      JOIN feature_definitions definition ON definition.id = value.feature_definition_id
      WHERE snapshot.feature_version = $4 AND snapshot.snapshot_status = 'complete'
        AND event.action = 'label' AND event.label = 'winner'
        AND event.firebase_uid = $2
        AND ($3::text IS NULL OR snapshot.market = $3)
        AND outcome.benchmark_excess_return_percent IS NOT NULL
      GROUP BY snapshot.rating_event_id, snapshot.market, snapshot.ticker,
               snapshot.feature_as_of_date, event.event_at_utc, event.id,
               outcome.benchmark_excess_return_percent, outcome.maximum_drawdown_percent
    )
    SELECT * FROM evaluated
    WHERE week_rank = 1
    ORDER BY feature_as_of_date, rating_event_id
    LIMIT 25000
    `,
    [horizon, rule.owner_uid, marketFilter, INSIGHT_FEATURE_VERSION, INSIGHT_OUTCOME_VERSION]
  );
  const rows: RuleEvaluationRow[] = rowsResult.rows.map((row) => ({
    market: String(row.market),
    excess: Number(row.benchmark_excess_return_percent),
    drawdown: Number(row.maximum_drawdown_percent),
    featureDate: row.feature_as_of_date,
    eventAt: new Date(row.event_at_utc).getTime(),
    retained: ruleMatches(row.features ?? {}, condition)
  }));
  if (rows.length < 20) throw new ApiError(409, "At least 20 mature winner appraisals are required to evaluate a rule");
  // Appraisals made after the rule was saved could not have shaped it: they
  // are the out-of-sample (shadow) test. Earlier appraisals are in-sample.
  const ruleCreatedAt = new Date(rule.created_at_utc).getTime();
  const historical = summarizeRuleEvaluation(rows.filter((row) => row.eventAt < ruleCreatedAt));
  const shadow = summarizeRuleEvaluation(rows.filter((row) => row.eventAt >= ruleCreatedAt));
  const reported = shadow && shadow.eligible_count >= RULE_SHADOW_MINIMUM ? shadow : historical ?? shadow;
  if (!reported) throw new ApiError(409, "No mature appraisals are available to evaluate this rule");
  const period = reported === shadow ? "shadow" : "historical";
  const metrics = {
    market, horizon_days: horizon, period,
    rule_created_at_utc: new Date(ruleCreatedAt).toISOString(),
    shadow_minimum: RULE_SHADOW_MINIMUM,
    shadow, historical,
    evaluated_at_utc: new Date().toISOString()
  };
  const inserted = await db().query(
    `
    INSERT INTO insight_rule_evaluations (
      rule_set_id, evaluated_from, evaluated_to, eligible_count, retained_count,
      removed_count, high_retained_count, high_removed_count, low_removed_count,
      low_retained_count, baseline_hit_rate, filtered_hit_rate,
      benchmark_excess_change, drawdown_change, metrics_json
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15::jsonb)
    RETURNING *
    `,
    [ruleId, reported.evaluated_from, reported.evaluated_to,
      reported.eligible_count, reported.retained_count, reported.removed_count,
      reported.high_retained_count, reported.high_removed_count, reported.low_removed_count, reported.low_retained_count,
      reported.baseline_hit_rate, reported.filtered_hit_rate,
      reported.benchmark_excess_change, reported.drawdown_change,
      JSON.stringify(metrics)]
  );
  res.json({ok: true, evaluation: inserted.rows[0]});
}));

apiApp.get("/api/analysis/insights/picks/:ratingEventId", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const ratingEventId = Number(req.params.ratingEventId);
  if (!Number.isSafeInteger(ratingEventId) || ratingEventId <= 0) {
    throw new ApiError(400, "A valid rating event ID is required");
  }
  const eventResult = await db().query(
    `
    SELECT event.id, event.firebase_uid, event.user_email, event.market, event.ticker,
           event.label, event.note, event.event_at_utc, event.signal_date,
           event.close_price, event.scan_id, event.source_id,
           snapshot.id AS snapshot_id, snapshot.origin_event_id,
           snapshot.feature_as_of_date::text AS feature_as_of_date,
           snapshot.feature_version, snapshot.snapshot_status,
           snapshot.technical_json, snapshot.fundamental_json,
           snapshot.context_json, snapshot.quality_json, snapshot.error,
           snapshot.completed_at_utc
    FROM rating_events event
    LEFT JOIN LATERAL (
      SELECT *
      FROM pick_feature_snapshots
      WHERE rating_event_id = event.id
      ORDER BY feature_version DESC
      LIMIT 1
    ) snapshot ON TRUE
    WHERE event.id = $1 AND event.action = 'label'
    `,
    [ratingEventId]
  );
  const event = eventResult.rows[0];
  if (!event) throw new ApiError(404, "Appraisal event not found");
  if (user.role !== "admin" && String(event.firebase_uid ?? "") !== user.uid) {
    throw new ApiError(403, "This appraisal belongs to another user");
  }
  const [valueResult, outcomeResult] = await Promise.all([
    db().query(
      `
      SELECT definition.feature_name, definition.feature_version,
             definition.category, definition.value_type, definition.unit,
             definition.description, definition.formula,
             value.numeric_value, value.boolean_value, value.categorical_value,
             value.is_missing, value.missing_reason, value.source_as_of_utc
      FROM pick_feature_values value
      JOIN feature_definitions definition ON definition.id = value.feature_definition_id
      WHERE value.snapshot_id = $1
      ORDER BY definition.category, definition.feature_name
      `,
      [event.snapshot_id ?? null]
    ),
    db().query(
      `SELECT * FROM rating_outcomes WHERE rating_event_id = $1 ORDER BY horizon_days`,
      [ratingEventId]
    )
  ]);
  res.json({
    ok: true,
    event: {
      id: event.id,
      owner_uid: event.firebase_uid,
      owner_email: event.user_email,
      market: event.market,
      ticker: event.ticker,
      label: event.label,
      note: event.note,
      appraisal_at_utc: event.event_at_utc,
      signal_date: event.signal_date,
      signal_price: event.close_price,
      scan_id: event.scan_id,
      source_id: event.source_id
    },
    snapshot: event.snapshot_id ? {
      id: event.snapshot_id,
      origin_event_id: event.origin_event_id,
      feature_as_of_date: event.feature_as_of_date,
      feature_version: event.feature_version,
      status: event.snapshot_status,
      technical: event.technical_json ?? {},
      fundamentals: event.fundamental_json ?? {},
      context: event.context_json ?? {},
      quality: event.quality_json ?? {},
      error: event.error,
      completed_at_utc: event.completed_at_utc,
      values: valueResult.rows
    } : null,
    outcomes: outcomeResult.rows,
    chart: {
      ticker: event.ticker,
      market: event.market,
      end_date: event.feature_as_of_date ?? dateOnly(event.event_at_utc)
    }
  });
}));

apiApp.get("/api/chart", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  const ticker = String(req.query.ticker ?? "").trim().toUpperCase();
  if (!ticker) throw new ApiError(400, "ticker is required");
  const market = currentMarket(req.query.market, ticker);
  const provider = String(req.query.provider ?? "yfinance");
  const interval = String(req.query.interval ?? "daily").toLowerCase();
  if (!["daily", "weekly", "monthly"].includes(interval)) throw new ApiError(400, "Invalid chart interval");
  const range = String(req.query.range ?? "1y").toLowerCase();
  const requestedEndDate = String(req.query.end_date ?? "").trim();
  if (requestedEndDate && !/^\d{4}-\d{2}-\d{2}$/.test(requestedEndDate)) {
    throw new ApiError(400, "end_date must use YYYY-MM-DD");
  }
  if (requestedEndDate) {
    const parsedEndDate = new Date(`${requestedEndDate}T00:00:00.000Z`);
    if (Number.isNaN(parsedEndDate.getTime()) || parsedEndDate.toISOString().slice(0, 10) !== requestedEndDate) {
      throw new ApiError(400, "end_date is not a valid date");
    }
  }
  const endDate = requestedEndDate || null;
  const maPeriods = String(req.query.ma ?? "")
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0 && value <= 10000);
  const [history, companyResult, appraisalRows] = await Promise.all([
    db().query(
      `
      SELECT price_date::text AS date, open_price AS open, high_price AS high,
             low_price AS low, close_price AS close, volume
      FROM price_history
      WHERE market = $1 AND provider = $2 AND ticker = $3
        AND ($4::date IS NULL OR price_date <= $4::date)
      ORDER BY price_date
      `,
      [market, provider, ticker, endDate]
    ),
    db().query("SELECT info_json FROM companies WHERE market = $1 AND ticker = $2", [market, ticker]),
    latestUserAppraisals(user, market, [ticker])
  ]);
  const chartSeries = buildChartSeries(history.rows as PriceRow[], interval, range, maPeriods);
  const rows = chartSeries.rows;
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
    requested_end_date: endDate,
    effective_end_date: candles[candles.length - 1]?.date ?? null,
    company: normalizeCompanyProfile(companyResult.rows[0]?.info_json, ticker),
    appraisal: applyLatestAppraisals([{ticker}], appraisalRows)[0],
    candles,
    moving_averages: chartSeries.movingAverages,
    moving_average_availability: chartSeries.availability,
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

    const inserted = await client.query(
      `
      INSERT INTO rating_events
        (event_at_utc, action, rated_by, market, scan_id, ticker, label,
         note, rank, signal_date, close_price, market_cap, avg_volume,
         volume_ratio, sector, industry, result_json, yahoo_url,
         firebase_uid, user_email)
      VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
              $13, $14, $15, $16::jsonb, $17, $18, $19)
      RETURNING id, event_at_utc
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
    const ratingEventId = Number(inserted.rows[0].id);
    const snapshotId = label ? await insertSnapshotStub(client, ratingEventId) : null;
    await client.query("COMMIT");
    const snapshotJob = label ? await queueSnapshotBuild(ratingEventId, scan.market, user) : null;
    res.json({
      ok: true,
      scan_id: scanId,
      ticker,
      label: label || null,
      note: note || null,
      online: true,
      feature_snapshot: label ? {id: snapshotId, status: "queued", job: snapshotJob} : null,
      user
    });
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
  await ensureDatabaseRunning();
  const payload = manualRefreshPayload(req.body ?? {});
  res.json(await startScheduledMarketRefresh(payload, user));
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
    const firstResult = payload.results[0] as Record<string, unknown> | undefined;
    const market = currentMarket(summary?.market ?? payload.market, firstResult?.ticker);
    payload.results = await overlayUserAppraisals(user, market, payload.results as Array<Record<string, unknown>>);
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
    horizons: Array.isArray(body.horizons) ? body.horizons : [28, 56, 84, 182],
    limit: positiveInt(body.limit, 100000, 1, 500000),
    // An explicit recalculation re-measures outcomes that are already final.
    remeasure: true,
    requested_by_uid: user.uid,
    requested_by_email: user.email
  };
  res.json(await startRatingOutcomesJob(payload));
}));

apiApp.post("/api/admin/publish-snapshot", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const result = await startSnapshotPublishJob({
    requested_by_uid: user.uid,
    requested_by_email: user.email,
    reason: String(req.body.reason ?? "manual").slice(0, 120)
  });
  res.status(202).json(result);
}));

apiApp.get("/api/admin/database-status", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  res.json({ok: true, database: await databaseState()});
}));

apiApp.post("/api/admin/start-database", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  res.status(202).json({ok: true, database: await startDatabase()});
}));

apiApp.post("/api/admin/stop-database", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const active = await db().query("SELECT COUNT(*)::int AS count FROM job_runs WHERE status IN ('queued', 'running')");
  if (Number(active.rows[0]?.count ?? 0) > 0) throw new ApiError(409, "The database cannot be stopped while background jobs are active");
  res.json({ok: true, database: await stopDatabase()});
}));

apiApp.post("/api/admin/insights/backfill-snapshots", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const body = req.body ?? {};
  const market = strictMarket(body.market, true);
  const requestedLabels = Array.isArray(body.labels) ? body.labels : ["winner", "maybe", "bad", "needs_confirmation"];
  const labels = requestedLabels
    .map((value: unknown) => String(value ?? "").trim().toLowerCase().replace(/\s+/g, "_"))
    .filter((value: string) => VALID_LABELS.has(value));
  if (!labels.length) throw new ApiError(400, "At least one valid appraisal label is required");
  const ownerUid = String(body.owner_uid ?? "").trim() || null;
  if (ownerUid && ownerUid.length > 128) throw new ApiError(400, "Invalid appraisal owner");
  const payload = {
    market,
    owner_uid: ownerUid,
    labels: [...new Set(labels)],
    feature_version: INSIGHT_FEATURE_VERSION,
    resume: body.resume !== false,
    limit: positiveInt(body.limit, 100000, 1, 500000),
    requested_by_uid: user.uid,
    requested_by_email: user.email
  };
  res.json(await createJob("insight-snapshot-backfill", payload));
}));

apiApp.post("/api/admin/insights/refresh-fundamentals", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const body = req.body ?? {};
  const requestedTickers = Array.isArray(body.tickers)
    ? body.tickers.map((value: unknown) => String(value ?? "").trim().toUpperCase()).filter(Boolean).slice(0, 10000)
    : [];
  res.json(await startFundamentalsRefreshJob({
    market: "us",
    tickers: requestedTickers,
    limit: positiveInt(body.limit, 6000, 1, 10000),
    force: body.force === true,
    requested_by_uid: user.uid,
    requested_by_email: user.email
  }));
}));

apiApp.use((_req, res) => {
  res.status(404).json({ok: false, error: "Not found"});
});

export {apiApp};

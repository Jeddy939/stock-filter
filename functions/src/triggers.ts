import express, {type Request, type Response} from "express";
import {getFirestore, type DocumentReference, type DocumentSnapshot} from "firebase-admin/firestore";
import {logger} from "firebase-functions";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {ApiError, requireAdmin, requireAnalyst, requireAuth, type UserContext} from "./auth";
import {db} from "./db";
import {TRIGGER_SEED_TEXT} from "./trigger-seed";
import {
  evaluateTrigger,
  fetchYahooBars,
  normalizeTriggerMarket,
  parseTriggerLines,
  triggerConfigKey,
  triggerId,
  TRIGGER_ACTIVE_MONTHS,
  TRIGGER_MIN_BARS,
  validateTriggerConfig,
  yahooSymbol,
  type TriggerConfig,
  type YahooFetchOptions
} from "./trigger-levels";

// Trigger levels live in Firestore, not Cloud SQL, because the database only
// runs during the weekly window and these scans run every day. Firestore is in
// the US (nam5) and Functions run in Sydney, so every round trip is slow:
// reads and writes are batched rather than made one document at a time.
const COLLECTION = "trigger_levels";
const SCAN_CONCURRENCY = 6;
const SCAN_LOCK_MS = 15 * 60_000;
// Firebase Hosting gives up on a function after 60 seconds, so requests made
// from the page scan in short steps and stop starting new stocks after this.
const BATCH_DEADLINE_MS = 30_000;
const BATCH_MAX_IDS = 60;
const INTERACTIVE_FETCH: YahooFetchOptions = {attempts: 2, timeoutMs: 8_000};

export interface TriggerScan {
  config_key: string;
  status: "triggered" | "watching" | "insufficient" | "error";
  price: number | null;
  currency: string | null;
  name: string | null;
  ma_value: number | null;
  ma_period_used: number | null;
  bars_available: number;
  fallback: boolean;
  distance_pct: number | null;
  first_touch_on: string | null;
  last_touch_on: string | null;
  active_until: string | null;
  touching_now: boolean;
  data_through: string | null;
  scanned_at_utc: string;
  error: string | null;
}

export interface TriggerDocument extends TriggerConfig {
  yahoo_symbol: string;
  created_at_utc: string;
  created_by: string | null;
  updated_at_utc: string;
  updated_by: string | null;
  scan: TriggerScan | null;
}

function collection() {
  return getFirestore().collection(COLLECTION);
}

function statusReference() {
  return getFirestore().collection("system").doc("trigger_scan");
}

/** Scans one entry. A fetch failure keeps the last good result for the same trigger. */
export async function scanTrigger(
  config: TriggerConfig,
  previous: TriggerScan | null,
  now = new Date(),
  fetchOptions: YahooFetchOptions = {}
): Promise<TriggerScan> {
  const configKey = triggerConfigKey(config);
  const scannedAt = now.toISOString();
  try {
    const data = await fetchYahooBars(yahooSymbol(config.market, config.ticker), config.timeframe, config.ma_period, fetch, fetchOptions);
    const evaluation = evaluateTrigger(data.bars, config.ma_period, config.timeframe, {now, price: data.price});
    return {...evaluation, config_key: configKey, currency: data.currency, name: data.name, scanned_at_utc: scannedAt, error: null};
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (previous && previous.config_key === configKey && previous.status !== "error") {
      return {...previous, error: message, scanned_at_utc: previous.scanned_at_utc};
    }
    return {
      config_key: configKey,
      status: "error",
      price: null,
      currency: null,
      name: previous?.name ?? null,
      ma_value: null,
      ma_period_used: null,
      bars_available: 0,
      fallback: false,
      distance_pct: null,
      first_touch_on: null,
      last_touch_on: null,
      active_until: null,
      touching_now: false,
      data_through: null,
      scanned_at_utc: scannedAt,
      error: message
    };
  }
}

interface ScanOptions {
  deadlineMs?: number;
  fetch?: YahooFetchOptions;
}

function emptyCounts(total = 0) {
  return {total, triggered: 0, watching: 0, insufficient: 0, error: 0, fetch_errors: 0};
}

/**
 * Scans the given entries with one batched read and one bulk write. With a
 * deadline, entries not started in time are returned in `remaining`.
 */
async function scanReferences(references: DocumentReference[], options: ScanOptions = {}) {
  const startedAt = Date.now();
  const counts = emptyCounts(references.length);
  const errors: string[] = [];
  const scanned = new Map<string, {data: TriggerDocument; scan: TriggerScan}>();
  const remaining: string[] = [];
  if (!references.length) return {counts, errors, scanned, remaining};

  const snapshots: DocumentSnapshot[] = await getFirestore().getAll(...references);
  const writer = getFirestore().bulkWriter();
  // A NOT_FOUND write means the entry was removed while it was being scanned.
  writer.onWriteError((error) => {
    if (error.code === 5) return false;
    if (error.failedAttempts < 3) return true;
    if (errors.length < 25) errors.push(`${error.documentRef.id}: ${error.message}`);
    return false;
  });
  let next = 0;
  async function worker() {
    while (next < snapshots.length) {
      const snapshot = snapshots[next++];
      if (!snapshot.exists) continue;
      if (options.deadlineMs && Date.now() - startedAt > options.deadlineMs) {
        remaining.push(snapshot.id);
        continue;
      }
      const data = snapshot.data() as TriggerDocument;
      const scan = await scanTrigger(data, data.scan ?? null, new Date(), options.fetch);
      writer.update(snapshot.ref, {scan}).catch(() => undefined);
      scanned.set(snapshot.id, {data, scan});
      counts[scan.status] += 1;
      if (scan.error) {
        counts.fetch_errors += 1;
        if (errors.length < 25) errors.push(scan.error);
      }
    }
  }
  await Promise.all(Array.from({length: SCAN_CONCURRENCY}, worker));
  await writer.close();
  return {counts, errors, scanned, remaining};
}

/** Scans the whole watchlist; a second full scan within the lock window is refused. */
export async function runFullTriggerScan(source: string): Promise<Record<string, unknown>> {
  const startedAt = new Date();
  const claimed = await getFirestore().runTransaction(async (transaction) => {
    const status = (await transaction.get(statusReference())).data() ?? {};
    if (status.running && startedAt.getTime() - Number(status.started_at_ms ?? 0) < SCAN_LOCK_MS) return false;
    transaction.set(statusReference(), {
      running: true,
      source,
      started_at_utc: startedAt.toISOString(),
      started_at_ms: startedAt.getTime()
    }, {merge: true});
    return true;
  });
  if (!claimed) return {ok: true, skipped: true, reason: "A trigger scan is already running"};

  try {
    const references = await collection().listDocuments();
    const {counts, errors} = await scanReferences(references);
    const result = scanStatus(source, startedAt, counts, errors);
    await statusReference().set(result, {merge: true});
    return {ok: true, ...result};
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await statusReference().set({running: false, last_error: message, finished_at_utc: new Date().toISOString()}, {merge: true});
    throw error;
  }
}

function scanStatus(source: string, startedAt: Date, counts: ReturnType<typeof emptyCounts>, errors: string[]) {
  const finishedAt = new Date();
  return {
    running: false,
    source,
    started_at_utc: startedAt.toISOString(),
    started_at_ms: startedAt.getTime(),
    finished_at_utc: finishedAt.toISOString(),
    duration_seconds: Math.round((finishedAt.getTime() - startedAt.getTime()) / 1000),
    counts,
    errors,
    last_error: null
  };
}

/** Records a scan the page ran in steps, counted from what is now stored. */
async function recordSteppedScan(source: string, startedAt: Date) {
  const snapshot = await collection().get();
  const counts = emptyCounts(snapshot.size);
  for (const item of snapshot.docs) {
    const data = item.data() as TriggerDocument;
    const scan = data.scan && data.scan.config_key === triggerConfigKey(data) ? data.scan : null;
    if (!scan) continue;
    counts[scan.status] += 1;
    if (scan.error) counts.fetch_errors += 1;
  }
  const status = scanStatus(source, startedAt, counts, []);
  await statusReference().set(status, {merge: true});
  return status;
}

/** Saves entries with one batched read and one bulk write. Scanning is separate. */
async function upsertTriggers(configs: TriggerConfig[], user: UserContext, overwrite: boolean) {
  const now = new Date().toISOString();
  const added: string[] = [];
  const updated: string[] = [];
  const unchanged: string[] = [];
  if (!configs.length) return {added, updated, unchanged, scan_ids: []};
  const references = configs.map((config) => collection().doc(triggerId(config.market, config.ticker)));
  const snapshots = await getFirestore().getAll(...references);
  const writer = getFirestore().bulkWriter();
  const writes: Promise<unknown>[] = [];
  configs.forEach((config, index) => {
    const reference = references[index];
    const existing = snapshots[index].data() as TriggerDocument | undefined;
    if (existing && (!overwrite || triggerConfigKey(existing) === triggerConfigKey(config))) {
      unchanged.push(reference.id);
      return;
    }
    const values = {
      ...config,
      yahoo_symbol: yahooSymbol(config.market, config.ticker),
      updated_at_utc: now,
      updated_by: user.email
    };
    if (existing) {
      writes.push(writer.update(reference, values));
      updated.push(reference.id);
    } else {
      writes.push(writer.set(reference, {...values, created_at_utc: now, created_by: user.email, scan: null}));
      added.push(reference.id);
    }
  });
  await writer.close();
  await Promise.all(writes);
  return {added, updated, unchanged, scan_ids: [...added, ...updated]};
}

function publicItem(id: string, data: TriggerDocument) {
  const scan = data.scan && data.scan.config_key === triggerConfigKey(data) ? data.scan : null;
  return {id, ...data, scan, scan_pending: !scan};
}

async function listTriggers() {
  const [snapshot, status] = await Promise.all([collection().get(), statusReference().get()]);
  return {
    items: snapshot.docs.map((item) => publicItem(item.id, item.data() as TriggerDocument)),
    scan: status.data() ?? null,
    rules: {min_bars: TRIGGER_MIN_BARS, active_months: TRIGGER_ACTIVE_MONTHS}
  };
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

function badRequest(error: unknown): never {
  throw new ApiError(400, error instanceof Error ? error.message : String(error));
}

export const triggerRouter = express.Router();

triggerRouter.get("/api/triggers", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  res.json({ok: true, role: user.role, ...(await listTriggers())});
}));

triggerRouter.post("/api/triggers", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  let config: TriggerConfig;
  try {
    config = validateTriggerConfig(req.body ?? {});
  } catch (error) {
    badRequest(error);
  }
  const result = await upsertTriggers([config], user, true);
  const reference = collection().doc(triggerId(config.market, config.ticker));
  const {scanned} = await scanReferences([reference], {fetch: INTERACTIVE_FETCH});
  const saved = scanned.get(reference.id);
  const item = saved
    ? publicItem(reference.id, {...saved.data, scan: saved.scan})
    : publicItem(reference.id, (await reference.get()).data() as TriggerDocument);
  res.json({ok: true, ...result, item});
}));

triggerRouter.post("/api/triggers/bulk", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const market = normalizeTriggerMarket(req.body?.market) ?? "us";
  const parsed = parseTriggerLines(String(req.body?.text ?? ""), market);
  if (!parsed.entries.length) {
    const first = parsed.errors[0];
    throw new ApiError(400, first ? `No valid lines found. Line ${first.line} (${first.text}): ${first.error}` : "No valid lines found");
  }
  if (parsed.entries.length > 400) throw new ApiError(400, "Add at most 400 stocks at a time");
  const result = await upsertTriggers(parsed.entries, user, req.body?.overwrite !== false);
  res.json({ok: true, ...result, errors: parsed.errors});
}));

triggerRouter.delete("/api/triggers/:id", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const reference = collection().doc(String(req.params.id));
  if (!(await reference.get()).exists) throw new ApiError(404, "Trigger not found");
  await reference.delete();
  res.json({ok: true, id: reference.id});
}));

// One step of a scan run from the page. Returns the entries it scanned and any
// it had no time for; the page sends those again.
triggerRouter.post("/api/triggers/scan-batch", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const ids = Array.isArray(req.body?.ids) ? [...new Set(req.body.ids.map((id: unknown) => String(id)))] as string[] : [];
  if (!ids.length) throw new ApiError(400, "ids is required");
  if (ids.length > BATCH_MAX_IDS) throw new ApiError(400, `Scan at most ${BATCH_MAX_IDS} stocks per request`);
  if (ids.some((id) => !/^(us|asx|crypto)__[A-Z0-9._^=-]{1,20}$/.test(id))) throw new ApiError(400, "Invalid trigger id");
  const {counts, errors, scanned, remaining} = await scanReferences(
    ids.map((id) => collection().doc(id)),
    {deadlineMs: BATCH_DEADLINE_MS, fetch: INTERACTIVE_FETCH}
  );
  const fullScan = req.body?.full_scan;
  let scan = null;
  if (fullScan?.final && !remaining.length) {
    const startedAt = new Date(String(fullScan.started_at_utc ?? ""));
    scan = await recordSteppedScan(`manual:${user.email ?? user.uid}`, Number.isNaN(startedAt.getTime()) ? new Date() : startedAt);
  }
  res.json({
    ok: true,
    items: [...scanned.entries()].map(([id, saved]) => publicItem(id, {...saved.data, scan: saved.scan})),
    remaining,
    counts,
    errors,
    scan
  });
}));

triggerRouter.post("/api/triggers/scan", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAnalyst(user);
  const result = await runFullTriggerScan(`manual:${user.email ?? user.uid}`);
  res.json({ok: true, result, ...(await listTriggers())});
}));

triggerRouter.post("/api/triggers/seed", asyncRoute(async (req, res) => {
  const user = await requireAuth(req, db());
  requireAdmin(user);
  const parsed = parseTriggerLines(TRIGGER_SEED_TEXT, "us");
  // Never replaces a trigger someone has already set.
  const result = await upsertTriggers(parsed.entries, user, false);
  res.json({ok: true, ...result, errors: parsed.errors});
}));

// After the US close (07:30) and the ASX close (16:30), Brisbane time. Each
// bar's low covers every price during the period, so intraday touches between
// scans are still caught.
export const scheduledTriggerScan = onSchedule(
  {
    region: "australia-southeast1",
    schedule: "30 7,16 * * *",
    timeZone: "Australia/Brisbane",
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => {
    const result = await runFullTriggerScan("schedule");
    logger.info("Scheduled trigger scan complete", result);
  }
);

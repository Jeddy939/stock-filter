import express, {type Request, type Response} from "express";
import {getFirestore, type DocumentReference} from "firebase-admin/firestore";
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
  type TriggerConfig
} from "./trigger-levels";

// Trigger levels live in Firestore, not Cloud SQL, because the database only
// runs during the weekly window and these scans run every day.
const COLLECTION = "trigger_levels";
const SCAN_CONCURRENCY = 4;
const SCAN_LOCK_MS = 15 * 60_000;

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
export async function scanTrigger(config: TriggerConfig, previous: TriggerScan | null, now = new Date()): Promise<TriggerScan> {
  const configKey = triggerConfigKey(config);
  const scannedAt = now.toISOString();
  try {
    const data = await fetchYahooBars(yahooSymbol(config.market, config.ticker), config.timeframe, config.ma_period);
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

async function scanReference(reference: DocumentReference): Promise<TriggerScan | null> {
  const snapshot = await reference.get();
  if (!snapshot.exists) return null;
  const data = snapshot.data() as TriggerDocument;
  const scan = await scanTrigger(data, data.scan ?? null);
  try {
    await reference.update({scan});
  } catch (error) {
    // The entry was removed while it was being scanned.
    if ((error as {code?: unknown})?.code !== 5) throw error;
  }
  return scan;
}

async function scanReferences(references: DocumentReference[]) {
  const counts = {total: references.length, triggered: 0, watching: 0, insufficient: 0, error: 0, fetch_errors: 0};
  const errors: string[] = [];
  let next = 0;
  async function worker() {
    while (next < references.length) {
      const reference = references[next++];
      const scan = await scanReference(reference).catch((error) => {
        errors.push(`${reference.id}: ${error instanceof Error ? error.message : String(error)}`);
        return null;
      });
      if (!scan) continue;
      counts[scan.status] += 1;
      if (scan.error) {
        counts.fetch_errors += 1;
        if (errors.length < 25) errors.push(scan.error);
      }
    }
  }
  await Promise.all(Array.from({length: SCAN_CONCURRENCY}, worker));
  return {counts, errors};
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
    const finishedAt = new Date();
    const result = {
      running: false,
      source,
      started_at_utc: startedAt.toISOString(),
      finished_at_utc: finishedAt.toISOString(),
      duration_seconds: Math.round((finishedAt.getTime() - startedAt.getTime()) / 1000),
      counts,
      errors,
      last_error: null
    };
    await statusReference().set(result, {merge: true});
    return {ok: true, ...result};
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await statusReference().set({running: false, last_error: message, finished_at_utc: new Date().toISOString()}, {merge: true});
    throw error;
  }
}

async function upsertTriggers(configs: TriggerConfig[], user: UserContext, overwrite: boolean) {
  const now = new Date().toISOString();
  const added: string[] = [];
  const updated: string[] = [];
  const unchanged: string[] = [];
  const references: DocumentReference[] = [];
  for (const config of configs) {
    const reference = collection().doc(triggerId(config.market, config.ticker));
    const snapshot = await reference.get();
    const existing = snapshot.data() as TriggerDocument | undefined;
    if (existing && (!overwrite || triggerConfigKey(existing) === triggerConfigKey(config))) {
      unchanged.push(reference.id);
      continue;
    }
    const values = {
      ...config,
      yahoo_symbol: yahooSymbol(config.market, config.ticker),
      updated_at_utc: now,
      updated_by: user.email
    };
    if (existing) {
      await reference.update(values);
      updated.push(reference.id);
    } else {
      await reference.set({...values, created_at_utc: now, created_by: user.email, scan: null});
      added.push(reference.id);
    }
    references.push(reference);
  }
  // Scan changed entries straight away so the list is current.
  if (references.length) await scanReferences(references);
  return {added, updated, unchanged};
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
  const item = publicItem(reference.id, (await reference.get()).data() as TriggerDocument);
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

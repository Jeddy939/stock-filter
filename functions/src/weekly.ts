import crypto from "node:crypto";
import {FieldValue, getFirestore, type DocumentReference} from "firebase-admin/firestore";
import {logger} from "firebase-functions";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {db} from "./db";
import {
  defaultScanPayload,
  defaultScheduledFetchPayload,
  startFilterJob,
  startRatingOutcomesJob,
  startScheduledMarketRefresh,
  startSnapshotPublishJob
} from "./api";
import {databaseState, startDatabase, stopDatabase} from "./database-lifecycle";

const region = "australia-southeast1";
const timeZone = "Australia/Brisbane";
const leaseMs = 10 * 60_000;
const maxRetries = {refresh_asx: 2, refresh_us: 2, scan_asx: 2, scan_us: 2, publish: 2} as const;

export type WeeklyState = Record<string, unknown> & {phase?: string; run_id?: string};

export type WeeklyJobStatus = {id: string; status: string; stage: string; error?: string} | null;

export type WeeklyActions = {
  isDatabaseRunning: () => Promise<{running: boolean; state: string; activationPolicy: string}>;
  startDatabase: () => Promise<unknown>;
  stopDatabase: () => Promise<unknown>;
  startAsxRefresh: () => Promise<Record<string, unknown>>;
  startUsRefresh: () => Promise<Record<string, unknown>>;
  startAsxScan: () => Promise<Record<string, unknown>>;
  startUsScan: () => Promise<Record<string, unknown>>;
  startPublish: () => Promise<Record<string, unknown>>;
  startOutcomes: () => Promise<Record<string, unknown>>;
  jobStatus: (id: unknown) => Promise<WeeklyJobStatus>;
};

export function recordLaunchFailure(state: WeeklyState, message: string): {retry: boolean; state: WeeklyState} {
  const phase = String(state.phase ?? "starting_database");
  const counts = {...(state.launch_retry_counts as Record<string, number> | undefined)};
  const count = Number(counts[phase] ?? 0) + 1;
  counts[phase] = count;
  return {
    retry: count <= 2,
    state: {
      ...state,
      launch_retry_counts: counts,
      launch_error: message,
      launch_error_phase: phase,
      launch_error_at: new Date().toISOString()
    }
  };
}

function brisbaneParts(now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    hourCycle: "h23"
  }).formatToParts(now);
  return Object.fromEntries(parts.map((part) => [part.type, part.value]));
}

export function weeklyRunId(now = new Date()) {
  const parts = brisbaneParts(now);
  const weekday = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(parts.weekday);
  const localDate = Date.UTC(Number(parts.year), Number(parts.month) - 1, Number(parts.day));
  return new Date(localDate - ((weekday + 5) % 7) * 86_400_000).toISOString().slice(0, 10);
}

function resultJobId(result: Record<string, unknown>): string | null {
  const job = result.job as Record<string, unknown> | undefined;
  if (job?.id) return String(job.id);
  if (result.refresh_job_id) return String(result.refresh_job_id);
  if (result.job_id) return String(result.job_id);
  return null;
}

async function jobStatus(id: unknown): Promise<WeeklyJobStatus> {
  if (!id) return null;
  const run = await db().query("SELECT id, status, stage, error FROM job_runs WHERE id = $1", [String(id)]);
  if (run.rows[0]) return run.rows[0];
  const refresh = await db().query("SELECT id, status, stage, error FROM refresh_jobs WHERE id = $1", [String(id)]);
  return refresh.rows[0] ?? null;
}

function migrateLegacyState(state: WeeklyState): WeeklyState {
  const phase = String(state.phase ?? "");
  if (phase === "screening" && !state.asx_scan_job_id) {
    return {...state, phase: "screening_asx", legacy_order: true};
  }
  if (phase === "screening" && state.asx_scan_job_id && state.us_scan_job_id) {
    return {...state, phase: "screening_legacy", legacy_order: true};
  }
  if (phase === "screening" && state.asx_scan_job_id) {
    return {...state, phase: "screening_us", legacy_order: true};
  }
  if (phase === "refreshing_us" && state.us_job_id && !state.asx_scan_job_id) {
    return {...state, legacy_order: true};
  }
  return state;
}

function active(job: WeeklyJobStatus): boolean {
  return Boolean(job && ["queued", "running"].includes(job.status));
}

async function retryStage(
  state: WeeklyState,
  actions: WeeklyActions,
  phase: string,
  retryKey: keyof typeof maxRetries,
  retryCountField: string,
  jobIdField: string,
  errorMessage: string,
  start: () => Promise<Record<string, unknown>>,
  nowIso: string
): Promise<{state: WeeklyState; result: Record<string, unknown>}> {
  const retryCount = Number(state[retryCountField] ?? 0);
  if (retryCount < maxRetries[retryKey]) {
    const result = await start();
    const jobId = resultJobId(result);
    if (!jobId) throw new Error(`${errorMessage}: retry did not return a job ID`);
    return {
      state: {
        ...state,
        phase,
        [jobIdField]: jobId,
        [retryCountField]: retryCount + 1,
        [`${jobIdField}_error`]: errorMessage,
        [`${jobIdField}_recovered_at`]: nowIso
      },
      result: {ok: true, phase, retried: true, job_id: jobId}
    };
  }
  throw new Error(`${errorMessage}: exceeded 2 retries`);
}

async function completeRun(
  actions: WeeklyActions,
  state: WeeklyState,
  nowIso: string
): Promise<{state: WeeklyState; result: Record<string, unknown>}> {
  await actions.stopDatabase();
  return {
    state: {...state, phase: "complete", database_stopped_at: nowIso, database_stopped: true},
    result: {ok: true, phase: "complete", outcomes_error: state.outcomes_error ?? null}
  };
}

export async function advanceWeeklyState(
  inputState: WeeklyState,
  actions: WeeklyActions
): Promise<{state: WeeklyState; result: Record<string, unknown>}> {
  if (["complete", "failed"].includes(String(inputState.phase ?? ""))) {
    return {state: inputState, result: {ok: true, skipped: true, phase: inputState.phase}};
  }
  const state = migrateLegacyState(inputState);
  const phase = String(state.phase ?? "starting_database");
  const nowIso = new Date().toISOString();

  if (phase === "starting_database") {
    const sql = await actions.isDatabaseRunning();
    if (!sql.running) {
      await actions.startDatabase();
      return {state: {...state, phase}, result: {ok: true, phase, started_database: true}};
    }
    const result = await actions.startAsxRefresh();
    const jobId = resultJobId(result);
    if (!jobId) throw new Error("ASX refresh did not return a job ID");
    return {state: {...state, phase: "refreshing_asx", asx_job_id: jobId}, result: {ok: true, phase: "refreshing_asx", job_id: jobId}};
  }

  if (phase === "refreshing_asx") {
    if (!state.asx_job_id) {
      const result = await actions.startAsxRefresh();
      const jobId = resultJobId(result);
      if (!jobId) throw new Error("ASX refresh did not return a job ID");
      return {state: {...state, phase, asx_job_id: jobId}, result: {ok: true, phase, job_id: jobId}};
    }
    const job = await actions.jobStatus(state.asx_job_id);
    if (active(job)) return {state, result: {ok: true, phase, job}};
    if (job?.status !== "succeeded") {
      return retryStage(state, actions, phase, "refresh_asx", "asx_refresh_retry_count", "asx_job_id",
        `ASX refresh failed: ${job?.error ?? job?.stage ?? "unknown error"}`,
        actions.startAsxRefresh, nowIso);
    }
    const result = await actions.startAsxScan();
    const jobId = resultJobId(result);
    if (!jobId) throw new Error("ASX scan did not return a job ID");
    return {
      state: {...state, phase: "screening_asx", asx_refreshed_at: nowIso, asx_scan_job_id: jobId},
      result: {ok: true, phase: "screening_asx", job_id: jobId}
    };
  }

  if (phase === "screening_asx") {
    if (!state.asx_scan_job_id) {
      const result = await actions.startAsxScan();
      const jobId = resultJobId(result);
      if (!jobId) throw new Error("ASX scan did not return a job ID");
      return {state: {...state, phase, asx_scan_job_id: jobId}, result: {ok: true, phase, job_id: jobId}};
    }
    const job = await actions.jobStatus(state.asx_scan_job_id);
    if (active(job)) return {state, result: {ok: true, phase, job}};
    if (job?.status !== "succeeded") {
      return retryStage(state, actions, phase, "scan_asx", "asx_scan_retry_count", "asx_scan_job_id",
        `ASX scan failed: ${job?.error ?? job?.stage ?? "unknown error"}`,
        actions.startAsxScan, nowIso);
    }
    const result = state.legacy_order ? await actions.startUsScan() : await actions.startUsRefresh();
    const jobId = resultJobId(result);
    if (!jobId) throw new Error(state.legacy_order ? "US scan did not return a job ID" : "US refresh did not return a job ID");
    if (state.legacy_order) {
      return {
        state: {...state, phase: "screening_us", asx_screened_at: nowIso, us_scan_job_id: jobId},
        result: {ok: true, phase: "screening_us", job_id: jobId}
      };
    }
    return {
      state: {...state, phase: "refreshing_us", asx_screened_at: nowIso, us_job_id: jobId},
      result: {ok: true, phase: "refreshing_us", job_id: jobId}
    };
  }

  if (phase === "refreshing_us") {
    if (!state.us_job_id) {
      const result = await actions.startUsRefresh();
      const jobId = resultJobId(result);
      if (!jobId) throw new Error("US refresh did not return a job ID");
      return {state: {...state, phase, us_job_id: jobId}, result: {ok: true, phase, job_id: jobId}};
    }
    const job = await actions.jobStatus(state.us_job_id);
    if (active(job)) return {state, result: {ok: true, phase, job}};
    if (job?.status !== "succeeded") {
      return retryStage(state, actions, phase, "refresh_us", "us_refresh_retry_count", "us_job_id",
        `US refresh failed: ${job?.error ?? job?.stage ?? "unknown error"}`,
        actions.startUsRefresh, nowIso);
    }
    if (state.legacy_order && !state.asx_scan_job_id) {
      const result = await actions.startAsxScan();
      const jobId = resultJobId(result);
      if (!jobId) throw new Error("ASX scan did not return a job ID");
      return {
        state: {...state, phase: "screening_asx", us_refreshed_at: nowIso, asx_scan_job_id: jobId},
        result: {ok: true, phase: "screening_asx", job_id: jobId}
      };
    }
    const result = await actions.startUsScan();
    const jobId = resultJobId(result);
    if (!jobId) throw new Error("US scan did not return a job ID");
    return {
      state: {...state, phase: "screening_us", us_refreshed_at: nowIso, us_scan_job_id: jobId},
      result: {ok: true, phase: "screening_us", job_id: jobId}
    };
  }

  if (phase === "screening_us") {
    if (!state.us_scan_job_id) {
      const result = await actions.startUsScan();
      const jobId = resultJobId(result);
      if (!jobId) throw new Error("US scan did not return a job ID");
      return {state: {...state, phase, us_scan_job_id: jobId}, result: {ok: true, phase, job_id: jobId}};
    }
    const job = await actions.jobStatus(state.us_scan_job_id);
    if (active(job)) return {state, result: {ok: true, phase, job}};
    if (job?.status !== "succeeded") {
      return retryStage(state, actions, phase, "scan_us", "us_scan_retry_count", "us_scan_job_id",
        `US scan failed: ${job?.error ?? job?.stage ?? "unknown error"}`,
        actions.startUsScan, nowIso);
    }
    const result = await actions.startPublish();
    const jobId = resultJobId(result);
    if (!jobId) throw new Error("Snapshot publisher did not return a job ID");
    return {
      state: {...state, phase: "publishing", us_screened_at: nowIso, publish_job_id: jobId},
      result: {ok: true, phase: "publishing", job_id: jobId}
    };
  }

  if (phase === "screening_legacy") {
    const [asx, us] = await Promise.all([
      actions.jobStatus(state.asx_scan_job_id),
      actions.jobStatus(state.us_scan_job_id)
    ]);
    if ([asx, us].some(active)) return {state, result: {ok: true, phase, jobs: {asx, us}}};
    if (asx?.status !== "succeeded" || us?.status !== "succeeded") {
      throw new Error(`Legacy default screen failed: ${asx?.error ?? us?.error ?? "unknown error"}`);
    }
    return {state: {...state, phase: "publishing", asx_screened_at: nowIso, us_screened_at: nowIso}, result: {ok: true, phase: "publishing"}};
  }

  if (phase === "publishing") {
    if (!state.publish_job_id) {
      const result = await actions.startPublish();
      const jobId = resultJobId(result);
      if (!jobId) throw new Error("Snapshot publisher did not return a job ID");
      return {state: {...state, phase, publish_job_id: jobId}, result: {ok: true, phase, job_id: jobId}};
    }
    const job = await actions.jobStatus(state.publish_job_id);
    if (active(job)) return {state, result: {ok: true, phase, job}};
    if (job?.status !== "succeeded") {
      return retryStage(state, actions, phase, "publish", "publish_retry_count", "publish_job_id",
        `Snapshot publication failed: ${job?.error ?? job?.stage ?? "unknown error"}`,
        actions.startPublish, nowIso);
    }
    // The database only runs during this weekly window, so measure appraisal
    // and screen outcomes before stopping it. Publication has already
    // succeeded; an outcomes problem is recorded but never fails the run.
    try {
      const result = await actions.startOutcomes();
      const jobId = resultJobId(result);
      if (jobId) {
        return {
          state: {...state, phase: "measuring_outcomes", published_at: nowIso, outcomes_job_id: jobId},
          result: {ok: true, phase: "measuring_outcomes", job_id: jobId}
        };
      }
      return completeRun(actions, {...state, published_at: nowIso, outcomes_error: "Outcomes job did not return a job ID"}, nowIso);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return completeRun(actions, {...state, published_at: nowIso, outcomes_error: message}, nowIso);
    }
  }

  if (phase === "measuring_outcomes") {
    const job = await actions.jobStatus(state.outcomes_job_id);
    if (active(job)) return {state, result: {ok: true, phase, job}};
    const outcome = job?.status === "succeeded"
      ? {outcomes_completed_at: nowIso}
      : {outcomes_error: `Outcomes job ${job?.status ?? "missing"}: ${job?.error ?? job?.stage ?? "unknown"}`};
    return completeRun(actions, {...state, ...outcome}, nowIso);
  }

  throw new Error(`Unknown weekly publication phase: ${phase}`);
}

async function updateState(reference: DocumentReference, values: Record<string, unknown>) {
  await reference.set({...values, updated_at: FieldValue.serverTimestamp()}, {merge: true});
}

async function failRun(reference: DocumentReference, message: string) {
  logger.error("Weekly publication failed", {message});
  await updateState(reference, {phase: "failed", error: message, failed_at: FieldValue.serverTimestamp()});
  await stopDatabase().catch((error) => logger.error("Could not stop database after weekly failure", error));
}

class LeaseHeldError extends Error {}

export function leaseAllowed(state: WeeklyState, nowMs: number, ttlMs: number): {ok: boolean; reason?: string} {
  if (["complete", "failed"].includes(String(state.phase ?? ""))) {
    return {ok: false, reason: "run is already finished"};
  }
  const owner = String(state.lease_owner ?? "");
  if (owner && nowMs < Number(state.lease_until_ms ?? 0)) {
    return {ok: false, reason: "another coordinator holds the lease"};
  }
  return {ok: true};
}

async function claimLease(reference: DocumentReference, token: string): Promise<{ok: boolean; reason?: string}> {
  try {
    await getFirestore().runTransaction(async (transaction) => {
      const snapshot = await transaction.get(reference);
      const data = (snapshot.data() ?? {}) as WeeklyState;
      const decision = leaseAllowed(data, Date.now(), leaseMs);
      if (!decision.ok) throw new LeaseHeldError(decision.reason ?? "lease not available");
      transaction.set(reference, {
        lease_owner: token,
        lease_until_ms: Date.now() + leaseMs,
        updated_at: FieldValue.serverTimestamp()
      }, {merge: true});
    });
    return {ok: true};
  } catch (error) {
    if (error instanceof LeaseHeldError) return {ok: false, reason: error.message};
    throw error;
  }
}

async function releaseLease(reference: DocumentReference, token: string) {
  await getFirestore().runTransaction(async (transaction) => {
    const snapshot = await transaction.get(reference);
    if (String(snapshot.data()?.lease_owner ?? "") !== token) return;
    transaction.set(reference, {
      lease_owner: FieldValue.delete(),
      lease_until_ms: FieldValue.delete()
    }, {merge: true});
  });
}

function buildActions(runId: string): WeeklyActions {
  return {
    isDatabaseRunning: databaseState,
    startDatabase,
    stopDatabase,
    startAsxRefresh: () => startScheduledMarketRefresh(defaultScheduledFetchPayload("asx")),
    startUsRefresh: () => startScheduledMarketRefresh(defaultScheduledFetchPayload("us")),
    startAsxScan: () => startFilterJob(defaultScanPayload("asx")),
    startUsScan: () => startFilterJob(defaultScanPayload("us")),
    startPublish: () => startSnapshotPublishJob({weekly_run_id: runId}),
    startOutcomes: () => startRatingOutcomesJob({market: "all", horizons: [28, 56, 84, 182]}),
    jobStatus: async (value) => jobStatus(value)
  };
}

export async function advanceWeeklyPublication(now = new Date()): Promise<Record<string, unknown>> {
  const parts = brisbaneParts(now);
  const id = weeklyRunId(now);
  const reference = getFirestore().collection("weekly_runs").doc(id);
  const snapshot = await reference.get();
  const state = (snapshot.data() ?? {}) as WeeklyState;
  if (!snapshot.exists && parts.weekday !== "Tue") return {ok: true, skipped: true, reason: "No Tuesday run is active"};
  if (!snapshot.exists && Number(parts.hour) < 3) return {ok: true, skipped: true, reason: "Tuesday build window has not opened"};
  if (["complete", "failed"].includes(String(state.phase ?? ""))) return {ok: true, skipped: true, phase: state.phase};

  const token = crypto.randomUUID();
  const lease = await claimLease(reference, token);
  if (!lease.ok) return {ok: true, skipped: true, reason: lease.reason};

  let latest = state;
  try {
    latest = ((await reference.get()).data() ?? {}) as WeeklyState;
    if (!snapshot.exists) {
      await updateState(reference, {run_id: id, phase: "starting_database", started_at: FieldValue.serverTimestamp()});
      latest.phase = "starting_database";
      latest.run_id = id;
    }
    const {state: next, result} = await advanceWeeklyState(latest, buildActions(id));
    await updateState(reference, next);
    return {...result, run_id: id, phase: next.phase};
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const launchFailure = recordLaunchFailure(latest, message);
    if (launchFailure.retry) {
      logger.warn("Weekly publication stage launch will be retried", {
        run_id: id,
        phase: launchFailure.state.phase,
        error: message,
        launch_retry_counts: launchFailure.state.launch_retry_counts
      });
      await updateState(reference, launchFailure.state);
      return {ok: false, retrying: true, phase: launchFailure.state.phase, error: message, run_id: id};
    }
    await failRun(reference, message);
    return {ok: false, phase: "failed", error: message, run_id: id};
  } finally {
    await releaseLease(reference, token).catch((error) => logger.error("Could not release weekly lease", error));
  }
}

export const scheduledWeeklyPublication = onSchedule(
  {
    region,
    schedule: "*/15 * * * *",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => {
    const result = await advanceWeeklyPublication();
    logger.info("Weekly publication coordinator advanced", result);
  }
);

export async function stopIdleDatabase(now = new Date()): Promise<Record<string, unknown>> {
  const sql = await databaseState();
  if (!sql.running) return {ok: true, skipped: true, reason: "Database is already stopped"};

  const run = await getFirestore().collection("weekly_runs").doc(weeklyRunId(now)).get();
  const phase = String(run.data()?.phase ?? "");
  if (run.exists && !["complete", "failed"].includes(phase)) {
    return {ok: true, skipped: true, reason: "Weekly publication is active", phase};
  }

  const activity = await db().query(
    `SELECT
       COUNT(*) FILTER (WHERE status IN ('queued', 'running'))::int AS active_count,
       MAX(updated_at_utc) AS last_activity_at
     FROM job_runs`
  );
  const activeCount = Number(activity.rows[0]?.active_count ?? 0);
  if (activeCount > 0) return {ok: true, skipped: true, reason: "Background jobs are active", active_count: activeCount};
  const lastActivity = activity.rows[0]?.last_activity_at ? new Date(activity.rows[0].last_activity_at) : null;
  if (lastActivity && now.getTime() - lastActivity.getTime() < 30 * 60_000) {
    return {ok: true, skipped: true, reason: "Database has not been idle for 30 minutes", last_activity_at: lastActivity};
  }
  return {ok: true, stopped: true, database: await stopDatabase()};
}

export const scheduledDatabaseIdleShutdown = onSchedule(
  {
    region,
    schedule: "15 * * * *",
    timeZone,
    timeoutSeconds: 540,
    memory: "256MiB"
  },
  async () => {
    const result = await stopIdleDatabase();
    logger.info("Database idle shutdown check complete", result);
  }
);

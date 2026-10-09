import {logger} from "firebase-functions";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {
  defaultScanPayload,
  defaultScheduledFetchPayload,
  reconcileStaleJobs,
  startFundamentalsRefreshJob,
  startFilterJob,
  startMarketRefresh,
  startRatingOutcomesJob
} from "./api";
import {databaseState, isDatabaseDownError, type DatabaseState} from "./database-lifecycle";

const region = "australia-southeast1";
const timeZone = "Australia/Brisbane";

type StateReader = () => Promise<DatabaseState>;

/**
 * Skip only when SQL Admin confirms STOPPED + NEVER. State-check failures,
 * transitional states, and unexpected outages remain failed. A connection
 * failure after the precheck is safe to skip only after a stopped-state recheck.
 */
export async function runDatabaseScheduledJob<T>(
  job: string,
  action: () => Promise<T>,
  readState: StateReader = databaseState
): Promise<T | undefined> {
  const before = await readState();
  if (before.intentionallyStopped) {
    logger.info("Skipping scheduled job while database is intentionally stopped", {job, state: before});
    return undefined;
  }
  if (!before.running) {
    throw new Error(`Cloud SQL is not ready for scheduled ${job}: ${before.state}/${before.activationPolicy}`);
  }
  try {
    return await action();
  } catch (error) {
    if (!isDatabaseDownError(error)) throw error;
    const after = await readState();
    if (after.intentionallyStopped) {
      logger.info("Skipping scheduled job after database stopped during execution", {job, state: after});
      return undefined;
    }
    throw error;
  }
}

async function rebuildRatingOutcomes() {
  return runDatabaseScheduledJob("rating-outcomes", async () => {
    const result = await startRatingOutcomesJob({market: "all", horizons: [28, 56, 84, 182]});
    logger.info("Scheduled rating outcome rebuild queued", {result});
    return result;
  });
}

async function reconcileJobs() {
  return runDatabaseScheduledJob("reconciliation", async () => {
    const result = await reconcileStaleJobs();
    logger.info("Scheduled job reconciliation complete", {result});
    return result;
  });
}

async function refreshFundamentals() {
  return runDatabaseScheduledJob("fundamentals-refresh", async () => {
    const result = await startFundamentalsRefreshJob({market: "us", limit: 6000, force: false});
    logger.info("Scheduled SEC fundamentals refresh queued", {result});
    return result;
  });
}

export const scheduledRefreshAsx = onSchedule(
  {
    region,
    schedule: "30 6 * * 1-5",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => {
    const result = await startMarketRefresh(defaultScheduledFetchPayload("asx"));
    logger.info("Scheduled ASX market refresh queued", {market: "asx", result});
  }
);

export const scheduledRefreshUs = onSchedule(
  {
    region,
    schedule: "30 7 * * 2",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => {
    const result = await startMarketRefresh(defaultScheduledFetchPayload("us"));
    logger.info("Scheduled US market refresh queued", {market: "us", result});
  }
);

export const scheduledDefaultScanAsx = onSchedule(
  {
    region,
    schedule: "0 7 * * 1-5",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => {
    const result = await startFilterJob(defaultScanPayload("asx"));
    logger.info("Scheduled ASX default scan queued", {market: "asx", result});
  }
);

export const scheduledDefaultScanUs = onSchedule(
  {
    region,
    schedule: "0 8 * * 2-6",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => {
    const result = await startFilterJob(defaultScanPayload("us"));
    logger.info("Scheduled US default scan queued", {market: "us", result});
  }
);

export const scheduledRatingOutcomes = onSchedule(
  {
    region,
    schedule: "30 9 * * *",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => {
    await rebuildRatingOutcomes();
  }
);

export const scheduledJobReconciliation = onSchedule(
  {
    region,
    schedule: "*/30 * * * *",
    timeZone,
    timeoutSeconds: 300,
    memory: "256MiB"
  },
  async () => {
    await reconcileJobs();
  }
);

export const scheduledFundamentalsRefresh = onSchedule(
  {
    region,
    schedule: "0 10 * * 0",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => {
    await refreshFundamentals();
  }
);

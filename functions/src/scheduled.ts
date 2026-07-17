import {logger} from "firebase-functions";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {
  defaultScanPayload,
  defaultScheduledFetchPayload,
  reconcileStaleJobs,
  startFilterJob,
  startMarketRefresh,
  startRatingOutcomesJob
} from "./api";

const region = "australia-southeast1";
const timeZone = "Australia/Brisbane";

async function rebuildRatingOutcomes() {
  const result = await startRatingOutcomesJob({market: "all", horizons: [30, 90, 180, 360]});
  logger.info("Scheduled rating outcome rebuild queued", {result});
}

async function reconcileJobs() {
  const result = await reconcileStaleJobs();
  logger.info("Scheduled job reconciliation complete", {result});
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
  async () => rebuildRatingOutcomes()
);

export const scheduledJobReconciliation = onSchedule(
  {
    region,
    schedule: "*/30 * * * *",
    timeZone,
    timeoutSeconds: 300,
    memory: "256MiB"
  },
  async () => reconcileJobs()
);

import {logger} from "firebase-functions";
import {onSchedule} from "firebase-functions/v2/scheduler";
import {
  defaultScanPayload,
  defaultScheduledFetchPayload,
  startFilterJob,
  startMarketRefresh
} from "./api";

const region = "australia-southeast1";
const timeZone = "Australia/Brisbane";

async function refreshMarket(market: "asx" | "us") {
  const result = await startMarketRefresh(defaultScheduledFetchPayload(market));
  logger.info("Scheduled market refresh queued", {market, result});
}

async function rebuildDefaultScan(market: "asx" | "us") {
  const result = await startFilterJob(defaultScanPayload(market));
  logger.info("Scheduled default scan queued", {market, result});
}

export const scheduledRefreshAsx = onSchedule(
  {
    region,
    schedule: "30 6 * * 1-5",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => refreshMarket("asx")
);

export const scheduledRefreshUs = onSchedule(
  {
    region,
    schedule: "30 7 * * 2-6",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => refreshMarket("us")
);

export const scheduledDefaultScanAsx = onSchedule(
  {
    region,
    schedule: "0 7 * * 1-5",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => rebuildDefaultScan("asx")
);

export const scheduledDefaultScanUs = onSchedule(
  {
    region,
    schedule: "0 8 * * 2-6",
    timeZone,
    timeoutSeconds: 540,
    memory: "512MiB"
  },
  async () => rebuildDefaultScan("us")
);

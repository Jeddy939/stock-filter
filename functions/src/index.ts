import {onRequest} from "firebase-functions/v2/https";
import {apiApp} from "./api";
export {
  scheduledDefaultScanAsx,
  scheduledDefaultScanUs,
  scheduledRefreshAsx,
  scheduledRefreshUs
} from "./scheduled";
export {refreshTickerBatch} from "./tasks";

export const api = onRequest(
  {
    region: "australia-southeast1",
    memory: "1GiB",
    timeoutSeconds: 540,
    maxInstances: 10
  },
  apiApp
);

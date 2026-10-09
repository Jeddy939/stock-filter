import {onRequest} from "firebase-functions/v2/https";
import {apiApp} from "./api";
export {refreshTickerBatch} from "./tasks";
export {scheduledJobReconciliation, scheduledRatingOutcomes} from "./scheduled";
export {scheduledDatabaseIdleShutdown, scheduledWeeklyPublication} from "./weekly";

export const api = onRequest(
  {
    region: "australia-southeast1",
    memory: "1GiB",
    timeoutSeconds: 540,
    maxInstances: 10
  },
  apiApp
);

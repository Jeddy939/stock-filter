import crypto from "node:crypto";
import {onTaskDispatched} from "firebase-functions/v2/tasks";
import {dispatchCloudRunJob} from "./cloudrun";
import {db} from "./db";

interface RefreshTickerBatchPayload {
  refreshJobId: string;
  refreshBatchId: string;
  parentJobId: string;
  market: string;
  provider?: string;
  tickers: string[];
  fetchPayload?: Record<string, unknown>;
}

export const refreshTickerBatch = onTaskDispatched<RefreshTickerBatchPayload>(
  {
    omit: !["1", "true", "yes"].includes(String(process.env.MONEYMAKER_DEPLOY_TASK_FUNCTIONS ?? "false").toLowerCase()),
    region: "australia-southeast1",
    timeoutSeconds: 1800,
    memory: "512MiB",
    retryConfig: {
      maxAttempts: 3,
      minBackoffSeconds: 300
    },
    rateLimits: {
      maxConcurrentDispatches: 1
    }
  },
  async (request) => {
    const data = request.data;
    if (!data.refreshJobId || !data.refreshBatchId || !data.parentJobId || !data.market || !Array.isArray(data.tickers)) {
      throw new Error("refreshJobId, refreshBatchId, parentJobId, market, and tickers are required");
    }

    await db().query(
      `
      UPDATE refresh_batches
      SET status = 'running', attempts = attempts + 1, started_at_utc = COALESCE(started_at_utc, NOW())
      WHERE id = $1 AND refresh_job_id = $2
      `,
      [data.refreshBatchId, data.refreshJobId]
    );

    const jobId = crypto.randomUUID();
    const payload = {
      ...(data.fetchPayload ?? {}),
      market: data.market,
      provider: data.provider ?? data.fetchPayload?.provider ?? "yfinance",
      tickers: data.tickers,
      refresh_job_id: data.refreshJobId,
      refresh_batch_id: data.refreshBatchId,
      parent_job_id: data.parentJobId
    };
    await db().query(
      `
      INSERT INTO job_runs
        (id, job_type, market, status, stage, detail, started_at_utc, total_count, parameters_json)
      VALUES ($1, 'fetch', $2, 'queued', 'Queued', $3, NOW(), $4, $5::jsonb)
      `,
      [
        jobId,
        data.market,
        `Queued ticker batch ${data.refreshBatchId}`,
        data.tickers.length,
        JSON.stringify(payload)
      ]
    );
    const cloudRunJob = await dispatchCloudRunJob("fetch", payload, jobId);

    await db().query(
      `
      UPDATE refresh_batches
      SET result_json = jsonb_set(
            COALESCE(result_json, '{}'::jsonb),
            '{cloud_run_dispatch}',
            $1::jsonb,
            true
          )
      WHERE id = $2 AND refresh_job_id = $3
      `,
      [JSON.stringify({job_id: jobId, cloud_run_job: cloudRunJob}), data.refreshBatchId, data.refreshJobId]
    );
  }
);

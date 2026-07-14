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

const childJobPollMs = Math.max(Number(process.env.MONEYMAKER_CHILD_JOB_POLL_MS ?? 30000), 5000);
const childJobWaitMs = Math.max(Number(process.env.MONEYMAKER_CHILD_JOB_WAIT_MS ?? 1_700_000), childJobPollMs);

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForChildFetchJob(jobId: string): Promise<void> {
  const deadline = Date.now() + childJobWaitMs;
  let lastStatus = "queued";
  let lastDetail = "";
  while (Date.now() < deadline) {
    const result = await db().query(
      "SELECT status, detail, error FROM job_runs WHERE id = $1",
      [jobId]
    );
    const row = result.rows[0];
    if (!row) {
      throw new Error(`Child fetch job ${jobId} disappeared from job_runs`);
    }
    lastStatus = String(row.status ?? "");
    lastDetail = String(row.error ?? row.detail ?? "");
    if (lastStatus === "succeeded") return;
    if (["failed", "cancelled"].includes(lastStatus)) {
      throw new Error(`Child fetch job ${jobId} ${lastStatus}: ${lastDetail.slice(0, 1000)}`);
    }
    await sleep(childJobPollMs);
  }
  throw new Error(`Timed out waiting for child fetch job ${jobId}; last status ${lastStatus}: ${lastDetail.slice(0, 1000)}`);
}

export const refreshTickerBatch = onTaskDispatched<RefreshTickerBatchPayload>(
  {
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

    const existingDispatch = await db().query(
      `
      SELECT result_json #>> '{cloud_run_dispatch,job_id}' AS child_job_id
      FROM refresh_batches
      WHERE id = $1 AND refresh_job_id = $2
      `,
      [data.refreshBatchId, data.refreshJobId]
    );
    const existingChildJobId = String(existingDispatch.rows[0]?.child_job_id ?? "").trim();
    if (existingChildJobId) {
      await waitForChildFetchJob(existingChildJobId);
      return;
    }

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
    await waitForChildFetchJob(jobId);
  }
);

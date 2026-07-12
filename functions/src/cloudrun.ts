import {GoogleAuth} from "google-auth-library";

export async function dispatchCloudRunJob(
  jobType: string,
  payload: Record<string, unknown>,
  jobId: string
): Promise<string> {
  const project = process.env.GOOGLE_CLOUD_PROJECT ?? process.env.GCLOUD_PROJECT ?? "moneymaker-aedf7";
  const region = process.env.MONEYMAKER_RUN_REGION ?? "australia-southeast1";
  const jobName = process.env[jobType === "fetch" ? "MONEYMAKER_FETCH_JOB" : "MONEYMAKER_FILTER_JOB"] ?? `moneymaker-${jobType}`;
  const url = `https://run.googleapis.com/v2/projects/${project}/locations/${region}/jobs/${jobName}:run`;
  const auth = new GoogleAuth({scopes: ["https://www.googleapis.com/auth/cloud-platform"]});
  const client = await auth.getClient();
  await client.request({
    url,
    method: "POST",
    data: {
      overrides: {
        containerOverrides: [
          {
            env: [
              {name: "MONEYMAKER_JOB_ID", value: jobId},
              {name: "MONEYMAKER_JOB_TYPE", value: jobType},
              {name: "MONEYMAKER_JOB_PAYLOAD", value: JSON.stringify(payload)}
            ]
          }
        ]
      }
    }
  });
  return `projects/${project}/locations/${region}/jobs/${jobName}`;
}

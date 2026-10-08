import {GoogleAuth} from "google-auth-library";

const project = process.env.GOOGLE_CLOUD_PROJECT ?? process.env.GCLOUD_PROJECT ?? "moneymaker-aedf7";
const instance = process.env.MONEYMAKER_SQL_INSTANCE ?? "moneymaker-db";
const baseUrl = `https://sqladmin.googleapis.com/sql/v1beta4/projects/${project}/instances/${instance}`;

export type DatabaseState = {
  state: string;
  activationPolicy: string;
  running: boolean;
  intentionallyStopped: boolean;
};

type SqlInstance = {state?: string; settings?: {activationPolicy?: string}};

async function authClient() {
  const auth = new GoogleAuth({scopes: ["https://www.googleapis.com/auth/cloud-platform"]});
  return auth.getClient();
}

/** Read authoritative SQL Admin state; activationPolicy records the operator's stop intent. */
export async function databaseState(): Promise<DatabaseState> {
  const client = await authClient();
  const response = await client.request<SqlInstance>({url: baseUrl, method: "GET"});
  const state = String(response.data.state ?? "UNKNOWN").toUpperCase();
  const activationPolicy = String(response.data.settings?.activationPolicy ?? "UNKNOWN").toUpperCase();
  return {
    state,
    activationPolicy,
    running: state === "RUNNABLE" && activationPolicy === "ALWAYS",
    intentionallyStopped: activationPolicy === "NEVER"
  };
}

async function setActivationPolicy(activationPolicy: "ALWAYS" | "NEVER") {
  const client = await authClient();
  await client.request({url: baseUrl, method: "PATCH", data: {settings: {activationPolicy}}});
  return databaseState();
}

export async function startDatabase() { return setActivationPolicy("ALWAYS"); }

export async function ensureDatabaseRunning(timeoutMs = 300_000) {
  let state = await databaseState();
  if (state.running) return state;
  await startDatabase();
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 5_000));
    state = await databaseState();
    if (state.running) return state;
  }
  throw new Error(`Cloud SQL did not become ready within ${Math.round(timeoutMs / 1000)} seconds`);
}

export async function stopDatabase() { return setActivationPolicy("NEVER"); }

export function isDatabaseDownError(error: unknown): boolean {
  const parts: string[] = [];
  if (error instanceof Error) {
    parts.push(error.message);
    const code = (error as {code?: unknown}).code;
    if (typeof code === "string") parts.push(code);
  } else parts.push(String(error));
  const message = parts.join(" ");
  return ["ECONNREFUSED", "ENOTFOUND", "EHOSTUNREACH", "ETIMEDOUT", ".s.PGSQL.5432",
    "Cloud SQL connection failed", "invalidState", "not in an appropriate state"].some((part) => message.includes(part));
}

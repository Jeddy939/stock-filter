import {getFirestore, Timestamp} from "firebase-admin/firestore";
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

// Signed-in API use is stored in Firestore (readable while Cloud SQL is stopped)
// so the idle shutdown leaves the database running while someone is using the app.
const APP_ACTIVITY_DOC = "system/database_activity";
const APP_ACTIVITY_THROTTLE_MS = 60_000;
let lastAppActivityWriteMs = 0;

/** Record app use at most once a minute per instance; returns whether it wrote. */
export async function recordAppActivity(
  nowMs = Date.now(),
  write: (at: Date) => Promise<unknown> = (at) =>
    getFirestore().doc(APP_ACTIVITY_DOC).set({last_app_activity_at: at}, {merge: true})
): Promise<boolean> {
  if (nowMs - lastAppActivityWriteMs < APP_ACTIVITY_THROTTLE_MS) return false;
  lastAppActivityWriteMs = nowMs;
  await write(new Date(nowMs));
  return true;
}

export async function lastAppActivity(): Promise<Date | null> {
  const snapshot = await getFirestore().doc(APP_ACTIVITY_DOC).get();
  const value = snapshot.data()?.last_app_activity_at;
  if (value instanceof Timestamp) return value.toDate();
  return value ? new Date(value) : null;
}

/** The most recent of several activity times, ignoring missing or invalid ones. */
export function latestActivity(...times: Array<Date | null | undefined>): Date | null {
  const valid = times.filter((time): time is Date => time instanceof Date && !Number.isNaN(time.getTime()));
  return valid.length ? new Date(Math.max(...valid.map((time) => time.getTime()))) : null;
}

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

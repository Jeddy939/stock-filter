import {Pool} from "pg";
import {ApiError} from "./auth";

let pool: Pool | undefined;

function databaseUrl(): string {
  const value = String(process.env.MONEYMAKER_DATABASE_URL ?? process.env.DATABASE_URL ?? "").trim();
  if (!value) throw new ApiError(503, "MONEYMAKER_DATABASE_URL is not configured");
  return value;
}

export function db(): Pool {
  if (!pool) {
    pool = new Pool({
      connectionString: databaseUrl(),
      max: Number(process.env.MONEYMAKER_DB_POOL_MAX ?? 5),
      idleTimeoutMillis: 30000
    });
  }
  return pool;
}

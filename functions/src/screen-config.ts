import crypto from "node:crypto";

export interface NormalizedScreenConfig {
  market: "asx" | "us";
  provider: string;
  limit: number;
  query: string;
  volume_multiplier: number;
  avg_volume_weeks: number;
  price_avg_weeks: number;
  lookback_weeks: number;
  ma_periods: Record<string, number>;
  min_market_cap: number;
  max_market_cap: number;
}

function finiteNumber(value: unknown, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, child]) => `${JSON.stringify(key)}:${stableJson(child)}`);
    return `{${entries.join(",")}}`;
  }
  return JSON.stringify(value);
}

export function normalizeScreenConfig(payload: Record<string, unknown>): NormalizedScreenConfig {
  const market = String(payload.market ?? "asx").toLowerCase() === "us" ? "us" : "asx";
  const rawPeriods = payload.ma_periods && typeof payload.ma_periods === "object" && !Array.isArray(payload.ma_periods)
    ? payload.ma_periods as Record<string, unknown>
    : {
      short: payload.ma_short ?? 90,
      intermediate: payload.ma_intermediate ?? 180,
      medium: payload.ma_medium ?? 360,
      long: payload.ma_long ?? 700
    };
  const maPeriods = Object.fromEntries(
    Object.entries(rawPeriods)
      .map(([name, period]) => [name, Math.max(0, Math.trunc(finiteNumber(period, 0)))])
      .filter(([, period]) => Number(period) > 0)
      .sort(([left], [right]) => String(left).localeCompare(String(right)))
  );
  return {
    market,
    provider: String(payload.provider ?? "yfinance").trim().toLowerCase() || "yfinance",
    limit: Math.max(0, Math.trunc(finiteNumber(payload.limit, 0))),
    query: String(payload.query ?? "").trim().toUpperCase(),
    volume_multiplier: finiteNumber(payload.volume_multiplier, 2),
    avg_volume_weeks: Math.max(1, Math.trunc(finiteNumber(payload.avg_volume_weeks, 52))),
    price_avg_weeks: Math.max(1, Math.trunc(finiteNumber(payload.price_avg_weeks, 1))),
    lookback_weeks: Math.max(1, Math.trunc(finiteNumber(payload.lookback_weeks, 1))),
    ma_periods: maPeriods,
    min_market_cap: Math.max(0, finiteNumber(payload.min_market_cap, 0)),
    max_market_cap: Math.max(0, finiteNumber(payload.max_market_cap, 0))
  };
}

export function screenConfigHash(payload: Record<string, unknown>): string {
  return crypto.createHash("sha256").update(stableJson(normalizeScreenConfig(payload))).digest("hex");
}

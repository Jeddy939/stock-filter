// Trigger levels: a fixed watchlist where each stock has one long-term simple
// moving average that acts as a buy floor. A stock is triggered when any bar's
// low reaches its MA, and stays triggered for six months after the last touch.

export type TriggerMarket = "us" | "asx" | "crypto";
export type TriggerTimeframe = "daily" | "weekly" | "monthly";

export const TRIGGER_MARKETS: TriggerMarket[] = ["us", "asx", "crypto"];
export const TRIGGER_MIN_BARS = 50;
export const TRIGGER_ACTIVE_MONTHS = 6;
export const TRIGGER_MAX_PERIOD = 2000;

export interface TriggerConfig {
  ticker: string;
  market: TriggerMarket;
  ma_period: number;
  timeframe: TriggerTimeframe;
}

export interface Bar {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface TriggerEvaluation {
  status: "triggered" | "watching" | "insufficient";
  price: number | null;
  ma_value: number | null;
  ma_period_used: number | null;
  bars_available: number;
  fallback: boolean;
  distance_pct: number | null;
  first_touch_on: string | null;
  last_touch_on: string | null;
  active_until: string | null;
  touching_now: boolean;
  data_through: string | null;
}

export function normalizeTriggerMarket(value: unknown): TriggerMarket | null {
  const text = String(value ?? "").trim().toLowerCase();
  if (text === "us" || text === "asx" || text === "crypto") return text;
  if (text === "cypto" || text === "crpyto") return "crypto";
  return null;
}

export function normalizeTriggerTimeframe(value: unknown): TriggerTimeframe | null {
  const text = String(value ?? "").trim().toLowerCase();
  if (["d", "day", "days", "daily"].includes(text)) return "daily";
  if (["w", "wk", "week", "weeks", "weekly"].includes(text)) return "weekly";
  if (["m", "mo", "month", "months", "monthly"].includes(text)) return "monthly";
  return null;
}

/** Accepts plain codes or Yahoo symbols (BHP.AX, BTC-USD) and returns the stored code. */
export function normalizeTriggerTicker(value: unknown, market: TriggerMarket): string | null {
  let ticker = String(value ?? "").trim().toUpperCase();
  if (market === "asx") ticker = ticker.replace(/\.AX$/, "");
  if (market === "crypto") ticker = ticker.replace(/-USD$/, "");
  if (!/^[A-Z0-9][A-Z0-9.\-^=]{0,14}$/.test(ticker)) return null;
  return ticker;
}

export function triggerId(market: TriggerMarket, ticker: string): string {
  return `${market}__${ticker.toUpperCase().replace(/[^A-Z0-9._^=-]+/g, "_")}`;
}

export function yahooSymbol(market: TriggerMarket, ticker: string): string {
  if (market === "asx") return `${ticker}.AX`;
  if (market === "crypto") return ticker.includes("-") ? ticker : `${ticker}-USD`;
  return ticker.replace(/\./g, "-");
}

export function triggerConfigKey(config: Pick<TriggerConfig, "ma_period" | "timeframe">): string {
  return `${config.ma_period}:${config.timeframe}`;
}

export function triggerLabel(config: Pick<TriggerConfig, "ma_period" | "timeframe">): string {
  return `${config.ma_period} ${config.timeframe.charAt(0).toUpperCase()}${config.timeframe.slice(1)}`;
}

export function validateTriggerConfig(input: Record<string, unknown>): TriggerConfig {
  const market = normalizeTriggerMarket(input.market);
  if (!market) throw new Error("Market must be US, ASX or Crypto");
  const ticker = normalizeTriggerTicker(input.ticker, market);
  if (!ticker) throw new Error("Enter a valid ticker code");
  const period = Number(input.ma_period);
  if (!Number.isInteger(period) || period < 2 || period > TRIGGER_MAX_PERIOD) {
    throw new Error(`MA length must be a whole number from 2 to ${TRIGGER_MAX_PERIOD}`);
  }
  const timeframe = normalizeTriggerTimeframe(input.timeframe);
  if (!timeframe) throw new Error("Timeframe must be Daily, Weekly or Monthly");
  return {ticker, market, ma_period: period, timeframe};
}

export interface ParsedTriggerLines {
  entries: TriggerConfig[];
  errors: {line: number; text: string; error: string}[];
}

const PAIR_PATTERN = /^([A-Za-z0-9][A-Za-z0-9.\-^=]*)[\s,;:]+(\d+)\s*[,;]?\s*([A-Za-z]+)$/;

/**
 * Parses pasted lines such as "MCD 700 Weekly", "BHP,240,monthly" or "NFLX 360W".
 * A tab-separated row copied from the spreadsheet holds up to three
 * ticker/trigger pairs, read as US, ASX, then Crypto. Lines without digits
 * (headings) are skipped.
 */
export function parseTriggerLines(text: string, defaultMarket: TriggerMarket): ParsedTriggerLines {
  const entries = new Map<string, TriggerConfig>();
  const errors: ParsedTriggerLines["errors"] = [];
  String(text ?? "").split(/\r?\n/).forEach((rawLine, index) => {
    const line = rawLine.trim();
    if (!line || !/\d/.test(line)) return;
    const cells = rawLine.split("\t").map((cell) => cell.trim());
    const pairs: {market: TriggerMarket; text: string}[] = [];
    if (cells.length > 2) {
      for (let column = 0; column * 2 < cells.length; column += 1) {
        const pairText = `${cells[column * 2] ?? ""} ${cells[column * 2 + 1] ?? ""}`.trim();
        if (pairText) pairs.push({market: TRIGGER_MARKETS[column] ?? defaultMarket, text: pairText});
      }
    } else {
      pairs.push({market: defaultMarket, text: line.replace(/\t/g, " ")});
    }
    for (const pair of pairs) {
      const compact = pair.text.replace(/(\d+)\s*([dwmDWM])$/, "$1 $2");
      const match = PAIR_PATTERN.exec(compact);
      if (!match) {
        errors.push({line: index + 1, text: pair.text, error: "Expected TICKER LENGTH TIMEFRAME, e.g. MCD 700 Weekly"});
        continue;
      }
      try {
        const config = validateTriggerConfig({market: pair.market, ticker: match[1], ma_period: Number(match[2]), timeframe: match[3]});
        entries.set(triggerId(config.market, config.ticker), config);
      } catch (error) {
        errors.push({line: index + 1, text: pair.text, error: error instanceof Error ? error.message : String(error)});
      }
    }
  });
  return {entries: [...entries.values()], errors};
}

export function addDays(date: string, days: number): string {
  const value = new Date(`${date}T00:00:00.000Z`);
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

export function addMonths(date: string, months: number): string {
  const [year, month, day] = date.split("-").map(Number);
  const target = new Date(Date.UTC(year, month - 1 + months, 1));
  const lastDay = new Date(Date.UTC(target.getUTCFullYear(), target.getUTCMonth() + 1, 0)).getUTCDate();
  target.setUTCDate(Math.min(day, lastDay));
  return target.toISOString().slice(0, 10);
}

function periodStart(date: string, timeframe: TriggerTimeframe): string {
  if (timeframe === "monthly") return `${date.slice(0, 7)}-01`;
  if (timeframe === "weekly") {
    const weekday = new Date(`${date}T00:00:00.000Z`).getUTCDay();
    return addDays(date, -((weekday + 6) % 7));
  }
  return date;
}

function periodEnd(start: string, timeframe: TriggerTimeframe): string {
  if (timeframe === "weekly") return addDays(start, 6);
  if (timeframe === "monthly") return addDays(addMonths(start, 1), -1);
  return start;
}

/** Brisbane calendar date, which is never behind the ASX, US or crypto trading date. */
export function brisbaneDate(now = new Date()): string {
  return new Date(now.getTime() + 10 * 3_600_000).toISOString().slice(0, 10);
}

interface YahooChartResult {
  meta?: {gmtoffset?: number; regularMarketPrice?: number; currency?: string; longName?: string; shortName?: string};
  timestamp?: number[];
  indicators?: {quote?: {open?: (number | null)[]; high?: (number | null)[]; low?: (number | null)[]; close?: (number | null)[]; volume?: (number | null)[]}[]};
}

/**
 * Converts a Yahoo chart response into one bar per period. Yahoo appends the
 * live quote as an extra element inside the current period; bars that share a
 * period are merged so the current bar keeps its full low.
 */
export function barsFromYahoo(result: YahooChartResult, timeframe: TriggerTimeframe): Bar[] {
  const timestamps = result.timestamp ?? [];
  const quote = result.indicators?.quote?.[0] ?? {};
  const offset = Number(result.meta?.gmtoffset ?? 0);
  const bars: Bar[] = [];
  timestamps.forEach((timestamp, index) => {
    const close = quote.close?.[index];
    if (close === null || close === undefined || !Number.isFinite(close) || close <= 0) return;
    const values = [quote.open?.[index], quote.high?.[index], quote.low?.[index], close]
      .filter((value): value is number => typeof value === "number" && Number.isFinite(value) && value > 0);
    const open = quote.open?.[index] ?? close;
    const high = Math.max(...values);
    const low = Math.min(...values);
    const rawVolume = Number(quote.volume?.[index] ?? 0);
    const volume = Number.isFinite(rawVolume) && rawVolume > 0 ? rawVolume : 0;
    const localDate = new Date((timestamp + offset) * 1000).toISOString().slice(0, 10);
    const date = periodStart(localDate, timeframe);
    const previous = bars[bars.length - 1];
    if (previous && previous.date === date) {
      previous.high = Math.max(previous.high, high);
      previous.low = Math.min(previous.low, low);
      previous.close = close;
      previous.volume = Math.max(previous.volume, volume);
      return;
    }
    bars.push({date, open: open > 0 ? open : close, high, low, close, volume});
  });
  return bars;
}

/**
 * Evaluates one watchlist entry. With fewer than `maPeriod` bars the MA uses
 * every bar available, but never fewer than `minBars`. A touch is any bar
 * whose low is at or below that bar's MA; it stays active for `activeMonths`
 * after the end of the touching bar (today, for the current bar).
 */
export function evaluateTrigger(
  bars: Bar[],
  maPeriod: number,
  timeframe: TriggerTimeframe,
  options: {now?: Date; price?: number | null; minBars?: number; activeMonths?: number} = {}
): TriggerEvaluation {
  const minBars = options.minBars ?? TRIGGER_MIN_BARS;
  const activeMonths = options.activeMonths ?? TRIGGER_ACTIVE_MONTHS;
  const today = brisbaneDate(options.now);
  const count = bars.length;
  const last = bars[count - 1];
  const livePrice = Number(options.price);
  const price = Number.isFinite(livePrice) && livePrice > 0 ? livePrice : last?.close ?? null;
  const base = {
    price,
    bars_available: count,
    data_through: last?.date ?? null,
    first_touch_on: null,
    last_touch_on: null,
    active_until: null,
    touching_now: false
  };
  if (count < Math.max(1, Math.min(minBars, maPeriod))) {
    return {...base, status: "insufficient", ma_value: null, ma_period_used: null, fallback: count < maPeriod, distance_pct: null};
  }

  const sums = [0];
  for (const bar of bars) sums.push(sums[sums.length - 1] + bar.close);
  const maAt = (index: number): number | null => {
    const length = Math.min(maPeriod, index + 1);
    if (length < Math.min(minBars, maPeriod)) return null;
    return (sums[index + 1] - sums[index + 1 - length]) / length;
  };

  const maValue = maAt(count - 1) as number;
  const usedPeriod = Math.min(maPeriod, count);
  const cutoff = addMonths(today, -activeMonths);
  let firstTouch: string | null = null;
  let lastTouch: string | null = null;
  let lastTouchEnd: string | null = null;
  for (let index = count - 1; index >= 0; index -= 1) {
    const bar = bars[index];
    const end = index === count - 1 ? today : periodEnd(bar.date, timeframe);
    const cappedEnd = end > today ? today : end;
    if (cappedEnd < cutoff) break;
    const ma = maAt(index);
    if (ma === null || bar.low > ma) continue;
    firstTouch = bar.date;
    if (!lastTouch) {
      lastTouch = bar.date;
      lastTouchEnd = cappedEnd;
    }
  }

  return {
    ...base,
    status: lastTouch ? "triggered" : "watching",
    ma_value: maValue,
    ma_period_used: usedPeriod,
    fallback: usedPeriod < maPeriod,
    distance_pct: price !== null && maValue > 0 ? ((price - maValue) / maValue) * 100 : null,
    first_touch_on: firstTouch,
    last_touch_on: lastTouch,
    active_until: lastTouchEnd ? addMonths(lastTouchEnd, activeMonths) : null,
    touching_now: Boolean(last && last.low <= maValue)
  };
}

/**
 * The trigger MA at every bar, using the same short-history rule as
 * evaluateTrigger: all bars so far until there are `maPeriod`, and nothing
 * before `minBars` (or `maPeriod`, if that is shorter).
 */
export function triggerMaSeries(bars: Bar[], maPeriod: number, minBars = TRIGGER_MIN_BARS): (number | null)[] {
  const required = Math.min(minBars, maPeriod);
  let sum = 0;
  return bars.map((bar, index) => {
    sum += bar.close;
    if (index >= maPeriod) sum -= bars[index - maPeriod].close;
    const length = Math.min(maPeriod, index + 1);
    return length < required ? null : sum / length;
  });
}

/** The first bar of each run of bars whose low reached the MA. */
export function touchStarts(bars: Bar[], ma: (number | null)[]): string[] {
  const starts: string[] = [];
  let touching = false;
  bars.forEach((bar, index) => {
    const value = ma[index];
    const touched = value !== null && value !== undefined && bar.low <= value;
    if (touched && !touching) starts.push(bar.date);
    touching = touched;
  });
  return starts;
}

// Monday 1969-12-29 UTC. Yahoo aligns weekly bars to the weekday of period1,
// and range=max silently switches to coarser bars, so request explicit ranges.
const YAHOO_WEEK_ANCHOR = -259_200;
const YAHOO_INTERVALS: Record<TriggerTimeframe, string> = {daily: "1d", weekly: "1wk", monthly: "1mo"};

export interface YahooFetchResult {
  bars: Bar[];
  price: number | null;
  currency: string | null;
  name: string | null;
  /** True when a daily request started after the stock's first trade, so the earliest bars are missing. */
  truncated: boolean;
}

export function dailyHistoryDays(maPeriod: number, extraDays = 0): number {
  // The MA length plus the six-month lookback, in calendar days.
  return Math.ceil(maPeriod * 1.5) + 300 + extraDays;
}

export function yahooChartUrl(symbol: string, timeframe: TriggerTimeframe, maPeriod: number, now = new Date(), host = "query1", extraDays = 0): string {
  const end = Math.floor(now.getTime() / 1000);
  const start = timeframe === "daily"
    ? end - dailyHistoryDays(maPeriod, extraDays) * 86_400
    : YAHOO_WEEK_ANCHOR;
  const params = new URLSearchParams({
    interval: YAHOO_INTERVALS[timeframe],
    period1: String(start),
    period2: String(end),
    includePrePost: "false",
    events: "split"
  });
  return `https://${host}.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?${params}`;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface YahooFetchOptions {
  attempts?: number;
  timeoutMs?: number;
  /** Extra calendar days of daily history, e.g. for a chart. */
  extraDays?: number;
}

export async function fetchYahooBars(
  symbol: string,
  timeframe: TriggerTimeframe,
  maPeriod: number,
  fetchImpl: typeof fetch = fetch,
  options: YahooFetchOptions = {}
): Promise<YahooFetchResult> {
  const attempts = options.attempts ?? 4;
  const timeoutMs = options.timeoutMs ?? 20_000;
  const extraDays = options.extraDays ?? 0;
  let lastError = "Yahoo request failed";
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (attempt) await sleep(1000 * 2 ** attempt);
    const requestedAt = new Date();
    const url = yahooChartUrl(symbol, timeframe, maPeriod, requestedAt, attempt % 2 ? "query2" : "query1", extraDays);
    let response: Response;
    try {
      response = await fetchImpl(url, {
        headers: {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", Accept: "application/json"},
        signal: AbortSignal.timeout(timeoutMs)
      });
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      continue;
    }
    const payload = await response.json().catch(() => null) as {chart?: {result?: YahooChartResult[] | null; error?: {description?: string} | null}} | null;
    const chartError = payload?.chart?.error?.description;
    if (response.status === 404 || (chartError && /no data|not found|delisted/i.test(chartError))) {
      throw new Error(`Yahoo has no data for ${symbol}`);
    }
    if (!response.ok || !payload?.chart?.result?.[0]) {
      lastError = chartError || `Yahoo returned HTTP ${response.status}`;
      continue;
    }
    const result = payload.chart.result[0];
    const bars = barsFromYahoo(result, timeframe);
    if (!bars.length) throw new Error(`Yahoo has no price history for ${symbol}`);
    const price = Number(result.meta?.regularMarketPrice);
    const requestStart = new Date(requestedAt.getTime() - dailyHistoryDays(maPeriod, extraDays) * 86_400_000).toISOString().slice(0, 10);
    return {
      bars,
      price: Number.isFinite(price) && price > 0 ? price : null,
      currency: result.meta?.currency ?? null,
      name: result.meta?.longName || result.meta?.shortName || null,
      // A first bar within ten days of the requested start means older bars exist.
      truncated: timeframe === "daily" && bars[0].date <= addDays(requestStart, 10)
    };
  }
  throw new Error(`${symbol}: ${lastError}`);
}

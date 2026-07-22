export const VALID_LABELS = new Set([
  "winner",
  "potential_winner",
  "needs_confirmation",
  "maybe",
  "bad"
]);

export const MARKET_DEFAULTS = {
  asx: {
    label: "ASX",
    cache_file: "stock_cache.sqlite",
    ticker_file: "asx_yfinance_valid_stocks_2026-05-11.txt",
    provider: "yfinance",
    chart_ticker: "CBA.AX",
    output_file: "stock_data_web.json"
  },
  us: {
    label: "US",
    cache_file: "stock_cache_us.sqlite",
    ticker_file: "us_tickers_nasdaqtrader.txt",
    provider: "yfinance",
    chart_ticker: "AAPL",
    output_file: "stock_data_us_web.json"
  }
};

export function currentMarket(market?: unknown, ticker?: unknown): "asx" | "us" {
  const value = String(market ?? "").trim().toLowerCase();
  if (value === "asx" || value === "us") return value;
  return String(ticker ?? "").trim().toUpperCase().endsWith(".AX") ? "asx" : "us";
}

export function rangeDays(range: unknown): number {
  switch (String(range ?? "1y").toLowerCase()) {
    case "3m": return 92;
    case "6m": return 184;
    case "2y": return 731;
    case "5y": return 1827;
    case "10y": return 3653;
    case "1y":
    default: return 366;
  }
}

export function exclusiveHistoryEndDate(now = new Date()): string {
  const end = new Date(now.getTime());
  end.setUTCDate(end.getUTCDate() + 1);
  return end.toISOString().slice(0, 10);
}

export function yahooUrl(ticker: string): string {
  return `https://finance.yahoo.com/quote/${encodeURIComponent(ticker)}`;
}

export function analysisRangeDays(range: unknown): number | null | undefined {
  const ranges: Record<string, number | null> = {
    "3m": 92,
    "6m": 184,
    "1y": 366,
    "2y": 731,
    "5y": 1827,
    all: null
  };
  const key = String(range ?? "all").trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(ranges, key) ? ranges[key] : undefined;
}

function profileText(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

export type CompanyProfile = {
  name: string;
  sector: string;
  industry: string;
  country: string;
  website: string;
  yahoo_url: string;
  summary: string;
};

export function normalizeCompanyProfile(info: unknown, ticker: string): CompanyProfile {
  const raw = info && typeof info === "object" && !Array.isArray(info) ? info as Record<string, unknown> : {};
  let summary = profileText(raw.summary) || profileText(raw.longBusinessSummary) || profileText(raw.description);
  if (summary.length > 620) {
    const shortened = summary.slice(0, 617).replace(/\s+\S*$/, "").replace(/[.,;:]+$/, "");
    summary = `${shortened || summary.slice(0, 617)}...`;
  }

  const symbol = profileText(raw.symbol).toUpperCase() || ticker.trim().toUpperCase();
  return {
    name: profileText(raw.name) || profileText(raw.longName) || profileText(raw.shortName) || symbol,
    sector: profileText(raw.sector),
    industry: profileText(raw.industry),
    country: profileText(raw.country),
    website: profileText(raw.website),
    yahoo_url: profileText(raw.yahoo_url) || yahooUrl(symbol),
    summary
  };
}

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

export function yahooUrl(ticker: string): string {
  return `https://finance.yahoo.com/quote/${encodeURIComponent(ticker)}`;
}

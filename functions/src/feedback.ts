export const FEEDBACK_CATEGORIES = new Set(["bug", "data", "idea", "usability", "other"]);
export const FEEDBACK_STATUSES = new Set(["new", "reviewed", "planned", "done", "dismissed"]);

export interface FeedbackInput {
  category: string;
  message: string;
  pagePath: string | null;
  market: "asx" | "us" | null;
  ticker: string | null;
  context: Record<string, unknown>;
}

function optionalText(value: unknown, maxLength: number): string | null {
  const text = String(value ?? "").trim();
  return text ? text.slice(0, maxLength) : null;
}

export function normalizeFeedbackInput(body: unknown): FeedbackInput {
  const input = body && typeof body === "object" && !Array.isArray(body)
    ? body as Record<string, unknown>
    : {};
  const category = String(input.category ?? "other").trim().toLowerCase();
  if (!FEEDBACK_CATEGORIES.has(category)) throw new Error("Invalid feedback category");

  const message = String(input.message ?? "").trim();
  if (message.length < 3) throw new Error("Feedback must be at least 3 characters");
  if (message.length > 4000) throw new Error("Feedback must be 4,000 characters or fewer");

  const marketValue = String(input.market ?? "").trim().toLowerCase();
  const market = marketValue === "asx" || marketValue === "us" ? marketValue : null;
  const tickerValue = optionalText(input.ticker, 24)?.toUpperCase() ?? null;
  const ticker = tickerValue && /^[A-Z0-9.^=-]{1,24}$/.test(tickerValue) ? tickerValue : null;
  const rawContext = input.context && typeof input.context === "object" && !Array.isArray(input.context)
    ? input.context as Record<string, unknown>
    : {};
  const context = Object.fromEntries(Object.entries(rawContext).slice(0, 20).map(([key, value]) => [
    String(key).slice(0, 60),
    typeof value === "string" ? value.slice(0, 500) : value
  ]));

  return {
    category,
    message,
    pagePath: optionalText(input.page_path ?? input.pagePath, 300),
    market,
    ticker,
    context
  };
}

export function normalizeFeedbackStatus(value: unknown): string {
  const status = String(value ?? "").trim().toLowerCase();
  if (!FEEDBACK_STATUSES.has(status)) throw new Error("Invalid feedback status");
  return status;
}

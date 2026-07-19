const assert = require("node:assert/strict");

const {
  analysisRangeDays,
  exclusiveHistoryEndDate,
  normalizeCompanyProfile,
  VALID_LABELS
} = require("../lib/market.js");
const {normalizeFeedbackInput, normalizeFeedbackStatus} = require("../lib/feedback.js");

assert.deepEqual([...VALID_LABELS], [
  "winner",
  "potential_winner",
  "needs_confirmation",
  "maybe",
  "bad"
]);
assert.equal(VALID_LABELS.has("needs_confirmation"), true);
assert.equal(VALID_LABELS.has("needs confirmation"), false);
assert.equal(VALID_LABELS.has("clear"), false);

const rawProfile = normalizeCompanyProfile({
  symbol: "CBA.AX",
  longName: "Commonwealth Bank of Australia",
  longBusinessSummary: "Provides banking and financial services.",
  sector: "Financial Services",
  industry: "Banks - Diversified",
  country: "Australia",
  website: "https://www.commbank.com.au"
}, "CBA.AX");
assert.equal(rawProfile.name, "Commonwealth Bank of Australia");
assert.equal(rawProfile.summary, "Provides banking and financial services.");
assert.equal(rawProfile.yahoo_url, "https://finance.yahoo.com/quote/CBA.AX");

const normalizedProfile = normalizeCompanyProfile({
  name: "Existing Name",
  summary: "Existing summary",
  yahoo_url: "https://example.test/quote"
}, "TEST");
assert.equal(normalizedProfile.name, "Existing Name");
assert.equal(normalizedProfile.summary, "Existing summary");
assert.equal(normalizedProfile.yahoo_url, "https://example.test/quote");

const longProfile = normalizeCompanyProfile({longBusinessSummary: "word ".repeat(200)}, "LONG");
assert.ok(longProfile.summary.length <= 620);
assert.ok(longProfile.summary.endsWith("..."));

assert.equal(analysisRangeDays("3m"), 92);
assert.equal(analysisRangeDays("2Y"), 731);
assert.equal(analysisRangeDays("all"), null);
assert.equal(analysisRangeDays("invalid"), undefined);
assert.equal(exclusiveHistoryEndDate(new Date("2026-07-17T23:59:00Z")), "2026-07-18");

assert.deepEqual(normalizeFeedbackInput({
  category: " Data ",
  message: " ARI should not pass MA360. ",
  market: "US",
  ticker: "ari",
  page_path: "/analysis",
  context: {view: "analysis"}
}), {
  category: "data",
  message: "ARI should not pass MA360.",
  market: "us",
  ticker: "ARI",
  pagePath: "/analysis",
  context: {view: "analysis"}
});
assert.throws(() => normalizeFeedbackInput({category: "bug", message: "x"}), /at least 3/);
assert.throws(() => normalizeFeedbackInput({category: "invalid", message: "valid message"}), /category/);
assert.equal(normalizeFeedbackStatus("Done"), "done");
assert.throws(() => normalizeFeedbackStatus("deleted"), /status/);

console.log("market and company profile tests passed");

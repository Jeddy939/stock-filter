const assert = require("node:assert/strict");

const {
  analysisRangeDays,
  exclusiveHistoryEndDate,
  normalizeCompanyProfile,
  VALID_LABELS
} = require("../lib/market.js");
const {applyLatestAppraisals, buildChartSeries, manualRefreshPayload, normalizeAnalysisTicker} = require("../lib/api.js");
const {normalizeFeedbackInput, normalizeFeedbackStatus} = require("../lib/feedback.js");

assert.deepEqual([...VALID_LABELS], [
  "winner",
  "needs_confirmation",
  "maybe",
  "bad"
]);
assert.equal(VALID_LABELS.has("needs_confirmation"), true);
assert.equal(VALID_LABELS.has("potential_winner"), false);
assert.equal(VALID_LABELS.has("confirmed"), false);
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
assert.equal(analysisRangeDays(undefined), null);
assert.equal(analysisRangeDays("invalid"), undefined);
assert.equal(exclusiveHistoryEndDate(new Date("2026-07-17T23:59:00Z")), "2026-07-18");

function weeklyRows(count) {
  const start = new Date("2010-01-01T00:00:00.000Z");
  return Array.from({length: count}, (_, index) => {
    const date = new Date(start);
    date.setUTCDate(date.getUTCDate() + index * 7);
    const close = 10 + index / 10;
    return {
      date: date.toISOString().slice(0, 10),
      open: close - 0.1,
      high: close + 0.2,
      low: close - 0.2,
      close,
      volume: 1000 + index
    };
  });
}

const warmedWeeklyChart = buildChartSeries(weeklyRows(760), "weekly", "1y", [30, 700]);
assert.ok(warmedWeeklyChart.rows.length >= 52 && warmedWeeklyChart.rows.length <= 54);
assert.equal(warmedWeeklyChart.movingAverages["700"].length, warmedWeeklyChart.rows.length);
assert.ok(Number.isFinite(warmedWeeklyChart.movingAverages["700"][0]));
assert.equal(warmedWeeklyChart.availability["700"].available, true);
assert.equal(warmedWeeklyChart.availability["700"].cached_bars, 760);

const youngWeeklyChart = buildChartSeries(weeklyRows(100), "weekly", "all", [180]);
assert.equal(youngWeeklyChart.rows.length, 100);
assert.equal(youngWeeklyChart.availability["180"].available, false);
assert.equal(youngWeeklyChart.availability["180"].initialized_at, null);
assert.ok(youngWeeklyChart.movingAverages["180"].every((value) => value === null));

const crossScanAppraisal = applyLatestAppraisals(
  [{ticker: "CBA.AX", scan_id: 900}],
  [{ticker: "CBA.AX", action: "label", label: "winner", note: "Checked", event_at_utc: "2026-07-24T00:00:00Z"}]
)[0];
assert.equal(crossScanAppraisal.scan_id, 900);
assert.equal(crossScanAppraisal.label, "winner");
assert.equal(crossScanAppraisal.personal_note, "Checked");

const clearedAppraisal = applyLatestAppraisals(
  [{ticker: "CBA.AX", scan_id: 901}],
  [{ticker: "CBA.AX", action: "clear", label: null, event_at_utc: "2026-07-24T01:00:00Z"}]
)[0];
assert.equal(clearedAppraisal.label, null);
assert.equal(clearedAppraisal.appraised_at_utc, null);

const manualUsRefresh = manualRefreshPayload({market: "US", limit: 10, workers: 8});
assert.equal(manualUsRefresh.market, "us");
assert.equal(manualUsRefresh.ticker_file, "us_tickers_nasdaqtrader.txt");
assert.equal(manualUsRefresh.provider, "yfinance");
assert.equal(manualUsRefresh.limit, undefined);
assert.equal(manualUsRefresh.workers, 1);
assert.equal(manualUsRefresh.scheduled, false);
assert.equal(manualUsRefresh.manual, true);
assert.throws(() => manualRefreshPayload({}), /Market must be asx or us/);
assert.equal(normalizeAnalysisTicker("asx", "cba"), "CBA.AX");
assert.equal(normalizeAnalysisTicker("asx", "CBA.AX"), "CBA.AX");
assert.equal(normalizeAnalysisTicker("us", "ibrx"), "IBRX");
assert.throws(() => normalizeAnalysisTicker("us", "bad ticker!"), /valid stock ticker/);

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

require("./auth.test.cjs");

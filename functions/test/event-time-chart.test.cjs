const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

// Load the browser helpers straight from the hosted page so the test covers
// the code that actually ships.
const html = fs.readFileSync(path.join(__dirname, "..", "..", "hosting", "index.html"), "utf8");
function extract(name) {
  const start = html.indexOf(`function ${name}(`);
  assert.ok(start >= 0, `${name} not found`);
  let depth = 0;
  for (let index = html.indexOf("{", start); index < html.length; index += 1) {
    if (html[index] === "{") depth += 1;
    if (html[index] === "}" && --depth === 0) return html.slice(start, index + 1);
  }
  throw new Error(`${name} is unterminated`);
}
const {appraisalCutoffDate, eventTimeCurves} = new Function(
  `${extract("appraisalCutoffDate")}\n${extract("eventTimeCurves")}\nreturn {appraisalCutoffDate, eventTimeCurves};`
)();

// Matches cloud_backend/insights/anchor.py and migration 013.
assert.equal(appraisalCutoffDate("asx", "2026-10-08T00:00:00Z"), "2026-10-07", "ASX mid-session uses the previous close");
assert.equal(appraisalCutoffDate("asx", "2026-10-08T06:00:00Z"), "2026-10-08", "ASX after 16:30 uses the same day");
assert.equal(appraisalCutoffDate("us", "2026-10-08T01:00:00Z"), "2026-10-07", "US evening rating does not see the next UTC day");
assert.equal(appraisalCutoffDate("us", "2026-10-08T14:00:00Z"), "2026-10-07", "US morning rating uses the previous close");

const days = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09", "2026-01-12"];
const rows = (closes) => closes.map((close, index) => ({date: days[index], close}));
const benchmark = rows([100, 100, 101, 102, 103, 104]);
const items = [
  {label: "winner", rows: rows([10, 11, 12, 13, 14, 15]), entryIndex: 0},
  {label: "bad", rows: rows([20, 20, 19, 18, 17, 16]), entryIndex: 1}
];

const daily = eventTimeCurves(items, benchmark, "daily", null);
assert.equal(daily.measure, "excess");
const winner = daily.series.find((series) => series.label === "winner").points;
assert.equal(winner[0].step, 0);
assert.equal(winner[0].excess_percent, 0, "every pick starts at zero");
// Day 2 for the winner: +20% versus a +1% benchmark move.
assert.ok(Math.abs(winner[2].excess_percent - 19) < 1e-9);
const bad = daily.series.find((series) => series.label === "bad").points;
// The bad pick entered a day later, so its day 1 is 2026-01-07: -5% vs +1%.
assert.ok(Math.abs(bad[1].excess_percent - (-6)) < 1e-9);
const all = daily.series.find((series) => series.label === "all_picks").points;
assert.equal(all[0].sample_count, 2);
assert.equal(all.at(-1).sample_count, 1, "only the older pick reaches day 5");

const capped = eventTimeCurves(items, benchmark, "daily", 2);
assert.equal(capped.series.find((series) => series.label === "winner").points.length, 3, "range caps trading days");

const weekly = eventTimeCurves(items, benchmark, "weekly", null);
assert.deepEqual(weekly.series.find((series) => series.label === "winner").points.map((point) => point.step), [0, 1]);

const noBenchmark = eventTimeCurves(items, [], "daily", null);
assert.equal(noBenchmark.measure, "return", "without a benchmark the chart falls back to raw returns");

console.log("event-time chart tests passed");

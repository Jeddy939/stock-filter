const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const html = fs.readFileSync(path.join(__dirname, "..", "..", "hosting", "index.html"), "utf8");

assert.match(html, /<h3 id="analysisReviewHeading">Needs confirmation<\/h3>/);
assert.match(html, /id="analysisReviewChart"/);
assert.match(html, /label=needs_confirmation&scope=team&horizon=0&limit=5000/);
assert.match(html, /<th>Owner<\/th>/);
assert.match(html, /data-review-owner=/);
assert.match(html, /target_uid:\s*tableRow\.dataset\.reviewOwner/);
assert.ok(html.indexOf('id="analysisReviewHeading"') < html.indexOf('id="analysisChartHeading"'));
assert.match(html, /id="analysisManualTicker"/);
assert.match(html, /id="analysisAddNeeds"/);
assert.match(html, /data-confirm-label="winner"/);
assert.match(html, /data-confirm-label="bad"/);
assert.match(html, /id="analysisWinnerBody"/);
assert.match(html, /id="analysisMaybeBody"/);
assert.match(html, /id="analysisBadBody"/);
assert.doesNotMatch(html, /Winner and potential-winner trends/);
assert.doesNotMatch(html, /Potential Winner/);
assert.match(html, /\.analysis-picks thead\s*\{\s*position:\s*sticky/);
assert.match(html, /scrollbar-gutter:\s*stable/);

for (const key of [
  "market",
  "ticker",
  "event_at_utc",
  "signal_price",
  "latest_price",
  "return_percent",
  "latest_date"
]) {
  assert.match(html, new RegExp(`data-sort-key="${key}"`));
}

for (const group of ["winner", "maybe", "bad"]) {
  assert.match(html, new RegExp(`data-analysis-group="${group}"`));
}

const migration = fs.readFileSync(
  path.join(__dirname, "..", "..", "firebase", "migrations", "009_simplify_appraisal_categories.sql"),
  "utf8"
);
assert.match(migration, /SET label = 'maybe' WHERE label = 'potential_winner'/);
assert.match(migration, /SET label = 'winner' WHERE label = 'confirmed'/);

console.log("analysis UI structure tests passed");

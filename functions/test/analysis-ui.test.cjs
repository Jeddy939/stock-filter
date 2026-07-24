const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const html = fs.readFileSync(path.join(__dirname, "..", "..", "hosting", "index.html"), "utf8");

assert.match(html, /<h3>Needs confirmation<\/h3>/);
assert.match(html, /id="analysisReviewChart"/);
assert.match(html, /label=needs_confirmation&horizon=0&limit=5000/);
assert.match(html, /\.analysis-picks thead\s*\{\s*position:\s*sticky/);
assert.match(html, /scrollbar-gutter:\s*stable/);

for (const key of [
  "market",
  "ticker",
  "label",
  "event_at_utc",
  "signal_price",
  "latest_price",
  "return_percent",
  "latest_date"
]) {
  assert.match(html, new RegExp(`data-sort-key="${key}"`));
}

console.log("analysis UI structure tests passed");

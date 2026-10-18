const assert = require("node:assert/strict");
const {summarizeRuleEvaluation} = require("../lib/api.js");

function row(market, excess, retained) {
  return {market, excess, drawdown: -5, featureDate: "2026-02-02", eventAt: 0, retained};
}

// ASX outcomes sit around 0-30%, US around 100-130%: pooled quartiles would
// call every US pick high and every ASX pick low.
const rows = [
  ...[0, 10, 20, 30].map((excess) => row("asx", excess, excess >= 20)),
  ...[100, 110, 120, 130].map((excess) => row("us", excess, excess >= 120))
];
const summary = summarizeRuleEvaluation(rows);
assert.equal(summary.eligible_count, 8);
assert.equal(summary.retained_count, 4);
assert.deepEqual(Object.keys(summary.cutoffs_by_market).sort(), ["asx", "us"]);
// One high and one low per market (quartiles of four values).
assert.equal(summary.high_retained_count + summary.high_removed_count, 2);
assert.equal(summary.low_retained_count + summary.low_removed_count, 2);
assert.equal(summary.high_removed_count, 0, "the rule keeps each market's best pick");
assert.equal(summary.low_removed_count, 2, "the rule drops each market's worst pick");
assert.equal(summary.filtered_hit_rate, 0.5);
assert.equal(summarizeRuleEvaluation([]), null);

console.log("rule evaluation tests passed");

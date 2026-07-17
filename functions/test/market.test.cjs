const assert = require("node:assert/strict");

const {VALID_LABELS} = require("../lib/market.js");

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

console.log("market label tests passed");

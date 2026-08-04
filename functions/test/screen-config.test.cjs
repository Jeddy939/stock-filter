const assert = require("node:assert/strict");
const {normalizeScreenConfig, screenConfigHash} = require("../lib/screen-config.js");

const legacyPayload = {
  market: "US",
  provider: "YFinance",
  limit: "0",
  query: "  ibrx ",
  volume_multiplier: "2",
  avg_volume_weeks: "52",
  price_avg_weeks: "1",
  lookback_weeks: "1",
  ma_short: "90",
  ma_intermediate: "180",
  ma_medium: "360",
  ma_long: "700",
  scheduled: true
};

const normalizedPayload = {
  market: "us",
  provider: "yfinance",
  limit: 0,
  query: "IBRX",
  volume_multiplier: 2,
  avg_volume_weeks: 52,
  price_avg_weeks: 1,
  lookback_weeks: 1,
  ma_periods: {short: 90, intermediate: 180, medium: 360, long: 700},
  min_market_cap: 0,
  max_market_cap: 0
};

assert.deepEqual(normalizeScreenConfig(legacyPayload), normalizeScreenConfig(normalizedPayload));
assert.equal(screenConfigHash(legacyPayload), screenConfigHash(normalizedPayload));

const withoutScheduled = {...legacyPayload};
delete withoutScheduled.scheduled;
assert.equal(screenConfigHash(legacyPayload), screenConfigHash(withoutScheduled));

const sortedNames = normalizeScreenConfig({
  ma_periods: {long: 700, short: 90, medium: 360, intermediate: 180}
}).ma_periods;
assert.deepEqual(Object.keys(sortedNames), ["intermediate", "long", "medium", "short"]);

console.log("screen-config tests passed");

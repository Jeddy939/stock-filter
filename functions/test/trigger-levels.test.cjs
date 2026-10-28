const assert = require("node:assert/strict");
const {
  addMonths,
  barsFromYahoo,
  evaluateTrigger,
  parseTriggerLines,
  triggerId,
  validateTriggerConfig,
  yahooChartUrl,
  yahooSymbol
} = require("../lib/trigger-levels.js");
const {TRIGGER_SEED_TEXT} = require("../lib/trigger-seed.js");

// Symbols and validation
assert.equal(yahooSymbol("asx", "BHP"), "BHP.AX");
assert.equal(yahooSymbol("crypto", "BTC"), "BTC-USD");
assert.equal(yahooSymbol("us", "BRK.B"), "BRK-B");
assert.equal(triggerId("us", "tsla"), "us__TSLA");
assert.deepEqual(validateTriggerConfig({market: "ASX", ticker: "bhp.ax", ma_period: "240", timeframe: "Monthly"}),
  {ticker: "BHP", market: "asx", ma_period: 240, timeframe: "monthly"});
assert.throws(() => validateTriggerConfig({market: "us", ticker: "MCD", ma_period: 1, timeframe: "weekly"}), /MA length/);
assert.throws(() => validateTriggerConfig({market: "us", ticker: "MCD", ma_period: 700, timeframe: "hourly"}), /Timeframe/);
assert.throws(() => validateTriggerConfig({market: "nyse", ticker: "MCD", ma_period: 700, timeframe: "weekly"}), /Market/);

// Pasted lines
const pasted = parseTriggerLines("Trigger Levels\nMCD 700 Weekly\nNFLX,360,weekly\nGOOG 180W\nbad line 7\nTSLA  360 Weekly", "us");
assert.deepEqual(pasted.entries.map((entry) => `${entry.ticker}:${entry.ma_period}:${entry.timeframe}`),
  ["MCD:700:weekly", "NFLX:360:weekly", "GOOG:180:weekly", "TSLA:360:weekly"]);
assert.equal(pasted.errors.length, 1);
const spreadsheetRow = parseTriggerLines("MCD\t700 Weekly\tMAQ\t700 Weekly\tBTC\t360 Weekly\n\t\tPME\t360 Weekly", "us");
assert.deepEqual(spreadsheetRow.entries.map((entry) => triggerId(entry.market, entry.ticker)),
  ["us__MCD", "asx__MAQ", "crypto__BTC", "asx__PME"]);
assert.equal(parseTriggerLines("BHP\t240 Monthly", "asx").entries[0].market, "asx");

// Brady's list parses cleanly, with duplicates merged
const seed = parseTriggerLines(TRIGGER_SEED_TEXT, "us");
assert.equal(seed.errors.length, 0, JSON.stringify(seed.errors));
const seedById = new Map(seed.entries.map((entry) => [triggerId(entry.market, entry.ticker), entry]));
assert.equal(seedById.size, seed.entries.length);
assert.equal(seedById.get("us__HON").ma_period, 700);
assert.equal(seedById.get("asx__ZIM").timeframe, "weekly");
assert.equal(seedById.get("us__XBI").timeframe, "monthly");
assert.equal(seedById.get("crypto__BTC").ma_period, 360);
assert.equal(seedById.get("asx__4DX").timeframe, "daily");
assert.ok(seedById.has("us__TSLA"));

// Dates
assert.equal(addMonths("2026-08-31", 6), "2027-02-28");
assert.equal(addMonths("2026-10-23", -6), "2026-04-23");

// Yahoo bars: weekly alignment and the live quote merged into the current week
const weekly = barsFromYahoo({
  meta: {gmtoffset: 39600},
  timestamp: [1760878800, 1760930000],
  indicators: {quote: [{open: [60, 59.6], high: [61, 59.8], low: [59, 58.5], close: [59.5, 59.7]}]}
}, "weekly");
assert.equal(weekly.length, 1);
assert.equal(weekly[0].date, "2025-10-20");
assert.equal(weekly[0].low, 58.5);
assert.equal(weekly[0].high, 61);
assert.equal(weekly[0].close, 59.7);
const withNulls = barsFromYahoo({timestamp: [0, 86400], indicators: {quote: [{open: [1, null], high: [2, null], low: [null, null], close: [1.5, null]}]}}, "daily");
assert.deepEqual(withNulls, [{date: "1970-01-01", open: 1, high: 2, low: 1, close: 1.5, volume: 0}]);
assert.match(yahooChartUrl("MCD", "weekly", 700, new Date("2026-10-23T00:00:00Z")), /interval=1wk&period1=-259200&/);

function weeklyBars(closes, lows, lastMonday) {
  const end = new Date(`${lastMonday}T00:00:00Z`);
  return closes.map((close, index) => {
    const date = new Date(end);
    date.setUTCDate(end.getUTCDate() - 7 * (closes.length - 1 - index));
    const low = lows?.[index] ?? close * 0.98;
    return {date: date.toISOString().slice(0, 10), open: close, high: close * 1.02, low, close};
  });
}
const now = new Date("2026-10-23T02:00:00Z");

// Watching: price well above a flat MA
const above = weeklyBars([...Array(370).fill(100), ...Array(40).fill(150)], null, "2026-10-19");
let result = evaluateTrigger(above, 360, "weekly", {now, price: 150});
assert.equal(result.status, "watching");
assert.equal(result.ma_period_used, 360);
assert.equal(result.fallback, false);
assert.ok(Math.abs(result.ma_value - (320 * 100 + 40 * 150) / 360) < 1e-9);
assert.ok(result.distance_pct > 40);

// Touch on the current bar: the low reaches the MA even though the close is above it
const lows = Array(410).fill(null);
lows[409] = 100;
const touch = weeklyBars([...Array(370).fill(100), ...Array(40).fill(150)], lows, "2026-10-19");
result = evaluateTrigger(touch, 360, "weekly", {now});
assert.equal(result.status, "triggered");
assert.equal(result.touching_now, true);
assert.equal(result.last_touch_on, "2026-10-19");
assert.equal(result.active_until, "2027-04-23");

// A touch 20 weeks ago is still active; one 30 weeks ago has expired
for (const [weeksAgo, expected] of [[20, "triggered"], [30, "watching"]]) {
  const pastLows = Array(410).fill(null);
  pastLows[409 - weeksAgo] = 100;
  result = evaluateTrigger(weeklyBars([...Array(380).fill(100), ...Array(30).fill(150)], pastLows, "2026-10-19"), 360, "weekly", {now});
  assert.equal(result.status, expected, `${weeksAgo} weeks ago`);
}

// Not enough history: falls back to every bar available, down to 50 bars
const young = weeklyBars(Array(120).fill(20), null, "2026-10-19");
result = evaluateTrigger(young, 180, "weekly", {now});
assert.equal(result.fallback, true);
assert.equal(result.ma_period_used, 120);
assert.equal(result.status, "triggered");
result = evaluateTrigger(weeklyBars(Array(40).fill(20), null, "2026-10-19"), 180, "weekly", {now});
assert.equal(result.status, "insufficient");
assert.equal(result.bars_available, 40);
assert.equal(result.ma_value, null);
// A short MA (30 Monthly) only needs its own length
result = evaluateTrigger(weeklyBars(Array(35).fill(20), null, "2026-10-19"), 30, "weekly", {now});
assert.notEqual(result.status, "insufficient");
assert.equal(result.fallback, false);

// Chart series match the scanner's MA, including the short-history fallback
{
  const {triggerMaSeries, touchStarts} = require("../lib/trigger-levels.js");
  const history = weeklyBars([...Array(60).fill(10), ...Array(60).fill(20)], null, "2026-10-19");
  const series = triggerMaSeries(history, 180);
  assert.equal(series[48], null);
  assert.equal(series[49], 10);
  const evaluation = evaluateTrigger(history, 180, "weekly", {now});
  assert.ok(Math.abs(series.at(-1) - evaluation.ma_value) < 1e-9);
  const long = weeklyBars(Array(400).fill(5).map((value, index) => value + (index % 7)), null, "2026-10-19");
  const longSeries = triggerMaSeries(long, 360);
  assert.ok(Math.abs(longSeries.at(-1) - evaluateTrigger(long, 360, "weekly", {now}).ma_value) < 1e-9);
  const runs = touchStarts(
    [{date: "a", low: 9}, {date: "b", low: 8}, {date: "c", low: 12}, {date: "d", low: 9}].map((bar) => ({...bar, open: 0, high: 0, close: 0, volume: 0})),
    [10, 10, 10, null]
  );
  assert.deepEqual(runs, ["a"]);
}
console.log("trigger-levels tests passed");

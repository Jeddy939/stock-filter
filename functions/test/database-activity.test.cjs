const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {latestActivity, recordAppActivity} = require("../lib/database-lifecycle.js");

async function main() {
  const writes = [];
  const write = async (at) => writes.push(at.toISOString());
  const start = Date.parse("2026-10-23T00:00:00Z");

  assert.equal(await recordAppActivity(start, write), true);
  assert.equal(await recordAppActivity(start + 30_000, write), false, "writes are throttled to once a minute");
  assert.equal(await recordAppActivity(start + 60_000, write), true);
  assert.deepEqual(writes, ["2026-10-23T00:00:00.000Z", "2026-10-23T00:01:00.000Z"]);

  const job = new Date("2026-10-20T03:00:00Z");
  const app = new Date("2026-10-23T01:00:00Z");
  assert.equal(latestActivity(job, app).toISOString(), app.toISOString());
  assert.equal(latestActivity(null, job).toISOString(), job.toISOString());
  assert.equal(latestActivity(null, undefined, new Date("invalid")), null);

  const weekly = fs.readFileSync(path.join(__dirname, "../src/weekly.ts"), "utf8");
  assert.match(weekly, /latestActivity\(lastJobActivity, await lastAppActivity\(\)\)/, "idle shutdown counts app use");

  const html = fs.readFileSync(path.join(__dirname, "../../hosting/index.html"), "utf8");
  for (const id of ["startLiveDatabase", "stopLiveDatabase", "liveDatabaseStatus"]) {
    assert.match(html, new RegExp(`id="${id}"`), `${id} is in the operations panel`);
  }
  assert.match(html, /\/api\/admin\/\$\{action\}-database/);

  console.log("database activity tests passed");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

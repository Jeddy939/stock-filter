const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {scheduledDatabaseIdleShutdown, weeklyRunId} = require("../lib/weekly.js");

assert.equal(weeklyRunId(new Date("2026-08-04T03:00:00Z")), "2026-08-04");
assert.equal(weeklyRunId(new Date("2026-08-05T03:00:00Z")), "2026-08-04");
assert.equal(weeklyRunId(new Date("2026-08-06T03:00:00Z")), "2026-08-04");
assert.ok(scheduledDatabaseIdleShutdown);

const html = fs.readFileSync(path.join(__dirname, "../../hosting/index.html"), "utf8");
const api = fs.readFileSync(path.join(__dirname, "../src/api.ts"), "utf8");
for (const required of [
  "snapshots/current.json",
  "loadSnapshotChart",
  "runSnapshotScreen",
  "saveLiveAppraisal",
  "team_appraisals",
  "rating_events"
]) {
  assert.ok(html.includes(required), `weekly snapshot UI is missing ${required}`);
}
assert.match(api, /\/api\/snapshot-object/);
assert.match(api, /storageObjectPath\(req\.query\.path, \["snapshots\/"\]\)/);
assert.match(html, /api\/snapshot-object\?path=/);

console.log("weekly snapshot tests passed");

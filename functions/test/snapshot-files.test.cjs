const assert = require("node:assert/strict");
const {gzipSync} = require("node:zlib");
const {isGzip, snapshotCacheControl, SnapshotFileCache} = require("../lib/snapshot-files.js");

assert.equal(isGzip(gzipSync(Buffer.from("{}"))), true);
assert.equal(isGzip(Buffer.from("{}")), false);
assert.equal(snapshotCacheControl("snapshots/versions/20261019T203023Z/charts/asx/BHP.AX.json"), "private, max-age=31536000, immutable");
assert.equal(snapshotCacheControl("snapshots/current.json"), "private, no-cache");

const cache = new SnapshotFileCache(100);
cache.set("snapshots/current.json", Buffer.alloc(10));
assert.equal(cache.size, 0, "the mutable manifest is never cached");
cache.set("snapshots/versions/a", Buffer.alloc(20));
cache.set("snapshots/versions/b", Buffer.alloc(20));
cache.set("snapshots/versions/huge", Buffer.alloc(40));
assert.equal(cache.get("snapshots/versions/huge"), undefined, "files over a quarter of the limit are skipped");
cache.get("snapshots/versions/a");
cache.set("snapshots/versions/c", Buffer.alloc(20));
cache.set("snapshots/versions/d", Buffer.alloc(20));
cache.set("snapshots/versions/e", Buffer.alloc(20));
cache.set("snapshots/versions/f", Buffer.alloc(20));
assert.ok(cache.totalBytes <= 100);
assert.ok(cache.get("snapshots/versions/a"), "recently used files stay");
assert.equal(cache.get("snapshots/versions/b"), undefined, "least recently used files go first");

console.log("snapshot-files tests passed");

const assert = require("node:assert/strict");
const {IpRateLimiter, requestClientIp} = require("../lib/ip-rate-limit.js");

const limiter = new IpRateLimiter({limit: 3, windowMs: 60_000, blockMs: 600_000, maxEntries: 10});
assert.equal(limiter.consume("203.0.113.1", 0).allowed, true);
assert.equal(limiter.consume("203.0.113.1", 1).remaining, 1);
assert.equal(limiter.consume("203.0.113.1", 2).allowed, true);
const blocked = limiter.consume("203.0.113.1", 3);
assert.equal(blocked.allowed, false);
assert.equal(blocked.firstBlockedRequest, true);
assert.equal(blocked.retryAfterSeconds, 600);
assert.equal(limiter.consume("203.0.113.1", 4).firstBlockedRequest, false);
assert.equal(limiter.consume("203.0.113.2", 4).allowed, true);
assert.equal(limiter.consume("203.0.113.1", 600_004).allowed, true);

assert.equal(requestClientIp({
  header: (name) => name === "x-forwarded-for" ? "198.51.100.7, 10.0.0.1" : undefined,
  socket: {remoteAddress: "127.0.0.1"},
  ip: "127.0.0.1"
}), "198.51.100.7");

console.log("IP rate-limit tests passed");

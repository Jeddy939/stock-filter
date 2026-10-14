const assert = require("node:assert/strict");
const {requiresAppCheck} = require("../lib/request-policy.js");

assert.equal(requiresAppCheck("/scheduled-fetch"), false);
assert.equal(requiresAppCheck("scheduled-fetch/"), false);
assert.equal(requiresAppCheck("/filter/start"), true);
assert.equal(requiresAppCheck("/admin/refresh-market"), true);
assert.equal(requiresAppCheck("/markets/status"), true);

console.log("request-policy tests passed");

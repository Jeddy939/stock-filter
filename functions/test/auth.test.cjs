const assert = require("node:assert/strict");

const {ApiError, requireAuth} = require("../lib/auth.js");

const request = {
  header(name) {
    return name.toLowerCase() === "authorization" ? "Bearer test-token" : undefined;
  }
};

async function run() {
  const databaseError = new Error("database unavailable");
  const unavailablePool = {
    async query() {
      throw databaseError;
    }
  };
  const validToken = async () => ({uid: "user-1", email: "owner@example.com"});

  const originalConsoleError = console.error;
  console.error = () => {};
  try {
    await assert.rejects(
      requireAuth(request, unavailablePool, validToken),
      (error) => error instanceof ApiError && error.status === 503 && error.message === "Database temporarily unavailable"
    );
  } finally {
    console.error = originalConsoleError;
  }

  let databaseWasQueried = false;
  const unusedPool = {
    async query() {
      databaseWasQueried = true;
      return {rows: []};
    }
  };
  await assert.rejects(
    requireAuth(request, unusedPool, async () => {
      throw new Error("expired token");
    }),
    (error) => error instanceof ApiError && error.status === 401 && error.message === "Invalid Firebase ID token"
  );
  assert.equal(databaseWasQueried, false);

  console.log("authentication error classification tests passed");
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

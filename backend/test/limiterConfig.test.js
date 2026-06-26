const { loadLimiterConfig } = require("../src/limiterConfig");

test("defaults", () => {
  const c = loadLimiterConfig({});
  expect(c.maxGlobalSessions).toBe(110);
  expect(c.geminiTpmBudget).toBe(60);
  expect(c.reservationLeaseTtl).toBe(45);
  expect(c.heartbeatLeaseTtl).toBe(60);
});

test("env overrides parse to numbers", () => {
  const c = loadLimiterConfig({ MAX_GLOBAL_SESSIONS: "200", STT_STREAM_BUDGET: "80" });
  expect(c.maxGlobalSessions).toBe(200);
  expect(c.sttStreamBudget).toBe(80);
});

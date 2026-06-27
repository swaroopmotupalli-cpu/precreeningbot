const { makeHealthz } = require("../src/health");

function mockRes() {
  return {
    code: null, body: null,
    status(c) { this.code = c; return this; },
    json(b) { this.body = b; return this; },
  };
}

test("healthz 200 when redis ping ok", async () => {
  const redis = { ping: async () => "PONG" };
  const res = mockRes();
  await makeHealthz(redis)({}, res);
  expect(res.code).toBe(200);
  expect(res.body.status).toBe("ok");
});

test("healthz 503 when redis ping fails", async () => {
  const redis = { ping: async () => { throw new Error("down"); } };
  const res = mockRes();
  await makeHealthz(redis)({}, res);
  expect(res.code).toBe(503);
  expect(res.body.status).toBe("unhealthy");
});

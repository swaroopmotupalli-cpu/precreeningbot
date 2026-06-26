// backend/test/limiter.test.js
const { RedisMemoryServer } = require("redis-memory-server");
const Redis = require("ioredis");
const { Limiter } = require("../src/limiter");

let server, redis;
beforeAll(async () => {
  server = new RedisMemoryServer();
  const host = await server.getHost(); const port = await server.getPort();
  redis = new Redis({ host, port });
});
afterAll(async () => { await redis.quit(); await server.stop(); });
beforeEach(async () => { await redis.flushall(); });

const caps = { global: 2, geminiTpm: 2, sttStreams: 2, ttsStreams: 2 };
const mk = () => new Limiter(redis, { caps, reservationTtlMs: 45000 });

test("admits until cap then rejects naming the bucket", async () => {
  const lim = mk();
  expect((await lim.tryAdmit("r1", 1000)).admitted).toBe(true);
  expect((await lim.tryAdmit("r2", 1000)).admitted).toBe(true);
  const r = await lim.tryAdmit("r3", 1000);
  expect(r.admitted).toBe(false);
  expect(r.bucket).toBe("global");
});

test("reconnect refreshes without double-count", async () => {
  const lim = mk();
  await lim.tryAdmit("r1", 1000);
  const again = await lim.tryAdmit("r1", 5000);
  expect(again.state).toBe("refreshed");
  expect(Number(await redis.get("{tara-limiter}:count:global"))).toBe(1);
});

test("custom prefix is honored in keys and ARGV", async () => {
  const customPrefix = "{tara-test}";
  const lim = new Limiter(redis, { prefix: customPrefix, caps, reservationTtlMs: 45000 });
  await lim.tryAdmit("r1", 1000);
  expect(Number(await redis.get(`${customPrefix}:count:global`))).toBe(1);
  expect(await redis.get("{tara-limiter}:count:global")).toBe(null);
});

// backend/test/scorer-queue.test.js
const { consumeOnce } = require("../src/scorer/queue");

function fakeRedis(initial) {
  const lists = { ...initial };
  const counters = {};
  return {
    lists, counters,
    brpop: async (q) => (lists[q] && lists[q].length ? [q, lists[q].shift()] : null),
    lpush: async (q, v) => { (lists[q] = lists[q] || []).unshift(v); },
    incr: async (k) => (counters[k] = (counters[k] || 0) + 1),
    del: async (k) => { delete counters[k]; },
  };
}

const OPTS = { queue: "q", dlq: "dlq", maxAttempts: 3 };

test("processes one message with the handler", async () => {
  const r = fakeRedis({ q: ["s1"] });
  const seen = [];
  const id = await consumeOnce(r, async (s) => seen.push(s), OPTS);
  expect(id).toBe("s1");
  expect(seen).toEqual(["s1"]);
});

test("re-enqueues on failure, then DLQs after maxAttempts", async () => {
  const r = fakeRedis({ q: ["s1"] });
  const boom = async () => { throw new Error("fail"); };
  await consumeOnce(r, boom, OPTS); // attempt 1 → requeue
  await consumeOnce(r, boom, OPTS); // attempt 2 → requeue
  await consumeOnce(r, boom, OPTS); // attempt 3 → DLQ
  expect(r.lists.dlq).toEqual(["s1"]);
  expect(r.lists.q.length).toBe(0);
});

test("timeout returns null", async () => {
  const r = fakeRedis({});
  expect(await consumeOnce(r, async () => {}, OPTS)).toBeNull();
});

test("clears attempts counter after successful process", async () => {
  const r = fakeRedis({ q: ["s1"] });
  r.counters["tara:score:attempts:s1"] = 2;
  await consumeOnce(r, async () => {}, OPTS);
  expect(r.counters["tara:score:attempts:s1"]).toBeUndefined();
});

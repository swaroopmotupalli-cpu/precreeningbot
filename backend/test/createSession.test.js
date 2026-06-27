const { createSession } = require("../src/createSession");

const fakeLimiterAdmitted = { tryAdmit: async () => ({ admitted: true, state: "new" }) };

test("writes session blob to redis and returns token", async () => {
  const store = {};
  const redis = {
    set: async (k, v, ...rest) => { store[k] = v; },
  };
  const tokenFactory = (room, identity) => `tok:${room}:${identity}`;
  const out = await createSession(redis, tokenFactory, fakeLimiterAdmitted, {
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j",
  });
  expect(out.room).toBe(out.sessionId);
  expect(out.token).toBe(`tok:${out.room}:u1`);
  expect(out.status).toBe("admitted");
  const blob = JSON.parse(store[`session:${out.sessionId}`]);
  expect(blob).toMatchObject({
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j", maxQuestions: 12,
  });
});

test("rejects missing required fields", async () => {
  await expect(createSession({}, () => "t", fakeLimiterAdmitted, { contestId: "c1" }))
    .rejects.toThrow(/required/);
});

test("rejected admission returns queued with bucket and no token", async () => {
  const redis = { set: async () => {}, expire: async () => {} };
  const limiter = { tryAdmit: async () => ({ admitted: false, bucket: "gemini_tpm" }) };
  const out = await createSession(redis, () => "tok", limiter, {
    contestId: "c", candidateId: "u", skills: ["x"], resumeText: "r", jdText: "j" });
  expect(out.status).toBe("queued");
  expect(out.bucket).toBe("gemini_tpm");
  expect(out.token).toBeUndefined();
});

test("rejected session blob is re-expired to a short TTL", async () => {
  const calls = [];
  const redis = {
    set: async (k, v, ...rest) => { calls.push(["set", k, ...rest]); },
    expire: async (k, ttl) => { calls.push(["expire", k, ttl]); },
  };
  const fakeLimiterRejected = { tryAdmit: async () => ({ admitted: false, bucket: "global" }) };
  const tokenFactory = () => "tok";
  const out = await createSession(redis, tokenFactory, fakeLimiterRejected, {
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j",
  }, { rejectedBlobTtl: 120 });
  expect(out.status).toBe("queued");
  // the blob TTL was shortened on reject
  const expireCall = calls.find((c) => c[0] === "expire");
  expect(expireCall).toBeTruthy();
  expect(expireCall[2]).toBe(120);
});

test("stores recruiterId and jsId in the session blob", async () => {
  const store = {};
  const redis = { set: async (k, v) => { store[k] = v; } };
  const fakeLimiter = { tryAdmit: async () => ({ admitted: true, state: "new" }) };
  const out = await createSession(redis, () => "tok", fakeLimiter, {
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j", recruiterId: "rec1", jsId: "js1",
  });
  const blob = JSON.parse(store[`session:${out.sessionId}`]);
  expect(blob.recruiterId).toBe("rec1");
  expect(blob.jsId).toBe("js1");
});

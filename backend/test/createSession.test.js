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
  const redis = { set: async () => {} };
  const limiter = { tryAdmit: async () => ({ admitted: false, bucket: "gemini_tpm" }) };
  const out = await createSession(redis, () => "tok", limiter, {
    contestId: "c", candidateId: "u", skills: ["x"], resumeText: "r", jdText: "j" });
  expect(out.status).toBe("queued");
  expect(out.bucket).toBe("gemini_tpm");
  expect(out.token).toBeUndefined();
});

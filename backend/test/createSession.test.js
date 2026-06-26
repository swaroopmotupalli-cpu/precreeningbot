const { createSession } = require("../src/createSession");

test("writes session blob to redis and returns token", async () => {
  const store = {};
  const redis = {
    set: async (k, v, ...rest) => { store[k] = v; },
  };
  const tokenFactory = (room, identity) => `tok:${room}:${identity}`;
  const out = await createSession(redis, tokenFactory, {
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j",
  });
  expect(out.room).toBe(out.sessionId);
  expect(out.token).toBe(`tok:${out.room}:u1`);
  const blob = JSON.parse(store[`session:${out.sessionId}`]);
  expect(blob).toMatchObject({
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j", maxQuestions: 12,
  });
});

test("rejects missing required fields", async () => {
  await expect(createSession({}, () => "t", { contestId: "c1" }))
    .rejects.toThrow(/required/);
});

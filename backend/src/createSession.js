const crypto = require("crypto");
const REQUIRED = ["contestId", "candidateId", "skills", "resumeText", "jdText"];

async function createSession(redis, tokenFactory, body) {
  for (const f of REQUIRED) {
    if (body[f] === undefined) throw new Error(`field ${f} is required`);
  }
  const sessionId = crypto.randomUUID();
  const blob = {
    contestId: body.contestId, candidateId: body.candidateId,
    skills: body.skills, resumeText: body.resumeText, jdText: body.jdText,
    maxQuestions: body.maxQuestions ?? 12, status: "created",
  };
  await redis.set(`session:${sessionId}`, JSON.stringify(blob), "EX", 7200);
  const token = await tokenFactory(sessionId, body.candidateId);
  return { sessionId, room: sessionId, token, livekitUrl: process.env.LIVEKIT_URL };
}

module.exports = { createSession };

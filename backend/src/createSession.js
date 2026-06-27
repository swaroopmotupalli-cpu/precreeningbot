const crypto = require("crypto");
const REQUIRED = ["contestId", "candidateId", "skills", "resumeText", "jdText"];

async function createSession(redis, tokenFactory, limiter, body, opts = {}) {
  for (const f of REQUIRED) {
    if (body[f] === undefined) throw new Error(`field ${f} is required`);
  }
  const rejectedBlobTtl = opts.rejectedBlobTtl ?? 120;
  const sessionId = crypto.randomUUID();
  const blob = {
    contestId: body.contestId, candidateId: body.candidateId,
    skills: body.skills, resumeText: body.resumeText, jdText: body.jdText,
    recruiterId: body.recruiterId ?? "", jsId: body.jsId ?? "",
    maxQuestions: body.maxQuestions ?? 12, status: "created",
  };
  await redis.set(`session:${sessionId}`, JSON.stringify(blob), "EX", 7200);

  const admit = await limiter.tryAdmit(sessionId, Date.now());
  if (!admit.admitted) {
    // Don't let rejected attempts linger 2h — expire promptly (no slot leak fix).
    await redis.expire(`session:${sessionId}`, rejectedBlobTtl);
    return { sessionId, status: "queued", bucket: admit.bucket };   // starting shortly, NO token
  }
  const token = await tokenFactory(sessionId, body.candidateId);
  return { sessionId, room: sessionId, token,
           livekitUrl: process.env.LIVEKIT_URL, status: "admitted" };
}

module.exports = { createSession };

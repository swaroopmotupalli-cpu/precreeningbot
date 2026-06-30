const crypto = require("crypto");
const { loadContext } = require("./marketplace");

// New contract: the caller supplies only IDs; JD + resume are derived from the
// Marketplace DB. opts.db must be the "Marketplace" database handle.
const REQUIRED = ["contestId", "jsId", "recruiterId"];

async function createSession(redis, tokenFactory, limiter, body, opts = {}) {
  for (const f of REQUIRED) {
    if (body[f] === undefined || body[f] === "") throw new Error(`field ${f} is required`);
  }
  if (!opts.db) throw new Error("Marketplace db handle is required (opts.db)");
  const rejectedBlobTtl = opts.rejectedBlobTtl ?? 120;

  // Fetch + compose JD/resume/skills from Marketplace (throws NotFoundError on
  // missing/invalid contest or jobseeker — surfaced to the caller for a 404).
  const ctx = await loadContext(opts.db, body.contestId, body.jsId);

  const sessionId = crypto.randomUUID();
  const blob = {
    contestId: body.contestId,
    candidateId: body.jsId,        // the jobseeker is the candidate
    jsId: body.jsId,
    recruiterId: body.recruiterId,
    candidateName: ctx.candidateName,
    jobTitle: ctx.jobTitle,
    skills: ctx.skills,            // must-have skills → coverage tracking
    resumeText: ctx.resumeText,
    jdText: ctx.jdText,
    maxQuestions: body.maxQuestions ?? 12,
    status: "created",
  };
  await redis.set(`session:${sessionId}`, JSON.stringify(blob), "EX", 7200);

  const admit = await limiter.tryAdmit(sessionId, Date.now());
  if (!admit.admitted) {
    await redis.expire(`session:${sessionId}`, rejectedBlobTtl);
    return { sessionId, status: "queued", bucket: admit.bucket };
  }
  const token = await tokenFactory(sessionId, body.jsId);
  return {
    sessionId, room: sessionId, token, livekitUrl: process.env.LIVEKIT_URL,
    status: "admitted", jobTitle: ctx.jobTitle, candidateName: ctx.candidateName,
  };
}

module.exports = { createSession };

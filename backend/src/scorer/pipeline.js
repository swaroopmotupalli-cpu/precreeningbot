// backend/src/scorer/pipeline.js
const { loadInterview, buildQAPairs } = require("./transcript");
const { loadSkills } = require("./skills");
const { scoreInterview } = require("./score");
const { persistReport } = require("./persist");

async function scoreSession({ db, geminiCall }, sessionId) {
  const doc = await loadInterview(db, sessionId);
  if (!doc) return { ok: false, reason: "not_found" };
  const qaPairs = buildQAPairs(doc.transcript || []);
  const { mustHave, goodToHave } = await loadSkills(db, doc.contestId, doc.skills || []);
  // scoreInterview returns the full { report: {prescreeningreport}, verdict }.
  const { report, verdict } = await scoreInterview(geminiCall, {
    resumeText: doc.resumeText || "", jdText: doc.jdText || "",
    qaPairs, mustHave, goodToHave,
  });
  const { ats } = await persistReport(db, {
    sessionId, contestId: doc.contestId, candidateId: doc.candidateId,
    recruiterId: doc.recruiterId || "", jsId: doc.jsId || "", report, verdict,
  });
  return { ok: true, ats, overall_score: verdict.overall_score };
}

module.exports = { scoreSession };

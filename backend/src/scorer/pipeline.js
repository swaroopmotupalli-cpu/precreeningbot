// backend/src/scorer/pipeline.js
const { loadInterview, buildQAPairs } = require("./transcript");
const { loadSkills } = require("./skills");
const { scoreInterview, computeVerdict } = require("./score");
const { persistReport } = require("./persist");

async function scoreSession({ db, geminiCall }, sessionId) {
  const doc = await loadInterview(db, sessionId);
  if (!doc) return { ok: false, reason: "not_found" };
  const qaPairs = buildQAPairs(doc.transcript || []);
  const { mustHave, goodToHave } = await loadSkills(db, doc.contestId, doc.skills || []);
  const report = await scoreInterview(geminiCall, {
    resumeText: doc.resumeText || "", jdText: doc.jdText || "",
    qaPairs, mustHave, goodToHave,
  });
  const verdict = computeVerdict(report);
  report.recommendation = verdict.recommendation; // keep report + verdict consistent
  const { ats } = await persistReport(db, {
    sessionId, contestId: doc.contestId, candidateId: doc.candidateId,
    recruiterId: doc.recruiterId || "", jsId: doc.jsId || "", report, verdict,
  });
  return { ok: true, ats };
}

module.exports = { scoreSession };

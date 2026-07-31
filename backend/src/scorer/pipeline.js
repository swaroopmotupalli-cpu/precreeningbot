// backend/src/scorer/pipeline.js
const { loadInterview, buildQAPairs } = require("./transcript");
const { loadSkills } = require("./skills");
const { scoreInterview } = require("./score");
const { persistReport } = require("./persist");

async function scoreSession({ db, geminiCall }, sessionId) {
  const doc = await loadInterview(db, sessionId);
  if (!doc) return { ok: false, reason: "not_found" };
  // Deterministic pairing (a Tara line only starts a new question if it
  // contains "?" — see transcript.js) — the model only cleans up question
  // wording afterward, it does not decide how many questions there were or
  // which answer goes where.
  const qaPairs = buildQAPairs(doc.transcript || []);
  const { mustHave, goodToHave } = await loadSkills(db, doc.contestId, doc.skills || []);
  // scoreInterview returns the full { report: {prescreeningreport}, verdict }.
  const { report, verdict } = await scoreInterview(geminiCall, {
    resumeText: doc.resumeText || "", jdText: doc.jdText || "",
    qaPairs, mustHave, goodToHave, rawTranscript: doc.transcript || [],
  });
  const { ats } = await persistReport(db, {
    sessionId, contestId: doc.contestId, candidateId: doc.candidateId,
    recruiterId: doc.recruiterId || "", jsId: doc.jsId || "", report, verdict,
  });
  return { ok: true, ats, overall_score: verdict.overall_score };
}

module.exports = { scoreSession };

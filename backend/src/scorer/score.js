function fallbackReport(mustHave, goodToHave) {
  const rate = (skills) => (skills || []).slice(0, 10).map((s) => ({ skill: s, rating: 2.5 }));
  return {
    overall_evaluation: "Automated fallback: the model response could not be parsed.",
    recommendation: "Maybe",
    key_strengths: ["Interview participation"],
    remarks: { communication: "Not assessed (fallback)." },
    primarySkillsRatings: rate(mustHave),
    secondarySkillsRatings: rate(goodToHave),
    comment: "Fallback report generated; manual review recommended.",
  };
}

function buildPrompt({ resumeText, jdText, qaPairs, mustHave, goodToHave }) {
  const qa = (qaPairs || [])
    .map((p, i) => `Q${i + 1}: ${p.question}\nA${i + 1}: ${p.answer}`)
    .join("\n");
  return `Expert technical interview analysis. Return ONLY valid JSON.
JOB DESCRIPTION:\n${jdText || ""}\nRESUME:\n${resumeText || ""}
MUST-HAVE SKILLS: ${(mustHave || []).join(", ")}
GOOD-TO-HAVE SKILLS: ${(goodToHave || []).join(", ")}
TRANSCRIPT (${(qaPairs || []).length} questions):\n${qa}

Return JSON exactly:
{
  "overall_evaluation": "3-4 sentence summary",
  "recommendation": "Strong Hire|Hire|Maybe|No Hire",
  "key_strengths": ["s1","s2","s3"],
  "remarks": { "communication": "string" },
  "primarySkillsRatings":   [{"skill":"...","rating": 4.0}],
  "secondarySkillsRatings": [{"skill":"...","rating": 3.0}],
  "comment": "one paragraph recommendation"
}
Rules: Rate ALL listed skills 0-5. remarks.communication = STRING. ratings = NUMBERS.`;
}

function parseReport(rawText, { mustHave, goodToHave }) {
  try {
    let s = (rawText || "").trim();
    for (const fence of ["```json", "```"]) {
      if (s.startsWith(fence)) s = s.slice(fence.length);
    }
    if (s.endsWith("```")) s = s.slice(0, -3);
    const r = JSON.parse(s.trim());
    if (r.remarks && r.remarks.communication != null) {
      r.remarks.communication = String(r.remarks.communication);
    }
    for (const key of ["primarySkillsRatings", "secondarySkillsRatings"]) {
      if (Array.isArray(r[key])) {
        r[key] = r[key].map((it) => ({ skill: it.skill, rating: Number(it.rating) || 0 }));
      } else {
        r[key] = [];
      }
    }
    if (!r.recommendation) r.recommendation = "Maybe";
    return r;
  } catch (e) {
    return fallbackReport(mustHave, goodToHave);
  }
}

function computeVerdict(report) {
  const ratings = (report.primarySkillsRatings || []).map((r) => Number(r.rating) || 0);
  const meanRating = ratings.length ? ratings.reduce((a, b) => a + b, 0) / ratings.length : 0;
  const avg = meanRating * 2; // 0-5 skill scale → 0-10
  let recommendation;
  if (avg >= 8.5) recommendation = "Strong Hire";
  else if (avg >= 7) recommendation = "Hire";
  else if (avg >= 5.5) recommendation = "Maybe";
  else recommendation = "No Hire";
  return { avg, recommendation, empStatus: "Completed", copilotScore: Math.round(avg * 10) };
}

async function scoreInterview(geminiCall, args) {
  const raw = await geminiCall(buildPrompt(args));
  return parseReport(raw, { mustHave: args.mustHave, goodToHave: args.goodToHave });
}

module.exports = { buildPrompt, parseReport, computeVerdict, scoreInterview, fallbackReport };

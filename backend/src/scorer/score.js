// Scoring → the rich "prescreeningreport" the ATS expects: per-question scores +
// keywords (detailed_qa), interview_statistics, ai_analysis, skill ratings, remarks.

function fmtTimestamp(d) {
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function fallbackAnalysis(qaPairs, mustHave, goodToHave) {
  const rate = (skills) => (skills || []).slice(0, 10).map((s) => ({ skill: s, rating: 2.5 }));
  return {
    questions: (qaPairs || []).map(() => ({ score: 3, keywords: [] })),
    overall_evaluation: "Automated fallback: the model response could not be parsed; manual review recommended.",
    key_strengths: ["Completed the interview session"],
    areas_for_improvement: ["Manual review needed — automated analysis unavailable"],
    remarks: { communication: "Not assessed (fallback)." },
    primarySkillsRatings: rate(mustHave),
    secondarySkillsRatings: rate(goodToHave),
    comment: "Fallback report generated; manual review recommended.",
  };
}

function buildPrompt({ resumeText, jdText, qaPairs, mustHave, goodToHave }) {
  const qa = (qaPairs || [])
    .map((p, i) => `Q${i + 1}: ${p.question}\nA${i + 1}: ${p.answer || "(no answer)"}`)
    .join("\n\n");
  return `You are Tara, a Senior Technical Interviewer. Analyze this interview transcript and return ONLY valid JSON.

JOB DESCRIPTION:
${jdText || ""}

RESUME:
${resumeText || ""}

MUST-HAVE SKILLS: ${(mustHave || []).join(", ")}
GOOD-TO-HAVE SKILLS: ${(goodToHave || []).join(", ")}

TRANSCRIPT (${(qaPairs || []).length} questions):
${qa}

Return JSON EXACTLY in this shape:
{
  "questions": [ { "score": <1-10 integer>, "keywords": ["k1","k2","k3"] } ],  // one object per question, IN ORDER (Q1 first)
  "overall_evaluation": "3-4 sentence summary of performance",
  "key_strengths": ["s1","s2","s3"],
  "areas_for_improvement": ["a1","a2","a3"],
  "remarks": { "communication": "<1-5 rating as string with brief note>" },
  "primarySkillsRatings":   [ {"skill":"<must-have>", "rating": <0-5>} ],
  "secondarySkillsRatings": [ {"skill":"<good-to-have>", "rating": <0-5>} ],
  "comment": "one-paragraph hiring recommendation"
}
Rules: "questions" MUST have exactly ${(qaPairs || []).length} items in order. Score each answer 1-10 (technical correctness + depth + relevance). keywords = 3-5 key technical terms the answer touched. Rate ALL listed skills 0-5. remarks.communication is a STRING. All ratings/scores are NUMBERS.`;
}

function clampScore(n) {
  const v = Math.round(Number(n) || 0);
  return Math.max(1, Math.min(10, v));
}

function parseAnalysis(rawText, qaPairs, { mustHave, goodToHave }) {
  const n = (qaPairs || []).length;
  try {
    let s = (rawText || "").trim();
    for (const fence of ["```json", "```"]) if (s.startsWith(fence)) s = s.slice(fence.length);
    if (s.endsWith("```")) s = s.slice(0, -3);
    const r = JSON.parse(s.trim());

    // questions[] aligned to qaPairs length (pad/truncate, coerce).
    const q = Array.isArray(r.questions) ? r.questions : [];
    const questions = [];
    for (let i = 0; i < n; i++) {
      const item = q[i] || {};
      questions.push({ score: clampScore(item.score), keywords: Array.isArray(item.keywords) ? item.keywords.map(String) : [] });
    }
    if (r.remarks && r.remarks.communication != null) r.remarks.communication = String(r.remarks.communication);
    else r.remarks = { communication: "Not assessed." };
    const fixRatings = (key) => (Array.isArray(r[key]) ? r[key].map((it) => ({ skill: it.skill, rating: Number(it.rating) || 0 })) : []);
    return {
      questions,
      overall_evaluation: String(r.overall_evaluation || ""),
      key_strengths: Array.isArray(r.key_strengths) ? r.key_strengths.map(String) : [],
      areas_for_improvement: Array.isArray(r.areas_for_improvement) ? r.areas_for_improvement.map(String) : [],
      remarks: r.remarks,
      primarySkillsRatings: fixRatings("primarySkillsRatings"),
      secondarySkillsRatings: fixRatings("secondarySkillsRatings"),
      comment: String(r.comment || ""),
    };
  } catch (e) {
    return fallbackAnalysis(qaPairs, mustHave, goodToHave);
  }
}

function scoreDistribution(scores) {
  const d = { excellent_9_10: 0, good_7_8: 0, average_5_6: 0, below_average_1_4: 0 };
  for (const s of scores) {
    if (s >= 9) d.excellent_9_10++;
    else if (s >= 7) d.good_7_8++;
    else if (s >= 5) d.average_5_6++;
    else d.below_average_1_4++;
  }
  return d;
}

function computeStats(scores) {
  const total = scores.length;
  const overall = total ? Math.round((scores.reduce((a, b) => a + b, 0) / total) * 10) / 10 : 0;
  return { total_questions: total, overall_score: overall, score_distribution: scoreDistribution(scores) };
}

function verdict(overallScore) {
  let recommendation;
  if (overallScore >= 8.5) recommendation = "Strong Hire";
  else if (overallScore >= 7) recommendation = "Hire";
  else if (overallScore >= 5.5) recommendation = "Maybe";
  else recommendation = "No Hire";
  return { overall_score: overallScore, recommendation, copilotScore: Math.round(overallScore * 10), empStatus: "Completed" };
}

function buildReport({ qaPairs, analysis, jdText, resumeText, now }) {
  const scores = analysis.questions.map((q) => q.score);
  const stats = computeStats(scores);
  const v = verdict(stats.overall_score);
  const detailed_qa = (qaPairs || []).map((qa, i) => ({
    question: qa.question,
    answer: qa.answer,
    score: analysis.questions[i] ? analysis.questions[i].score : 0,
    keywords: analysis.questions[i] ? analysis.questions[i].keywords : [],
  }));
  const report = {
    prescreeningreport: {
      timestamp: fmtTimestamp(now || new Date()),
      interviewer: "Tara (Senior Technical Interviewer)",
      candidate_details: {
        resume_summary: resumeText || "",
        job_description: jdText || "",
        interview_type: "technical",
      },
      interview_statistics: stats,
      ai_analysis: {
        overall_evaluation: analysis.overall_evaluation,
        recommendation: v.recommendation,
        key_strengths: analysis.key_strengths,
        areas_for_improvement: analysis.areas_for_improvement,
      },
      remarks: analysis.remarks,
      primarySkillsRatings: analysis.primarySkillsRatings,
      secondarySkillsRatings: analysis.secondarySkillsRatings,
      comment: analysis.comment,
      detailed_qa,
    },
  };
  return { report, verdict: v };
}

async function scoreInterview(geminiCall, args) {
  const raw = await geminiCall(buildPrompt(args));
  const analysis = parseAnalysis(raw, args.qaPairs, { mustHave: args.mustHave, goodToHave: args.goodToHave });
  return buildReport({ qaPairs: args.qaPairs, analysis, jdText: args.jdText, resumeText: args.resumeText, now: args.now });
}

module.exports = {
  fmtTimestamp, fallbackAnalysis, buildPrompt, parseAnalysis,
  scoreDistribution, computeStats, verdict, buildReport, scoreInterview,
};

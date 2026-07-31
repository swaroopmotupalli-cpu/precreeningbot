// Scoring → the rich "prescreeningreport" the ATS expects: per-question
// question/answer (detailed_qa), a single holistic overall_rating,
// ai_analysis, skill ratings, remarks.
//
// Q&A pairing itself is deterministic (buildQAPairs in transcript.js), which
// skips Tara lines flagged `interrupted` (set live, at the moment a barge-in
// cuts her off — see TranscriptStore.truncate_last) so a mid-sentence
// interjection never fractures one answer into two entries or gets guessed
// at. The model's only job here is to clean up the wording of each already
//-correctly-paired question (the raw line can still be an informal
// acknowledgment+question combo) and produce the holistic rating/analysis —
// NOT to decide how many questions there were or which answer goes where.
const { mergeTurns } = require("./transcript");

function fmtTimestamp(d) {
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function fallbackAnalysis(qaPairs, mustHave, goodToHave) {
  const rate = (skills) => (skills || []).slice(0, 10).map((s) => ({ skill: s, rating: 2.5 }));
  return {
    questions: (qaPairs || []).map(() => ({ question: "" })),
    overall_rating: 30,
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
  // The "Q" lines are already correctly paired with their answers (interrupted
  // fragments were filtered out before this point) — they can still be
  // informally phrased or combined with a leading acknowledgment, so the
  // model reconstructs clean question wording, but it does NOT decide pairing.
  const qa = (qaPairs || [])
    .map((p, i) => `Q${i + 1} (raw, may be informal or combined with an acknowledgment): ${p.question}\nA${i + 1}: ${p.answer || "(no answer)"}`)
    .join("\n\n");
  return `You are Tara, a Senior Technical Interviewer. Analyze this interview transcript and return ONLY valid JSON.

IMPORTANT — the transcript is AUTOMATED speech-to-text of a spoken interview. It
WILL contain transcription errors, mis-heard words, missing punctuation, and
fragmented phrasing the candidate did not actually intend. Score based on the
candidate's evident KNOWLEDGE and INTENT, not on transcription artifacts:
- Do NOT penalize a technical answer just because the wording looks broken or
  garbled — infer what they clearly meant and judge the substance.
- Only score low when the CONTENT is genuinely weak, wrong, or absent — never
  because the text merely reads awkwardly due to speech-to-text noise.
- If part of an answer appears mis-transcribed into another language/script,
  interpret it as the intended English technical term when judging.
- If a candidate's turn is only asking you to repeat/clarify (e.g. "repeat
  that", "pardon", "say again", "I didn't catch that") treat that specific
  turn as noise, not content — judge the answer using their other turns for
  that same question, not this one.
Ground every judgment strictly in what the transcript shows — do not invent
or assume evidence that isn't there.

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
  "questions": [ { "question": "<the full, real interview question being answered>", "answer": "<2-3 sentence summary of what the candidate actually said>" } ],  // one object per question, IN ORDER (Q1 first)
  "overall_rating": <0-100 integer>,
  "overall_evaluation": "3-4 sentence summary of performance",
  "key_strengths": ["s1","s2","s3"],
  "areas_for_improvement": ["a1","a2","a3"],
  "remarks": { "communication": "<1-5 rating as string with brief note>" },
  "primarySkillsRatings":   [ {"skill":"<must-have>", "rating": <0-5>} ],
  "secondarySkillsRatings": [ {"skill":"<good-to-have>", "rating": <0-5>} ],
  "comment": "one-paragraph hiring recommendation"
}
Rules: "questions" MUST have exactly ${(qaPairs || []).length} items in order — do NOT merge, split, add, or drop items; each corresponds 1:1 to the numbered Q/A pair above. For each, restate the ACTUAL question cleanly (strip any leading acknowledgment, fix informal phrasing) without changing which pair it belongs to. For "answer", summarize in your OWN words what the candidate actually said (2-3 sentences) — preserve every concrete technical detail and their real position (tool names, approaches, numbers, examples); do NOT embellish, invent, or improve on what they said; if they said they didn't know or had no experience, state that plainly rather than softening it; ignore pure noise turns (e.g. "can you repeat that?") when summarizing — base the summary only on their substantive answer content. "overall_rating" is a SINGLE holistic 0-100 score for the entire interview — do not average per-question scores; instead judge as a whole: did the candidate answer correctly, did the conversation cover BOTH the must-have and good-to-have skills listed above, and how well did they communicate throughout (communication = clarity and organization of their IDEAS, not speech-to-text quality). Rate ALL listed skills 0-5 based on evidence in the answers — 0 if a skill was never demonstrated or discussed, not as a penalty. remarks.communication is a STRING. All ratings/scores are NUMBERS. Write the entire report in English, translating any non-English fragments.`;
}

function clampRating(n) {
  const v = Math.round(Number(n) || 0);
  return Math.max(0, Math.min(100, v));
}

function parseAnalysis(rawText, qaPairs, { mustHave, goodToHave }) {
  const n = (qaPairs || []).length;
  try {
    let s = (rawText || "").trim();
    for (const fence of ["```json", "```"]) if (s.startsWith(fence)) s = s.slice(fence.length);
    if (s.endsWith("```")) s = s.slice(0, -3);
    const r = JSON.parse(s.trim());

    // questions[] aligned to qaPairs length (pad/truncate, coerce) — pairing
    // itself is already correct (deterministic), this only cleans wording
    // and summarizes the answer; it never changes which pair is which.
    const q = Array.isArray(r.questions) ? r.questions : [];
    const questions = [];
    for (let i = 0; i < n; i++) {
      const item = q[i] || {};
      questions.push({
        question: item.question ? String(item.question).trim() : "",
        answer: item.answer ? String(item.answer).trim() : "",
      });
    }
    if (r.remarks && r.remarks.communication != null) r.remarks.communication = String(r.remarks.communication);
    else r.remarks = { communication: "Not assessed." };
    const fixRatings = (key) => (Array.isArray(r[key]) ? r[key].map((it) => ({ skill: it.skill, rating: Number(it.rating) || 0 })) : []);
    return {
      questions,
      overall_rating: clampRating(r.overall_rating),
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

function verdict(overallRating) {
  let recommendation;
  if (overallRating >= 85) recommendation = "Strong Hire";
  else if (overallRating >= 70) recommendation = "Hire";
  else if (overallRating >= 55) recommendation = "Maybe";
  else recommendation = "No Hire";
  return { overall_score: overallRating, recommendation, copilotScore: overallRating, empStatus: "Completed" };
}

function buildReport({ qaPairs, analysis, jdText, resumeText, now, rawTranscript }) {
  const v = verdict(analysis.overall_rating);
  const detailed_qa = (qaPairs || []).map((qa, i) => {
    const a = analysis.questions[i];
    // Prefer the model's cleaned-up question wording and summarized answer;
    // fall back to the raw paired line only if the model didn't provide one
    // (e.g. fallback path) — pairing itself is untouched either way.
    const question = (a && a.question) ? a.question : qa.question;
    const answer = (a && a.answer) ? a.answer : qa.answer;
    return { question, answer };
  });
  const report = {
    prescreeningreport: {
      timestamp: fmtTimestamp(now || new Date()),
      interviewer: "Tara (Senior Technical Interviewer)",
      candidate_details: {
        resume_summary: resumeText || "",
        job_description: jdText || "",
        interview_type: "technical",
      },
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
      // Full conversation, same speaker-fragments merged into one turn each —
      // an audit/display copy alongside the Q&A pairs, not used for scoring.
      transcript: mergeTurns(rawTranscript || []),
    },
  };
  return { report, verdict: v };
}

async function scoreInterview(geminiCall, args) {
  const raw = await geminiCall(buildPrompt(args));
  const analysis = parseAnalysis(raw, args.qaPairs, { mustHave: args.mustHave, goodToHave: args.goodToHave });
  return buildReport({
    qaPairs: args.qaPairs, analysis, jdText: args.jdText, resumeText: args.resumeText,
    now: args.now, rawTranscript: args.rawTranscript,
  });
}

module.exports = {
  fmtTimestamp, fallbackAnalysis, buildPrompt, parseAnalysis,
  verdict, buildReport, scoreInterview,
};

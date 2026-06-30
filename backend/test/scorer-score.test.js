const { parseAnalysis, scoreDistribution, computeStats, verdict, buildReport, scoreInterview } = require("../src/scorer/score");

const QA = [
  { question: "Tell me about yourself", answer: "I am a Python dev" },
  { question: "Explain Docker", answer: "No idea" },
];

test("scoreDistribution buckets 1-10 scores", () => {
  expect(scoreDistribution([10, 9, 8, 7, 6, 5, 4, 1])).toEqual({
    excellent_9_10: 2, good_7_8: 2, average_5_6: 2, below_average_1_4: 2,
  });
});

test("computeStats: total, rounded overall, distribution", () => {
  const s = computeStats([4, 4, 3, 3, 2, 2]); // mean 3.0
  expect(s.total_questions).toBe(6);
  expect(s.overall_score).toBe(3);
  expect(s.score_distribution.below_average_1_4).toBe(6);
});

test("verdict thresholds + copilotScore", () => {
  expect(verdict(9).recommendation).toBe("Strong Hire");
  expect(verdict(7.2).recommendation).toBe("Hire");
  expect(verdict(5.6).recommendation).toBe("Maybe");
  expect(verdict(3.2).recommendation).toBe("No Hire");
  expect(verdict(3.2).copilotScore).toBe(32);
});

test("parseAnalysis aligns questions to qaPairs length + coerces", () => {
  const raw = JSON.stringify({
    questions: [{ score: "4", keywords: ["Python"] }], // only 1; should pad to 2
    overall_evaluation: "ok", key_strengths: ["a"], areas_for_improvement: ["b"],
    remarks: { communication: 3 }, primarySkillsRatings: [{ skill: "Docker", rating: "1.2" }],
    secondarySkillsRatings: [], comment: "c",
  });
  const a = parseAnalysis(raw, QA, { mustHave: ["Docker"], goodToHave: [] });
  expect(a.questions).toHaveLength(2);
  expect(a.questions[0].score).toBe(4);
  expect(a.questions[1].score).toBeGreaterThanOrEqual(1); // padded default
  expect(typeof a.remarks.communication).toBe("string");
  expect(a.primarySkillsRatings[0].rating).toBe(1.2);
});

test("parseAnalysis falls back on malformed JSON (never throws)", () => {
  const a = parseAnalysis("not json", QA, { mustHave: ["X"], goodToHave: [] });
  expect(a.questions).toHaveLength(2);
  expect(a.areas_for_improvement.length).toBeGreaterThan(0);
});

test("buildReport produces the prescreeningreport envelope with detailed_qa", () => {
  const analysis = {
    questions: [{ score: 4, keywords: ["Python"] }, { score: 2, keywords: ["Docker"] }],
    overall_evaluation: "Struggled", key_strengths: ["Participated"],
    areas_for_improvement: ["Depth"], remarks: { communication: "1.6" },
    primarySkillsRatings: [{ skill: "testing", rating: 1.2 }],
    secondarySkillsRatings: [{ skill: "recops", rating: 1.4 }], comment: "avg 3",
  };
  const now = new Date(2026, 3, 7, 11, 3, 2); // 2026-04-07 11:03:02
  const { report, verdict: v } = buildReport({ qaPairs: QA, analysis, jdText: "JD here", resumeText: "Resume here", now });
  const pr = report.prescreeningreport;
  expect(pr.timestamp).toBe("2026-04-07 11:03:02");
  expect(pr.interviewer).toBe("Tara (Senior Technical Interviewer)");
  expect(pr.candidate_details.resume_summary).toBe("Resume here");
  expect(pr.candidate_details.job_description).toBe("JD here");
  expect(pr.candidate_details.interview_type).toBe("technical");
  expect(pr.interview_statistics.total_questions).toBe(2);
  expect(pr.interview_statistics.overall_score).toBe(3); // (4+2)/2
  expect(pr.ai_analysis.recommendation).toBe("No Hire");
  expect(pr.ai_analysis.areas_for_improvement).toEqual(["Depth"]);
  expect(pr.detailed_qa).toHaveLength(2);
  expect(pr.detailed_qa[0]).toEqual({ question: "Tell me about yourself", answer: "I am a Python dev", score: 4, keywords: ["Python"] });
  expect(pr.detailed_qa[1].score).toBe(2);
  expect(v.copilotScore).toBe(30);
});

test("scoreInterview returns {report, verdict} via injected geminiCall", async () => {
  const fakeGemini = async () => JSON.stringify({
    questions: [{ score: 5, keywords: ["a"] }, { score: 3, keywords: ["b"] }],
    overall_evaluation: "x", key_strengths: [], areas_for_improvement: [],
    remarks: { communication: "ok" }, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "c",
  });
  const { report, verdict: v } = await scoreInterview(fakeGemini, {
    jdText: "j", resumeText: "r", qaPairs: QA, mustHave: ["X"], goodToHave: [], now: new Date(2026, 0, 1),
  });
  expect(report.prescreeningreport.detailed_qa[0].question).toBe("Tell me about yourself");
  expect(report.prescreeningreport.interview_statistics.overall_score).toBe(4); // (5+3)/2
  expect(v.recommendation).toBe("No Hire");
});

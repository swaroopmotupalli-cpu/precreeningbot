const { parseAnalysis, verdict, buildReport, scoreInterview, fallbackAnalysis } = require("../src/scorer/score");

const QA = [
  { question: "Tell me about yourself", answer: "I am a Python dev" },
  { question: "Explain Docker", answer: "No idea" },
];

test("verdict thresholds + copilotScore (0-100 scale)", () => {
  expect(verdict(90).recommendation).toBe("Strong Hire");
  expect(verdict(72).recommendation).toBe("Hire");
  expect(verdict(56).recommendation).toBe("Maybe");
  expect(verdict(32).recommendation).toBe("No Hire");
  expect(verdict(32).copilotScore).toBe(32);
  expect(verdict(72).overall_score).toBe(72);
});

test("parseAnalysis aligns questions to qaPairs length + coerces (pairing itself is not re-decided)", () => {
  const raw = JSON.stringify({
    questions: [{ question: "What is your experience with Python?" }], // only 1; should pad to 2
    overall_rating: "76", overall_evaluation: "ok", key_strengths: ["a"], areas_for_improvement: ["b"],
    remarks: { communication: 3 }, primarySkillsRatings: [{ skill: "Docker", rating: "1.2" }],
    secondarySkillsRatings: [], comment: "c",
  });
  const a = parseAnalysis(raw, QA, { mustHave: ["Docker"], goodToHave: [] });
  expect(a.questions).toHaveLength(2);
  expect(a.questions[0].question).toBe("What is your experience with Python?");
  expect(a.questions[1].question).toBe(""); // padded default — no reconstruction provided
  expect(a.overall_rating).toBe(76);
  expect(typeof a.remarks.communication).toBe("string");
  expect(a.primarySkillsRatings[0].rating).toBe(1.2);
});

test("parseAnalysis captures a summarized answer per question when provided", () => {
  const raw = JSON.stringify({
    questions: [{ question: "What is your experience with Python?", answer: "3 years, mostly Django APIs." }],
    overall_rating: 70, overall_evaluation: "ok", key_strengths: [], areas_for_improvement: [],
    remarks: { communication: "3" }, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "c",
  });
  const a = parseAnalysis(raw, [QA[0]], { mustHave: [], goodToHave: [] });
  expect(a.questions[0].answer).toBe("3 years, mostly Django APIs.");
});

test("parseAnalysis clamps overall_rating to 0-100", () => {
  const raw = (rating) => JSON.stringify({
    questions: [], overall_rating: rating, overall_evaluation: "", key_strengths: [],
    areas_for_improvement: [], remarks: {}, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "",
  });
  expect(parseAnalysis(raw(150), [], { mustHave: [], goodToHave: [] }).overall_rating).toBe(100);
  expect(parseAnalysis(raw(-20), [], { mustHave: [], goodToHave: [] }).overall_rating).toBe(0);
});

test("parseAnalysis falls back on malformed JSON (never throws)", () => {
  const a = parseAnalysis("not json", QA, { mustHave: ["X"], goodToHave: [] });
  expect(a.questions).toHaveLength(2);
  expect(a.areas_for_improvement.length).toBeGreaterThan(0);
  expect(a.overall_rating).toBe(30);
});

test("fallbackAnalysis pads questions to qaPairs length with empty reconstruction", () => {
  const a = fallbackAnalysis(QA, ["Docker"], []);
  expect(a.questions).toEqual([{ question: "" }, { question: "" }]);
  expect(a.overall_rating).toBe(30);
});

test("buildReport produces the prescreeningreport envelope with detailed_qa (question+answer only, no interview_statistics)", () => {
  const analysis = {
    questions: [{ question: "" }, { question: "" }],
    overall_rating: 35,
    overall_evaluation: "Struggled", key_strengths: ["Participated"],
    areas_for_improvement: ["Depth"], remarks: { communication: "1.6" },
    primarySkillsRatings: [{ skill: "testing", rating: 1.2 }],
    secondarySkillsRatings: [{ skill: "recops", rating: 1.4 }], comment: "avg 3",
  };
  const now = new Date(2026, 3, 7, 11, 3, 2); // 2026-04-07 11:03:02
  const rawTranscript = [
    { seq: 0, speaker: "tara", text: "Tell me about yourself" },
    { seq: 1, speaker: "candidate", text: "I am a Python dev" },
    { seq: 2, speaker: "tara", text: "Explain Docker" },
    { seq: 3, speaker: "candidate", text: "No idea" },
  ];
  const { report, verdict: v } = buildReport({ qaPairs: QA, analysis, jdText: "JD here", resumeText: "Resume here", now, rawTranscript });
  const pr = report.prescreeningreport;
  expect(pr.timestamp).toBe("2026-04-07 11:03:02");
  expect(pr.interviewer).toBe("Tara (Senior Technical Interviewer)");
  expect(pr.candidate_details.resume_summary).toBe("Resume here");
  expect(pr.candidate_details.job_description).toBe("JD here");
  expect(pr.candidate_details.interview_type).toBe("technical");
  expect(pr.interview_statistics).toBeUndefined();
  expect(pr.ai_analysis.recommendation).toBe("No Hire");
  expect(pr.ai_analysis.areas_for_improvement).toEqual(["Depth"]);
  expect(pr.detailed_qa).toHaveLength(2);
  expect(pr.detailed_qa[0]).toEqual({ question: "Tell me about yourself", answer: "I am a Python dev" });
  expect(pr.detailed_qa[1]).toEqual({ question: "Explain Docker", answer: "No idea" });
  expect(pr.transcript).toEqual([
    { speaker: "tara", text: "Tell me about yourself" },
    { speaker: "user", text: "I am a Python dev" },
    { speaker: "tara", text: "Explain Docker" },
    { speaker: "user", text: "No idea" },
  ]);
  expect(v.copilotScore).toBe(35);
  expect(v.overall_score).toBe(35);
});

test("buildReport defaults transcript to an empty array when rawTranscript is not provided", () => {
  const analysis = {
    questions: [{ question: "" }, { question: "" }], overall_rating: 35,
    overall_evaluation: "", key_strengths: [], areas_for_improvement: [],
    remarks: { communication: "1" }, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "",
  };
  const { report } = buildReport({ qaPairs: QA, analysis, jdText: "", resumeText: "", now: new Date(2026, 0, 1) });
  expect(report.prescreeningreport.transcript).toEqual([]);
});

test("buildReport prefers the model's cleaned-up question wording over the raw informal/combined line", () => {
  const rawQA = [
    { question: "That's the right idea. Could you explain how you would specifically handle element locators?", answer: "5 years of Python and Django." },
  ];
  const analysis = {
    questions: [{ question: "How would you specifically handle element locators?" }],
    overall_rating: 80,
    overall_evaluation: "Good", key_strengths: [], areas_for_improvement: [],
    remarks: { communication: "4" }, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "ok",
  };
  const { report } = buildReport({ qaPairs: rawQA, analysis, jdText: "", resumeText: "", now: new Date(2026, 0, 1) });
  expect(report.prescreeningreport.detailed_qa[0]).toEqual({
    question: "How would you specifically handle element locators?",
    answer: "5 years of Python and Django.",
  });
});

test("buildReport prefers the model's summarized answer over the raw concatenated transcript text", () => {
  const rawQA = [
    { question: "Tell me about your React experience?", answer: "Um yeah so like I have worked I think maybe three years or so with React building dashboards and stuff." },
  ];
  const analysis = {
    questions: [{ question: "Tell me about your React experience?", answer: "About 3 years of React experience, mainly building dashboards." }],
    overall_rating: 70, overall_evaluation: "", key_strengths: [], areas_for_improvement: [],
    remarks: { communication: "3" }, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "",
  };
  const { report } = buildReport({ qaPairs: rawQA, analysis, jdText: "", resumeText: "", now: new Date(2026, 0, 1) });
  expect(report.prescreeningreport.detailed_qa[0].answer).toBe("About 3 years of React experience, mainly building dashboards.");
});

test("buildReport falls back to the raw paired line when no reconstruction is provided", () => {
  const analysis = {
    questions: [{ question: "" }], // e.g. fallbackAnalysis path
    overall_rating: 30, overall_evaluation: "", key_strengths: [], areas_for_improvement: [],
    remarks: { communication: "3" }, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "",
  };
  const { report } = buildReport({
    qaPairs: [{ question: "That's a good question about POM", answer: "..." }],
    analysis, jdText: "", resumeText: "", now: new Date(2026, 0, 1),
  });
  expect(report.prescreeningreport.detailed_qa[0].question).toBe("That's a good question about POM");
});

test("scoreInterview never drops or merges a pair — questions array stays 1:1 with qaPairs even across a fixed interrupted-fragment gap", async () => {
  const fakeGemini = async () => JSON.stringify({
    questions: [
      { question: "Tell me about yourself" },
      { question: "Explain Docker" },
    ],
    overall_rating: 62,
    overall_evaluation: "x", key_strengths: [], areas_for_improvement: [],
    remarks: { communication: "ok" }, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "c",
  });
  const rawTranscript = [
    { seq: 0, speaker: "tara", text: "Tell me about yourself" },
    { seq: 1, speaker: "candidate", text: "I am a Python dev" },
    { seq: 2, speaker: "tara", text: "Explain Docker" },
    { seq: 3, speaker: "candidate", text: "No idea" },
  ];
  const { report, verdict: v } = await scoreInterview(fakeGemini, {
    jdText: "j", resumeText: "r", qaPairs: QA, mustHave: ["X"], goodToHave: [], now: new Date(2026, 0, 1), rawTranscript,
  });
  expect(report.prescreeningreport.detailed_qa).toHaveLength(2);
  expect(report.prescreeningreport.detailed_qa[0]).toEqual({ question: "Tell me about yourself", answer: "I am a Python dev" });
  expect(report.prescreeningreport.detailed_qa[1]).toEqual({ question: "Explain Docker", answer: "No idea" });
  expect(report.prescreeningreport.transcript).toHaveLength(4);
  expect(report.prescreeningreport.interview_statistics).toBeUndefined();
  expect(v.overall_score).toBe(62);
  expect(v.recommendation).toBe("Maybe");
});

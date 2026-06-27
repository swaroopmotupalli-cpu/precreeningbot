const { parseReport, computeVerdict, scoreInterview } = require("../src/scorer/score");

test("parseReport strips fences and coerces types", () => {
  const raw = "```json\n" + JSON.stringify({
    overall_evaluation: "ok", recommendation: "Hire", key_strengths: ["a"],
    remarks: { communication: 4 },
    primarySkillsRatings: [{ skill: "Python", rating: "4.0" }],
    secondarySkillsRatings: [], comment: "c",
  }) + "\n```";
  const r = parseReport(raw, { mustHave: ["Python"], goodToHave: [] });
  expect(typeof r.remarks.communication).toBe("string");
  expect(r.primarySkillsRatings[0].rating).toBe(4);
});

test("parseReport falls back on malformed JSON (never throws)", () => {
  const r = parseReport("not json", { mustHave: ["SQL"], goodToHave: ["AWS"] });
  expect(r.recommendation).toBeDefined();
  expect(Array.isArray(r.primarySkillsRatings)).toBe(true);
});

test("computeVerdict applies thresholds", () => {
  const mk = (rating) => ({ primarySkillsRatings: [{ skill: "x", rating }] });
  expect(computeVerdict(mk(4.5)).recommendation).toBe("Strong Hire"); // 9.0
  expect(computeVerdict(mk(3.6)).recommendation).toBe("Hire");        // 7.2
  expect(computeVerdict(mk(2.8)).recommendation).toBe("Maybe");       // 5.6
  expect(computeVerdict(mk(2.0)).recommendation).toBe("No Hire");     // 4.0
  expect(computeVerdict(mk(4.5)).copilotScore).toBe(90);
  expect(computeVerdict(mk(4.5)).empStatus).toBe("Completed");
});

test("scoreInterview uses injected geminiCall", async () => {
  const fakeGemini = async () => JSON.stringify({
    overall_evaluation: "x", recommendation: "Maybe", key_strengths: [],
    remarks: { communication: "ok" },
    primarySkillsRatings: [{ skill: "Python", rating: 3 }],
    secondarySkillsRatings: [], comment: "c",
  });
  const r = await scoreInterview(fakeGemini, {
    resumeText: "r", jdText: "j", qaPairs: [{ question: "q", answer: "a" }],
    mustHave: ["Python"], goodToHave: [],
  });
  expect(r.primarySkillsRatings[0].rating).toBe(3);
});

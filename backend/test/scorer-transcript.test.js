const { buildQAPairs, loadInterview } = require("../src/scorer/transcript");

test("pairs tara questions with following candidate answers", () => {
  const lines = [
    { seq: 0, speaker: "tara", text: "Q1?" },
    { seq: 1, speaker: "candidate", text: "A1a" },
    { seq: 2, speaker: "candidate", text: "A1b" },
    { seq: 3, speaker: "tara", text: "Q2?" },
    { seq: 4, speaker: "candidate", text: "A2" },
  ];
  expect(buildQAPairs(lines)).toEqual([
    { question: "Q1?", answer: "A1a A1b" },
    { question: "Q2?", answer: "A2" },
  ]);
});

test("trailing unanswered question yields empty answer", () => {
  const lines = [{ seq: 0, speaker: "tara", text: "Q?" }];
  expect(buildQAPairs(lines)).toEqual([{ question: "Q?", answer: "" }]);
});

test("loadInterview queries aiInterview by room", async () => {
  const db = { collection: (n) => ({ findOne: async (q) => ({ n, q }) }) };
  const out = await loadInterview(db, "sess1");
  expect(out.n).toBe("aiInterview");
  expect(out.q).toEqual({ room: "sess1" });
});

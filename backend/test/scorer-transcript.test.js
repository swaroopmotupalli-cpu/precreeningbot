const { buildQAPairs, mergeTurns, loadInterview } = require("../src/scorer/transcript");

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

test("an interrupted Tara fragment is not treated as a new question boundary", () => {
  const lines = [
    { seq: 0, speaker: "tara", text: "Could you explain how you structure your POM classes?" },
    { seq: 1, speaker: "candidate", text: "Like the page object model separates page elements and page" },
    { seq: 2, speaker: "candidate", text: "actions from the test cases" },
    { seq: 3, speaker: "tara", text: "That's the right idea. Could you explain how you would specifically handle", interrupted: true },
    { seq: 4, speaker: "candidate", text: "Actually, it improves code reusability by keeping page-specific code in one place." },
    { seq: 5, speaker: "tara", text: "Moving on, how have you used TestNG?" },
    { seq: 6, speaker: "candidate", text: "I have not used TestNG in production." },
  ];
  expect(buildQAPairs(lines)).toEqual([
    {
      question: "Could you explain how you structure your POM classes?",
      answer: "Like the page object model separates page elements and page actions from the test cases Actually, it improves code reusability by keeping page-specific code in one place.",
    },
    { question: "Moving on, how have you used TestNG?", answer: "I have not used TestNG in production." },
  ]);
});

test("mergeTurns merges consecutive same-speaker fragments into one turn each", () => {
  const lines = [
    { seq: 0, speaker: "tara", text: "Hi, could you tell me about your Java experience?" },
    { seq: 1, speaker: "candidate", text: "Yeah, thank you." },
    { seq: 2, speaker: "candidate", text: "I have 5 years of experience." },
    { seq: 3, speaker: "tara", text: "That's great." },
    { seq: 4, speaker: "tara", text: "Could you tell me about Spring Boot?" },
    { seq: 5, speaker: "candidate", text: "Sure, I've used it extensively." },
  ];
  expect(mergeTurns(lines)).toEqual([
    { speaker: "tara", text: "Hi, could you tell me about your Java experience?" },
    { speaker: "user", text: "Yeah, thank you. I have 5 years of experience." },
    { speaker: "tara", text: "That's great. Could you tell me about Spring Boot?" },
    { speaker: "user", text: "Sure, I've used it extensively." },
  ]);
});

test("mergeTurns sorts by seq even if storage is unordered", () => {
  const lines = [
    { seq: 2, speaker: "candidate", text: "b" },
    { seq: 1, speaker: "tara", text: "a?" },
  ];
  expect(mergeTurns(lines)).toEqual([
    { speaker: "tara", text: "a?" },
    { speaker: "user", text: "b" },
  ]);
});

test("loadInterview queries aiInterview by room", async () => {
  const db = { collection: (n) => ({ findOne: async (q) => ({ n, q }) }) };
  const out = await loadInterview(db, "sess1");
  expect(out.n).toBe("aiInterview");
  expect(out.q).toEqual({ room: "sess1" });
});

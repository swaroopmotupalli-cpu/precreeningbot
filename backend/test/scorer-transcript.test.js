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

test("a rephrased question is merged into the current pair, not counted as a new one", () => {
  // Regression: the live agent doesn't count a rephrase toward its question
  // cap, but it still saves the rephrased line to the transcript like any
  // other Tara line — buildQAPairs must independently recognize it too, or
  // the final report ends up with more Q&A pairs than questions actually asked.
  const lines = [
    { seq: 0, speaker: "tara", text: "When you have analyzed data, what tools or methods have you used to visualize it?" },
    { seq: 1, speaker: "candidate", text: "Would you tell me how many questions we have completed?" },
    { seq: 2, speaker: "tara", text: "Could you please share what tools or methods you have used to visualize data?" },
    { seq: 3, speaker: "candidate", text: "Yes, but I am not aware of this." },
    { seq: 4, speaker: "tara", text: "How do you typically ensure data quality when integrating information?" },
    { seq: 5, speaker: "candidate", text: "I have no idea about this." },
  ];
  expect(buildQAPairs(lines)).toEqual([
    {
      question: "Could you please share what tools or methods you have used to visualize data?",
      answer: "Would you tell me how many questions we have completed? Yes, but I am not aware of this.",
    },
    { question: "How do you typically ensure data quality when integrating information?", answer: "I have no idea about this." },
  ]);
});

test("generic interview phrasing shared between two unrelated questions does not cause a false merge", () => {
  // Regression: a MongoDB schema question and a later GraphQL question share
  // only generic interview boilerplate ("have", "working", "with", "your",
  // "projects") — that shared filler pushed similarity over threshold and
  // wrongly merged them, dropping a real, distinctly-answered question.
  const lines = [
    { seq: 0, speaker: "tara", text: "How have you approached schema design when working with MongoDB in your previous projects?" },
    { seq: 1, speaker: "candidate", text: "I used embedded documents for one-to-few relationships." },
    { seq: 2, speaker: "tara", text: "Have you had experience working with GraphQL for data fetching in any of your projects?" },
    { seq: 3, speaker: "candidate", text: "Yes, I built a few resolvers for our API." },
  ];
  expect(buildQAPairs(lines)).toEqual([
    { question: "How have you approached schema design when working with MongoDB in your previous projects?", answer: "I used embedded documents for one-to-few relationships." },
    { question: "Have you had experience working with GraphQL for data fetching in any of your projects?", answer: "Yes, I built a few resolvers for our API." },
  ]);
});

test("a genuinely new question on a different topic is not merged", () => {
  const lines = [
    { seq: 0, speaker: "tara", text: "Could you tell me about your React experience?" },
    { seq: 1, speaker: "candidate", text: "I've built several dashboards." },
    { seq: 2, speaker: "tara", text: "How do you handle database migrations?" },
    { seq: 3, speaker: "candidate", text: "I use versioned migration scripts." },
  ];
  expect(buildQAPairs(lines)).toEqual([
    { question: "Could you tell me about your React experience?", answer: "I've built several dashboards." },
    { question: "How do you handle database migrations?", answer: "I use versioned migration scripts." },
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

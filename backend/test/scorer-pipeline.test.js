// backend/test/scorer-pipeline.test.js
const { scoreSession } = require("../src/scorer/pipeline");

test("not found returns ok:false", async () => {
  const db = { collection: () => ({ findOne: async () => null }) };
  const out = await scoreSession({ db, geminiCall: async () => "{}" }, "missing");
  expect(out.ok).toBe(false);
});

test("happy path scores and persists", async () => {
  const writes = [];
  const db = {
    collection(name) {
      return {
        findOne: async (q) => {
          if (name === "aiInterview") return {
            room: "s1", contestId: "c", candidateId: "u", recruiterId: "", jsId: "",
            transcript: [{ seq: 0, speaker: "tara", text: "Q?" }, { seq: 1, speaker: "candidate", text: "A" }],
          };
          return null; // contests / profile miss → fallbacks
        },
        deleteMany: async () => {}, insertOne: async (d) => writes.push(name),
        updateOne: async () => ({ modifiedCount: 1 }),
      };
    },
  };
  const gemini = async () => JSON.stringify({
    overall_evaluation: "x", recommendation: "Hire", key_strengths: [],
    remarks: { communication: "ok" }, primarySkillsRatings: [{ skill: "Python", rating: 4 }],
    secondarySkillsRatings: [], comment: "c",
  });
  const out = await scoreSession({ db, geminiCall: gemini }, "s1");
  expect(out.ok).toBe(true);
  expect(writes).toContain("aiInterview");
});

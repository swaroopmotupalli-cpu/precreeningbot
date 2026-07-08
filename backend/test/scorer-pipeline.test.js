// backend/test/scorer-pipeline.test.js
const { scoreSession } = require("../src/scorer/pipeline");

test("not found returns ok:false", async () => {
  const db = { collection: () => ({ findOne: async () => null }) };
  const out = await scoreSession({ db, geminiCall: async () => "{}" }, "missing");
  expect(out.ok).toBe(false);
});

test("happy path scores and persists", async () => {
  const writes = [];
  const aiDocs = [];
  const db = {
    collection(name) {
      return {
        findOne: async (q) => {
          if (name === "aiInterview") return {
            room: "s1", contestId: "c", candidateId: "u", recruiterId: "", jsId: "",
            jdText: "JD", resumeText: "Resume",
            transcript: [{ seq: 0, speaker: "tara", text: "Q?" }, { seq: 1, speaker: "candidate", text: "A" }],
          };
          return null; // contests / profile miss → fallbacks
        },
        deleteMany: async () => {},
        insertOne: async (d) => { writes.push(name); if (name === "aiInterview") aiDocs.push(d); },
        updateOne: async () => ({ modifiedCount: 1 }),
      };
    },
  };
  const gemini = async () => JSON.stringify({
    questions: [{ question: "Q?" }], // 1:1 with the deterministically-built qaPairs
    overall_rating: 64,
    overall_evaluation: "x", key_strengths: [], areas_for_improvement: [],
    remarks: { communication: "ok" }, primarySkillsRatings: [], secondarySkillsRatings: [], comment: "c",
  });
  const out = await scoreSession({ db, geminiCall: gemini }, "s1");
  expect(out.ok).toBe(true);
  expect(writes).toContain("aiInterview");
  // the rich report (with detailed_qa) is what gets persisted
  const pr = aiDocs[0].report.prescreeningreport;
  expect(pr.detailed_qa[0]).toEqual({ question: "Q?", answer: "A" });
  expect(pr.transcript).toEqual([{ speaker: "tara", text: "Q?" }, { speaker: "user", text: "A" }]);
  expect(pr.candidate_details.job_description).toBe("JD");
  expect(out.overall_score).toBe(64);
});

const { createSession } = require("../src/createSession");
const { ObjectId } = require("mongodb");

const admittedLimiter = { tryAdmit: async () => ({ admitted: true, state: "new" }) };
const rejectedLimiter = { tryAdmit: async () => ({ admitted: false, bucket: "global" }) };

// Mock Marketplace db: a contest + a jobseeker so loadContext composes context.
function mockDb() {
  return {
    collection(name) {
      if (name === "contests") return { findOne: async () => ({ details: { jobDetails: {
        jobTitle: "Frontend Dev", mustHaveSkills: ["React", "TypeScript"], experience: "4 5",
        goodToHave: ["Next.js"], shortDescription: "Build UIs.",
      } } }) };
      if (name === "jobSeekerProfile") return { findOne: async () => ({
        personal_info: { firstName: "Ada", lastName: "Lovelace" },
        work_experience: { totalExperience: "5 years", designation: "Engineer", companyName: "Hiringhood" },
        completeResumeDetails: { parsed_resume: { ResumeParserData: { SkillBlock: "JavaScript, React, Node" } } },
      }) };
      return { findOne: async () => null };
    },
  };
}

function body() {
  return { contestId: new ObjectId().toString(), jsId: new ObjectId().toString(), recruiterId: new ObjectId().toString() };
}

test("derives JD/resume/skills from Marketplace and writes the blob", async () => {
  const store = {};
  const redis = { set: async (k, v) => { store[k] = v; }, expire: async () => {} };
  const out = await createSession(redis, () => "tok", admittedLimiter, body(), { db: mockDb() });
  expect(out.status).toBe("admitted");
  expect(out.token).toBe("tok");
  expect(out.jobTitle).toBe("Frontend Dev");
  const blob = JSON.parse(store[`session:${out.sessionId}`]);
  expect(blob.candidateId).toBe(blob.jsId);          // jobseeker is the candidate
  expect(blob.candidateName).toBe("Ada Lovelace");
  expect(blob.skills).toEqual(["React", "TypeScript"]); // must-have skills → coverage
  expect(blob.goodToHaveSkills).toEqual(["Next.js"]);
  expect(blob.jdText).toContain("Job Title: Frontend Dev");
  expect(blob.resumeText).toContain("Candidate: Ada Lovelace");
  expect(blob.resumeText).toContain("Skills: JavaScript, React, Node");
});

test("rejected admission → queued + bucket, no token, blob re-expired", async () => {
  const calls = [];
  const redis = { set: async () => calls.push("set"), expire: async (k, ttl) => calls.push(["expire", ttl]) };
  const out = await createSession(redis, () => "tok", rejectedLimiter, body(), { db: mockDb(), rejectedBlobTtl: 120 });
  expect(out.status).toBe("queued");
  expect(out.bucket).toBe("global");
  expect(out.token).toBeUndefined();
  expect(calls.find((c) => Array.isArray(c) && c[0] === "expire")[1]).toBe(120);
});

test("missing required id throws", async () => {
  const redis = { set: async () => {} };
  await expect(createSession(redis, () => "tok", admittedLimiter, { contestId: "x", jsId: "y" }, { db: mockDb() }))
    .rejects.toThrow(/recruiterId is required/);
});

test("requires the Marketplace db handle", async () => {
  const redis = { set: async () => {} };
  await expect(createSession(redis, () => "tok", admittedLimiter, body(), {}))
    .rejects.toThrow(/Marketplace db handle/);
});

test("contest not found propagates (→ 404 in server)", async () => {
  const redis = { set: async () => {} };
  const emptyDb = { collection: () => ({ findOne: async () => null }) };
  await expect(createSession(redis, () => "tok", admittedLimiter, body(), { db: emptyDb }))
    .rejects.toMatchObject({ code: "CONTEST_NOT_FOUND" });
});

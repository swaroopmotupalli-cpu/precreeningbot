// backend/test/scorer-persist.test.js
const { persistReport } = require("../src/scorer/persist");
const { ObjectId } = require("mongodb");

function mockDb(calls, profileDoc) {
  return {
    collection(name) {
      return {
        deleteMany: async (q) => calls.push(["deleteMany", name, q]),
        insertOne: async (d) => { calls.push(["insertOne", name, d]); return { insertedId: 1 }; },
        updateOne: async (q, u) => { calls.push(["updateOne", name, q, u]); return { modifiedCount: 1 }; },
        findOne: async () => profileDoc,
      };
    },
  };
}

test("writes aiInterview + recruiterAddProfiles $set + auditTrail with valid ids", async () => {
  const calls = [];
  const recruiterId = new ObjectId().toString();
  const jsId = new ObjectId().toString();
  const contestId = new ObjectId().toString();
  const profileDoc = { jobseekerDetails: [{ jsId: new ObjectId(jsId), firstName: "Ada", lastName: "L" }] };
  const out = await persistReport(mockDb(calls, profileDoc), {
    sessionId: "s1", contestId, candidateId: "u1", recruiterId, jsId,
    report: { recommendation: "Hire" },
    verdict: { avg: 7.2, recommendation: "Hire", empStatus: "Completed", copilotScore: 72 },
  });
  expect(out.ats).toBe(true);
  const ai = calls.find((c) => c[0] === "insertOne" && c[1] === "aiInterview")[2];
  expect(ai.session_id).toBe("s1");
  expect(ai.prescreening_status).toBe("True");
  const upd = calls.find((c) => c[0] === "updateOne" && c[1] === "recruiterAddProfiles")[3];
  expect(upd.$set["jobseekerDetails.$.copilotScore"]).toBe(72);
  expect(upd.$set["jobseekerDetails.$.prescreeningreport"]).toBeDefined();
  // scoring must NOT change the candidate's pipeline stage:
  expect(upd.$set["jobseekerDetails.$.empStatus"]).toBeUndefined();
  expect(upd.$set["jobseekerDetails.$.status"]).toBeUndefined();
  const audit = calls.find((c) => c[0] === "insertOne" && c[1] === "auditTrail")[2];
  expect(audit.action).toBe("Prescreening Completed");
  expect(audit.userName).toBe("Ada L");
});

test("missing ids → aiInterview only, ats=false", async () => {
  const calls = [];
  const out = await persistReport(mockDb(calls, null), {
    sessionId: "s2", contestId: "", candidateId: "u1", recruiterId: "", jsId: "",
    report: {}, verdict: { avg: 5, recommendation: "Maybe", empStatus: "Completed", copilotScore: 50 },
  });
  expect(out.ats).toBe(false);
  expect(calls.some((c) => c[1] === "aiInterview")).toBe(true);
  expect(calls.some((c) => c[1] === "recruiterAddProfiles")).toBe(false);
});

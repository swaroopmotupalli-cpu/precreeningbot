// backend/src/scorer/persist.js
const { ObjectId } = require("mongodb");

function toOid(s) { try { return new ObjectId(s); } catch (e) { return null; } }

async function persistReport(db, p) {
  const { sessionId, contestId, candidateId, recruiterId, jsId, report, verdict } = p;
  // 1. aiInterview — idempotent replace on session_id.
  const ai = db.collection("aiInterview");
  await ai.deleteMany({ session_id: sessionId });
  await ai.insertOne({
    session_id: sessionId, contest_id: contestId, js_id: jsId, recruiter_id: recruiterId,
    report, emp_status: verdict.empStatus, prescreening_status: "True", created_at: new Date(),
  });

  const cOid = toOid(contestId), rOid = toOid(recruiterId), jOid = toOid(jsId);
  if (!cOid || !rOid || !jOid) return { ats: false };

  // 2. recruiterAddProfiles — positional $set on the matched jobseeker.
  // NOTE: deliberately does NOT touch empStatus/status — scoring must not move
  // the candidate's recruiter-pipeline stage; it only attaches the report+score.
  await db.collection("recruiterAddProfiles").updateOne(
    { contestId: cOid, recruiterId: rOid, "jobseekerDetails.jsId": jOid },
    { $set: {
      // store the inner prescreeningreport object (report === {prescreeningreport})
      "jobseekerDetails.$.prescreeningreport": (report && report.prescreeningreport) || report,
      "jobseekerDetails.$.copilotScore": verdict.copilotScore,
      "jobseekerDetails.$.prescreeningStatus": "True",
    } },
  );

  // 3. auditTrail — userName from the matched profile jobseeker.
  let userName = "Unknown User";
  const prof = await db.collection("recruiterAddProfiles").findOne(
    { contestId: cOid, recruiterId: rOid, "jobseekerDetails.jsId": jOid });
  if (prof) {
    for (const js of prof.jobseekerDetails || []) {
      if (String(js.jsId) === jsId) {
        userName = `${js.firstName || ""} ${js.lastName || ""}`.trim() || "Unknown User";
        break;
      }
    }
  }
  await db.collection("auditTrail").insertOne({
    dateTime: new Date(), userRole: "JobSeeker", subRole: null, userName,
    stage: verdict.empStatus, action: "Prescreening Completed",
    reasonOrComments: "Jobseeker has completed his Pre screening",
    jobSeekerId: jOid, contestId: cOid, recruiterId: rOid, createdBy: "JobSeeker",
  });
  return { ats: true };
}

module.exports = { persistReport };

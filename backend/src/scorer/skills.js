// backend/src/scorer/skills.js
const { ObjectId } = require("mongodb");

async function loadSkills(db, contestId, blobSkills) {
  const fallback = { mustHave: blobSkills || [], goodToHave: [] };
  let oid;
  try { oid = new ObjectId(contestId); } catch (e) { return fallback; }
  const col = db.collection("contests");
  let doc = await col.findOne({ contestId: oid });
  if (!doc) doc = await col.findOne({ _id: oid });
  if (!doc) return fallback;
  const jd = doc.job_details || {};
  return {
    mustHave: jd.mustHaveSkills || blobSkills || [],
    goodToHave: jd.goodToHave || [],
  };
}

module.exports = { loadSkills };

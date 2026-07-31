// Marketplace ATS reads: derive JD + resume context for a session from
// contestId + jsId. DB = "Marketplace"; collections = contests, jobSeekerProfile.
// Pure compose helpers are exported for unit testing; loadContext does the I/O.
const { ObjectId } = require("mongodb");

function oid(id) {
  try { return new ObjectId(String(id)); } catch (e) { return null; }
}

// Marketplace data is user-supplied and has been observed to contain HTML/JS
// injection payloads (e.g. <img onerror=fetch(...)> in a skill entry). Strip
// HTML tags before this text reaches the LLM prompt or any DOM, neutralizing
// both XSS-on-render and prompt-injection-via-markup. Returns a plain string.
function clean(s) {
  if (s == null) return "";
  return String(s).replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}
function cleanArr(a) {
  return (Array.isArray(a) ? a : []).map(clean).filter(Boolean);
}

// SkillBlock is a comma-separated string; Domain (when present) may be array|string.
function splitSkills(src) {
  if (!src) return [];
  const arr = Array.isArray(src) ? src : String(src).split(/[,\n;]+/);
  return arr.map((s) => clean(String(s).replace(/\.+$/, ""))).filter(Boolean);
}

// JD experience comes as e.g. "4 5" (min max years). Render readably.
function fmtExperience(exp) {
  if (!exp) return "";
  const parts = String(exp).trim().split(/\s+/);
  if (parts.length === 2 && parts.every((p) => /^\d+$/.test(p))) return `${parts[0]}-${parts[1]} years`;
  return String(exp).trim();
}

function composeJd(jd) {
  jd = jd || {};
  const lines = [];
  const title = clean(jd.jobTitle);
  if (title) lines.push(`Job Title: ${title}`);
  const exp = fmtExperience(jd.experience);
  if (exp) lines.push(`Experience required: ${exp}`);
  const must = cleanArr(jd.mustHaveSkills);
  if (must.length) lines.push(`Must-have skills: ${must.join(", ")}`);
  const good = cleanArr(jd.goodToHave);
  if (good.length) lines.push(`Good-to-have skills: ${good.join(", ")}`);
  const desc = clean(jd.shortDescription);
  if (desc) lines.push(`Description: ${desc}`);
  return lines.join("\n");
}

// work_experience is object | array | undefined across docs; ResumeParserData.Experience
// is a free string ("5+ years"/"18 years"/""). Produce one experience phrase, robustly.
function summarizeExperience(workExperience, resumeParserData) {
  const we = workExperience;
  if (Array.isArray(we) && we.length) {
    const roles = we
      .map((e) => [e.designation || e.role || e.currentRole, e.companyName || e.company].filter(Boolean).join(" at "))
      .filter(Boolean);
    const total = we.find((e) => e.totalExperience) && we.find((e) => e.totalExperience).totalExperience;
    return [total ? `${total} total` : "", roles.length ? `Roles: ${roles.join("; ")}` : ""].filter(Boolean).join(". ");
  }
  if (we && typeof we === "object") {
    const total = we.totalExperience || we.relevantExperience;
    const role = [we.designation || we.currentRole, we.companyName].filter(Boolean).join(" at ");
    return [total ? `${total}` : "", role].filter(Boolean).join(" — ");
  }
  if (resumeParserData && resumeParserData.Experience) return String(resumeParserData.Experience);
  return "Not specified";
}

function composeResume(js) {
  js = js || {};
  const pi = js.personal_info || {};
  const name = [pi.firstName, pi.middleName, pi.lastName].map((x) => clean(x)).filter(Boolean).join(" ");
  const rpd = js.completeResumeDetails && js.completeResumeDetails.parsed_resume && js.completeResumeDetails.parsed_resume.ResumeParserData;
  const skills = pi.Domain ? splitSkills(pi.Domain) : splitSkills(rpd && rpd.SkillBlock);
  const experience = clean(summarizeExperience(js.work_experience, rpd));
  const lines = [];
  lines.push(`Candidate: ${name || "Unknown"}`);
  lines.push(`Experience: ${experience}`);
  lines.push(`Skills: ${skills.length ? skills.join(", ") : "Not specified"}`);
  return { resumeText: lines.join("\n"), candidateName: name, candidateSkills: skills };
}

class NotFoundError extends Error {
  constructor(code, message) { super(message); this.code = code; }
}

// Fetch contest + jobseeker from Marketplace and build the session context.
async function loadContext(db, contestId, jsId) {
  const cOid = oid(contestId), jOid = oid(jsId);
  if (!cOid) throw new NotFoundError("INVALID_CONTEST_ID", `invalid contestId: ${contestId}`);
  if (!jOid) throw new NotFoundError("INVALID_JS_ID", `invalid jsId: ${jsId}`);

  const contests = db.collection("contests");
  let contest = await contests.findOne({ contestId: cOid });
  if (!contest) contest = await contests.findOne({ _id: cOid });
  if (!contest) throw new NotFoundError("CONTEST_NOT_FOUND", `no contest for ${contestId}`);

  const js = await db.collection("jobSeekerProfile").findOne({ _id: jOid });
  if (!js) throw new NotFoundError("JOBSEEKER_NOT_FOUND", `no jobseeker for ${jsId}`);

  const jd = (contest.details && contest.details.jobDetails) || {};
  const { resumeText, candidateName } = composeResume(js);
  return {
    jobTitle: clean(jd.jobTitle),
    jdText: composeJd(jd),
    resumeText,
    skills: cleanArr(jd.mustHaveSkills),
    goodToHaveSkills: cleanArr(jd.goodToHave),
    candidateName,
  };
}

module.exports = { splitSkills, fmtExperience, composeJd, summarizeExperience, composeResume, loadContext, NotFoundError };

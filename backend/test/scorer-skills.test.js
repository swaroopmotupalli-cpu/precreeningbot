// backend/test/scorer-skills.test.js
const { loadSkills } = require("../src/scorer/skills");
const { ObjectId } = require("mongodb");

const OID = new ObjectId().toString();

test("reads mustHave/goodToHave from contests", async () => {
  const db = { collection: () => ({ findOne: async () => ({
    job_details: { mustHaveSkills: ["Python"], goodToHave: ["AWS"] } }) }) };
  expect(await loadSkills(db, OID, ["x"])).toEqual({ mustHave: ["Python"], goodToHave: ["AWS"] });
});

test("falls back to blob skills when contest not found", async () => {
  const db = { collection: () => ({ findOne: async () => null }) };
  expect(await loadSkills(db, OID, ["Java"])).toEqual({ mustHave: ["Java"], goodToHave: [] });
});

test("invalid ObjectId falls back to blob skills", async () => {
  const db = { collection: () => ({ findOne: async () => null }) };
  expect(await loadSkills(db, "not-an-oid", ["Go"])).toEqual({ mustHave: ["Go"], goodToHave: [] });
});

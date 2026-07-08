# Tara Phase 5 — Async Scoring Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A separate Node consumer that, when a Tara interview ends, reads the Mongo transcript, runs one `gemini-3.1-flash-lite` scoring pass, and writes an ATS-compatible report into `aiInterview`/`recruiterAddProfiles`/`auditTrail`.

**Architecture:** Pure, unit-testable units under `backend/src/scorer/` (transcript→Q&A, score+verdict, skills, persist, pipeline) driven by a Redis-queue consumer (`tara:score:queue`). The Python worker enqueues the sessionId at natural interview end; `recruiterId`+`jsId` are captured at session creation. Gemini is called via Node 20 global `fetch`; Mongo via the `mongodb` driver. Providers are dependency-injected so unit tests never make network calls.

**Tech Stack:** Node 20 (jest, ioredis, `mongodb` driver [new], global fetch for Gemini); Python 3.12 (the worker enqueue seam); kubeconform for the manifest.

## Global Constraints

Copied verbatim from the spec:

- **Trigger = Redis list `tara:score:queue`** (worker `LPUSH`, scorer `BRPOP`); dead-letter `tara:score:dlq` after **3** attempts.
- **Report JSON is ATS-compatible, verbatim shape:** `overall_evaluation`, `recommendation` ∈ {`Strong Hire`,`Hire`,`Maybe`,`No Hire`}, `key_strengths[]`, `remarks` (`communication` is a STRING), `primarySkillsRatings[{skill,rating:number}]`, `secondarySkillsRatings[{skill,rating:number}]`, `comment`.
- **Recommendation thresholds (on avg /10):** ≥ 8.5 `Strong Hire`, ≥ 7 `Hire`, ≥ 5.5 `Maybe`, else `No Hire`. `copilotScore = round(avg * 10)`. `empStatus = "Completed"`.
- **Write shapes (verbatim from the prior app):**
  - `aiInterview` insert: `{session_id, contest_id, js_id, recruiter_id, report, emp_status, prescreening_status:"True", created_at}` (idempotent on `session_id`).
  - `recruiterAddProfiles` `update_one({contestId:ObjectId, recruiterId:ObjectId, "jobseekerDetails.jsId":ObjectId}, {$set:{"jobseekerDetails.$.prescreeningreport":report, "jobseekerDetails.$.copilotScore":round(avg*10), "jobseekerDetails.$.prescreeningStatus":"True", "jobseekerDetails.$.empStatus":empStatus, "jobseekerDetails.$.status":empStatus}})`.
  - `auditTrail` insert: `{dateTime, userRole:"JobSeeker", subRole:null, userName, stage:empStatus, action:"Prescreening Completed", reasonOrComments:"Jobseeker has completed his Pre screening", jobSeekerId:ObjectId, contestId:ObjectId, recruiterId:ObjectId, createdBy:"JobSeeker"}`.
- **Model:** `gemini-3.1-flash-lite` via `https://generativelanguage.googleapis.com/v1beta/models/<model>:generateContent` (header `x-goog-api-key`). No Vertex.
- **Providers dependency-injected** — unit tests mock Mongo/Redis/Gemini; NO real network calls in tests.
- **Scoring never affects the live interview** — the worker enqueue is best-effort, isolated in its own try/except.
- **Scope:** only natural-end interviews (their transcript reaches Mongo). No secrets in code/manifests; env only.

---

### Task 1: Capture recruiterId + jsId at session creation

**Files:**
- Modify: `backend/src/createSession.js`
- Modify: `agent/tara_agent/session_store.py`
- Test: `backend/test/createSession.test.js`, `agent/tests/test_session_store.py`

**Interfaces:**
- Produces: the Redis `session:<id>` blob gains `recruiterId` + `jsId` (strings, default `""`); Python `SessionBlob` gains `recruiter_id` + `js_id`.

- [ ] **Step 1: Write the failing Node test**

Add to `backend/test/createSession.test.js`:

```javascript
test("stores recruiterId and jsId in the session blob", async () => {
  const store = {};
  const redis = { set: async (k, v) => { store[k] = v; } };
  const fakeLimiter = { tryAdmit: async () => ({ admitted: true, state: "new" }) };
  const out = await createSession(redis, () => "tok", fakeLimiter, {
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j", recruiterId: "rec1", jsId: "js1",
  });
  const blob = JSON.parse(store[`session:${out.sessionId}`]);
  expect(blob.recruiterId).toBe("rec1");
  expect(blob.jsId).toBe("js1");
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npx jest createSession`
Expected: FAIL — blob has no `recruiterId`/`jsId`.

- [ ] **Step 3: Add the fields in createSession.js**

In `backend/src/createSession.js`, in the `blob` object literal, add the two fields (after `jdText`):

```javascript
    recruiterId: body.recruiterId ?? "", jsId: body.jsId ?? "",
```

- [ ] **Step 4: Run to green**

Run: `cd backend && npx jest createSession`
Expected: PASS

- [ ] **Step 5: Write the failing Python test**

Add to `agent/tests/test_session_store.py` (match the file's existing async style):

```python
async def test_blob_carries_recruiter_and_js_ids(real_redis):
    import json
    from tara_agent.session_store import load_session
    await real_redis.set("session:s1", json.dumps({
        "contestId": "c1", "candidateId": "u1", "skills": ["Python"],
        "resumeText": "r", "jdText": "j", "maxQuestions": 5,
        "recruiterId": "rec1", "jsId": "js1",
    }))
    blob = await load_session(real_redis, "s1")
    assert blob.recruiter_id == "rec1"
    assert blob.js_id == "js1"
```

> If `test_session_store.py` doesn't use the `real_redis` fixture, mirror whatever redis stub it already uses; the assertion on the two fields is the point.

- [ ] **Step 6: Run to verify it fails**

Run: `cd agent && source .venv/bin/activate && pytest tests/test_session_store.py -v`
Expected: FAIL — `SessionBlob` has no `recruiter_id`.

- [ ] **Step 7: Add the fields to SessionBlob**

In `agent/tara_agent/session_store.py`, add two fields to the dataclass (with defaults so older blobs still load) and read them in `load_session`:

```python
@dataclass(frozen=True)
class SessionBlob:
    contest_id: str
    candidate_id: str
    skills: list[str]
    resume_text: str
    jd_text: str
    max_questions: int
    recruiter_id: str = ""
    js_id: str = ""
```

And in `load_session`'s `return SessionBlob(...)`, add:

```python
        recruiter_id=d.get("recruiterId", ""), js_id=d.get("jsId", ""),
```

- [ ] **Step 8: Run both suites to green**

Run: `cd backend && npx jest` then `cd agent && pytest -q`
Expected: PASS (no regressions).

- [ ] **Step 9: Commit**

```bash
git add backend/src/createSession.js backend/test/createSession.test.js agent/tara_agent/session_store.py agent/tests/test_session_store.py
git commit -m "feat(scoring): capture recruiterId+jsId at session creation (blob + SessionBlob)"
```

---

### Task 2: Worker enqueue + persist ATS IDs at natural interview end

**Files:**
- Modify: `agent/tara_agent/persistence.py`
- Modify: `agent/tara_agent/interview_agent.py`
- Modify: `agent/tara_agent/worker.py`
- Test: `agent/tests/test_persistence.py`

**Interfaces:**
- Consumes: `SessionBlob.recruiter_id`/`js_id` (Task 1).
- Produces: `end_interview(...)` gains `recruiter_id`, `js_id`, and `enqueue_fn` params; the persisted `aiInterview` doc carries `recruiterId`/`jsId`; after a successful Mongo write, `enqueue_fn(room)` is called best-effort. The worker wires `enqueue_fn` to `redis.lpush("tara:score:queue", room)`.

- [ ] **Step 1: Write the failing test**

Add to `agent/tests/test_persistence.py`:

```python
async def test_end_interview_persists_ids_and_enqueues():
    written = {}
    async def mongo_write(doc):
        written.update(doc); return {"ok": 1}
    enqueued = []
    async def enqueue(room):
        enqueued.append(room)

    class _T:
        async def assemble(self):
            return [{"seq": 0, "speaker": "tara", "text": "hi"}]

    from tara_agent.persistence import end_interview
    await end_interview(
        say_fn=lambda: __import__("asyncio").sleep(0),
        transcript_store=_T(), mongo_write_fn=mongo_write,
        room="room1", contest_id="c1", candidate_id="u1",
        recruiter_id="rec1", js_id="js1", enqueue_fn=enqueue,
        say_timeout=1.0, write_timeout=1.0, on_finally=lambda: None,
    )
    assert written["recruiterId"] == "rec1" and written["jsId"] == "js1"
    assert enqueued == ["room1"]   # enqueued once, after the write
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_persistence.py::test_end_interview_persists_ids_and_enqueues -v`
Expected: FAIL — `end_interview` has no `recruiter_id`/`enqueue_fn` params.

- [ ] **Step 3: Update end_interview**

In `agent/tara_agent/persistence.py`, add the three params (defaulted) and the doc fields + best-effort enqueue. The full function (preserving the Task-2-Phase-4 retry wrapping and exactly-once `on_finally`):

```python
async def end_interview(*, say_fn, transcript_store, mongo_write_fn, room,
                        contest_id, candidate_id, say_timeout, write_timeout,
                        on_finally,
                        recruiter_id: str = "", js_id: str = "", enqueue_fn=None,
                        offpath_retry_attempts: int = 4,
                        retry_base_delay_ms: int = 50,
                        retry_max_delay_ms: int = 400):
    try:
        try:
            await asyncio.wait_for(say_fn(), timeout=say_timeout)
        except (asyncio.TimeoutError, Exception) as e:
            log.warning("closing say failed/hung: %s", e)

        lines = await transcript_store.assemble()
        from tara_agent.audit import audit_transcript
        problems = audit_transcript(lines)
        if problems:
            log.warning("transcript audit problems for room %s: %s", room, problems)
        doc = {"room": room, "contestId": contest_id,
               "candidateId": candidate_id, "recruiterId": recruiter_id,
               "jsId": js_id, "transcript": lines}
        wrote = False
        try:
            async def _write():
                return await asyncio.wait_for(mongo_write_fn(doc), timeout=write_timeout)
            await retry_async(
                _write, attempts=offpath_retry_attempts,
                base_delay=retry_base_delay_ms / 1000.0,
                max_delay=retry_max_delay_ms / 1000.0,
            )
            wrote = True
        except (asyncio.TimeoutError, Exception) as e:
            log.error("transcript persist failed/hung (recoverable from Redis): %s", e)
        # Best-effort: enqueue for async scoring only after a successful write.
        if wrote and enqueue_fn is not None:
            try:
                await enqueue_fn(room)
            except Exception as e:  # never let enqueue break teardown
                log.warning("score enqueue failed for %s (re-queue later): %s", room, e)
    finally:
        on_finally()
```

- [ ] **Step 4: Wire the call site in interview_agent.py**

In `agent/tara_agent/interview_agent.py` `_wrap_up`, add to the `end_interview(...)` call:

```python
            recruiter_id=self._blob.recruiter_id,
            js_id=self._blob.js_id,
            enqueue_fn=self._enqueue_fn,
```

And add `enqueue_fn=None` to `InterviewAgent.__init__` params + `self._enqueue_fn = enqueue_fn`.

- [ ] **Step 5: Wire the worker**

In `agent/tara_agent/worker.py`, where `InterviewAgent(...)` is constructed, add the enqueue closure:

```python
        enqueue_fn=lambda room: redis.lpush("tara:score:queue", room),
```

(`redis` is the `aioredis` client already in scope; `lpush` returns a coroutine that `end_interview` awaits.)

- [ ] **Step 6: Run to green + full suite**

Run: `cd agent && pytest tests/test_persistence.py -v && pytest -q`
Expected: PASS (new test green; no regressions — existing `end_interview` callers use defaults).

- [ ] **Step 7: Verify worker import**

Run: `cd agent && python -c "from tara_agent import worker"`
Expected: exits 0.

- [ ] **Step 8: Commit**

```bash
git add agent/tara_agent/persistence.py agent/tara_agent/interview_agent.py agent/tara_agent/worker.py agent/tests/test_persistence.py
git commit -m "feat(scoring): persist recruiterId/jsId + enqueue sessionId to tara:score:queue at end"
```

---

### Task 3: Scorer — transcript loading + Q&A pairing (+ mongodb dep)

**Files:**
- Create: `backend/src/scorer/transcript.js`
- Test: `backend/test/scorer-transcript.test.js`
- Modify: `backend/package.json` (add `mongodb`)

**Interfaces:**
- Produces:
  - `buildQAPairs(lines) -> [{question, answer}]` — pure; pairs each `tara` line (question) with the concatenation of following `candidate` line(s) until the next `tara`.
  - `async loadInterview(db, sessionId) -> doc|null` — `db.collection("aiInterview").findOne({room: sessionId})`.

- [ ] **Step 1: Add the mongodb dependency**

Run: `cd backend && npm install mongodb@^6`
Expected: `mongodb` added to `package.json` dependencies. (Network install; if it fails, report BLOCKED with the error — do not fake it.)

- [ ] **Step 2: Write the failing test**

```javascript
// backend/test/scorer-transcript.test.js
const { buildQAPairs, loadInterview } = require("../src/scorer/transcript");

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

test("loadInterview queries aiInterview by room", async () => {
  const db = { collection: (n) => ({ findOne: async (q) => ({ n, q }) }) };
  const out = await loadInterview(db, "sess1");
  expect(out.n).toBe("aiInterview");
  expect(out.q).toEqual({ room: "sess1" });
});
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd backend && npx jest scorer-transcript`
Expected: FAIL — `Cannot find module '../src/scorer/transcript'`.

- [ ] **Step 4: Implement transcript.js**

```javascript
// backend/src/scorer/transcript.js
function buildQAPairs(lines) {
  const ordered = [...lines].sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
  const pairs = [];
  let cur = null;
  for (const l of ordered) {
    if (l.speaker === "tara") {
      if (cur) pairs.push(cur);
      cur = { question: l.text || "", answer: "" };
    } else if (l.speaker === "candidate" && cur) {
      cur.answer = cur.answer ? `${cur.answer} ${l.text || ""}`.trim() : (l.text || "");
    }
  }
  if (cur) pairs.push(cur);
  return pairs;
}

async function loadInterview(db, sessionId) {
  return db.collection("aiInterview").findOne({ room: sessionId });
}

module.exports = { buildQAPairs, loadInterview };
```

- [ ] **Step 5: Run to green + commit**

Run: `cd backend && npx jest scorer-transcript`
Expected: PASS

```bash
git add backend/src/scorer/transcript.js backend/test/scorer-transcript.test.js backend/package.json backend/package-lock.json
git commit -m "feat(scoring): scorer transcript loader + Q&A pairing (+ mongodb dep)"
```

---

### Task 4: Scorer — scoring prompt, parse/coerce, verdict

**Files:**
- Create: `backend/src/scorer/score.js`
- Test: `backend/test/scorer-score.test.js`

**Interfaces:**
- Produces:
  - `buildPrompt({resumeText, jdText, qaPairs, mustHave, goodToHave}) -> string`.
  - `parseReport(rawText, {mustHave, goodToHave}) -> report` — strips ``` fences, JSON-parses, coerces (`remarks.communication`→String, every `rating`→Number), and on any failure returns `fallbackReport(mustHave, goodToHave)`. NEVER throws.
  - `computeVerdict(report) -> {avg, recommendation, empStatus, copilotScore}` — `avg = mean(primarySkillsRatings.rating) * 2` (0 if none); thresholds ≥8.5/≥7/≥5.5; `empStatus="Completed"`; `copilotScore=round(avg*10)`.
  - `async scoreInterview(geminiCall, {resumeText, jdText, qaPairs, mustHave, goodToHave}) -> report` — `geminiCall(prompt) -> rawText`, then `parseReport`. `geminiCall` is injected (real one in index.js).

- [ ] **Step 1: Write the failing test**

```javascript
// backend/test/scorer-score.test.js
const { parseReport, computeVerdict, scoreInterview } = require("../src/scorer/score");

test("parseReport strips fences and coerces types", () => {
  const raw = "```json\n" + JSON.stringify({
    overall_evaluation: "ok", recommendation: "Hire", key_strengths: ["a"],
    remarks: { communication: 4 },
    primarySkillsRatings: [{ skill: "Python", rating: "4.0" }],
    secondarySkillsRatings: [], comment: "c",
  }) + "\n```";
  const r = parseReport(raw, { mustHave: ["Python"], goodToHave: [] });
  expect(typeof r.remarks.communication).toBe("string");
  expect(r.primarySkillsRatings[0].rating).toBe(4);
});

test("parseReport falls back on malformed JSON (never throws)", () => {
  const r = parseReport("not json", { mustHave: ["SQL"], goodToHave: ["AWS"] });
  expect(r.recommendation).toBeDefined();
  expect(Array.isArray(r.primarySkillsRatings)).toBe(true);
});

test("computeVerdict applies thresholds", () => {
  const mk = (rating) => ({ primarySkillsRatings: [{ skill: "x", rating }] });
  expect(computeVerdict(mk(4.5)).recommendation).toBe("Strong Hire"); // 9.0
  expect(computeVerdict(mk(3.6)).recommendation).toBe("Hire");        // 7.2
  expect(computeVerdict(mk(2.8)).recommendation).toBe("Maybe");       // 5.6
  expect(computeVerdict(mk(2.0)).recommendation).toBe("No Hire");     // 4.0
  expect(computeVerdict(mk(4.5)).copilotScore).toBe(90);
  expect(computeVerdict(mk(4.5)).empStatus).toBe("Completed");
});

test("scoreInterview uses injected geminiCall", async () => {
  const fakeGemini = async () => JSON.stringify({
    overall_evaluation: "x", recommendation: "Maybe", key_strengths: [],
    remarks: { communication: "ok" },
    primarySkillsRatings: [{ skill: "Python", rating: 3 }],
    secondarySkillsRatings: [], comment: "c",
  });
  const r = await scoreInterview(fakeGemini, {
    resumeText: "r", jdText: "j", qaPairs: [{ question: "q", answer: "a" }],
    mustHave: ["Python"], goodToHave: [],
  });
  expect(r.primarySkillsRatings[0].rating).toBe(3);
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npx jest scorer-score`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement score.js**

```javascript
// backend/src/scorer/score.js
function fallbackReport(mustHave, goodToHave) {
  const rate = (skills) => (skills || []).slice(0, 10).map((s) => ({ skill: s, rating: 2.5 }));
  return {
    overall_evaluation: "Automated fallback: the model response could not be parsed.",
    recommendation: "Maybe",
    key_strengths: ["Interview participation"],
    remarks: { communication: "Not assessed (fallback)." },
    primarySkillsRatings: rate(mustHave),
    secondarySkillsRatings: rate(goodToHave),
    comment: "Fallback report generated; manual review recommended.",
  };
}

function buildPrompt({ resumeText, jdText, qaPairs, mustHave, goodToHave }) {
  const qa = (qaPairs || [])
    .map((p, i) => `Q${i + 1}: ${p.question}\nA${i + 1}: ${p.answer}`)
    .join("\n");
  return `Expert technical interview analysis. Return ONLY valid JSON.
JOB DESCRIPTION:\n${jdText || ""}\nRESUME:\n${resumeText || ""}
MUST-HAVE SKILLS: ${(mustHave || []).join(", ")}
GOOD-TO-HAVE SKILLS: ${(goodToHave || []).join(", ")}
TRANSCRIPT (${(qaPairs || []).length} questions):\n${qa}

Return JSON exactly:
{
  "overall_evaluation": "3-4 sentence summary",
  "recommendation": "Strong Hire|Hire|Maybe|No Hire",
  "key_strengths": ["s1","s2","s3"],
  "remarks": { "communication": "string" },
  "primarySkillsRatings":   [{"skill":"...","rating": 4.0}],
  "secondarySkillsRatings": [{"skill":"...","rating": 3.0}],
  "comment": "one paragraph recommendation"
}
Rules: Rate ALL listed skills 0-5. remarks.communication = STRING. ratings = NUMBERS.`;
}

function parseReport(rawText, { mustHave, goodToHave }) {
  try {
    let s = (rawText || "").trim();
    for (const fence of ["```json", "```"]) {
      if (s.startsWith(fence)) s = s.slice(fence.length);
    }
    if (s.endsWith("```")) s = s.slice(0, -3);
    const r = JSON.parse(s.trim());
    if (r.remarks && r.remarks.communication != null) {
      r.remarks.communication = String(r.remarks.communication);
    }
    for (const key of ["primarySkillsRatings", "secondarySkillsRatings"]) {
      if (Array.isArray(r[key])) {
        r[key] = r[key].map((it) => ({ skill: it.skill, rating: Number(it.rating) || 0 }));
      } else {
        r[key] = [];
      }
    }
    if (!r.recommendation) r.recommendation = "Maybe";
    return r;
  } catch (e) {
    return fallbackReport(mustHave, goodToHave);
  }
}

function computeVerdict(report) {
  const ratings = (report.primarySkillsRatings || []).map((r) => Number(r.rating) || 0);
  const meanRating = ratings.length ? ratings.reduce((a, b) => a + b, 0) / ratings.length : 0;
  const avg = meanRating * 2; // 0-5 skill scale → 0-10
  let recommendation;
  if (avg >= 8.5) recommendation = "Strong Hire";
  else if (avg >= 7) recommendation = "Hire";
  else if (avg >= 5.5) recommendation = "Maybe";
  else recommendation = "No Hire";
  return { avg, recommendation, empStatus: "Completed", copilotScore: Math.round(avg * 10) };
}

async function scoreInterview(geminiCall, args) {
  const raw = await geminiCall(buildPrompt(args));
  return parseReport(raw, { mustHave: args.mustHave, goodToHave: args.goodToHave });
}

module.exports = { buildPrompt, parseReport, computeVerdict, scoreInterview, fallbackReport };
```

- [ ] **Step 4: Run to green + commit**

Run: `cd backend && npx jest scorer-score`
Expected: PASS

```bash
git add backend/src/scorer/score.js backend/test/scorer-score.test.js
git commit -m "feat(scoring): scoring prompt + parse/coerce/fallback + verdict thresholds"
```

---

### Task 5: Scorer — skills loader

**Files:**
- Create: `backend/src/scorer/skills.js`
- Test: `backend/test/scorer-skills.test.js`

**Interfaces:**
- Produces: `async loadSkills(db, contestId, blobSkills) -> {mustHave, goodToHave}` — reads `contests` (`findOne({contestId: ObjectId})`, fallback `findOne({_id: ObjectId})`), returns `job_details.mustHaveSkills`/`goodToHave`; on miss or invalid ObjectId, `{mustHave: blobSkills||[], goodToHave: []}`.

- [ ] **Step 1: Write the failing test**

```javascript
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npx jest scorer-skills`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement skills.js**

```javascript
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
```

- [ ] **Step 4: Run to green + commit**

Run: `cd backend && npx jest scorer-skills`
Expected: PASS

```bash
git add backend/src/scorer/skills.js backend/test/scorer-skills.test.js
git commit -m "feat(scoring): contests skills loader with blob fallback"
```

---

### Task 6: Scorer — idempotent ATS persist

**Files:**
- Create: `backend/src/scorer/persist.js`
- Test: `backend/test/scorer-persist.test.js`

**Interfaces:**
- Consumes: `computeVerdict` output, the report, the interview doc ids.
- Produces: `async persistReport(db, { sessionId, contestId, candidateId, recruiterId, jsId, report, verdict }) -> {ats: boolean}` — idempotent `aiInterview` write (replace on `session_id`); when `recruiterId`+`jsId`+`contestId` are valid ObjectIds, also `$set` `recruiterAddProfiles` and insert `auditTrail` (with `userName` looked up from the matched profile); returns `{ats:false}` (report still stored) if ids are missing/invalid.

- [ ] **Step 1: Write the failing test**

```javascript
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
  expect(upd.$set["jobseekerDetails.$.empStatus"]).toBe("Completed");
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npx jest scorer-persist`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement persist.js**

```javascript
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
  await db.collection("recruiterAddProfiles").updateOne(
    { contestId: cOid, recruiterId: rOid, "jobseekerDetails.jsId": jOid },
    { $set: {
      "jobseekerDetails.$.prescreeningreport": report,
      "jobseekerDetails.$.copilotScore": verdict.copilotScore,
      "jobseekerDetails.$.prescreeningStatus": "True",
      "jobseekerDetails.$.empStatus": verdict.empStatus,
      "jobseekerDetails.$.status": verdict.empStatus,
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
```

- [ ] **Step 4: Run to green + commit**

Run: `cd backend && npx jest scorer-persist`
Expected: PASS

```bash
git add backend/src/scorer/persist.js backend/test/scorer-persist.test.js
git commit -m "feat(scoring): idempotent ATS write-back (aiInterview/recruiterAddProfiles/auditTrail)"
```

---

### Task 7: Scorer — pipeline orchestration

**Files:**
- Create: `backend/src/scorer/pipeline.js`
- Test: `backend/test/scorer-pipeline.test.js`

**Interfaces:**
- Consumes: `loadInterview`/`buildQAPairs` (T3), `loadSkills` (T5), `scoreInterview` (T4), `computeVerdict` (T4), `persistReport` (T6).
- Produces: `async scoreSession({ db, geminiCall }, sessionId) -> {ok, ats}` — loads the interview doc; if absent, returns `{ok:false, reason:"not_found"}`; else builds Q&A, loads skills (using the doc's `contestId` + the session blob `skills` if present on the doc), scores, computes verdict, persists, returns `{ok:true, ats}`.

> The interview doc carries `resumeText`/`jdText`? It carries `transcript` + ids but NOT resume/jd (those are in the Redis blob, expired by scoring time). Use what the doc has; pass `resumeText`/`jdText` as `""` when absent (the prompt tolerates empty). Skills come from `contests` (authoritative) via `loadSkills(db, doc.contestId, [])`.

- [ ] **Step 1: Write the failing test**

```javascript
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npx jest scorer-pipeline`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement pipeline.js**

```javascript
// backend/src/scorer/pipeline.js
const { loadInterview, buildQAPairs } = require("./transcript");
const { loadSkills } = require("./skills");
const { scoreInterview, computeVerdict } = require("./score");
const { persistReport } = require("./persist");

async function scoreSession({ db, geminiCall }, sessionId) {
  const doc = await loadInterview(db, sessionId);
  if (!doc) return { ok: false, reason: "not_found" };
  const qaPairs = buildQAPairs(doc.transcript || []);
  const { mustHave, goodToHave } = await loadSkills(db, doc.contestId, doc.skills || []);
  const report = await scoreInterview(geminiCall, {
    resumeText: doc.resumeText || "", jdText: doc.jdText || "",
    qaPairs, mustHave, goodToHave,
  });
  const verdict = computeVerdict(report);
  report.recommendation = verdict.recommendation; // keep report + verdict consistent
  const { ats } = await persistReport(db, {
    sessionId, contestId: doc.contestId, candidateId: doc.candidateId,
    recruiterId: doc.recruiterId || "", jsId: doc.jsId || "", report, verdict,
  });
  return { ok: true, ats };
}

module.exports = { scoreSession };
```

- [ ] **Step 4: Run to green + commit**

Run: `cd backend && npx jest scorer-pipeline`
Expected: PASS

```bash
git add backend/src/scorer/pipeline.js backend/test/scorer-pipeline.test.js
git commit -m "feat(scoring): pipeline orchestration (transcript→skills→score→persist)"
```

---

### Task 8: Scorer — queue consumer + entrypoint

**Files:**
- Create: `backend/src/scorer/queue.js`, `backend/src/scorer/index.js`
- Test: `backend/test/scorer-queue.test.js`

**Interfaces:**
- Consumes: `scoreSession` (T7).
- Produces:
  - `async consumeOnce(redis, handle, { queue, dlq, maxAttempts }) -> sessionId|null` — `BRPOP queue` (1s timeout); on a message, `handle(sessionId)`; on throw, increment `tara:score:attempts:<id>` (`redis.incr`) and re-`LPUSH` to `queue`, or `LPUSH dlq` + reset the counter once attempts ≥ `maxAttempts`. Returns the sessionId processed (or null on timeout).
  - `async runConsumer(redis, handle, opts)` — loops `consumeOnce` until `opts.stop()` is true.
  - `index.js` — builds `MongoClient`, `ioredis`, and a fetch-based `geminiCall`, then `runConsumer(redis, (id) => scoreSession({db, geminiCall}, id), ...)`; installs SIGTERM to flip a stop flag.

- [ ] **Step 1: Write the failing test**

```javascript
// backend/test/scorer-queue.test.js
const { consumeOnce } = require("../src/scorer/queue");

function fakeRedis(initial) {
  const lists = { ...initial };
  const counters = {};
  return {
    lists, counters,
    brpop: async (q) => (lists[q] && lists[q].length ? [q, lists[q].shift()] : null),
    lpush: async (q, v) => { (lists[q] = lists[q] || []).unshift(v); },
    incr: async (k) => (counters[k] = (counters[k] || 0) + 1),
    del: async (k) => { delete counters[k]; },
  };
}

const OPTS = { queue: "q", dlq: "dlq", maxAttempts: 3 };

test("processes one message with the handler", async () => {
  const r = fakeRedis({ q: ["s1"] });
  const seen = [];
  const id = await consumeOnce(r, async (s) => seen.push(s), OPTS);
  expect(id).toBe("s1");
  expect(seen).toEqual(["s1"]);
});

test("re-enqueues on failure, then DLQs after maxAttempts", async () => {
  const r = fakeRedis({ q: ["s1"] });
  const boom = async () => { throw new Error("fail"); };
  await consumeOnce(r, boom, OPTS); // attempt 1 → requeue
  await consumeOnce(r, boom, OPTS); // attempt 2 → requeue
  await consumeOnce(r, boom, OPTS); // attempt 3 → DLQ
  expect(r.lists.dlq).toEqual(["s1"]);
  expect(r.lists.q.length).toBe(0);
});

test("timeout returns null", async () => {
  const r = fakeRedis({});
  expect(await consumeOnce(r, async () => {}, OPTS)).toBeNull();
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npx jest scorer-queue`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement queue.js**

```javascript
// backend/src/scorer/queue.js
async function consumeOnce(redis, handle, { queue, dlq, maxAttempts }) {
  const popped = await redis.brpop(queue, 1); // [queue, value] | null
  if (!popped) return null;
  const sessionId = popped[1];
  try {
    await handle(sessionId);
  } catch (e) {
    const n = await redis.incr(`tara:score:attempts:${sessionId}`);
    if (n >= maxAttempts) {
      await redis.lpush(dlq, sessionId);
      await redis.del(`tara:score:attempts:${sessionId}`);
    } else {
      await redis.lpush(queue, sessionId);
    }
  }
  return sessionId;
}

async function runConsumer(redis, handle, opts) {
  while (!opts.stop()) {
    await consumeOnce(redis, handle, opts);
  }
}

module.exports = { consumeOnce, runConsumer };
```

- [ ] **Step 4: Implement index.js (wiring — no unit test; integration seam)**

```javascript
// backend/src/scorer/index.js
const { MongoClient } = require("mongodb");
const Redis = require("ioredis");
const { runConsumer } = require("./queue");
const { scoreSession } = require("./pipeline");

const MODEL = process.env.GEMINI_MODEL || "gemini-3.1-flash-lite";
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:generateContent`;

async function geminiCall(prompt) {
  const r = await fetch(GEMINI_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-goog-api-key": process.env.GEMINI_API_KEY },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: { temperature: 0.2, responseMimeType: "application/json" },
    }),
  });
  const data = await r.json();
  return data?.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
}

async function main() {
  const mongo = new MongoClient(process.env.MONGODB_URI);
  await mongo.connect();
  const db = mongo.db("tara");
  const redis = new Redis(process.env.REDIS_URL);
  let stopping = false;
  const stop = () => stopping;
  process.on("SIGTERM", () => { stopping = true; });
  console.log("scorer up");
  await runConsumer(redis, (id) => scoreSession({ db, geminiCall }, id),
    { queue: "tara:score:queue", dlq: "tara:score:dlq", maxAttempts: 3, stop });
  await redis.quit();
  await mongo.close();
}

if (require.main === module) main().catch((e) => { console.error(e); process.exit(1); });
module.exports = { geminiCall };
```

- [ ] **Step 5: Run to green + full suite + commit**

Run: `cd backend && npx jest scorer-queue && npx jest`
Expected: PASS (whole backend suite green).

```bash
git add backend/src/scorer/queue.js backend/src/scorer/index.js backend/test/scorer-queue.test.js
git commit -m "feat(scoring): redis-queue consumer (re-enqueue + DLQ) + scorer entrypoint"
```

---

### Task 9: Scorer deployment manifest

**Files:**
- Create: `deploy/scorer-deployment.yaml`
- Modify: `deploy/README.md` (one line for the scorer)

**Interfaces:**
- Produces: a `tara-scorer` Deployment running the backend image with `command: ["node","src/scorer/index.js"]`, env from `tara-config` + `tara-secrets`, no ports, 2 replicas.

- [ ] **Step 1: Write the manifest**

```yaml
# deploy/scorer-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tara-scorer
  labels:
    app: tara-scorer
    app.kubernetes.io/part-of: tara
spec:
  replicas: 2
  selector:
    matchLabels:
      app: tara-scorer
  template:
    metadata:
      labels:
        app: tara-scorer
        app.kubernetes.io/part-of: tara
    spec:
      containers:
        - name: tara-scorer
          image: IMAGE_REGISTRY/tara-backend:TAG
          imagePullPolicy: IfNotPresent
          command: ["node", "src/scorer/index.js"]
          envFrom:
            - configMapRef:
                name: tara-config
          env:
            - name: REDIS_URL
              valueFrom: { secretKeyRef: { name: tara-secrets, key: REDIS_URL } }
            - name: MONGODB_URI
              valueFrom: { secretKeyRef: { name: tara-secrets, key: MONGODB_URI } }
            - name: GEMINI_API_KEY
              valueFrom: { secretKeyRef: { name: tara-secrets, key: GEMINI_API_KEY } }
          resources:
            requests: { cpu: "100m", memory: 256Mi }
            limits: { cpu: "500m", memory: 512Mi }
      terminationGracePeriodSeconds: 30
```

- [ ] **Step 2: Validate + secret scan**

Run: `kubeconform -strict -summary deploy/scorer-deployment.yaml`
Expected: `Valid: 1`.
Run: `grep -rinE '(AKIA|BEGIN PRIVATE KEY|mongodb\+srv://[a-z])' deploy/scorer-deployment.yaml | grep -viE '(secretKeyRef|REPLACE_ME)' || echo CLEAN`
Expected: `CLEAN`.

- [ ] **Step 3: README + commit**

Add one line to `deploy/README.md` describing `scorer-deployment.yaml` (backend image, `node src/scorer/index.js`, consumes `tara:score:queue`; future KEDA on `LLEN`).

```bash
git add deploy/scorer-deployment.yaml deploy/README.md
git commit -m "feat(scoring): tara-scorer Deployment (backend image, queue consumer)"
```

---

## Self-Review

**Spec coverage:**
- Trigger (Redis queue + DLQ) → Task 2 (enqueue) + Task 8 (consume/DLQ).
- recruiterId/jsId capture → Task 1; persisted on the doc → Task 2.
- transcript→Q&A → Task 3; scoring+verdict+report schema+thresholds → Task 4; skills → Task 5; idempotent ATS write-back (3 collections, exact shapes) → Task 6; orchestration → Task 7; entrypoint+model fetch → Task 8; deployment → Task 9.
- Error handling: best-effort enqueue (Task 2), fallback report (Task 4), re-enqueue+DLQ (Task 8), idempotent writes + missing-id skip (Task 6). Covered.
- Providers injected; no real network in unit tests — every scorer test passes mocks/fakes. Covered.

**Placeholder scan:** No TBD/"add error handling"/"similar to". Every code step is complete. Image ref `IMAGE_REGISTRY/tara-backend:TAG` is an intentional placeholder (operator/Phase-4 convention), not a gap.

**Type/name consistency:** `buildQAPairs`/`loadInterview` (T3) consumed by `scoreSession` (T7); `scoreInterview(geminiCall,args)`/`computeVerdict`/`parseReport`/`fallbackReport` (T4) consistent T4↔T7; `loadSkills(db,contestId,blobSkills)` (T5) consistent; `persistReport(db, {sessionId,contestId,candidateId,recruiterId,jsId,report,verdict})` (T6) matches the call in T7; `consumeOnce(redis,handle,{queue,dlq,maxAttempts})`/`runConsumer` (T8) consistent; verdict fields `{avg,recommendation,empStatus,copilotScore}` consistent T4↔T6↔T7. `SessionBlob.recruiter_id/js_id` (T1) consumed in T2. Queue name `tara:score:queue` consistent T2↔T8.

**Note on scope honesty:** the scorer's runtime correctness (real Mongo/Redis/Gemini) is exercised only on the cluster/integration; unit tests prove the logic with injected mocks. The interview doc lacks `resumeText`/`jdText` (those expire with the Redis blob) — the prompt tolerates empty; skills come from `contests` (authoritative). This is stated in Task 7.

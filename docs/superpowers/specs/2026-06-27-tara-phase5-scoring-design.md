# Tara Phase 5 — Async Scoring Service — Design

> **Phase 5 (final) of the Tara cascaded voice-interview agent.** Builds on merged `main`
> (Phases 1–4; tag `phase-4-merged`). Implements the master spec's §8.5 "Scoring service —
> async, separate, Node; reads transcript from Mongo, writes ATS collections." The live
> pipeline defers ALL scoring (master spec §9); this phase is where scoring happens — off the
> latency path, reacting to interview completion.

**Branch:** `tara/phase5-scoring` (off `main` @ `phase-4-merged` = merge `13b2bed`).
**Back-out target:** tag `phase-4-merged`.

## Goal

After a Tara interview ends, produce a recruiter-facing AI evaluation from the raw transcript and
write it into the existing ATS collections, with a schema **byte-compatible with the prior
production app** (`AI Interview Final Draft/report.py`) so existing recruiter views render it
unchanged. Scoring is async, decoupled, and never touches the live interview.

## Locked decisions (from brainstorming)
- **Trigger:** Redis queue `tara:score:queue` (worker `LPUSH`es sessionId at natural end; scorer `BRPOP`s).
- **ATS IDs:** captured at session creation (`recruiterId` + `jsId`), stored in the session blob and the persisted `aiInterview` transcript doc.
- **Model:** `gemini-3.1-flash-lite` (off-path; structured-JSON prompt).
- **Report schema:** verbatim from the prior app (ATS-compatible).
- **Scope boundary:** only *natural-end* interviews are scored (their transcript is flushed to Mongo). Abandoned/disconnected interviews (Phase-4 deferral) are out — same logged follow-up.

---

## Section A — Architecture, trigger & data model

A **separate Node consumer process** at `backend/src/scorer/`, run as `node src/scorer/index.js`
(its own k8s Deployment). It shares Redis + MongoDB with the existing services; no HTTP surface.

**End-to-end flow:**
1. **Session creation** (`backend/src/createSession.js`): accept `recruiterId` + `jsId` in the body and store them in the Redis session blob (`session:<id>`) alongside `contestId`/`candidateId`/`skills`/`resumeText`/`jdText`/`maxQuestions`. Both are optional in the request for backward-compat, but the scorer's ATS write-back requires them (logged + skipped if absent — the report is still stored in `aiInterview`).
2. **Worker reads the IDs** (`agent/tara_agent/session_store.py` blob + `agent/tara_agent/prompts.py`/wherever the blob dataclass lives): surface `recruiter_id` + `js_id` from the blob so `end_interview` can persist them.
3. **Interview end** (Python, *natural-end path only* — `InterviewAgent._wrap_up` → `persistence.end_interview`): the persisted `aiInterview` transcript doc gains `recruiterId`/`jsId` fields; after the Mongo write succeeds, the worker `LPUSH`es the `sessionId` onto `tara:score:queue`. The enqueue is **best-effort** (own try/except, never blocks/raises into the interview teardown; a missed enqueue is recoverable — the doc is in Mongo and can be re-queued).
4. **Scorer** `BRPOP tara:score:queue` → loads the transcript doc → scores → writes ATS targets.

**Data-model additions (minimal):**
- Redis `session:<id>` blob: `+recruiterId, +jsId`.
- Mongo `aiInterview` transcript doc (written at end): `+recruiterId, +jsId` (already has `room`/`contestId`/`candidateId`/`transcript`).
- Redis keys: `tara:score:queue` (work list), `tara:score:dlq` (dead-letter list).

---

## Section B — The scorer pipeline & report schema

Five small, independently-testable units under `backend/src/scorer/`:

### `transcript.js`
- `loadInterview(mongo, sessionId)` → the `aiInterview` transcript doc (`{room, contestId, candidateId, recruiterId, jsId, transcript:[{seq,speaker,text,ts}]}`).
- `buildQAPairs(lines)` → `[{question, answer}]`: walk seq-ordered lines; each `tara` line opens a question, the following `candidate` line(s) (concatenated until the next `tara`) is the answer. Leading non-tara and trailing unanswered questions handled gracefully.

### `score.js`
- `buildPrompt(resumeText, jdText, qaPairs, mustHave, goodToHave)` → the structured-JSON prompt (adapted from the prior app's `generate_ai_analysis`).
- `scoreInterview(gemini, ...)` → one `gemini-3.1-flash-lite` call; parse the JSON (strip ``` fences), **coerce**: `remarks.communication`→string, every `rating`→number (0–5). On malformed/empty model output, return a deterministic **fallback report** (so a report is ALWAYS produced).
- `computeVerdict(report)` → `avgScore` (mean of `primarySkillsRatings` ratings, scaled to /10 = `rating*2`), `recommendation` (**avg ≥ 8.5 Strong Hire / ≥ 7 Hire / ≥ 5.5 Maybe / else No Hire**), `empStatus = "Completed"`, `copilotScore = round(avgScore * 10)`. (Recommendation also echoed inside the report JSON.)

**Report JSON (ATS-compatible, verbatim shape from the prior app):**
```json
{
  "overall_evaluation": "3-4 sentence summary",
  "recommendation": "Strong Hire|Hire|Maybe|No Hire",
  "key_strengths": ["s1","s2","s3"],
  "remarks": { "communication": "string", "...": "..." },
  "primarySkillsRatings":   [{"skill":"...","rating": 4.0}],
  "secondarySkillsRatings": [{"skill":"...","rating": 3.0}],
  "comment": "one paragraph recommendation"
}
```

### `persist.js` — idempotent ATS write-back (re-scoring overwrites, never duplicates)
- **`aiInterview`** insert: `{session_id, contest_id, js_id, recruiter_id, report, emp_status, prescreening_status:"True", created_at}`. Idempotency: delete-then-insert (or `update_one(..., upsert:true)`) keyed on `session_id`, so a re-score replaces.
- **`recruiterAddProfiles`** `update_one({contestId:ObjectId, recruiterId:ObjectId, "jobseekerDetails.jsId":ObjectId}, {$set:{ "jobseekerDetails.$.prescreeningreport": report, "jobseekerDetails.$.copilotScore": round(avg*10), "jobseekerDetails.$.prescreeningStatus":"True", "jobseekerDetails.$.empStatus": empStatus, "jobseekerDetails.$.status": empStatus }})`.
- **`auditTrail`** insert: `{dateTime, userRole:"JobSeeker", subRole:null, userName (firstName+lastName from the matched recruiterAddProfiles jobseeker, else "Unknown User"), stage: empStatus, action:"Prescreening Completed", reasonOrComments:"Jobseeker has completed his Pre screening", jobSeekerId:ObjectId, contestId:ObjectId, recruiterId:ObjectId, createdBy:"JobSeeker"}`.
- All IDs converted string→`ObjectId`; invalid ObjectId → skip ATS write-back (log), still store the report in `aiInterview` under the raw ids.

### `skills.js`
- `loadSkills(mongo, contestId, blobSkills)` → `{mustHave, goodToHave}` from `contests` (`find_one({contestId:ObjectId})` fallback `{_id:ObjectId}`, read `job_details.mustHaveSkills` + `goodToHave`); on miss, fall back to the blob's `skills` for `mustHave` and `[]` for `goodToHave`.

### `queue.js` + `index.js`
- `queue.js`: `BRPOP tara:score:queue` loop; per sessionId, run the pipeline; on success, done. On failure, increment an attempt counter (`tara:score:attempts:<id>`) and re-enqueue; after **N=3** attempts push to `tara:score:dlq` and log (one poison message can't wedge the loop).
- `index.js`: construct Mongo + Redis (ioredis) + Gemini clients from env (`MONGODB_URI`, `REDIS_URL`, `GEMINI_API_KEY`, `GEMINI_MODEL`); run the consumer loop. Graceful SIGTERM (finish the in-flight job, stop BRPOP, exit).

---

## Error handling & idempotency
- **Worker enqueue** is best-effort and isolated — scoring failure NEVER affects the live interview.
- **Gemini call** uses a bounded retry (3 attempts, jittered backoff) on transient errors; a permanent/malformed result → deterministic fallback report (interview still gets a record).
- **Per-job isolation:** each scorer job is wrapped; a crash on one session doesn't kill the loop; bounded re-queue + DLQ.
- **Idempotent writes:** re-scoring the same session overwrites its `aiInterview` report and re-`$set`s the profile — safe to replay from the queue or DLQ.
- **Partial ATS data:** missing `recruiterId`/`jsId` or invalid ObjectId → store the report in `aiInterview` (raw ids), skip the profile/audit write-back, log a warning. The report is never lost.

## Testing (TDD, jest, mocked providers)
- `buildQAPairs`: pairs tara→candidate correctly; handles leading candidate line, trailing unanswered question, multi-line answers.
- `computeVerdict`: the four recommendation thresholds; `copilotScore` rounding; empStatus.
- `score.js` parsing: strips ``` fences; coerces string ratings→number and communication→string; **malformed JSON → fallback report** (never throws).
- `persist.js`: asserts the EXACT `recruiterAddProfiles` `$set` keys, the `aiInterview` insert shape, and the `auditTrail` record shape against a mocked Mongo; idempotency (re-score replaces, not duplicates); invalid-ObjectId path skips write-back but still inserts `aiInterview`.
- `queue.js`: a poison job re-enqueues up to N then DLQs (fake Redis); a good job processes once.
- `skills.js`: reads contests; falls back to blob skills on miss.
- **No real Gemini/Mongo/Redis calls in unit tests** — inject mocks. (Real-Redis BRPOP behavior can use `redis-memory-server` like the limiter tests if a queue integration test is warranted.)
- Python side: `createSession`/blob changes covered by existing Node tests; the worker enqueue + blob `recruiter_id`/`js_id` surfacing covered by a small pytest on `end_interview` (asserts the doc carries the ids + the enqueue fn is called) — wiring seam, mock Redis/Mongo.

## Deployment
- `deploy/scorer-deployment.yaml` — runs the **backend image** (Phase-4 Dockerfile) with `command: ["node","src/scorer/index.js"]`; env from `tara-config` + `tara-secrets` (`REDIS_URL`, `MONGODB_URI`, `GEMINI_API_KEY`, `GEMINI_MODEL`). Small fixed replica count (e.g. 2); no session-count coupling. kubeconform-validated, no secrets. (Future: KEDA on `LLEN tara:score:queue` — out of scope for Phase 5.)

## Non-goals / guardrails
- No scoring on the live latency path (unchanged). No change to the cascaded pipeline.
- No new model providers; `gemini-3.1-flash-lite` via the Developer API (Vertex unavailable).
- No re-architecting the ATS schema — write the existing collections in the existing shapes.
- Abandoned-interview scoring is OUT (depends on the deferred Phase-4 Mongo-flush follow-up).
- No hardcoded secrets; env only.

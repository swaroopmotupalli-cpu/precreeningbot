# Tara — Cascaded Voice Interview Agent — System Design

**Date**: 2026-06-25
**Status**: Design — awaiting approval before implementation plan
**Supersedes**: `2026-05-29-tara-realtime-architecture-options.md` (historical; mined only for the existing MongoDB/ATS data model)
**Scope**: Whole-system design. Implementation proceeds in phases (see Build Order).

---

## 0. Decisions locked during brainstorming

| # | Decision | Choice |
|---|---|---|
| 1 | Scope of this doc | Whole system, one design; phased implementation |
| 2 | Authority when docs conflict | The Tara build brief is authoritative; the 2026-05-29 doc is historical context |
| 3 | Architecture class | Cascaded streaming pipeline (STT→LLM→TTS). **Not** speech-to-speech / Live |
| 4 | LLM | `gemini-3.1-flash-lite` (Stable), token-streaming, **plain text** on the hot path |
| 5 | STT | Google Cloud Speech-to-Text, Chirp, streaming (interim during speech, final on turn-end) |
| 6 | TTS | Google Cloud Text-to-Speech, Chirp3-HD, streaming, sentence-chunked |
| 7 | Turn detection | LiveKit `turn-detector` plugin (semantic EOU), **not** a fixed silence timer |
| 8 | Interview flow | **Fully dynamic** — LLM generates each question from JD + skills + resume + history |
| 9 | Scoring | **Deferred entirely** to an async Node pass after the interview; nothing scored on the hot path |
| 10 | Interview end | **Coverage + question cap** (off-path coverage tagger + integer counter) |
| 11 | Audio recording | **None.** Transcript is the audit record. S3 is used by Node to fetch resume/JD |
| 12 | Resume/JD fetch+parse | **Node backend, at session creation** → plain text into Redis session blob |
| 13 | Persistence | **Integrate existing ATS schema**: read `contests`; write `aiInterview`, `recruiterAddProfiles`, `auditTrail` |

---

## 1. System topology & the core principle

Two cooperating services share Redis (coordination + live transcript) and MongoDB (ATS + audit record).

```
                    ┌─────────────────────────────────────────┐
   Candidate ◄─────►│  LiveKit Cloud (WebRTC rooms)            │
   browser          └───────────────┬─────────────────────────┘
                                    │ worker SDK (gRPC)
                                    ▼
  ┌──────────────────┐      ┌──────────────────────────────────┐
  │ Node/Express      │      │  Python — LiveKit Agents worker   │
  │ backend           │      │  (the real-time pipeline)         │
  │                   │      │                                   │
  │ • auth, ATS REST  │      │  turn-detector → STT → LLM        │
  │ • create session  │      │   → sentence-chunk → TTS          │
  │ • fetch+parse     │      │  transcript → Redis               │
  │   resume/JD (S3)  │      │  acquire/refresh/release          │
  │ • issue LK token  │      │   admission lease                 │
  │ • admission cap   │      └────────────┬──────────────────────┘
  │ • async SCORING   │                   │
  └────────┬──────────┘                   │
           │            ┌─────────────┐    │
           ├───────────►│   Redis     │◄───┘   session blob, transcript buffer,
           │            │             │        admission buckets + leases, coverage
           │            └─────────────┘
           ▼
     ┌─────────────┐
     │ MongoDB ATS │  contests (read) · aiInterview / recruiterAddProfiles / auditTrail (write)
     └─────────────┘
```

> **Note on the Node backend:** the brief describes it as "existing — extend, don't rebuild," but the working directory currently contains only `.env`. The implementation plan treats the Node service as either a separate existing repo to extend or a fresh build; exact ATS field shapes (`contests.mustHaveSkills`, `aiInterview`, `recruiterAddProfiles`, `auditTrail`) must be confirmed against the real `app.py`/DB before the scoring + persistence phases.

### The one principle everything obeys — Hot path vs. Off path

| HOT PATH (on the <800ms clock) | OFF PATH (never blocks speech) |
|---|---|
| turn-detector → STT → **LLM plain-text** → sentence-chunk → streaming TTS | resume/JD fetch + parse (Node, at session start) |
| transcript append to Redis | per-turn coverage tagger (tiny classify → Redis) |
| | **all scoring** → async Node report after the interview |
| | admission acquire (once, before agent joins — not per turn) |

If a thing emits structured data or does I/O that isn't "produce the next spoken sentence," it lives off-path. This is the rule the entire design is derived from.

---

## 2. The real-time pipeline (inside the Python worker)

Each stage streams into the next — **no stage waits for the previous to finish.**

```
candidate audio (WebRTC frames)
   │
   ▼
[1] TURN DETECTOR (LiveKit turn-detector) — semantic EOU ~150ms.
        Fires when the candidate is actually done, not on a silence timer.
   │  (STT has been running the whole time)
   ▼
[2] STREAMING STT (Google Chirp, en-IN + en-US) — interim during speech,
        FINAL on turn-end. Final line → Redis transcript:{room} (seq, speaker, ts).
   │ final transcript text
   ▼
[3] LLM (gemini-3.1-flash-lite, STREAMING, PLAIN TEXT)
        prompt = [CACHED: system + JD + must-have skills + resume]
                 + [live: conversation so far]
        output = next question, plain-text tokens. NO JSON, NO score.
        first token ~250-300ms (TTFT).
   │ token stream
   ▼
[4] SENTENCE CHUNKER — emits each complete sentence the instant it forms;
        sentence 1 leaves while the LLM is still generating sentence 2.
   │ sentence
   ▼
[5] STREAMING TTS (Google Chirp3-HD) — synthesis starts on sentence 1,
        audio frames published to LiveKit immediately.
        Tara's line → Redis transcript:{room}.
   ▼
candidate hears Tara  ≈ 600-800ms after they stopped talking
```

### Perceived-latency budget (time to first audio)

```
EOU detect (semantic)   ~150ms
STT final flush         ~150ms
LLM first token (TTFT)  ~300ms   ← independent of answer length; driven by prefill size + model
first sentence forms    ~150ms   (overlaps LLM)
TTS first chunk         ~200ms
────────────────────────────────
≈ 600-800ms to first audio
```

TTFT, not full-response time, is what the candidate feels — because every stage streams. Dynamism (the model choosing what to ask) is reasoning during prefill; it does not add latency. What *would* add latency, and is therefore forbidden on the hot path: JSON output, inline scoring, batch TTS, fixed-silence turn detection, large per-turn prefill.

### Cross-cutting behaviors

- **Context caching (the p50-at-scale lever):** the static block (system prompt + JD + must-have skills + resume text) is registered once per session as a Gemini cached context. Each turn prefills only the new exchange, keeping TTFT low even as the conversation grows.
- **Barge-in (designed now, hardened in Phase 4):** candidate speech while Tara is talking → stop TTS playback immediately, **truncate Tara's partial transcript line to only what was actually spoken** (audit record matches reality), cancel the in-flight LLM generation for that turn.

### Off-path side-car (fires in parallel at each user-turn-final; never awaited by the hot path)

- **Coverage tagger:** a tiny classify call — "which must-have skills did this answer touch?" → updates `coverage:{room}` set in Redis. This is flow-control tagging, not scoring; it does not violate the defer-all-scoring rule and never blocks speech. *Note: it is still a real Gemini call every user-turn-final, so its tokens count toward per-session consumption and must be included in the `GEMINI_TPM_BUDGET` calibration (§7 fold-in #2.1).*
- **End check:** `all skills covered` OR `question_count ≥ MAX_QUESTIONS` → trigger `_end_interview`.

---

## 3. Session lifecycle

The full journey of one interview. Everything is keyed by `reservation_id = room_name` — one key per session, cradle to grave (admit, heartbeat, reconnect, release).

```
A. CREATE (Node, REST)   POST /interviews/:contestId/start {candidateId}
     1. load must-have skills from Mongo `contests`
     2. resolve signed S3 URLs → download resume + JD → extract text
     3. write Redis session:{id} = {contestId, candidateId, skills[],
          resumeText, jdText, maxQuestions, status:"created"}  (TTL ~2h)
     4. ADMISSION CHECK ▼

B. ADMISSION (Redis limiter, atomic Lua)
     try_admit(reservation_id = room_name):
        acquire ALL buckets (global + gemini_tpm + stt + tts) or NONE
        ADMITTED  → set Tier-1 reservation lease (TTL 45s); issue LiveKit
                    token; return room info; status:"admitted"
        REJECTED  → status:"queued / starting shortly" (no partial acquire,
                    no silent over-admit)

C. AGENT JOINS (Python worker)
     • read session blob from Redis (skills, resumeText, jdText)
     • send FIRST heartbeat → PROMOTES lease to Tier-2 (heartbeat-refreshed)
     • register Gemini cached context
     • run the Section-2 pipeline turn after turn
     • each final line → Redis transcript:{room} (seq, speaker, ts)
     • coverage/end-check off-path → when done, _end_interview()

D. END (_end_interview, Python)
     try:
         session.say(closing_statement)        # best-effort; may throw/hang
         lines = read+sort transcript:{room}
         write lines → Mongo (aiInterview audit) # may throw/hang
         emit "interview_complete" → Node
     finally:
         limiter.release(reservation_id = room_name)   # ALWAYS runs
         teardown STT/TTS/LLM streams; leave room

E. ASYNC SCORING (Node — OFF the latency path; reacts to the event)
     • read ordered transcript from Mongo
     • build Q&A pairs + must-have skills
     • LLM scoring pass → AI analysis JSON
     • insert `aiInterview`, $set `recruiterAddProfiles`, insert `auditTrail`
```

### Lifecycle guarantees

- **Guarantee #1 — no-show reclaim.** The Tier-1 reservation lease (45s) covers admit→join→first-heartbeat. A candidate issued a token who never joins is reclaimed within 45s (see §4 for the number's derivation).
- **Guarantee #2 — release is downstream of nothing that can fail.** `release` lives in a `finally`. If the goodbye line or the Mongo write throws or hangs, the slot is still released. A missed transcript persist is recoverable (still in Redis under TTL, retryable by Node); a leaked slot is not. Release wins. *(Plan: `session.say` and the Mongo audit write are each wrapped in a hard timeout so a hung provider call converts to a throw and lets `finally`/release fire — a `finally` does not protect against a hang, only a throw.)*
- **Guarantee #3 — reconnect reuses the same slot.** The reconnect/resume path routes back through the same `try_admit(reservation_id = room_name)`. Because the limiter keys on `room_name`, a rejoin finds the existing live reservation and refreshes it idempotently — no counter increment, no double-count, no bypass of admission. First-join and rejoin are the same call against the same key.

---

## 4. The admission limiter (the part that decides if it holds at 100)

Global, Redis-backed, cleared **before** the agent joins. Replaces any per-process semaphore.

```
global:sessions     cap = MAX_GLOBAL_SESSIONS  (e.g. 110)
bucket:gemini_tpm   cap = GEMINI_TPM_BUDGET     ◄── tightest = real ceiling
bucket:stt_streams  cap = STT_STREAM_BUDGET
bucket:tts_streams  cap = TTS_STREAM_BUDGET
```

A session holds one slot in **every** bucket for its whole life. Acquire / release / refresh are **one Lua script each** — atomic, all-or-nothing, so two buckets are never taken while the other two are missed (no half-admit leak), and the cap is never exceeded by a read-then-write race across workers.

### Two-tier lease

```
TIER 1 — RESERVATION lease (set by Node at admission, B)
   TTL = 45s
   Covers: admit → candidate joins → agent dispatched → agent reads
           session → agent sends FIRST heartbeat.
   No first heartbeat within 45s → NO-SHOW → lease expires → all four
   buckets auto-reclaim.

   Worst-case admit→first-heartbeat budget (~30s):
       candidate clicks join & WebRTC/ICE connect   ~3-8s
       agent dispatch + model-conn warmup (warm pool) ~5-10s
       agent reads Redis session + first heartbeat    ~2-5s
       ───────────────────────────────────────────────
       ≈ 30s worst case  →  TTL 45s  (≈50% margin)

TIER 2 — HEARTBEAT lease (worker takes over, C)
   First heartbeat refreshes the same room_name key.
   Heartbeat every 15s, TTL 60s (4× margin for one missed beat).
   Worker alive  → continuously refreshed, slot held.
   Worker crash  → no refresh → lease expires in ≤60s → slot reclaimed.
```

| Failure | Slot held until |
|---|---|
| No-show (token issued, never joins) | ≤ **45s** (Tier-1) |
| Worker crash mid-interview | ≤ **60s** (Tier-2) |
| Normal end / disconnect | immediate (explicit release in `finally`) |

The 45s is a deliberate trade: long enough that a real candidate on a slow connection plus a warm-but-not-instant agent is never falsely reclaimed mid-join; short enough that a closed-tab no-show frees its slot inside a minute rather than squatting for the 2h session TTL. **The 45s assumes the warm headroom pool (§5) keeps dispatch in ~5–10s. If pods cold-start, dispatch alone can exceed 30s — so the warm buffer is what keeps this TTL honest.** This coupling is load-bearing, not a nicety.

### Acquire (atomic Lua, lease-aware and idempotent)

```
try_admit(reservation_id = room_name):
   if reservation_id exists (live):           # reconnect path (#3)
        refresh lease; return ADMITTED          # no counter change
   else if global<cap AND gemini<cap AND stt<cap AND tts<cap:
        incr all four; create reservation key, TTL=45s (Tier 1)
        return ADMITTED
   else:
        incr nothing; return REJECTED(tightest failing bucket)
```

Rejection → Node returns `"starting shortly"`/queued; never a degraded over-admitted interview. The tightest bucket (almost certainly Gemini TPM) is exported as a metric and the load test reports rejection rate **by bucket**, so the real ceiling is observable and the right quota gets raised.

**Reclaim instrumentation (passive expiry is self-healing but observes nothing on its own).** Because the limiter uses passive TTL expiry with no reaper, no code runs when a lease dies — so a reclaim must be *detected* to be counted. *(Plan: reclaim detection + `lease_reclaims{reason}`/`false_reclaims` emission via lazy reap during the next `try_admit` and/or Redis keyspace-expired notifications; each reservation carries a `heartbeat_ever_received` flag to classify `no_show` vs `crash`; and a false reclaim is distinguished from a true no-show by correlating the expiry against a "LiveKit participant actually joined this room" signal — a lease that expired while a real participant was present is a false reclaim.)*

### README TODO (surfaced, not hardcoded)
- Gemini limits are **per-project, not per-key** — extra keys don't multiply quota.
- Verify real `gemini-3.1-flash-lite` TPM/RPM and project tier in AI Studio (aistudio.google.com/rate-limit) before load testing; set `GEMINI_TPM_BUDGET` just under the verified ceiling.

---

## 5. Scaling (where the 45s lease is cashed or broken)

**Scale signal: active-session count per pod — never CPU.** Workers are I/O-bound (waiting on STT/Gemini/TTS sockets); CPU stays low even at saturation, so scaling on CPU under-provisions and silently pushes sessions past the per-pod limit.

```
each worker ──exports──► active_sessions{pod} ──► Prometheus
                                                     │
                                                     ▼
                                       KEDA / custom-metrics HPA
                                  target ≈ 10–15 sessions / pod
                                                     │
                        ┌────────────────────────────┴───────────┐
                        ▼                                          ▼
               scale UP — lead demand                    scale DOWN — DRAIN only
               (trigger ~70% of target;                  (never evict a pod with
                pods warming before buffer                live sessions)
                is consumed)
```

At ~10–15 sessions/pod and a cap of 110, steady state is ~8–11 worker pods. The HPA keeps `avg sessions/pod` under target *with headroom*, not packed to the cap.

### Warm headroom buffer = a correctness dependency, not a latency nicety

Keep ~20% spare warm pods (already up, model sockets pre-connected, ready in ~5–10s).

```
warm pool present   → dispatch ~5-10s  → first heartbeat inside 45s → lease honored ✓
warm pool exhausted → cold start (schedule + image pull + socket warmup)
                      30-60s+ → first heartbeat MISSES 45s → limiter reclaims a
                      real, mid-join candidate as a "no-show" → candidate turned away ✗
```

Safeguards making the coupling explicit:
- **Scale-up leads demand:** trigger scale-out at ~70% of target; tune KEDA polling/stabilization so new pods warm *before* the buffer is consumed.
- **Buffer is a hard floor, alarmed:** `warm_pods_available` dropping toward zero is a page — from that moment the lease is at risk. Optional absolute floor `WARM_POOL_MIN_PODS` (see §7 — confirmed or set by the load test).

### Scale-down safety
Scale-down **drains, never evicts**: PodDisruptionBudget + `preStop` drain hook + KEDA cooldown. A pod with active sessions stops accepting new sessions, finishes its live interviews, then terminates. Killing a pod mid-interview forces reconnects and briefly orphans slots until the Tier-2 lease reclaims them.

---

## 6. Observability, provider-429 handling, and config

### Metric freshness is a reliability parameter, not a dashboard preference

The scaling control loop is only as fast as the metric feeding it. The chain has a latency budget:

```
load rises → [scrape interval] → metric visible → [KEDA poll interval]
   → [HPA stabilization window] → pod scheduled → [warm-up] → ready
   ╰──────────────────────── T_react ──────────────────────────────╯

CORRECTNESS CONSTRAINT:  T_react < T_drain
   (time to bring warm capacity online must beat time the warm buffer
    is consumed). If T_react > T_drain → buffer empties before new pods
    are ready → dispatch falls to cold-start → 45s lease fires false
    no-shows on real candidates. Same failure as a dry pool.
```

Cadence is therefore pinned as a tuned reliability parameter, each documented with *why*:
- **Session-count scrape:** ~5–10s (not the 30–60s Prometheus default — too slow for this loop).
- **KEDA poll:** ~5–10s, with a small scale-up `stabilizationWindowSeconds` (react fast) and a larger scale-down one (drain calmly, no flapping).

### Exported signals (alarmed where noted)

| Metric | Why |
|---|---|
| `active_sessions{pod}` | the scale signal |
| `warm_pods_available` | **paged** — floor breach = lease at risk |
| `bucket_utilization{bucket}` + `bucket_rejections{bucket}` | tightest bucket = real ceiling; headline load-test metric |
| `turn_latency` histogram (p50/p95) + `llm_ttft` + per-stage timings | acceptance criteria; locates regressions |
| `lease_reclaims{reason="no_show"\|"crash"}` | rising no-shows = T_react breach or starving pool |
| `false_reclaims` (real sessions evicted mid-join) | **hard gate** — see §7; must be zero. Classified by correlating lease expiry with a "participant actually joined" signal (§4 reclaim instrumentation) |

### Provider 429 / retry — bounded on the hot path, generous off it

Admission control is the primary 429 defense (cap below verified quota → most turns never hit a limit). Retries handle only the residual tail.

```
HOT PATH (STT/LLM/TTS during a live turn):
   exponential backoff + JITTER, HARD-BOUNDED — at most ~1 quick retry
   (a few hundred ms). Still failing → graceful degrade ("one moment" /
   re-ask), never freeze the turn. A retry storm here wrecks p95.

OFF PATH (async scoring, coverage tagger):
   generous exponential backoff + jitter, many attempts, dead-letter on
   exhaustion. Nothing waits on it.
```

Jitter is mandatory: under a spike many sessions hit 429 simultaneously; un-jittered backoff retries them in lockstep (thundering herd) and re-triggers the limit.

### Config surface (env vars — secrets never hardcoded)

```
# transport / providers
LIVEKIT_URL  LIVEKIT_API_KEY  LIVEKIT_API_SECRET
GOOGLE_APPLICATION_CREDENTIALS  GOOGLE_CLOUD_PROJECT
GEMINI_API_KEY   GEMINI_MODEL=gemini-3.1-flash-lite
REDIS_URL   MONGODB_URI
AWS_ACCESS_KEY_ID  AWS_SECRET_ACCESS_KEY  AWS_REGION  S3_BUCKET   # resume/JD fetch
PORT

# admission caps / budgets
MAX_GLOBAL_SESSIONS=110   GEMINI_TPM_BUDGET   STT_STREAM_BUDGET   TTS_STREAM_BUDGET

# lease / lifecycle (§4)
RESERVATION_LEASE_TTL=45   HEARTBEAT_INTERVAL=15   HEARTBEAT_LEASE_TTL=60

# interview flow
MAX_QUESTIONS=12   INTERVIEW_LANGUAGES=en-IN,en-US   TTS_VOICE=en-IN-Chirp3-HD-Erinome

# scaling reliability (§5/§6 — documented as in-path)
SESSIONS_PER_POD_TARGET=12   WARM_POOL_PERCENT=20   WARM_POOL_MIN_PODS=  # set by load test
METRIC_SCRAPE_INTERVAL=5s    KEDA_POLL_INTERVAL=10s
```

---

## 7. Testing & the staged load-test acceptance plan

Tests are organized by the invariant they defend, so each acceptance criterion has an owning test.

### Component / unit (fast, providers mocked)

| Target | Proves |
|---|---|
| **Limiter Lua** | hammered at the cap boundary → counters never exceed cap, never half-admit; `try_admit(room_name)` on an existing reservation refreshes without incrementing (**idempotent reconnect, #3**) |
| **Lease expiry** | no first heartbeat within Tier-1 TTL → reclaim (no-show #1); heartbeat stops → Tier-2 reclaim (crash); explicit `release` in `finally` runs even when Mongo write is mocked to throw (**#2**) |
| **Sentence chunker** | token stream → complete sentences at boundaries; first sentence leaves before stream ends |
| **Transcript ordering** | interleaved appends → sorted by `seq` → correct dialogue order |
| **Barge-in** | speech mid-TTS → playback stops, partial line truncated to spoken portion, LLM gen cancelled |

### Single-stream latency gate (the p50 commit)
One live stream, real providers, per-stage instrumentation. Assert **p50 turn latency < 800ms** and capture the per-stage breakdown. Green-light before any load test — if one stream can't hit it, concurrency won't.

### Staged load test (headline acceptance event)

```
Stage 1: 50 concurrent   → baseline; ~zero rejections, p95 within target
Stage 2: 120 concurrent  → just over the 110 cap; rejections MUST appear, cap
                           MUST hold (no over-admission), admitted p95 within target
Stage 3: 200 concurrent  → stress; graceful (many rejects/queued), zero admitted
                           sessions degraded past budget, no crashes, no leaked slots
```

**Two headline metrics at every stage:** (1) p95 turn latency of admitted sessions; (2) bucket rejection rate broken down by bucket (names the tightest bucket → which quota to raise).

**The load test must actively *prove*, not just measure:**
- **Cap holds:** `global:sessions` never exceeds `MAX_GLOBAL_SESSIONS` at 120/200; over-cap queues/rejects gracefully.
- **No-show reclaim (#1):** inject token-but-never-join candidates → slots return within ~45s → capacity recovers; confirm via `lease_reclaims{no_show}`.
- **Crash reclaim:** kill a worker mid-interview → slots reclaim within ~60s (Tier-2); other pods unaffected.
- **Reconnect idempotency (#3):** drop & rejoin a live candidate → `global:sessions` does **not** increment.
- **T_react < T_drain (§5/§6):** ramp fast enough to consume the warm buffer; confirm scale-up brings warm pods online before dispatch falls to cold-start.

### Fold-in #1 — False-reclaim slope is a HARD GATE

Reclaiming *injected* no-shows is expected and healthy. Reclaiming a **real (should-have-connected) session** is not.

- **Hard fail condition:** any real candidate evicted mid-join at **any** stage. **`false_reclaims` must be zero across all three stages.** A real candidate evicted at 120 or 200 concurrent is a hard fail **even if p95 latency passes.** Latency passing does not buy back a dropped candidate.
- **Track the false-reclaim rate as a slope across 50 → 120 → 200.** Flat (zero) is healthy. A climbing slope means `T_react` is breaching `T_drain` — the warm buffer is emptying before scale-up reacts, and dispatch is sliding toward cold-start. The slope is the early-warning signal; the zero-tolerance gate is the pass/fail.
- **Measurability prerequisite:** this gate is only enforceable if `false_reclaims` is actually classifiable — which depends on the §4 reclaim instrumentation (correlating lease expiry against a "participant actually joined" signal). Without it the gate cannot be measured, and it is supposed to block launch — so the instrumentation is a first-class plan task, not optional telemetry.

### Fold-in #2 — Three calibrated numbers the load test must EMIT

The design currently runs on three estimates. The load test's job is not only to prove the mechanisms hold — it must **produce calibrated values** that replace those estimates. "What we walk away with":

1. **Measured TOTAL tokens-per-session — including the off-path coverage-tagger calls (§2), not just the hot-path generation** → sets `GEMINI_TPM_BUDGET`. Counting hot-path only would calibrate the budget low and let the tagger silently eat hot-path headroom under load.
2. **Measured sessions-per-pod before latency degrades** → confirms or corrects `SESSIONS_PER_POD_TARGET=12`.
3. **Warm-buffer behavior under the sharpest ramp** → confirms `WARM_POOL_PERCENT=20` and decides whether an absolute floor `WARM_POOL_MIN_PODS` is needed (a percentage alone under-protects at low pod counts).

A load test that proves the mechanism but does not emit these three numbers is **incomplete.** Every capacity guarantee in this design rests on them, and they are the only three values still based on estimates.

### Fold-in #3 — p95 resolution (deliberate call, not "honestly reported")

- **`p95 turn latency < 800ms @ 100 concurrent` is the GOAL.**
- **`p95 turn latency < 1000ms @ 100 concurrent` is the HARD FLOOR — below this we do not ship.**

This is a deliberate launch decision: p95 is gated by Google/Gemini provider *tail* latency, which we defend (admission control + warm headroom + bounded retries) but do not fully control. 800ms is what we engineer toward; 1000ms is the line that blocks launch.

### Acceptance criteria → owning test

| Criterion | Proven by |
|---|---|
| p50 < 800ms single stream | single-stream latency gate |
| p95 < 800ms @ 100 concurrent (goal) / < 1000ms (hard floor) | load Stage 1/2 (fold-in #3) |
| zero false reclaims of real sessions | load test (fold-in #1, hard gate) |
| `GEMINI_TPM_BUDGET`, `SESSIONS_PER_POD_TARGET`, warm-buffer sizing calibrated | load test (fold-in #2) |
| no over-admission past cap | limiter unit + load Stage 2/3 |
| over-cap queues/rejects gracefully | load Stage 3 |
| transcript ordered & complete | transcript-ordering unit + per-interview audit check post-load |

---

## 8. Build order (phased; check in at each milestone)

1. **Single-session happy path** — one worker, full streaming pipeline, turn-detector, one interview end to end, transcript → Redis → Mongo. **Prove p50 < 800ms on one stream** before anything else.
2. **Admission limiter** — Redis global cap + per-provider buckets + two-tier lease; acquire/refresh/release; graceful reject/queue; lifecycle guarantees #1–#3.
3. **Concurrency / scaling** — multiple workers, session-count metric exported, KEDA/HPA on session count, warm headroom pool, drain-on-scale-down.
4. **Hardening** — barge-in truncation, bounded hot-path retry with jittered backoff, clean teardown, audit-record completeness, reconnect/resume.
5. **Scoring service** — async, separate, Node; reads transcript from Mongo, writes ATS collections.

> Phase 1 must hit sub-800ms on one stream before the limiter; the limiter must be correct before scaling.

## 9. Explicit non-goals / guardrails

- No Live / speech-to-speech / native-audio model. Cascaded only.
- No scoring or report generation on the live latency path.
- No scaling on CPU.
- No relying on multiple API keys to raise quota (per-project).
- No buffering the full LLM response before starting TTS.
- No hardcoded secrets; env vars only.
```

# Tara Phase 4 — Hardening — Design

> **Phase 4 of the Tara cascaded voice-interview agent.** Builds on merged `main`
> (Phases 1–3; tag `phase-3-merged`). The whole-system architecture is LOCKED
> (`docs/superpowers/specs/2026-06-25-tara-cascaded-voice-interview-design.md`,
> §8.4 "Hardening") — this spec does NOT re-litigate it. It makes the existing
> pipeline production-safe: barge-in audit truthfulness, bounded hot-path retry,
> reconnect/resume, exactly-once teardown, audit completeness, and the carried
> operational follow-ups (drain endpoint, Dockerfiles, `/healthz`, limiter
> classification nuances).

**Branch:** `tara/phase4-hardening` (off `main` @ `phase-3-merged`).
**Back-out target:** tag `phase-3-merged`.

## Goal

Take the validated, scaling pipeline and harden the failure and edge paths so a
real candidate is never wrongly evicted, the audit transcript always matches what
was actually spoken, transient provider hiccups don't drop a turn, and pods build
and drain cleanly. Latency discipline from the spec still binds: nothing here may
push the hot path past the **<1000ms p95 hard floor** (§7).

## Scope

Three components, 11 work items. Components A and B are locally TDD-testable; C is
*more* locally-provable than Phase 3 (Dockerfiles build locally; `/healthz` + drain
are unit/integration testable) — only in-cluster drain-on-scale-down stays the GKE
operator check already covered by the Phase 3 runbook.

---

## Component A — Hot-path pipeline hardening (latency-sensitive)

### A1. Barge-in truncation (audit truthfulness)

**Behavior already provided:** LiveKit Agents 1.6.4 stops TTS playback and cancels
the in-flight turn when the candidate speaks over Tara. We do NOT rebuild that.

**What we harden:** the audit record. When an interruption fires, Tara's transcript
line MUST be truncated to **only what was actually spoken**, never the full intended
sentence — the Mongo transcript is an audit artifact and must match reality.

- **Truncation boundary source (verify at implementation):** prefer LiveKit's
  interruption event committed/played transcript segment (the words actually emitted
  to the candidate). If 1.6.4 does not reliably expose the exact playout offset, fall
  back to the **last fully-synthesized sentence chunk** already handed to TTS, plus an
  `[interrupted]` marker. **Invariant:** never persist words Tara did not utter. The
  chosen mechanism is documented in code at the truncation site.
- **Interaction:** the truncated line feeds B5 (audit completeness) and, if the turn
  is then re-asked via B1 (reconnect), the re-ask is a NEW transcript turn — the
  truncated original is retained, not overwritten.

**Test:** simulate an interruption mid-sentence; assert the stored agent line is the
spoken prefix (or last-chunk + marker), not the full sentence; assert ordering intact.

### A2. Bounded hot-path retry with jittered backoff

**Today:** a transient STT/LLM/TTS error fails the turn outright.

**Change:** a small retry helper — `retry_async(fn, *, attempts, base_delay, max_delay,
retry_on)` — with **jittered** exponential backoff, retrying ONLY transient errors
(timeouts, 5xx, connection reset/unavailable) and NEVER 4xx / auth / quota-exhausted
(those are not transient and must surface).

- **Hot path (TTFT-critical: STT, LLM first token, TTS first chunk):** `attempts=2`
  (i.e. 1 retry), tight per-attempt timeout so a hung provider fails fast enough to
  retry inside the latency budget; total added latency bounded and must respect the
  <1000ms p95 floor. The per-attempt timeout is a config knob.
- **Off path (coverage tagger, Mongo transcript write — nobody is waiting):**
  `attempts=4`, exponential, more generous.
- **Config:** `hot_retry_attempts` (default 2), `hot_attempt_timeout_ms`,
  `offpath_retry_attempts` (default 4) added to `config.py` Settings.

**Test:** a fake that fails N times then succeeds → hot path retries once and gives up
on the 2nd failure; a 4xx → no retry; off-path retries up to 4; backoff jitter applied
(assert delays fall in the jittered range, deterministic via injected RNG/clock).

---

## Component B — Session lifecycle hardening

> **B1 + B2 + B4 are one cohesive change** — the grace timer, the participant flag,
> and the single teardown routine all touch the disconnect path. They are specced
> separately for clarity but implemented/reviewed together to avoid a half-wired
> disconnect path.

### B1. Reconnect / resume

**Today:** `participant_disconnected` (candidate) and session `close` immediately call
`limiter.release(room)` + `done.set()` — a dropped connection kills the interview.

**Change:** a candidate disconnect starts a **reconnect grace window** instead of
releasing:

- `RECONNECT_GRACE_SECONDS` (config, default ~25s) — strictly **< Tier-1 lease (45s)**
  so the limiter never reclaims a slot we still intend to hold. (Assertion/`config`
  note enforces `reconnect_grace < reservation_lease_ttl`.)
- On candidate disconnect: start the grace timer; do NOT release; keep the worker job
  alive and keep heartbeating the lease (Tier-2) so the reaper classifies a genuine
  abandonment correctly (`no_show`/`crash`), never `false`.
- **Rejoin within window:** cancel the timer; Tara says a one-line "welcome back" and
  **re-asks the in-flight question** (`current_question`, preserved in Redis), then
  continues. The interrupted partial answer is truncated in the audit (A1/B5); the
  re-ask is a new turn.
- **Timer expiry:** funnel into the single teardown routine (B4) → release + end.
- **State preservation:** transcript, `coverage:{room}` set, and `current_question`
  already live in Redis keyed by `room_name` (cradle-to-grave) — resume reads them
  back; no new store needed beyond persisting `current_question` if not already.

**Test:** disconnect → assert NO release within grace; rejoin → assert timer cancelled,
re-ask issued, lease still held, cap not double-counted (limiter reconnect-idempotency
from Phase 2); no rejoin → assert release fires exactly once at grace expiry.

### B2. `mark_participant` at candidate-join (fixes carried M2)

**Today:** the participant flag (`participant_joined=1`, used by reap classification)
is set at **agent**-join, not **candidate**-join.

**Change:** set it the moment the **candidate** actually joins the room
(`participant_connected` for the candidate identity / the room's first remote
participant). This makes a lease lapse with a present candidate classify as `false`
(the hard-gate signal) accurately, and removes the Phase-2 "largely moot" caveat.

**Test:** candidate joins → `participant_joined==1`; force a lapse with candidate
present → reap classifies `false`.

### B3. `participant_disconnected` multi-participant guard

**Today:** the handler acts on ANY participant leaving.

**Change:** act only on the **candidate's** identity. A non-candidate participant
(future proctor/observer) leaving must NOT start the grace timer or tear down. Capture
the candidate identity at join and compare.

**Test:** a non-candidate disconnect → no grace timer, no teardown; candidate
disconnect → grace timer starts (B1).

### B4. Clean teardown invariant (exactly-once)

**Change:** one idempotent `_teardown(reason)` routine that flushes the transcript to
Mongo and releases the lease **exactly once**, regardless of how many exit paths fire.
ALL paths funnel through it: natural coverage/cap end, grace expiry (B1), job shutdown
(`add_shutdown_callback`), and error/exception. A guard flag makes a second call a
no-op (extends the existing `_started`/flush-guard pattern).

**Test:** drive two exit paths concurrently (e.g. disconnect-grace-expiry + shutdown)
→ assert exactly one Mongo flush and exactly one `limiter.release`.

### B5. Audit-record completeness

**Change:** the persisted transcript is ordered, complete, and matches reality —
including A1 truncations and B1 re-asks. Add a per-interview audit assertion run at
teardown (or a test helper): turns are monotonically ordered, no orphaned partial
agent line survives un-truncated, every user turn has its preceding agent prompt,
and re-asked questions appear as distinct turns.

**Test:** a scripted interview with one barge-in + one reconnect re-ask → audit check
passes; a deliberately corrupted/out-of-order transcript → audit check fails.

### B6. Rejected-session Redis blob lifetime (fixes carried M3)

**Today:** a rejected session's Redis blob lingers ~2h (no slot leak, but accumulates).

**Change:** set a short TTL (config `rejected_blob_ttl_seconds`, default ~120s) on the
reject path so rejected attempts expire promptly. No effect on admitted sessions.

**Test:** a rejected admission → its blob has the short TTL; an admitted session → its
blob keeps the normal lifetime.

---

## Component C — Deployment hardening (ops)

### C1. Real worker drain endpoint

**Today:** Phase 3 `preStop` is a blind `sleep 110` hoping the SIGTERM job-drain
finishes.

**Change:** the worker exposes a real drain trigger — preferably an HTTP endpoint on
the **existing metrics server** (e.g. `POST /drain`) or a SIGTERM handler — that:
1. flips the worker to **stop accepting new job dispatches**,
2. lets in-flight interviews finish (bounded by `terminationGracePeriodSeconds`),
3. exits as soon as the last session drains.

`deploy/agent-deployment.yaml` `preStop` calls it via `httpGet`/`exec`;
`terminationGracePeriodSeconds` stays as the safety ceiling, not the mechanism. A pod
with zero live sessions terminates immediately instead of idling 110s — making
`T_react < T_drain` a real guarantee, not a timing bet.

**Test:** unit — drain flag flips, new-dispatch rejected after drain, exits when active
count hits 0; manifest re-validated (`kubeconform`). In-cluster scale-down behavior
stays the GKE operator check (Phase 3 runbook).

### C2. Dockerfiles

Neither image exists (Phase 3 used placeholder refs). Add:

- **`agent/Dockerfile`** — Python 3.12 base; install the `tara_agent` package +
  LiveKit plugins; **download the turn-detector model at build time** (so cold-start
  doesn't pay for it — directly protects the warm-pool / 45s-lease correctness
  dependency); `CMD python -m tara_agent.worker start`.
- **`backend/Dockerfile`** — Node base; production deps only; runs the server.
- **Both:** pinned base images, **non-root** user, and a `.dockerignore` that never
  copies `.env`, the GCP SA JSON, `node_modules`, `.venv`, or test fixtures.

**Verify:** `docker build` BOTH images locally to green (NOT GKE-gated). Push/registry
is the operator step.

### C3. Backend `/healthz` route

**Today:** the backend Deployment probe is a bare TCP check.

**Change:** add `GET /healthz` returning 200 when the process is up and Redis is
reachable (ping), 503 otherwise. Switch the backend readiness/liveness probes in
`deploy/backend-deployment.yaml` from TCP to `httpGet /healthz`.

**Test:** Node test — `/healthz` 200 when Redis ok, 503 when Redis down (mock/inject);
manifest re-validated.

---

## Cross-cutting invariants (Phase 4 must preserve)

1. **No hot-path regression past <1000ms p95** — A2 retry budget is bounded; the
   reconnect/teardown changes are off the speaking path.
2. **A real, present candidate is NEVER wrongly evicted** — B1 grace < lease, B2
   accurate participant flag, B3 candidate-only guard all serve this; `false_reclaims`
   stays the hard gate.
3. **Exactly-once teardown** — one flush, one release, every path (B4).
4. **Audit matches reality** — never store unspoken words (A1); transcript ordered &
   complete (B5).
5. **No secrets in images or manifests** — `.dockerignore` + existing gitignore;
   secret-scan stays clean.
6. **Locally provable where possible** — A/B fully TDD; C2 builds locally; only
   in-cluster drain stays the operator check.

## Build order (within Phase 4)

A2 (retry helper — pure, foundational) → A1 (barge-in truncation) → B2/B3 (small
limiter/identity fixes) → **B1+B4 together** (reconnect grace + single teardown) → B5
(audit completeness, depends on A1/B1) → B6 (blob TTL) → C3 (`/healthz`) → C1 (drain
endpoint + manifest edit) → C2 (Dockerfiles, last so they package the finished code).

## Non-goals / guardrails (unchanged from the locked spec)

- No speech-to-speech; cascaded only. No scoring on the live path (that's Phase 5).
- No scaling on CPU. No buffering the full LLM response before TTS.
- No new providers. No hardcoded secrets.
- Barge-in detection itself is LiveKit-native — we harden the audit, not re-implement
  interruption.

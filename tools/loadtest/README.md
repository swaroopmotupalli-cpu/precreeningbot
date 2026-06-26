# Staged load-test driver

`run_loadtest.py` drives synthetic candidates against a running Tara stack at
increasing concurrency stages, injects no-shows, and runs the cross-stage
aggregation + hard-gate smoke asserts defined in `aggregate.py`.

## What it does

Per stage in `[50, 120, 200]` (overridable with `--stages`), the driver:

1. Launches that many concurrent candidate coroutines via `asyncio.gather`.
2. Injects a fraction (`--noshow-fraction`, default `0.1`) as **no-shows**:
   they `POST /sessions` to get a token, then **never join the room**
   (`was_real_participant=False, was_injected_noshow=True`). The server-side
   reaper reclaims their slot — a *true* reclaim, which must not count as a
   false reclaim.
3. Runs the remaining candidates as **real** participants: join the LiveKit
   room, publish a mic track, and publish the fixture WAVs (`tools/fixtures/*.wav`)
   one per turn, each gated on the worker's `tara_done` (`topic="gate"`) packet.
4. Collects one `SessionOutcome` per candidate and calls `stage_report(...)`.
5. After all stages, computes `false_reclaim_slope(...)` and
   `calibrated_budgets(...)`, prints them, and asserts the hard gates,
   exiting non-zero on failure.

Admission **rejects** are handled gracefully: the backend returns HTTP 503
(`queued` + `bucket`, no token); the candidate records
`SessionOutcome(admitted=False, rejected_bucket=<bucket>, ...)` instead of
crashing. Each candidate is wrapped in its own timeout + try/except, so a
single hung candidate produces a failed outcome rather than hanging the stage.

It reuses the existing primitives — `aggregate.py` (`SessionOutcome`, `p95`,
`stage_report`, `false_reclaim_slope`, `calibrated_budgets`) and
`latency_harness.py` (`BACKEND`, `FIXTURES`, `_publish_wav`) — nothing is
re-implemented.

## CLI flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--stages` | `50,120,200` | Comma-separated concurrency stages. |
| `--noshow-fraction` | `0.1` | Fraction of each stage injected as no-shows. |
| `--dry-run` | off | Run only 2 sessions total (two 1-session stages), print two `stage_reports`, exit 0. Still needs the local stack. |

Env overrides: `BACKEND_URL`, `LIVEKIT_URL`, `GLOBAL_CAP` (enables the
cap-not-exceeded assert), `LOADTEST_CANDIDATE_TIMEOUT_S`, `LOADTEST_TURN_WAIT_S`,
`LOADTEST_DRAIN_S`, `LOADTEST_SLOPE_EPSILON`, `LOADTEST_TARGET_POD_SESSIONS`.

## Local `--dry-run` prerequisite (3 terminals)

`--dry-run` is the operator's local smoke step and needs the same running stack
as `tools/latency_harness.py`:

```bash
# terminal 1
cd backend && npm start

# terminal 2
cd agent && source .venv/bin/activate
python -m tara_agent.worker dev

# terminal 3
python tools/loadtest/run_loadtest.py --dry-run
```

Without a live backend + worker + LiveKit, `--dry-run` will fail to connect —
that is expected. The driver wiring (parse/import) is what is verified offline.

## The real staged run is GKE-gated

A laptop cannot host 200 concurrent LiveKit rooms + worker sessions. The real
**50 / 120 / 200** staged run happens on the **deployed GKE cluster** (Tasks
5-6) and produces the **Task 7** numbers. Running the full `--stages 50,120,200`
locally is not the operator's job here.

## Honesty: client-side proxies vs. authoritative server-side metrics

This driver sees the system **only from the client side**. It therefore cannot
prove the hard gates on its own:

- **Per-turn latency** recorded here is a **client-side proxy** — the wall-clock
  interval between publishing an answer and receiving the next `tara_done`
  gate. It is **not** the worker's authoritative `LATENCY_BREAKDOWN_P50`
  (eou / stt / llm_ttft / tts_ttfb). Use it for relative comparison across
  stages only.
- **`total_tokens`** is not observable client-side and is set to `0`. Token
  calibration (the Gemini TPM budget basis) comes from the worker's
  **Prometheus** export aggregated separately, not from this driver.
- **`reclaimed_reason`** is not client-observable in the happy path and is set
  to `None` for both real and no-show candidates. As a result the in-driver
  `false_reclaims` assert reads `0` **by construction** from client outcomes —
  it is a **client-side smoke gate only**.

The **authoritative** `false_reclaims == 0` / cap-held / slope-flat
verification is performed by reading the **worker's Prometheus limiter metrics**
during the GKE run; the **Task 7 runbook** cross-checks them. Do not treat a
green run of this driver as proof of the hard gates.

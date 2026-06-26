"""Staged load-test driver — Phase-3 scaling smoke gate.

Drives synthetic candidates against a running Tara stack (backend + worker +
LiveKit) at increasing concurrency stages (default 50 / 120 / 200), injects a
configurable fraction of no-shows, collects one `SessionOutcome` per candidate,
and runs the cross-stage aggregation + hard-gate asserts.

USAGE (requires a running local stack — the same 3-terminal setup as
tools/latency_harness.py; see README.md):

    # terminal 1:  cd backend && npm start
    # terminal 2:  cd agent && source .venv/bin/activate && python -m tara_agent.worker dev
    # terminal 3:
    python tools/loadtest/run_loadtest.py --dry-run          # 2-session smoke
    python tools/loadtest/run_loadtest.py --stages 50,120,200 # full staged run (GKE)

The full 50/120/200 staged run is the GKE operator step (Tasks 5-6), NOT a
local one — a laptop cannot host 200 concurrent LiveKit rooms + worker sessions.

HONESTY / WHAT THIS DRIVER CAN AND CANNOT SEE
---------------------------------------------
This driver observes the system ONLY from the client side. Therefore:

  * Per-turn latency recorded here is a CLIENT-SIDE PROXY: the wall-clock
    interval between publishing a candidate answer and receiving the next
    `tara_done` gate packet. It is NOT the worker's authoritative
    LATENCY_BREAKDOWN_P50 (eou/stt/llm_ttft/tts_ttfb). Use it for relative
    smoke comparison across stages, not as the latency of record.

  * `total_tokens` is NOT observable client-side → set to 0 here. Token
    calibration (Gemini TPM budget) comes from the worker's Prometheus export
    aggregated separately, not from this driver.

  * `reclaimed_reason` is NOT client-observable in the happy path → set to
    None for both real joined candidates and injected no-shows. Consequently
    the in-driver `false_reclaims` assert reads 0 BY CONSTRUCTION from client
    outcomes — it is a client-side smoke gate only.

  * The AUTHORITATIVE false_reclaims==0 / cap-held / slope-flat verification is
    done by reading the worker's Prometheus limiter metrics during the GKE run
    (the Task 7 runbook cross-checks them). This driver does not, and cannot,
    prove the hard gates on its own.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import uuid

import httpx

# --- DRY: reuse existing primitives -----------------------------------------
# aggregate.py and latency_harness.py both live one level up / same dir; insert
# the script's own dir and the tools dir on sys.path so `import aggregate` and
# `import latency_harness` resolve without a package install.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_TOOLS_DIR = os.path.abspath(os.path.join(_THIS_DIR, ".."))
for _p in (_THIS_DIR, _TOOLS_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from aggregate import (  # noqa: E402
    SessionOutcome,
    calibrated_budgets,
    false_reclaim_slope,
    stage_report,
)
from latency_harness import (  # noqa: E402  (BACKEND mirrors BACKEND_URL env)
    BACKEND,
    FIXTURES,
    _publish_wav,
)

from livekit import rtc  # noqa: E402

# ---------------------------------------------------------------------------
# Tunables (env-overridable, mirror latency_harness conventions)
# ---------------------------------------------------------------------------

# Per-candidate ceiling so one hung candidate never hangs the whole stage.
CANDIDATE_TIMEOUT_S = float(os.environ.get("LOADTEST_CANDIDATE_TIMEOUT_S", "120.0"))
# How long to wait for the next tara_done gate before publishing anyway.
TURN_WAIT_S = float(os.environ.get("LOADTEST_TURN_WAIT_S", "40.0"))
# Drain after the last answer so the worker can finish the final turn.
DRAIN_S = float(os.environ.get("LOADTEST_DRAIN_S", "6.0"))
# Slope must be flat: |slope| <= this epsilon across stages.
SLOPE_EPSILON = float(os.environ.get("LOADTEST_SLOPE_EPSILON", "0.01"))
# Target sessions/pod assumption fed to calibrated_budgets.
TARGET_POD_SESSIONS = int(os.environ.get("LOADTEST_TARGET_POD_SESSIONS", "30"))


# ---------------------------------------------------------------------------
# Candidate coroutines
# ---------------------------------------------------------------------------

async def _create_session(client: httpx.AsyncClient, cand_id: str) -> dict | None:
    """POST /sessions. Returns the session blob, or None on admission reject.

    On admission REJECT the backend returns HTTP 503 with a body indicating
    `queued` + `bucket` and NO token. We surface that to the caller as a
    sentinel dict {"rejected": True, "bucket": ...} rather than raising.
    """
    resp = await client.post(
        f"{BACKEND}/sessions",
        json={
            "contestId": "loadtest-contest",
            "candidateId": cand_id,
            "skills": ["Python", "SQL"],
            "resumeText": "5 years backend Python and data engineering.",
            "jdText": "Backend engineer, Python + SQL, 3+ years experience.",
            "maxQuestions": len(FIXTURES) or 5,
        },
    )
    if resp.status_code == 503:
        body = {}
        try:
            body = resp.json()
        except Exception:  # noqa: BLE001
            pass
        return {"rejected": True, "bucket": body.get("bucket", "unknown")}
    resp.raise_for_status()
    return resp.json()


async def _run_real_candidate(cand_id: str) -> SessionOutcome:
    """Admitted candidate: join room, publish WAVs turn-by-turn, record proxy latency."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        sess = await _create_session(client, cand_id)

    if sess is None:
        # Defensive: should not happen (None only on a path we don't take here).
        return _failed_outcome(real=True)
    if sess.get("rejected"):
        return SessionOutcome(
            admitted=False,
            rejected_bucket=sess.get("bucket", "unknown"),
            turn_latencies_s=[],
            total_tokens=0,
            was_injected_noshow=False,
            was_real_participant=False,  # rejected before joining the room
            reclaimed_reason=None,
        )

    livekit_url = sess.get("livekitUrl") or os.environ.get("LIVEKIT_URL", "")
    token = sess["token"]
    if not livekit_url:
        raise RuntimeError("livekitUrl missing from /sessions response and LIVEKIT_URL unset")

    room = rtc.Room()
    tara_done_q: "asyncio.Queue[bool]" = asyncio.Queue()

    @room.on("data_received")
    def _on_data(pkt: "rtc.DataPacket") -> None:
        if getattr(pkt, "topic", "") == "gate":
            try:
                tara_done_q.put_nowait(True)
            except asyncio.QueueFull:
                pass

    turn_latencies: list[float] = []
    await room.connect(livekit_url, token)
    try:
        source = rtc.AudioSource(48000, 1)
        track = rtc.LocalAudioTrack.create_audio_track("candidate-mic", source)
        opts = rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
        await room.local_participant.publish_track(track, opts)

        fixtures = FIXTURES or []
        for path in fixtures:
            # Wait for Tara to finish her turn before we answer.
            try:
                await asyncio.wait_for(tara_done_q.get(), timeout=TURN_WAIT_S)
            except asyncio.TimeoutError:
                pass  # publish anyway; the proxy interval below will reflect it

            # CLIENT-SIDE PROXY latency: time from publishing this answer until
            # we receive the next tara_done. NOT the worker's authoritative
            # LATENCY_BREAKDOWN — see module docstring.
            t0 = time.monotonic()
            await _publish_wav(source, path)
            try:
                await asyncio.wait_for(tara_done_q.get(), timeout=TURN_WAIT_S)
                turn_latencies.append(time.monotonic() - t0)
                # Put it back so the next iteration's pre-answer wait sees it.
                tara_done_q.put_nowait(True)
            except asyncio.TimeoutError:
                pass

        await asyncio.sleep(DRAIN_S)
    finally:
        try:
            await room.disconnect()
        finally:
            try:
                await source.aclose()  # type: ignore[possibly-undefined]
            except Exception:  # noqa: BLE001
                pass

    return SessionOutcome(
        admitted=True,
        rejected_bucket=None,
        turn_latencies_s=turn_latencies,
        total_tokens=0,  # not client-observable; see docstring
        was_injected_noshow=False,
        was_real_participant=True,
        reclaimed_reason=None,  # not client-observable; see docstring
    )


async def _run_noshow_candidate(cand_id: str) -> SessionOutcome:
    """Injected no-show: get a token, then NEVER join the room."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        sess = await _create_session(client, cand_id)

    if sess is not None and sess.get("rejected"):
        # Even a no-show can be rejected at admission; record it as such.
        return SessionOutcome(
            admitted=False,
            rejected_bucket=sess.get("bucket", "unknown"),
            turn_latencies_s=[],
            total_tokens=0,
            was_injected_noshow=True,
            was_real_participant=False,
            reclaimed_reason=None,
        )

    # Admitted (token issued) but deliberately never joins → the server-side
    # reaper should reclaim this slot. That reclaim is a TRUE reclaim and must
    # NOT count as a false reclaim; the limiter metrics classify it server-side.
    return SessionOutcome(
        admitted=True,
        rejected_bucket=None,
        turn_latencies_s=[],
        total_tokens=0,
        was_injected_noshow=True,
        was_real_participant=False,
        reclaimed_reason=None,  # not client-observable; see docstring
    )


def _failed_outcome(real: bool, noshow: bool = False) -> SessionOutcome:
    """A candidate that errored/hung → treat as rejected-with-error, not a crash."""
    return SessionOutcome(
        admitted=False,
        rejected_bucket="error",
        turn_latencies_s=[],
        total_tokens=0,
        was_injected_noshow=noshow,
        was_real_participant=False,
        reclaimed_reason=None,
    )


async def _guarded_candidate(real: bool, cand_id: str) -> SessionOutcome:
    """Wrap a candidate in its own timeout + try/except so it can't hang/crash the stage."""
    try:
        coro = _run_real_candidate(cand_id) if real else _run_noshow_candidate(cand_id)
        return await asyncio.wait_for(coro, timeout=CANDIDATE_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001
        print(f"  [candidate {cand_id}] failed: {exc!r}", file=sys.stderr)
        return _failed_outcome(real=real, noshow=not real)


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------

async def run_stage(concurrency: int, noshow_fraction: float) -> tuple[list[SessionOutcome], dict]:
    """Launch `concurrency` candidates, a `noshow_fraction` of them no-shows."""
    n_noshow = int(round(concurrency * noshow_fraction))
    print(f"\n=== STAGE concurrency={concurrency}  (no-shows={n_noshow}) ===")

    tasks = []
    for i in range(concurrency):
        is_noshow = i < n_noshow
        cand_id = f"load-{concurrency}-{i}-{uuid.uuid4().hex[:8]}"
        tasks.append(_guarded_candidate(real=not is_noshow, cand_id=cand_id))

    outcomes = await asyncio.gather(*tasks)
    report = stage_report(f"concurrency-{concurrency}", concurrency, list(outcomes))
    print(f"  stage_report: {report}")
    return list(outcomes), report


# ---------------------------------------------------------------------------
# Hard-gate asserts (client-side smoke gate — see docstring)
# ---------------------------------------------------------------------------

def assert_hard_gates(stage_reports: list[dict]) -> list[str]:
    """Return a list of gate-failure strings (empty = all gates passed)."""
    failures: list[str] = []

    # Gate 1: false_reclaims == 0 at EVERY stage.
    for r in stage_reports:
        if r["false_reclaims"] != 0:
            failures.append(
                f"false_reclaims != 0 at stage {r['stage']}: {r['false_reclaims']}"
            )

    # Gate 2: cap never exceeded — admitted count per stage must not exceed the
    # configured global cap (GLOBAL_CAP env). Skip with a note if unset.
    global_cap_env = os.environ.get("GLOBAL_CAP")
    if global_cap_env is None:
        print("  [cap gate] GLOBAL_CAP not set — skipping cap-not-exceeded assert.")
    else:
        cap = int(global_cap_env)
        for r in stage_reports:
            admitted = int(round(r["concurrency"] * (1.0 - r["rejection_rate"])))
            if admitted > cap:
                failures.append(
                    f"cap exceeded at stage {r['stage']}: admitted~{admitted} > GLOBAL_CAP={cap}"
                )

    # Gate 3: false_reclaim_slope flat (<= epsilon).
    slope = false_reclaim_slope(stage_reports)
    if abs(slope) > SLOPE_EPSILON:
        failures.append(
            f"false_reclaim_slope not flat: |{slope}| > epsilon {SLOPE_EPSILON}"
        )

    return failures


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

async def _amain(stages: list[int], noshow_fraction: float) -> int:
    all_reports: list[dict] = []
    for conc in stages:
        _outcomes, report = await run_stage(conc, noshow_fraction)
        all_reports.append(report)

    print("\n" + "=" * 60)
    print("CROSS-STAGE AGGREGATION")
    print("=" * 60)
    slope = false_reclaim_slope(all_reports)
    budgets = calibrated_budgets(all_reports, TARGET_POD_SESSIONS)
    print(f"false_reclaim_slope: {slope}")
    print(f"calibrated_budgets:  {budgets}")

    failures = assert_hard_gates(all_reports)
    print("\n" + "-" * 60)
    if failures:
        print("HARD-GATE FAILURES (client-side smoke gate):")
        for f in failures:
            print(f"  FAIL: {f}")
        print("-" * 60)
        print(
            "NOTE: authoritative false_reclaims==0 / cap-held / slope-flat is\n"
            "verified server-side via the worker's Prometheus metrics during the\n"
            "GKE run (Task 7 runbook). This driver is a client-side smoke gate."
        )
        return 1
    print("HARD GATES PASSED (client-side smoke gate).")
    print(
        "NOTE: authoritative verification is server-side via Prometheus during\n"
        "the GKE run (Task 7 runbook) — this driver cannot see reclaim/token/cap\n"
        "classification from the client. See README.md."
    )
    print("-" * 60)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Staged load-test driver (50/120/200) with no-show injection and "
            "client-side hard-gate smoke asserts. Requires a running backend + "
            "worker + LiveKit (see README.md)."
        )
    )
    parser.add_argument(
        "--stages",
        default="50,120,200",
        help="Comma-separated concurrency stages. Default: 50,120,200.",
    )
    parser.add_argument(
        "--noshow-fraction",
        type=float,
        default=0.1,
        help="Fraction of each stage injected as no-shows (token issued, never joins). Default: 0.1.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Run only 2 sessions total against a local backend+worker, print two "
            "stage_reports, exit 0. Still requires the 3-terminal local stack."
        ),
    )
    args = parser.parse_args()

    if args.dry_run:
        # Tiny concurrency regardless of --stages: two single-session stages so
        # we exercise both the stage loop and stage_report twice.
        stages = [1, 1]
    else:
        stages = [int(s) for s in args.stages.split(",") if s.strip()]

    rc = asyncio.run(_amain(stages, args.noshow_fraction))
    sys.exit(0 if args.dry_run else rc)


if __name__ == "__main__":
    main()

"""Single-stream latency harness — Phase-1 acceptance GATE.

Usage (three terminals):
    # terminal 1
    cd backend && npm start

    # terminal 2
    cd agent && source .venv/bin/activate
    python -m tara_agent.worker dev

    # terminal 3
    source agent/.venv/bin/activate
    python tools/latency_harness.py

Pass criteria: the worker prints
    LATENCY_BREAKDOWN_P50={"eou_delay":..,"stt":..,"llm_ttft":..,"tts_ttfb":..,"total":..}
and `total` must be < 0.800 seconds.

This script reads that line from the worker subprocess stdout and exits
non-zero if the gate fails — or prints the breakdown and exits 0 on pass.

When run in STANDALONE mode (default: without --subprocess), it drives only
the LiveKit side (create session, join, publish WAVs) and lets the operator
read the worker log manually.  Add --subprocess to have the harness launch
the worker itself and capture its output automatically.

Environment variables:
    BACKEND_URL      default: http://localhost:3000
    LIVEKIT_URL      default: taken from /sessions response (already in blob)
    WORKER_LOG_FILE  path to pipe worker stdout to (used by --subprocess mode)
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import glob
import json
import os
import re
import subprocess
import sys
import wave

import httpx
from livekit import rtc

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BACKEND = os.environ.get("BACKEND_URL", "http://localhost:3000")
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
FIXTURES = sorted(glob.glob(os.path.join(FIXTURES_DIR, "*.wav")))
GATE_SECONDS = 0.800

# How long to wait (in seconds) for Tara to finish her turn before we publish
# the next candidate answer.  2 s gives the TTS time to complete for typical
# short answers; increase if Tara's answers are longer.
TURN_PAUSE_SECONDS = float(os.environ.get("TURN_PAUSE_SECONDS", "2.0"))

# After the last candidate utterance, wait this many seconds for the worker to
# print the breakdown before disconnecting.
DRAIN_SECONDS = float(os.environ.get("DRAIN_SECONDS", "8.0"))

# ---------------------------------------------------------------------------
# Audio helpers
# ---------------------------------------------------------------------------

def _read_wav(path: str) -> tuple[bytes, int, int]:
    """Return (pcm_data_bytes, sample_rate, num_channels) from a WAV file.

    Only PCM (uncompressed 16-bit) WAVs are supported.  The harness does NOT
    resample; use 48 kHz mono WAVs as described in tools/fixtures/README.md.
    """
    with wave.open(path, "rb") as w:
        if w.getsampwidth() != 2:
            raise ValueError(
                f"{path}: expected 16-bit PCM WAV (sampwidth=2), got {w.getsampwidth()}"
            )
        sr = w.getframerate()
        ch = w.getnchannels()
        data = w.readframes(w.getnframes())
    return data, sr, ch


async def _publish_wav(source: rtc.AudioSource, path: str) -> None:
    """Push all PCM frames from a WAV file into the AudioSource.

    The installed livekit-rtc AudioSource.capture_frame signature:
        async def capture_frame(self, frame: AudioFrame) -> None

    AudioFrame constructor signature (verified against installed version):
        AudioFrame(data, sample_rate, num_channels, samples_per_channel)
    where samples_per_channel = len(data) // (2 * num_channels).
    """
    data, sr, ch = _read_wav(path)
    samples_per_channel = len(data) // (2 * ch)
    frame = rtc.AudioFrame(data, sr, ch, samples_per_channel)
    await source.capture_frame(frame)
    # Wait for the audio to drain from the source queue before returning so
    # the harness pacing stays in sync with actual audio playback time.
    await source.wait_for_playout()


# ---------------------------------------------------------------------------
# Gate evaluation
# ---------------------------------------------------------------------------

_BREAKDOWN_RE = re.compile(
    r"LATENCY_BREAKDOWN_P50=(\{[^\n]+\})"
)


def evaluate_gate(breakdown_json: str) -> tuple[bool, dict]:
    """Parse the worker breakdown line and return (passed, breakdown_dict)."""
    breakdown = json.loads(breakdown_json)
    total = float(breakdown.get("total", float("inf")))
    passed = total < GATE_SECONDS
    return passed, breakdown


def print_gate_result(breakdown: dict, passed: bool) -> None:
    total = breakdown.get("total", "?")
    print()
    print("=" * 60)
    print(f"PHASE-1 LATENCY GATE  (threshold: total < {GATE_SECONDS:.3f} s)")
    print("-" * 60)
    for k, v in breakdown.items():
        marker = " <-- OVER BUDGET" if k == "total" and not passed else ""
        print(f"  {k:12s}: {v:.3f} s{marker}")
    print("-" * 60)
    if passed:
        print(f"  PASS  total={total:.3f} s  (< {GATE_SECONDS:.3f} s)")
    else:
        print(f"  FAIL  total={total:.3f} s  (>= {GATE_SECONDS:.3f} s)")
        print()
        print("  Remediation hints:")
        if breakdown.get("llm_ttft", 0) > 0.3:
            print("  * llm_ttft high → enable Gemini context caching for the static block.")
        if breakdown.get("eou_delay", 0) > 0.3:
            print("  * eou_delay high → check turn-detector config (MultilingualModel threshold).")
        if breakdown.get("tts_ttfb", 0) > 0.2:
            print("  * tts_ttfb high → confirm streaming TTS (use_streaming=True), not batch.")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main harness (LiveKit side only)
# ---------------------------------------------------------------------------

async def run_harness(subprocess_mode: bool = False) -> str | None:
    """Create session, join room, publish WAVs.

    Returns the raw LATENCY_BREAKDOWN_P50 JSON string if captured from a
    subprocess worker, or None in standalone mode.
    """
    if not FIXTURES:
        print(
            f"ERROR: no fixture WAVs found in {FIXTURES_DIR}/\n"
            "       See tools/fixtures/README.md for how to supply them.",
            file=sys.stderr,
        )
        sys.exit(2)

    print(f"Fixtures ({len(FIXTURES)}):")
    for f in FIXTURES:
        print(f"  {f}")

    # ------------------------------------------------------------------
    # 1. Create session via the backend /sessions endpoint
    # ------------------------------------------------------------------
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            f"{BACKEND}/sessions",
            json={
                "contestId": "gate-test-c1",
                "candidateId": "harness-cand-1",
                "skills": ["Python", "SQL"],
                "resumeText": "5 years backend Python and data engineering.",
                "jdText": "Backend engineer, Python + SQL, 3+ years experience.",
                "maxQuestions": len(FIXTURES),
            },
        )
        resp.raise_for_status()
        sess = resp.json()

    livekit_url = sess.get("livekitUrl") or os.environ.get("LIVEKIT_URL", "")
    token = sess["token"]
    session_id = sess.get("sessionId", "?")

    print(f"\nSession created: {session_id}")
    print(f"LiveKit URL:     {livekit_url}")

    if not livekit_url:
        print(
            "ERROR: livekitUrl missing from /sessions response and LIVEKIT_URL not set.",
            file=sys.stderr,
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # 2. Join the LiveKit room as the synthetic candidate
    # ------------------------------------------------------------------
    room = rtc.Room()
    await room.connect(livekit_url, token)
    print(f"Joined room: {room.name}  (local participant: {room.local_participant.identity})")

    # Build and publish a local audio track from a synthetic AudioSource.
    # AudioSource(sample_rate, num_channels) — verified against installed livekit-rtc.
    source = rtc.AudioSource(48000, 1)
    # LocalAudioTrack.create_audio_track(name, source) — verified; returns LocalAudioTrack.
    track = rtc.LocalAudioTrack.create_audio_track("candidate-mic", source)
    # publish_track(track, options=TrackPublishOptions()) — verified; returns LocalTrackPublication.
    await room.local_participant.publish_track(track)
    print("Audio track published.")

    # ------------------------------------------------------------------
    # 3. Publish WAV utterances, one per question slot
    # ------------------------------------------------------------------
    for i, path in enumerate(FIXTURES, start=1):
        print(f"\nTurn {i}: waiting {TURN_PAUSE_SECONDS:.1f}s for Tara's question …")
        await asyncio.sleep(TURN_PAUSE_SECONDS)
        print(f"Turn {i}: publishing {os.path.basename(path)}")
        await _publish_wav(source, path)
        print(f"Turn {i}: done.")

    # ------------------------------------------------------------------
    # 4. Drain: give the worker time to finish the last turn and print breakdown
    # ------------------------------------------------------------------
    print(f"\nAll fixtures published. Draining {DRAIN_SECONDS:.0f}s …")
    await asyncio.sleep(DRAIN_SECONDS)

    await room.disconnect()
    print("Disconnected from room.")

    return None  # breakdown captured by caller in subprocess mode


# ---------------------------------------------------------------------------
# Subprocess mode: launch worker and capture its output
# ---------------------------------------------------------------------------

async def run_with_subprocess() -> None:
    """Launch the worker as a subprocess, run the harness, capture breakdown."""
    agent_dir = os.path.join(os.path.dirname(__file__), "..", "agent")
    venv_python = os.path.join(agent_dir, ".venv", "bin", "python")
    if not os.path.exists(venv_python):
        venv_python = sys.executable  # fallback

    print("Starting worker subprocess …")
    proc = subprocess.Popen(
        [venv_python, "-m", "tara_agent.worker", "dev"],
        cwd=os.path.abspath(agent_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    breakdown_json: str | None = None
    captured_lines: list[str] = []

    async def read_worker_output() -> None:
        nonlocal breakdown_json
        assert proc.stdout is not None
        loop = asyncio.get_event_loop()
        while True:
            line = await loop.run_in_executor(None, proc.stdout.readline)
            if not line:
                break
            line = line.rstrip()
            captured_lines.append(line)
            print(f"[worker] {line}")
            m = _BREAKDOWN_RE.search(line)
            if m:
                breakdown_json = m.group(1)

    reader_task = asyncio.create_task(read_worker_output())

    # Give the worker a moment to connect before we create the session.
    await asyncio.sleep(3.0)

    await run_harness(subprocess_mode=True)

    # Wait for the worker to print its breakdown (up to 15 s after harness ends).
    deadline = asyncio.get_event_loop().time() + 15.0
    while breakdown_json is None and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.5)

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

    reader_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await reader_task

    if breakdown_json is None:
        print(
            "\nERROR: worker did not print LATENCY_BREAKDOWN_P50 within the timeout.",
            file=sys.stderr,
        )
        print(
            "       Check that the worker, backend, and LiveKit credentials are all configured.",
            file=sys.stderr,
        )
        sys.exit(1)

    passed, breakdown = evaluate_gate(breakdown_json)
    print_gate_result(breakdown, passed)
    sys.exit(0 if passed else 1)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Phase-1 latency gate harness. "
            "Drives one interview with pre-recorded candidate WAVs and checks p50 < 0.800 s."
        )
    )
    parser.add_argument(
        "--subprocess",
        action="store_true",
        default=False,
        help=(
            "Launch the worker as a subprocess and capture its LATENCY_BREAKDOWN_P50 output. "
            "Requires the agent venv and backend to be configured.  "
            "Default: standalone mode — you run the worker separately."
        ),
    )
    args = parser.parse_args()

    if args.subprocess:
        asyncio.run(run_with_subprocess())
    else:
        # Standalone: just drive the room; operator reads the worker log.
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(run_harness(subprocess_mode=False))
        print(
            "\nHarness finished (standalone mode).\n"
            "Read LATENCY_BREAKDOWN_P50 from the worker log and check: total < 0.800 s.\n"
            "Run with --subprocess to have the harness capture and assert the gate automatically."
        )

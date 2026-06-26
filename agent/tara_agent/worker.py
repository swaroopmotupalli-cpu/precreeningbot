"""Worker entrypoint — wires the streaming pipeline:

    turn-detector → STT → google.LLM (PLAIN TEXT) → TTS

Per-turn latency metrics are collected via the (deprecated but still functional)
`metrics_collected` event and mapped into LatencyCollector.record_turn().

DO NOT add structured output / JSON mode to the LLM.
DO NOT score on the hot path — scoring (Task 11) runs offline after session end.
"""
import asyncio
import json
import time
import warnings
import logging

import prometheus_client

import redis.asyncio as aioredis
from motor.motor_asyncio import AsyncIOMotorClient

from livekit.agents import (
    Agent,
    AgentSession,
    JobContext,
    WorkerOptions,
    cli,
    metrics,
    MetricsCollectedEvent,
)
from livekit.plugins import google
from google.genai import types as _genai_types

# MultilingualModel requires a live job context (inference executor) to instantiate;
# it is therefore created inside entrypoint(), not at module level.
# Suppress the deprecation warning — the plugin still works in 1.0.19 and is the
# on-device multilingual turn detector the brief targets.
with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    from livekit.plugins.turn_detector.multilingual import MultilingualModel  # noqa: E402

from tara_agent.config import get_settings
from tara_agent.session_store import load_session
from tara_agent.prompts import build_system_prompt
from tara_agent.transcript import TranscriptStore
from tara_agent.coverage import CoverageTracker
from tara_agent.gemini import make_classify_fn
from tara_agent.latency import LatencyCollector
from tara_agent.interview_agent import InterviewAgent
from tara_agent.metrics_export import TaraMetrics

log = logging.getLogger("tara.worker")

# ---------------------------------------------------------------------------
# Module-level Prometheus exporter — one instance per worker process.
# _ensure_metrics_server is idempotent: the LiveKit worker may run multiple
# jobs in the same process; the HTTP server must only be started once.
# ---------------------------------------------------------------------------
_METRICS = TaraMetrics()
_metrics_server_started = False


def _ensure_metrics_server(port: int) -> None:
    global _metrics_server_started
    if not _metrics_server_started:
        prometheus_client.start_http_server(port, registry=_METRICS.registry)
        _metrics_server_started = True


async def entrypoint(ctx: JobContext):
    s = get_settings()
    _ensure_metrics_server(s.metrics_port)
    caps_dict = {
        "global": s.max_global_sessions,
        "gemini_tpm": s.gemini_tpm_budget,
        "stt_streams": s.stt_stream_budget,
        "tts_streams": s.tts_stream_budget,
    }

    redis = aioredis.from_url(s.redis_url, decode_responses=True)
    mongo_col = AsyncIOMotorClient(s.mongodb_uri)["tara"]["aiInterview"]

    # --- Admission limiter -------------------------------------------------------
    from tara_agent.limiter import Limiter, Caps
    limiter = Limiter(
        redis,
        caps=Caps(
            global_=s.max_global_sessions,
            gemini_tpm=s.gemini_tpm_budget,
            stt_streams=s.stt_stream_budget,
            tts_streams=s.tts_stream_budget,
        ),
        reservation_ttl_ms=s.reservation_lease_ttl * 1000,
        heartbeat_ttl_ms=s.heartbeat_lease_ttl * 1000,
    )
    now_ms = lambda: int(time.time() * 1000)  # epoch-ms matching Node Date.now()

    _limiter_log = logging.getLogger("tara.limiter")

    async def _heartbeat_loop():
        while not done.is_set():
            await limiter.heartbeat(ctx.room.name, now_ms())
            await asyncio.sleep(s.heartbeat_interval)

    async def _reaper_loop():
        while not done.is_set():
            for rc in await limiter.reap(now_ms()):
                _limiter_log.info("reclaim %s reason=%s", rc.room, rc.reason)
            await asyncio.sleep(s.heartbeat_interval)

    async def _metrics_sync_loop():
        while not done.is_set():
            _METRICS.sync_limiter(await limiter.metrics(), caps_dict)
            await asyncio.sleep(s.metric_scrape_interval)
    # -----------------------------------------------------------------------------

    # Convention: room name == session id stored in Redis.
    session_id = ctx.room.name
    blob = await load_session(redis, session_id)

    transcript = TranscriptStore(redis, ctx.room.name, s.transcript_ttl_seconds)
    coverage = CoverageTracker(
        redis, ctx.room.name, blob.skills, make_classify_fn(s)
    )
    latency = LatencyCollector()
    done = asyncio.Event()

    # MultilingualModel must be instantiated inside the job entrypoint
    # because it needs get_job_context().inference_executor.
    # The DeprecationWarning is suppressed at import; a future task can migrate
    # to livekit.agents.inference.TurnDetector when LiveKit Cloud creds are available.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        turn_detector = MultilingualModel()

    session = AgentSession(
        stt=google.STT(
            # Proven config from the prior production app.py: STT V2 streaming,
            # model "latest_long" in the "global" location. global avoids the
            # cross-region (India→us-central1) round trip that made chirp_2 ~4s,
            # AND allows multi-language recognition (en-IN + en-US), which the
            # regional chirp_2 endpoints do not.
            languages=s.interview_languages,
            model="latest_long",
            location="global",
            interim_results=True,
        ),
        llm=google.LLM(
            model=s.gemini_model,
            # LLM latency note: base TTFT from a local India laptop to the global
            # Gemini Developer API is ~1.3s. Vertex AI in-region (the analog of the
            # STT region fix) is NOT available on this project, so the Developer API
            # is the only path. This is expected to be faster in production (worker
            # on GKE inside Google's network) — re-measure TTFT from the deployment
            # environment; do not treat the local laptop number as representative.
            temperature=0.6,  # PLAIN TEXT — no JSON mode
            # Minimize model "thinking" — Gemini 3 thinks by default, inflating
            # TTFT with no benefit for short conversational questions. Gemini 3
            # uses thinking_level (not thinking_budget); "low" is the minimum.
            # (Caching is NOT a lever here: the whole prompt is ~141-606 tokens,
            # far below Gemini's cache minimum — cached_tokens=0 every turn.)
            thinking_config=_genai_types.ThinkingConfig(thinking_level="low"),
        ),
        tts=google.TTS(                                         # Chirp3-HD, use_streaming=True default
            voice_name=s.tts_voice,
            language="-".join(s.tts_voice.split("-")[:2]),      # match voice locale (e.g. en-IN) — must not default to en-US
        ),
        turn_detection=turn_detector,
        # Cap how long we wait after speech stops before committing the turn.
        # The semantic detector still decides; these bound its endpointing wait
        # (default max was hitting 3.0s when the model was unsure). 0.3s floor
        # for confident ends, 1.5s ceiling for uncertain ones.
        min_endpointing_delay=0.3,
        max_endpointing_delay=1.5,
    )

    # ---------------------------------------------------------------------------
    # Per-turn latency → LatencyCollector
    # metrics_collected is deprecated in 1.0.19 but still functional: the
    # framework continues to emit EOUMetrics / LLMMetrics / TTSMetrics through
    # this event. A future task can migrate to ChatMessage.metrics.
    # Attribute mapping (verified against 1.0.19):
    #   EOUMetrics: end_of_utterance_delay, transcription_delay  ← brief is correct
    #   LLMMetrics: ttft                                          ← brief is correct
    #   TTSMetrics: ttfb                                          ← brief is correct
    # ---------------------------------------------------------------------------
    pending: dict = {}

    @session.on("metrics_collected")
    def _on_metrics(ev: MetricsCollectedEvent):
        m = ev.metrics
        if isinstance(m, metrics.EOUMetrics):
            pending["eou"] = m.end_of_utterance_delay
            pending["stt"] = m.transcription_delay
        elif isinstance(m, metrics.LLMMetrics):
            pending["ttft"] = m.ttft
            log.info(
                "LLM_DIAG ttft=%.3f prompt_tokens=%s cached_tokens=%s completion=%s",
                m.ttft, m.prompt_tokens, m.prompt_cached_tokens, m.completion_tokens,
            )
        elif isinstance(m, metrics.TTSMetrics):
            pending["ttfb"] = m.ttfb
            if {"eou", "ttft", "ttfb"} <= pending.keys():
                total = pending["eou"] + pending["ttft"] + pending["ttfb"]
                latency.record_turn(
                    pending["eou"],
                    pending.get("stt", 0.0),
                    pending["ttft"],
                    pending["ttfb"],
                )
                _METRICS.observe_turn(total, pending["ttft"])
                pending.clear()

    agent = InterviewAgent(
        instructions=build_system_prompt(blob),
        blob=blob,
        transcript=transcript,
        coverage=coverage,
        mongo_write_fn=lambda doc: mongo_col.insert_one(doc),
        settings=s,
        on_end=done.set,
        limiter=limiter,
        room=ctx.room.name,
    )

    # Capture Tara's spoken lines into the transcript as they are committed.
    # ConversationItemAddedEvent.item is ChatMessage | AgentHandoff; we only
    # care about assistant ChatMessages.
    @session.on("conversation_item_added")
    def _on_item(ev):
        item = ev.item
        if getattr(item, "role", None) == "assistant":
            asyncio.create_task(agent.on_tara_line(item.text_content or ""))

    # Signal "Tara finished her turn" to any participant (the latency harness
    # waits on this so it never publishes the next answer over Tara's speech).
    # Only fire on speaking → listening so the greeting counts but the initial
    # idle "listening" does not.
    async def _signal_tara_done():
        try:
            await ctx.room.local_participant.publish_data(
                "tara_done", topic="gate", reliable=True
            )
        except Exception as e:  # never let signalling break the session
            log.debug("tara_done publish failed: %s", e)

    @session.on("agent_state_changed")
    def _on_agent_state(ev):
        if ev.old_state == "speaking" and ev.new_state == "listening":
            asyncio.create_task(_signal_tara_done())

    # Flush the latency breakdown exactly once, however the session ends
    # (natural coverage/cap end, candidate disconnect, or job shutdown).
    _flushed = {"v": False}
    _started = {"v": False}  # True only after session.start() succeeded + session_started() called

    def _flush_breakdown():
        if _flushed["v"]:
            return
        _flushed["v"] = True
        if _started["v"]:   # guard: don't decrement if session.start() never succeeded
            _METRICS.session_ended()
        try:
            payload = json.dumps(latency.breakdown_p50())
        except Exception as e:  # empty collector (no turns) — report, don't crash
            payload = json.dumps({"error": "no_turns_recorded", "detail": str(e)})
        log.info("LATENCY_BREAKDOWN_P50=%s", payload)
        print("LATENCY_BREAKDOWN_P50=" + payload, flush=True)

    # session "close" fires on candidate disconnect — flush promptly and release
    # the entrypoint so the breakdown is printed well inside the harness window.
    @session.on("close")
    def _on_close(*_a):
        asyncio.create_task(limiter.release(ctx.room.name))
        done.set()

    async def _flush_on_shutdown():
        _flush_breakdown()

    ctx.add_shutdown_callback(_flush_on_shutdown)  # backstop if "close" path is missed

    # session.start automatically calls ctx.connect() when a room is passed.
    await session.start(agent=agent, room=ctx.room)
    _METRICS.session_started()
    _started["v"] = True

    # Promote the reservation to a Tier-2 heartbeat lease now that the agent has
    # joined (sets both mark_heartbeat and mark_participant flags).
    await limiter.heartbeat(ctx.room.name, now_ms(),
                            mark_heartbeat=True, mark_participant=True)

    # Start background heartbeat, reaper, and metrics-sync loops.
    hb = asyncio.create_task(_heartbeat_loop())
    rp = asyncio.create_task(_reaper_loop())
    ms = asyncio.create_task(_metrics_sync_loop())

    # Release the lease immediately when the candidate disconnects.
    # "participant_disconnected" is the canonical livekit-rtc event name (verified
    # against installed livekit-rtc room.py — emitted as
    # self.emit("participant_disconnected", rparticipant)).
    @ctx.room.on("participant_disconnected")
    def _on_part_left(p):
        asyncio.create_task(limiter.release(ctx.room.name))
        done.set()

    # Greet the candidate and ask the first question.
    await session.generate_reply(
        instructions="Greet the candidate warmly and ask your first interview question."
    )

    await done.wait()
    hb.cancel()
    rp.cancel()
    ms.cancel()
    _flush_breakdown()


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))

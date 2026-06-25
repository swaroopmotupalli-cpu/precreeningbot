"""Worker entrypoint — wires the streaming pipeline:

    turn-detector → STT → google.LLM (PLAIN TEXT) → TTS

Per-turn latency metrics are collected via the (deprecated but still functional)
`metrics_collected` event and mapped into LatencyCollector.record_turn().

DO NOT add structured output / JSON mode to the LLM.
DO NOT score on the hot path — scoring (Task 11) runs offline after session end.
"""
import asyncio
import json
import warnings
import logging

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

log = logging.getLogger("tara.worker")


async def entrypoint(ctx: JobContext):
    s = get_settings()

    redis = aioredis.from_url(s.redis_url, decode_responses=True)
    mongo_col = AsyncIOMotorClient(s.mongodb_uri)["tara"]["aiInterview"]

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
            languages=s.interview_languages,
            model="chirp",
            spoken_punctuation=False,
        ),
        llm=google.LLM(model=s.gemini_model, temperature=0.6),  # PLAIN TEXT — no JSON mode
        tts=google.TTS(voice_name=s.tts_voice),                 # Chirp3-HD, use_streaming=True default
        turn_detection=turn_detector,
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
        elif isinstance(m, metrics.TTSMetrics):
            pending["ttfb"] = m.ttfb
            if {"eou", "ttft", "ttfb"} <= pending.keys():
                latency.record_turn(
                    pending["eou"],
                    pending.get("stt", 0.0),
                    pending["ttft"],
                    pending["ttfb"],
                )
                pending.clear()

    agent = InterviewAgent(
        instructions=build_system_prompt(blob),
        blob=blob,
        transcript=transcript,
        coverage=coverage,
        mongo_write_fn=lambda doc: mongo_col.insert_one(doc),
        settings=s,
        on_end=done.set,
    )

    # Capture Tara's spoken lines into the transcript as they are committed.
    # ConversationItemAddedEvent.item is ChatMessage | AgentHandoff; we only
    # care about assistant ChatMessages.
    @session.on("conversation_item_added")
    def _on_item(ev):
        item = ev.item
        if getattr(item, "role", None) == "assistant":
            asyncio.create_task(agent.on_tara_line(item.text_content or ""))

    # session.start automatically calls ctx.connect() when a room is passed.
    await session.start(agent=agent, room=ctx.room)

    # Greet the candidate and ask the first question.
    await session.generate_reply(
        instructions="Greet the candidate warmly and ask your first interview question."
    )

    await done.wait()

    breakdown = latency.breakdown_p50()
    log.info("LATENCY_BREAKDOWN_P50=%s", json.dumps(breakdown))
    print("LATENCY_BREAKDOWN_P50=" + json.dumps(breakdown))


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))

"""InterviewAgent — Agent subclass that ties transcript, coverage, and end-decision together.

The agent NEVER scores or classifies on the hot path. Coverage tagging is off-path via
asyncio.create_task (fire-and-forget). Scoring (Task 11) runs after the session ends.
"""
import asyncio
import logging

from livekit.agents import Agent
from tara_agent.coverage import CoverageTracker
from tara_agent.interview import should_end
from tara_agent.transcript import TranscriptStore
from tara_agent.persistence import end_interview

log = logging.getLogger("tara.interview_agent")


class InterviewAgent(Agent):
    def __init__(
        self,
        *,
        instructions: str,
        blob,
        transcript: TranscriptStore,
        coverage: CoverageTracker,
        mongo_write_fn,
        settings,
        on_end,
        limiter=None,
        room: str = "",
    ):
        super().__init__(instructions=instructions)
        self._blob = blob
        self._transcript = transcript
        self._coverage = coverage
        self._mongo_write_fn = mongo_write_fn
        self._settings = settings
        self._on_end = on_end
        self._limiter = limiter
        self._room = room
        self._q_count = 0
        self._ending = False  # guard: ensure _wrap_up runs at most once

    async def on_user_turn_completed(self, turn_ctx, new_message):
        """Called by the framework after each complete user turn.

        Appends the candidate utterance to the transcript, kicks off off-path
        coverage tagging, and checks the end condition.
        """
        text = new_message.text_content or ""
        await self._transcript.append("candidate", text)
        # OFF PATH: fire-and-forget — never awaited on the hot path.
        asyncio.create_task(self._coverage.tag(text))
        self._q_count += 1
        covered = await self._coverage.covered()
        if not self._ending and should_end(
            covered, self._blob.skills, self._q_count, self._blob.max_questions
        ):
            self._ending = True
            await self._wrap_up()

    async def on_tara_line(self, text: str):
        """Append a Tara (assistant) line to the transcript store."""
        await self._transcript.append("tara", text)

    async def on_interruption(self, spoken_text: str) -> None:
        """Barge-in: truncate Tara's last line to only what was actually spoken.

        The Mongo transcript is an audit artifact — it must match reality, never
        the full intended sentence. No-op if there is no Tara line yet.
        """
        await self._transcript.truncate_last("tara", spoken_text)

    def _on_end_and_release(self):
        """Called in the guaranteed finally of end_interview.

        Schedules limiter.release (fire-and-scheduled — does not block the
        finally path) then signals done via self._on_end().
        The heartbeat TTL backstops release if the task is cut off.
        """
        if self._limiter is not None and self._room:
            asyncio.create_task(self._limiter.release(self._room))
        self._on_end()

    async def _wrap_up(self):
        """Say goodbye, persist transcript, then signal done and release lease."""
        room_name = self._transcript._key.split(":", 1)[1]
        await end_interview(
            say_fn=lambda: self.session.say(
                "Thank you, that's the end of the interview. We'll be in touch."
            ),
            transcript_store=self._transcript,
            mongo_write_fn=self._mongo_write_fn,
            room=room_name,
            contest_id=self._blob.contest_id,
            candidate_id=self._blob.candidate_id,
            say_timeout=self._settings.say_timeout_seconds,
            write_timeout=self._settings.mongo_write_timeout_seconds,
            on_finally=self._on_end_and_release,
            offpath_retry_attempts=self._settings.offpath_retry_attempts,
            retry_base_delay_ms=self._settings.retry_base_delay_ms,
            retry_max_delay_ms=self._settings.retry_max_delay_ms,
        )

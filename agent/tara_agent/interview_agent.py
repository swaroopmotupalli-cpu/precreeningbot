"""InterviewAgent — Agent subclass that ties transcript, coverage, and end-decision together.

The agent NEVER scores or classifies on the hot path. Coverage tagging is off-path via
asyncio.create_task (fire-and-forget). Scoring (Task 11) runs after the session ends.
"""
import asyncio
import logging

from livekit.agents import Agent, StopResponse
from tara_agent.coverage import CoverageTracker
from tara_agent.interview import should_end
from tara_agent.transcript import TranscriptStore
from tara_agent.retry import retry_async

log = logging.getLogger("tara.interview_agent")


class InterviewAgent(Agent):
    def __init__(
        self,
        *,
        instructions: str,
        blob,
        transcript: TranscriptStore,
        coverage: CoverageTracker,
        settings,
        on_end,
        limiter=None,
        room: str = "",
        enqueue_fn=None,
    ):
        super().__init__(instructions=instructions)
        self._blob = blob
        self._transcript = transcript
        self._coverage = coverage
        self._settings = settings
        self._on_end = on_end
        self._limiter = limiter
        self._room = room
        # Counts real Tara questions (lines containing "?"), NOT candidate
        # turns — a candidate's answer can arrive as several turns (they
        # pause to think), and counting turns let the cap trip mid-answer.
        self._questions_asked = 0
        self._ending = False  # guard: ensure _wrap_up runs at most once
        self._end_debounce_task: asyncio.Task | None = None  # counts down to the goodbye once end-eligible

    async def _append_line(self, speaker: str, text: str, *, interrupted: bool = False) -> bool:
        """Append a transcript line with bounded retry against transient
        Redis blips. Without this, a single dropped connection silently
        loses that line forever — no error, no log, and (for a Tara
        question) the scorer's Q&A pairing then wrongly glues the
        candidate's next answer onto the PREVIOUS question instead. Returns
        False (never raises) if the write ultimately failed after retries.
        """
        try:
            await retry_async(
                lambda: self._transcript.append(speaker, text, interrupted=interrupted),
                attempts=self._settings.offpath_retry_attempts,
                base_delay=self._settings.retry_base_delay_ms / 1000.0,
                max_delay=self._settings.retry_max_delay_ms / 1000.0,
            )
            return True
        except Exception as e:
            log.error(
                "failed to persist %s transcript line after retries "
                "(will be missing from scoring): %s — text=%r",
                speaker, e, text,
            )
            return False

    async def on_user_turn_completed(self, turn_ctx, new_message):
        """Called by the framework after each complete user turn.

        Appends the candidate utterance to the transcript, kicks off off-path
        coverage tagging, and checks the end condition.
        """
        text = new_message.text_content or ""
        await self._append_line("candidate", text)
        if self._ending:
            # The goodbye was already said (or is being said). The room may
            # not have actually disconnected yet, so if the candidate speaks
            # again (e.g. "continue"), never let it produce a normal reply —
            # the interview is over for good.
            raise StopResponse()
        # OFF PATH: fire-and-forget — never awaited on the hot path.
        asyncio.create_task(self._coverage.tag(text))
        covered = await self._coverage.covered()
        if should_end(
            covered, self._blob.skills, self._questions_asked, self._blob.max_questions
        ):
            # Don't cut the candidate off mid-answer — this turn may just be
            # one fragment of a longer answer to the last question. Wait for
            # a genuine pause (no new candidate speech) before saying goodbye;
            # every further turn while end-eligible still gets appended above
            # and restarts this wait.
            if self._end_debounce_task is not None:
                self._end_debounce_task.cancel()
            self._end_debounce_task = asyncio.create_task(self._debounced_wrap_up())
            # Suppress the framework's default reply for this turn — otherwise
            # Tara would ask a NEW question instead of waiting to say goodbye.
            raise StopResponse()

    async def _debounced_wrap_up(self):
        """Say goodbye only after a quiet period — see on_user_turn_completed."""
        try:
            await asyncio.sleep(self._settings.end_debounce_seconds)
        except asyncio.CancelledError:
            return  # candidate spoke again before the wait elapsed; a new wait was scheduled
        if self._ending:
            return
        self._ending = True
        await self._wrap_up()

    async def on_tara_line(self, text: str, *, interrupted: bool = False):
        """Append a Tara (assistant) line to the transcript store.

        `text` is already the actually-spoken text and `interrupted` already
        reflects whether it was a barge-in — both come from the same
        conversation-item event, so this single append is authoritative;
        there is no separate later "correct it" step (a prior version had
        one, which raced against this same append as two independent
        fire-and-forget tasks and could clobber the wrong line — see
        TranscriptStore.append).

        Only a line containing "?" counts as a real question asked — a short
        acknowledgment or interrupted fragment (e.g. "That's great" or "That's
        good, moving on") never does. This is the same rule the scorer's Q&A
        pairing uses, so live counting and DB storage agree on what "a
        question" is.
        """
        saved = await self._append_line("tara", text, interrupted=interrupted)
        # Only count it if it's actually recorded — if the write failed even
        # after retries, the scorer will never see this line as a pair
        # boundary either, so the live count must not silently drift ahead
        # of what's really in the transcript.
        if saved and "?" in text:
            self._questions_asked += 1

    def _on_end_and_release(self):
        """Schedules limiter.release (fire-and-scheduled — does not block the
        caller) then signals done via self._on_end().
        The heartbeat TTL backstops release if the task is cut off.
        """
        if self._limiter is not None and self._room:
            asyncio.create_task(self._limiter.release(self._room))
        self._on_end()

    async def _wrap_up(self):
        """Say goodbye, then hand off to the shared (exactly-once) teardown.

        Persistence (assemble transcript, Mongo write, enqueue for scoring)
        and force-ending the room live in the worker's teardown path, NOT
        here — that's the ONE place every end reason (this natural end,
        candidate disconnect, job shutdown, ...) funnels through, so a
        manually- or abnormally-ended interview is saved and scored exactly
        like a naturally-ended one, not silently dropped.
        """
        try:
            await asyncio.wait_for(
                self.session.say(
                    "Thank you, that's the end of the interview. We'll be in touch.",
                    # Scripted farewell, not a real conversational turn — keep
                    # it out of the chat/transcript so the scorer doesn't see
                    # it as a trailing unanswered "question".
                    add_to_chat_ctx=False,
                ),
                timeout=self._settings.say_timeout_seconds,
            )
        except Exception as e:
            log.warning("closing say failed/hung: %s", e)
        self._on_end_and_release()

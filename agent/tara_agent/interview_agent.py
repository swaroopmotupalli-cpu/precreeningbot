"""InterviewAgent — Agent subclass that ties transcript, coverage, and end-decision together.

The agent NEVER scores or classifies on the hot path. Coverage tagging is off-path via
asyncio.create_task (fire-and-forget). Scoring (Task 11) runs after the session ends.
"""
import asyncio
import logging
import re

from livekit.agents import Agent, StopResponse
from tara_agent.coverage import CoverageTracker
from tara_agent.interview import should_end
from tara_agent.transcript import TranscriptStore
from tara_agent.retry import retry_async

log = logging.getLogger("tara.interview_agent")

# A candidate turn matching this AND short enough to plausibly be just a
# clarification request (not a substantive answer that happens to contain
# one of these words, e.g. "let me explain how I implemented...") flags the
# NEXT Tara "?" line as a rephrase of the CURRENT question, not a new one —
# see _expecting_rephrase in on_user_turn_completed / on_tara_line.
_CLARIFICATION_RE = re.compile(
    r"\b(elaborate|clarify|rephrase|explain the question|repeat that|"
    r"repeat the question|say that again|come again|pardon|didn't catch that|"
    r"didnt catch that|didn't understand|didnt understand|"
    r"don't understand the question|dont understand the question|"
    r"what do you mean)\b",
    re.IGNORECASE,
)
# "Let me elaborate/explain..." is the CANDIDATE volunteering more detail on
# their own answer, not asking Tara to clarify the question — must not match.
_SELF_VOLUNTEERED_RE = re.compile(r"\blet me (elaborate|explain)\b", re.IGNORECASE)
# Candidates often wrap the request in a full sentence ("Can you elaborate me
# this question? I mean, repeat the question once again." is 13 words) — a
# cap that's too tight rejects an unambiguous request outright, which is what
# let a rephrase get miscounted as a new question in the first place.
_CLARIFICATION_MAX_WORDS = 25


def _is_clarification_request(text: str) -> bool:
    if not text or len(text.split()) > _CLARIFICATION_MAX_WORDS:
        return False
    if _SELF_VOLUNTEERED_RE.search(text):
        return False
    return bool(_CLARIFICATION_RE.search(text))


# Speech-to-text can mangle the very words a candidate uses to ask for
# clarification (e.g. "can you elaborate" transcribed as "can you allow me"),
# so _is_clarification_request never sees a recognizable request at all. As a
# second, independent signal: a rephrase from Tara herself tends to reuse most
# of the same key words as the question it's rephrasing, whereas a genuinely
# new question (different topic) shares few. Comparing Tara's new "?" line
# directly against her last counted question catches a rephrase even when the
# candidate's own request was never understood.
_REPHRASE_SIMILARITY_THRESHOLD = 0.4
_WORD_RE = re.compile(r"[a-z']+")


def _content_words(text):
    return {w for w in _WORD_RE.findall((text or "").lower()) if len(w) > 3}


def _is_rephrase_of(new_text, previous_text):
    if not previous_text:
        return False
    a, b = _content_words(new_text), _content_words(previous_text)
    if not a or not b:
        return False
    return (len(a & b) / min(len(a), len(b))) >= _REPHRASE_SIMILARITY_THRESHOLD


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
        # True right after the candidate asks Tara to clarify/repeat/elaborate
        # on the current question — the "?" she says next is a rephrase of
        # THAT question, not a new one, and must not bump _questions_asked.
        self._expecting_rephrase = False
        # Text of the last Tara "?" line that WAS counted as a real question —
        # used to detect a rephrase by content similarity when the candidate's
        # own clarification request never reached us as recognizable text.
        self._last_counted_question: str | None = None

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
        is_clarification = _is_clarification_request(text)
        if is_clarification:
            self._expecting_rephrase = True
        if self._ending:
            # The goodbye was already said (or is being said). The room may
            # not have actually disconnected yet, so if the candidate speaks
            # again (e.g. "continue"), never let it produce a normal reply —
            # the interview is over for good.
            raise StopResponse()
        # OFF PATH: fire-and-forget — never awaited on the hot path.
        asyncio.create_task(self._coverage.tag(text))
        if is_clarification:
            # The candidate hasn't actually answered the current question yet
            # — let Tara rephrase it, even if the question cap was already
            # reached. Checking should_end here would wrap up the interview
            # instead of letting her respond (previously: asking to elaborate
            # on the last question got a goodbye instead of a rephrase).
            return
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
        question" is. EXCEPT: if the candidate just asked to clarify/repeat/
        elaborate, this "?" is Tara rephrasing the CURRENT question, not a
        new one — it must not bump the count (previously it did, which could
        exhaust the question cap on a rephrase and cut off the real final
        question before it was ever asked).
        """
        saved = await self._append_line("tara", text, interrupted=interrupted)
        if "?" not in text:
            return
        is_rephrase = self._expecting_rephrase or _is_rephrase_of(text, self._last_counted_question)
        self._expecting_rephrase = False
        if is_rephrase:
            return
        # Only count it if it's actually recorded — if the write failed even
        # after retries, the scorer will never see this line as a pair
        # boundary either, so the live count must not silently drift ahead
        # of what's really in the transcript.
        if saved:
            self._questions_asked += 1
            self._last_counted_question = text

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

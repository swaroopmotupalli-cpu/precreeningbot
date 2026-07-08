# agent/tests/test_interview_agent_interruption.py
import pytest
from livekit.agents import StopResponse
from tara_agent.interview_agent import InterviewAgent


class _FakeTranscript:
    def __init__(self):
        self.lines = []
        self._key = "transcript:room1"
    async def append(self, speaker, text, *, interrupted=False):
        line = {"speaker": speaker, "text": text}
        if interrupted:
            line["interrupted"] = True
        self.lines.append(line)
        return len(self.lines) - 1
    async def assemble(self):
        return list(self.lines)


class _FakeCoverage:
    async def tag(self, text):
        return None
    async def covered(self):
        return set()


class _FakeSettings:
    say_timeout_seconds = 1.0
    mongo_write_timeout_seconds = 1.0
    offpath_retry_attempts = 1
    retry_base_delay_ms = 1
    retry_max_delay_ms = 1
    end_debounce_seconds = 0.01  # keep tests fast


class _RetryableSettings(_FakeSettings):
    offpath_retry_attempts = 3  # enough headroom to prove a transient failure is retried


class _FlakyTranscript(_FakeTranscript):
    """append() fails `fail_times` times (transient), or forever if always_fail."""
    def __init__(self, fail_times=0, always_fail=False):
        super().__init__()
        self._fail_times = fail_times
        self._always_fail = always_fail
        self.append_attempts = 0

    async def append(self, speaker, text, *, interrupted=False):
        self.append_attempts += 1
        if self._always_fail or self.append_attempts <= self._fail_times:
            raise ConnectionError("simulated transient redis blip")
        return await super().append(speaker, text, interrupted=interrupted)


def _agent(transcript, max_questions=1, settings=None, skills=()):
    blob = type("B", (), {"skills": list(skills), "max_questions": max_questions,
        "contest_id": "c", "candidate_id": "u", "recruiter_id": "r", "js_id": "j",
        "resume_text": "", "jd_text": ""})()
    return InterviewAgent(
        instructions="x", blob=blob,
        transcript=transcript, coverage=_FakeCoverage(),
        settings=settings or _FakeSettings(), on_end=lambda: None, limiter=None, room="room1",
    )


async def test_on_tara_line_records_interrupted_flag_inline():
    """The framework hands us the already-truncated text and the interrupted
    flag together on one event — on_tara_line records both in a single
    append, with no separate later correction step (see TranscriptStore.append)."""
    t = _FakeTranscript()
    agent = _agent(t)
    await agent.on_tara_line("Tell me about a time you", interrupted=True)
    assert t.lines[-1] == {"speaker": "tara", "text": "Tell me about a time you", "interrupted": True}


async def test_on_tara_line_defaults_interrupted_to_false():
    t = _FakeTranscript()
    agent = _agent(t)
    await agent.on_tara_line("A complete question?")
    assert "interrupted" not in t.lines[-1]


class _Msg:
    def __init__(self, text):
        self.text_content = text


async def test_hitting_cap_raises_stop_response_but_waits_before_ending():
    """The goodbye is NOT immediate — it waits for a quiet period so a
    candidate still mid-answer isn't cut off."""
    agent = _agent(_FakeTranscript(), max_questions=1)
    with pytest.raises(StopResponse):
        await agent.on_user_turn_completed(None, _Msg("answer 1"))
    assert agent._ending is False
    assert agent._end_debounce_task is not None
    await agent._end_debounce_task  # let the quiet period elapse
    assert agent._ending is True


async def test_turn_after_ending_still_raises_stop_response():
    """Regression: once the goodbye has been said, a later turn (e.g. the
    candidate says "continue" before the room actually disconnects) must
    NOT fall through to a normal reply — it must keep raising StopResponse."""
    agent = _agent(_FakeTranscript(), max_questions=1)
    with pytest.raises(StopResponse):
        await agent.on_user_turn_completed(None, _Msg("answer 1"))
    await agent._end_debounce_task
    assert agent._ending is True
    with pytest.raises(StopResponse):
        await agent.on_user_turn_completed(None, _Msg("continue"))


async def test_continued_answer_resets_the_wait_and_is_still_captured():
    """If the candidate keeps talking after the cap is hit, don't say goodbye
    yet — keep collecting their words into the transcript and wait again."""
    t = _FakeTranscript()
    agent = _agent(t, max_questions=1)
    with pytest.raises(StopResponse):
        await agent.on_user_turn_completed(None, _Msg("first part of the answer"))
    first_task = agent._end_debounce_task
    with pytest.raises(StopResponse):
        await agent.on_user_turn_completed(None, _Msg("still going"))
    assert agent._end_debounce_task is not first_task
    assert agent._ending is False  # goodbye not said yet — waiting again
    await agent._end_debounce_task
    assert agent._ending is True
    assert [l["text"] for l in t.lines] == ["first part of the answer", "still going"]


async def test_on_tara_line_counts_only_real_questions():
    t = _FakeTranscript()
    agent = _agent(t)
    await agent.on_tara_line("That's great, thanks for sharing.")
    assert agent._questions_asked == 0
    await agent.on_tara_line("Could you tell me about your AWS experience?")
    assert agent._questions_asked == 1


async def test_on_tara_line_retries_a_transient_append_failure():
    """Regression: a single dropped Redis write used to silently lose the
    question forever (fire-and-forget, no retry) — the scorer would then
    glue the candidate's next answer onto the PREVIOUS question instead."""
    t = _FlakyTranscript(fail_times=2)  # fails twice, succeeds on the 3rd try
    agent = _agent(t, settings=_RetryableSettings())
    await agent.on_tara_line("Could you tell me about your AWS experience?")
    assert t.lines[-1]["text"] == "Could you tell me about your AWS experience?"
    assert agent._questions_asked == 1


async def test_on_tara_line_does_not_count_a_question_that_never_saved():
    t = _FlakyTranscript(always_fail=True)
    agent = _agent(t, settings=_RetryableSettings())
    await agent.on_tara_line("Could you tell me about your AWS experience?")  # must not raise
    assert agent._questions_asked == 0  # never actually recorded — must not count


async def test_on_user_turn_completed_survives_a_permanent_append_failure():
    """A permanently failing transcript write must not crash the live turn —
    it should log and let the interview continue rather than take down the session."""
    t = _FlakyTranscript(always_fail=True)
    # Non-empty, uncovered skill + high cap so should_end() stays False here —
    # this test is only exercising append-failure resilience, not the ending path.
    agent = _agent(t, max_questions=100, settings=_RetryableSettings(), skills=["python"])
    await agent.on_user_turn_completed(None, _Msg("answer 1"))  # must not raise

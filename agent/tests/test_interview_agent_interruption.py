# agent/tests/test_interview_agent_interruption.py
import pytest
from livekit.agents import StopResponse
from tara_agent.interview_agent import InterviewAgent, _is_clarification_request


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
    max_questions = 12


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
    blob = type("B", (), {"skills": list(skills),
        "contest_id": "c", "candidate_id": "u", "recruiter_id": "r", "js_id": "j",
        "resume_text": "", "jd_text": ""})()
    s = settings or _FakeSettings()
    s.max_questions = max_questions  # should_end() reads the cap from settings, not the blob
    return InterviewAgent(
        instructions="x", blob=blob,
        transcript=transcript, coverage=_FakeCoverage(),
        settings=s, on_end=lambda: None, limiter=None, room="room1",
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
    agent = _agent(_FakeTranscript(), max_questions=0)  # cap already reached — every turn hits it
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
    agent = _agent(_FakeTranscript(), max_questions=0)  # cap already reached — every turn hits it
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
    agent = _agent(t, max_questions=0)  # cap already reached — every turn hits it
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


def test_is_clarification_request_matches_short_asks_for_elaboration():
    assert _is_clarification_request("Can you elaborate on that?") is True
    assert _is_clarification_request("Sorry, pardon?") is True
    assert _is_clarification_request("What do you mean by scalability") is True
    assert _is_clarification_request("Could you please repeat that question again for me") is True


def test_is_clarification_request_does_not_match_substantive_answers():
    # Long answer that happens to contain "explain" — must NOT be flagged,
    # or a real new question asked right after would be wrongly skipped.
    long_answer = (
        "Let me explain how I implemented the caching layer using Redis "
        "and handled invalidation across multiple services in production"
    )
    assert _is_clarification_request(long_answer) is False
    assert _is_clarification_request("I am not sure, but I think it scales well") is False
    assert _is_clarification_request("") is False


async def test_rephrase_after_clarification_request_is_not_counted_as_a_new_question():
    """Regression: asking Tara to elaborate on the CURRENT question used to
    bump the question count as if it were a new question — exhausting the
    cap on a rephrase and cutting off the real final question before it was
    ever asked."""
    t = _FakeTranscript()
    # Non-empty, uncovered skill so should_end() doesn't trip on the first
    # turn — this test is only exercising the rephrase-vs-new-question logic.
    agent = _agent(t, max_questions=4, skills=["aws"])
    await agent.on_tara_line("Could you tell me about your AWS experience?")
    assert agent._questions_asked == 1
    await agent.on_user_turn_completed(None, _Msg("Can you elaborate on that?"))
    assert agent._expecting_rephrase is True
    await agent.on_tara_line("Sure — I mean, how have you used AWS services in production?")
    assert agent._questions_asked == 1  # rephrase of Q1, not a new question
    assert agent._expecting_rephrase is False
    # A genuinely new question afterward still counts normally.
    await agent.on_tara_line("Moving on, how do you handle CI/CD?")
    assert agent._questions_asked == 2


def test_is_clarification_request_matches_a_longer_wrapped_request():
    """Regression: a real request wrapped in a fuller sentence ("Can you
    elaborate me this question? I mean, repeat the question once again." —
    13 words) used to be rejected by a too-tight word cap, silently letting
    the rephrase that followed get miscounted as a new question."""
    assert _is_clarification_request(
        "Can you elaborate me this question? I mean, repeat the question once again."
    ) is True


def test_is_clarification_request_excludes_self_volunteered_elaboration():
    # The candidate elaborating on THEIR OWN answer, unprompted, must not be
    # treated as asking Tara to clarify the question.
    assert _is_clarification_request(
        "Let me elaborate on how I designed the caching layer"
    ) is False


async def test_clarification_on_the_final_question_still_gets_a_rephrase_not_a_goodbye():
    """Regression: once the question cap was reached, a clarification request
    on that last question used to get a goodbye instead of an actual
    rephrase — should_end() was checked before the clarification check."""
    t = _FakeTranscript()
    agent = _agent(t, max_questions=1, skills=["aws"])
    await agent.on_tara_line("Could you tell me about your AWS experience?")
    assert agent._questions_asked == 1  # cap already reached
    # Must NOT raise StopResponse / schedule the wrap-up — Tara should reply.
    await agent.on_user_turn_completed(None, _Msg("Sorry, can you elaborate on that question again?"))
    assert agent._expecting_rephrase is True
    assert agent._end_debounce_task is None
    assert agent._ending is False


async def test_rephrase_detected_by_similarity_even_when_request_is_unrecognizable():
    """Regression: STT can mangle the candidate's actual words beyond
    recognition (e.g. "elaborate" heard as "allow me"), so the phrase-based
    detector never fires. Tara's rephrase still reuses most of the same key
    words as the question it's rephrasing, which this catches independently."""
    t = _FakeTranscript()
    agent = _agent(t, max_questions=4, skills=["aws"])
    original = (
        "Since you mentioned using Redux Toolkit, how do you go about defining "
        "the types or interfaces for your React component props to ensure type safety?"
    )
    await agent.on_tara_line(original)
    assert agent._questions_asked == 1
    # STT garbled the clarification request into something unrecognizable —
    # _expecting_rephrase never gets set.
    await agent.on_user_turn_completed(None, _Msg("Can you allow me? Let me regarding this question once again"))
    assert agent._expecting_rephrase is False
    rephrase = (
        "No problem. When you are building React components with TypeScript, how do "
        "you go about defining the types or interfaces for your props to make sure "
        "everything stays type-safe?"
    )
    await agent.on_tara_line(rephrase)
    assert agent._questions_asked == 1  # still recognized as a rephrase, by content
    # A genuinely new question (different topic) is not caught by similarity.
    await agent.on_tara_line("Moving on, how do you structure your Express routes?")
    assert agent._questions_asked == 2


async def test_generic_interview_phrasing_does_not_cause_a_false_rephrase_match():
    """Regression: two UNRELATED questions (MongoDB schema design, then a
    later GraphQL question) share only generic interview boilerplate ("have",
    "working", "with", "your", "projects") — that shared filler pushed
    similarity over threshold and silently dropped the GraphQL question from
    the count, even though the candidate gave it a real, distinct answer."""
    t = _FakeTranscript()
    agent = _agent(t, max_questions=12, skills=["mongodb", "graphql"])
    await agent.on_tara_line(
        "How have you approached schema design when working with MongoDB in your previous projects?"
    )
    assert agent._questions_asked == 1
    await agent.on_tara_line(
        "Have you had experience working with GraphQL for data fetching in any of your projects?"
    )
    assert agent._questions_asked == 2  # a genuinely new topic — must still count


async def test_taras_own_concluding_statement_ends_the_session_without_a_second_goodbye():
    """Regression: the LLM can decide to wrap up on its own judgment before
    the code ever reaches the question cap — previously nothing reacted to
    this, so the session just sat open until the candidate's client
    eventually disconnected on its own."""
    ended = {"v": False}
    t = _FakeTranscript()
    blob = type("B", (), {"skills": ["python"],
        "contest_id": "c", "candidate_id": "u", "recruiter_id": "r", "js_id": "j",
        "resume_text": "", "jd_text": ""})()
    settings = _FakeSettings()
    settings.max_questions = 12
    agent = InterviewAgent(
        instructions="x", blob=blob, transcript=t, coverage=_FakeCoverage(),
        settings=settings, on_end=lambda: ended.__setitem__("v", True), limiter=None, room="room1",
    )
    await agent.on_tara_line("Could you tell me about your Python experience?")
    assert agent._questions_asked == 1
    await agent.on_tara_line(
        "Thank you for your time today. This concludes our technical interview session. Have a great day."
    )
    assert agent._ending is True
    assert ended["v"] is True


async def test_ordinary_acknowledgment_does_not_trigger_a_premature_ending():
    """The concluding-statement detector must stay narrow — an everyday
    mid-interview acknowledgment must never be mistaken for Tara's goodbye."""
    t = _FakeTranscript()
    agent = _agent(t, max_questions=12, skills=["python"])
    await agent.on_tara_line("Could you tell me about your Python experience?")
    await agent.on_tara_line("Thank you for sharing that, moving on.")
    assert agent._ending is False

# agent/tests/test_interview_agent_interruption.py
import pytest
from tara_agent.interview_agent import InterviewAgent


class _FakeTranscript:
    def __init__(self):
        self.lines = []
        self._key = "transcript:room1"
    async def append(self, speaker, text):
        self.lines.append({"speaker": speaker, "text": text})
        return len(self.lines) - 1
    async def truncate_last(self, speaker, spoken_text):
        for i in range(len(self.lines) - 1, -1, -1):
            if self.lines[i]["speaker"] == speaker:
                self.lines[i]["text"] = spoken_text
                return True
        return False


def _agent(transcript):
    # Construct with only what on_interruption needs; other deps unused here.
    return InterviewAgent(
        instructions="x", blob=type("B", (), {"skills": [], "max_questions": 1,
            "contest_id": "c", "candidate_id": "u"})(),
        transcript=transcript, coverage=None, mongo_write_fn=None,
        settings=type("S", (), {})(), on_end=lambda: None, limiter=None, room="room1",
    )


async def test_on_interruption_truncates_last_tara_line():
    t = _FakeTranscript()
    await t.append("tara", "Tell me about a time you scaled a system to handle")
    agent = _agent(t)
    await agent.on_interruption("Tell me about a time you")
    assert t.lines[-1]["text"] == "Tell me about a time you"  # only what was spoken


async def test_on_interruption_no_tara_line_is_safe():
    t = _FakeTranscript()
    await t.append("candidate", "hello")
    agent = _agent(t)
    await agent.on_interruption("anything")  # must not raise
    assert t.lines[-1]["text"] == "hello"

# agent/tests/test_lifecycle.py
import asyncio
import pytest
from tara_agent.lifecycle import SessionLifecycle


def _make(grace, sleep):
    events = {"resume": 0, "teardown": []}
    async def on_resume():
        events["resume"] += 1
    async def on_teardown(reason):
        events["teardown"].append(reason)
    lc = SessionLifecycle(
        candidate_id_getter=lambda: "cand",
        grace_seconds=grace, on_resume=on_resume, on_teardown=on_teardown,
        sleep=sleep,
    )
    return lc, events


async def test_candidate_leave_starts_grace_no_immediate_teardown():
    gate = asyncio.Event()
    async def sleep(_):
        await gate.wait()  # hold the grace timer open
    lc, ev = _make(25, sleep)
    lc.note_candidate("cand")
    await lc.participant_left("cand")
    await asyncio.sleep(0)
    assert ev["teardown"] == []  # NOT torn down within grace


async def test_rejoin_within_grace_resumes_and_cancels():
    gate = asyncio.Event()
    async def sleep(_):
        await gate.wait()
    lc, ev = _make(25, sleep)
    lc.note_candidate("cand")
    await lc.participant_left("cand")
    await lc.participant_joined("cand")
    await asyncio.sleep(0)
    assert ev["resume"] == 1
    assert ev["teardown"] == []   # rejoin cancelled the teardown


async def test_grace_expiry_tears_down_once():
    async def sleep(_):
        return  # grace elapses immediately
    lc, ev = _make(0, sleep)
    lc.note_candidate("cand")
    await lc.participant_left("cand")
    await asyncio.sleep(0)
    assert ev["teardown"] == ["reconnect_grace_expiry"]


async def test_non_candidate_leave_ignored():
    async def sleep(_):
        return
    lc, ev = _make(0, sleep)
    lc.note_candidate("cand")
    await lc.participant_left("observer")
    await asyncio.sleep(0)
    assert ev["teardown"] == []   # a different participant must not tear down


async def test_teardown_is_exactly_once():
    async def sleep(_):
        return
    lc, ev = _make(0, sleep)
    await lc.teardown("path_a")
    await lc.teardown("path_b")
    assert ev["teardown"] == ["path_a"]  # second call is a no-op

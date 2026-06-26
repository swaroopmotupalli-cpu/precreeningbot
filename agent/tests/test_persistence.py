import asyncio
import fakeredis.aioredis
import pytest
from tara_agent.transcript import TranscriptStore
from tara_agent.persistence import end_interview

@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

async def _run(redis, say_fn, mongo_write_fn, finally_flag, **over):
    ts = TranscriptStore(redis, "r")
    await ts.append("tara", "q1"); await ts.append("candidate", "a1")
    kw = dict(say_fn=say_fn, transcript_store=ts, mongo_write_fn=mongo_write_fn,
              room="r", contest_id="c", candidate_id="u",
              say_timeout=0.2, write_timeout=0.2,
              on_finally=lambda: finally_flag.append(True))
    kw.update(over)
    await end_interview(**kw)

async def test_happy_path_writes_ordered_transcript(redis):
    written = {}
    async def say(): return None
    async def mongo(doc): written.update(doc)
    flag = []
    await _run(redis, say, mongo, flag)
    assert [l["seq"] for l in written["transcript"]] == [0, 1]
    assert written["contestId"] == "c" and written["candidateId"] == "u"
    assert flag == [True]

async def test_hung_say_still_runs_finally(redis):
    async def say(): await asyncio.sleep(10)        # hang
    async def mongo(doc): return None
    flag = []
    await _run(redis, say, mongo, flag)             # must NOT hang (timeout 0.2s)
    assert flag == [True]

async def test_hung_mongo_still_runs_finally(redis):
    async def say(): return None
    async def mongo(doc): await asyncio.sleep(10)   # hang
    flag = []
    await _run(redis, say, mongo, flag)
    assert flag == [True]

async def test_throwing_mongo_still_runs_finally(redis):
    async def say(): return None
    async def mongo(doc): raise RuntimeError("db down")
    flag = []
    await _run(redis, say, mongo, flag)
    assert flag == [True]


async def test_mongo_write_retries_transient():
    calls = {"n": 0}
    async def mongo_write(doc):
        calls["n"] += 1
        if calls["n"] < 2:
            raise ConnectionError("temporarily unavailable")
        return {"ok": 1}

    class _T:
        async def assemble(self):
            return [{"seq": 0, "speaker": "tara", "text": "hi"}]

    finally_called = {"v": False}
    await end_interview(
        say_fn=lambda: asyncio.sleep(0),
        transcript_store=_T(),
        mongo_write_fn=mongo_write,
        room="r", contest_id="c", candidate_id="u",
        say_timeout=1.0, write_timeout=1.0,
        on_finally=lambda: finally_called.__setitem__("v", True),
        offpath_retry_attempts=4, retry_base_delay_ms=1, retry_max_delay_ms=2,
    )
    assert calls["n"] == 2       # retried the transient write once
    assert finally_called["v"]   # finally still runs exactly once

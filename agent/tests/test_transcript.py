import fakeredis.aioredis
import pytest
from tara_agent.transcript import TranscriptStore

@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

async def test_append_assigns_monotonic_seq_and_orders(redis):
    ts = TranscriptStore(redis, room="r1")
    assert await ts.append("tara", "hello") == 0
    assert await ts.append("candidate", "hi") == 1
    assert await ts.append("tara", "next?") == 2
    lines = await ts.assemble()
    assert [l["seq"] for l in lines] == [0, 1, 2]
    assert [l["speaker"] for l in lines] == ["tara", "candidate", "tara"]
    assert all("ts" in l for l in lines)

async def test_assemble_sorts_even_if_storage_unordered(redis):
    ts = TranscriptStore(redis, room="r2")
    await redis.rpush("transcript:r2", '{"seq": 5, "speaker": "tara", "text": "b", "ts": 2}')
    await redis.rpush("transcript:r2", '{"seq": 1, "speaker": "candidate", "text": "a", "ts": 1}')
    lines = await ts.assemble()
    assert [l["seq"] for l in lines] == [1, 5]

async def test_append_with_interrupted_flags_the_line_inline(redis):
    """Interrupted state is set in the SAME append the caller already knows
    it for — no separate later correction step (a prior version used a
    second `truncate_last` call for this, which raced against the original
    append as two independent fire-and-forget tasks and could clobber the
    wrong, previous line — see interview_agent.on_tara_line)."""
    ts = TranscriptStore(redis, room="r6")
    await ts.append("tara", "Could you explain how you would specifically handle", interrupted=True)
    lines = await ts.assemble()
    assert lines[-1]["interrupted"] is True

async def test_append_does_not_set_interrupted_flag_by_default(redis):
    ts = TranscriptStore(redis, room="r7")
    await ts.append("tara", "A complete question?")
    lines = await ts.assemble()
    assert "interrupted" not in lines[-1]

async def test_ttl_is_set(redis):
    ts = TranscriptStore(redis, room="r4", ttl_seconds=7200)
    await ts.append("tara", "x")
    assert 0 < await redis.ttl("transcript:r4") <= 7200

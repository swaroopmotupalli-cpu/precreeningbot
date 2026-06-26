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

async def test_truncate_last_replaces_partial_line(redis):
    ts = TranscriptStore(redis, room="r3")
    await ts.append("candidate", "answer")
    await ts.append("tara", "This is a very long question that was")
    result = await ts.truncate_last("tara", "This is a very long question")
    assert result is True
    lines = await ts.assemble()
    assert lines[-1]["text"] == "This is a very long question"
    assert len(lines) == 2

async def test_truncate_last_returns_false_when_speaker_absent(redis):
    ts = TranscriptStore(redis, room="r5")
    await ts.append("candidate", "answer")
    result = await ts.truncate_last("tara", "x")
    assert result is False
    lines = await ts.assemble()
    assert len(lines) == 1
    assert lines[0]["speaker"] == "candidate"

async def test_ttl_is_set(redis):
    ts = TranscriptStore(redis, room="r4", ttl_seconds=7200)
    await ts.append("tara", "x")
    assert 0 < await redis.ttl("transcript:r4") <= 7200

import json
import fakeredis.aioredis
import pytest
from tara_agent.session_store import load_session, SessionBlob, SessionNotFound

@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

async def test_load_session_parses_blob(redis):
    await redis.set("session:s1", json.dumps({
        "contestId": "c1", "candidateId": "u1", "skills": ["python", "sql"],
        "resumeText": "resume", "jdText": "jd", "maxQuestions": 10,
    }))
    blob = await load_session(redis, "s1")
    assert blob == SessionBlob("c1", "u1", ["python", "sql"], "resume", "jd", 10)

async def test_missing_session_raises(redis):
    with pytest.raises(SessionNotFound):
        await load_session(redis, "nope")

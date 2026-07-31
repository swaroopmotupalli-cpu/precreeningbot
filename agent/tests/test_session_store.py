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
        "resumeText": "resume", "jdText": "jd",
    }))
    blob = await load_session(redis, "s1")
    assert blob == SessionBlob("c1", "u1", ["python", "sql"], "resume", "jd")

async def test_missing_session_raises(redis):
    with pytest.raises(SessionNotFound):
        await load_session(redis, "nope")

async def test_blob_carries_recruiter_and_js_ids(redis):
    await redis.set("session:s1", json.dumps({
        "contestId": "c1", "candidateId": "u1", "skills": ["Python"],
        "resumeText": "r", "jdText": "j",
        "recruiterId": "rec1", "jsId": "js1",
    }))
    blob = await load_session(redis, "s1")
    assert blob.recruiter_id == "rec1"
    assert blob.js_id == "js1"

async def test_blob_defaults_good_to_have_skills_to_empty(redis):
    await redis.set("session:s1", json.dumps({
        "contestId": "c1", "candidateId": "u1", "skills": ["Python"],
        "resumeText": "r", "jdText": "j",
    }))
    blob = await load_session(redis, "s1")
    assert blob.good_to_have_skills == []

async def test_blob_carries_good_to_have_skills(redis):
    await redis.set("session:s1", json.dumps({
        "contestId": "c1", "candidateId": "u1", "skills": ["Python"],
        "resumeText": "r", "jdText": "j",
        "goodToHaveSkills": ["Docker", "Kubernetes"],
    }))
    blob = await load_session(redis, "s1")
    assert blob.good_to_have_skills == ["Docker", "Kubernetes"]

async def test_blob_carries_candidate_name_and_job_title(redis):
    await redis.set("session:s1", json.dumps({
        "contestId": "c1", "candidateId": "u1", "skills": ["Python"],
        "resumeText": "r", "jdText": "j",
        "candidateName": "Ada Lovelace", "jobTitle": "Backend Engineer",
    }))
    blob = await load_session(redis, "s1")
    assert blob.candidate_name == "Ada Lovelace"
    assert blob.job_title == "Backend Engineer"

async def test_blob_defaults_candidate_name_and_job_title_to_empty(redis):
    await redis.set("session:s1", json.dumps({
        "contestId": "c1", "candidateId": "u1", "skills": ["Python"],
        "resumeText": "r", "jdText": "j",
    }))
    blob = await load_session(redis, "s1")
    assert blob.candidate_name == ""
    assert blob.job_title == ""

import fakeredis.aioredis
import pytest
from tara_agent.coverage import CoverageTracker

@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

async def test_tag_adds_matched_skills(redis):
    async def fake_classify(prompt): return "Python, SQL"
    t = CoverageTracker(redis, "r1", ["Python", "SQL", "AWS"], fake_classify)
    await t.tag("I built ETL with pandas and Postgres")
    assert await t.covered() == {"Python", "SQL"}

async def test_tag_none_adds_nothing(redis):
    async def fake_classify(prompt): return "none"
    t = CoverageTracker(redis, "r2", ["Python"], fake_classify)
    await t.tag("I like coffee")
    assert await t.covered() == set()

async def test_tag_ignores_skills_not_in_required(redis):
    async def fake_classify(prompt): return "Python, Rust"
    t = CoverageTracker(redis, "r3", ["Python"], fake_classify)
    await t.tag("...")
    assert await t.covered() == {"Python"}

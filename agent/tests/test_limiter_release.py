# agent/tests/test_limiter_release.py
import pytest
from tara_agent.limiter import Limiter, Caps

def make(redis):
    return Limiter(redis, caps=Caps(5,5,5,5), reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_release_decrements_all_buckets(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 1
    assert await lim.release("r1") is True
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 0
    assert await real_redis.zscore("{tara-limiter}:reservations", "r1") is None
    assert await real_redis.exists("{tara-limiter}:res:r1") == 0

async def test_release_is_idempotent_no_negative_counts(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    assert await lim.release("r1") is True
    assert await lim.release("r1") is False            # already gone
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 0   # not -1

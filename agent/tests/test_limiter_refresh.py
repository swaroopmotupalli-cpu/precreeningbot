# agent/tests/test_limiter_refresh.py
import pytest
from tara_agent.limiter import Limiter, Caps

def make(redis):
    return Limiter(redis, caps=Caps(5,5,5,5), reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_heartbeat_extends_lease_and_sets_flags(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    ok = await lim.heartbeat("r1", now_ms=2000, mark_heartbeat=True, mark_participant=True)
    assert ok is True
    score = await real_redis.zscore("{tara-limiter}:reservations", "r1")
    assert score == 2000 + 60000                       # Tier-2 ttl applied
    assert await real_redis.hget("{tara-limiter}:res:r1", "heartbeat_ever") == "1"
    assert await real_redis.hget("{tara-limiter}:res:r1", "participant_joined") == "1"

async def test_heartbeat_missing_reservation_returns_false(real_redis):
    lim = make(real_redis)
    assert await lim.heartbeat("ghost", now_ms=2000) is False

async def test_reconnect_via_admit_refreshes_without_double_count(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    again = await lim.try_admit("r1", now_ms=5000)     # same room = rejoin
    assert again.admitted and again.state == "refreshed"
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 1   # NOT 2

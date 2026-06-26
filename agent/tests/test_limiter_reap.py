import pytest
from tara_agent.limiter import Limiter, Caps, Reclaim

def make(redis):
    return Limiter(redis, caps=Caps(5,5,5,5), reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_reap_noshow_frees_slot_and_classifies(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)             # lease expires at 46000
    out = await lim.reap(now_ms=46001)                 # past Tier-1 ttl, no heartbeat
    assert out == [Reclaim("r1", "no_show")]
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 0
    assert int(await real_redis.get("{tara-limiter}:metric:reclaim:no_show")) == 1

async def test_reap_crash_when_heartbeat_seen_no_participant(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    await lim.heartbeat("r1", now_ms=1000, mark_heartbeat=True)   # worker took over, lease->61000
    out = await lim.reap(now_ms=61001)
    assert out == [Reclaim("r1", "crash")]
    assert int(await real_redis.get("{tara-limiter}:metric:reclaim:crash")) == 1

async def test_reap_false_reclaim_when_participant_present(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    await lim.heartbeat("r1", now_ms=1000, mark_heartbeat=True, mark_participant=True)
    out = await lim.reap(now_ms=61001)                 # lapsed while participant present
    assert out == [Reclaim("r1", "false")]             # the HARD-GATE signal
    assert int(await real_redis.get("{tara-limiter}:metric:reclaim:false")) == 1

async def test_reap_leaves_live_reservations(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)             # expires 46000
    out = await lim.reap(now_ms=2000)                  # still live
    assert out == []
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 1

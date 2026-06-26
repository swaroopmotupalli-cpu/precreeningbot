# agent/tests/test_limiter_lifecycle.py
import asyncio
import pytest
from tara_agent.limiter import Limiter, Caps, Reclaim

def make(redis, cap=3):
    return Limiter(redis, caps=Caps(cap, cap, cap, cap),
                   reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_cap_holds_then_noshow_reclaim_frees_a_slot(real_redis):
    lim = make(real_redis, cap=2)
    assert (await lim.try_admit("a", 1000)).admitted
    assert (await lim.try_admit("b", 2000)).admitted             # b admitted later (expires at 47000)
    assert not (await lim.try_admit("c", 2001)).admitted          # cap holds
    # 'a' is a no-show; next admit after its lease lapses reaps it inline and succeeds
    c2 = await lim.try_admit("c", now_ms=46001)
    assert c2.admitted                                            # slot freed by inline reap
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 2  # b and c remain

async def test_crash_vs_false_reclaim_distinguished(real_redis):
    lim = make(real_redis, cap=5)
    await lim.try_admit("crasher", 1000)
    await lim.heartbeat("crasher", 1000, mark_heartbeat=True)      # worker alive, no participant flag
    await lim.try_admit("dropped", 1000)
    await lim.heartbeat("dropped", 1000, mark_heartbeat=True, mark_participant=True)
    out = {r.room: r.reason for r in await lim.reap(now_ms=61001)}
    assert len(out) == 2                                           # exactly the two expired reservations
    assert "no_show" not in out.values()                           # both had heartbeats, neither misclassified
    assert out["crasher"] == "crash"
    assert out["dropped"] == "false"                              # hard-gate signal

async def test_concurrent_admits_never_exceed_cap(real_redis):
    lim = make(real_redis, cap=10)
    res = await asyncio.gather(*[lim.try_admit(f"r{i}", 1000) for i in range(100)])
    assert sum(1 for x in res if x.admitted) == 10
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 10

async def test_full_lifecycle_admit_heartbeat_release(real_redis):
    lim = make(real_redis, cap=1)
    assert (await lim.try_admit("r", 1000)).admitted
    assert await lim.heartbeat("r", 2000, mark_heartbeat=True, mark_participant=True)
    assert await lim.release("r") is True
    assert (await lim.try_admit("r2", 3000)).admitted             # slot returned on clean release
    m = await lim.metrics()
    assert m["reclaim_false"] == 0                                # clean release != reclaim

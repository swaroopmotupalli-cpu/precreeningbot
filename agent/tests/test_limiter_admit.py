# agent/tests/test_limiter_admit.py
import asyncio
import pytest
from tara_agent.limiter import Limiter, Caps

def make(redis, **over):
    caps = over.pop("caps", Caps(global_=3, gemini_tpm=3, stt_streams=3, tts_streams=3))
    return Limiter(redis, caps=caps, reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_admit_until_cap_then_reject(real_redis):
    lim = make(real_redis, caps=Caps(2, 2, 2, 2))
    a = await lim.try_admit("r1", now_ms=1000); assert a.admitted and a.state == "new"
    b = await lim.try_admit("r2", now_ms=1000); assert b.admitted
    c = await lim.try_admit("r3", now_ms=1000)
    assert not c.admitted and c.bucket == "global"   # global cap 2 hit first

async def test_reject_names_tightest_bucket(real_redis):
    # gemini cap is the smallest -> it should be the named bucket
    lim = make(real_redis, caps=Caps(global_=10, gemini_tpm=1, stt_streams=10, tts_streams=10))
    assert (await lim.try_admit("r1", now_ms=1000)).admitted
    r = await lim.try_admit("r2", now_ms=1000)
    assert not r.admitted and r.bucket == "gemini_tpm"

async def test_atomic_never_exceeds_cap_under_concurrency(real_redis):
    lim = make(real_redis, caps=Caps(5, 5, 5, 5))
    results = await asyncio.gather(*[
        lim.try_admit(f"room{i}", now_ms=1000) for i in range(50)
    ])
    admitted = [x for x in results if x.admitted]
    assert len(admitted) == 5                       # exactly cap, never more
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 5

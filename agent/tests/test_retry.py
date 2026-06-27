import asyncio
import pytest
from tara_agent.retry import retry_async, is_transient


class Boom(Exception):
    pass


def test_is_transient_classifies():
    assert is_transient(asyncio.TimeoutError())
    assert is_transient(ConnectionResetError())
    assert is_transient(Exception("503 Service Unavailable"))
    assert is_transient(Exception("deadline exceeded"))
    assert not is_transient(ValueError("bad input"))
    assert not is_transient(Exception("401 Unauthorized"))


async def test_retries_then_succeeds():
    calls = {"n": 0}
    async def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise asyncio.TimeoutError()
        return "ok"
    sleeps = []
    out = await retry_async(
        fn, attempts=4, base_delay=0.05, max_delay=0.4,
        sleep=lambda d: sleeps.append(d) or asyncio.sleep(0),
        rng=lambda: 1.0,
    )
    assert out == "ok"
    assert calls["n"] == 3
    assert len(sleeps) == 2  # slept before attempt 2 and 3


async def test_gives_up_after_attempts():
    calls = {"n": 0}
    async def fn():
        calls["n"] += 1
        raise asyncio.TimeoutError()
    with pytest.raises(asyncio.TimeoutError):
        await retry_async(fn, attempts=2, base_delay=0.01, max_delay=0.1,
                          sleep=lambda d: asyncio.sleep(0), rng=lambda: 1.0)
    assert calls["n"] == 2  # 1 try + 1 retry


async def test_non_transient_not_retried():
    calls = {"n": 0}
    async def fn():
        calls["n"] += 1
        raise ValueError("4xx")
    with pytest.raises(ValueError):
        await retry_async(fn, attempts=5, base_delay=0.01, max_delay=0.1,
                          sleep=lambda d: asyncio.sleep(0), rng=lambda: 1.0)
    assert calls["n"] == 1  # never retried


async def test_jitter_within_range():
    async def fn():
        raise asyncio.TimeoutError()
    sleeps = []
    with pytest.raises(asyncio.TimeoutError):
        await retry_async(fn, attempts=3, base_delay=0.1, max_delay=10.0,
                          sleep=lambda d: sleeps.append(d) or asyncio.sleep(0),
                          rng=lambda: 0.0)  # jitter factor 0.5 (min)
    # base 0.1 * 2**0 = 0.1 → *0.5 = 0.05 ; base*2**1=0.2 → *0.5 = 0.10
    assert sleeps == pytest.approx([0.05, 0.10])

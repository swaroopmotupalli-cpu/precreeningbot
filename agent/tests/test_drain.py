# agent/tests/test_drain.py
import asyncio
import pytest
from tara_agent.drain import DrainController


async def test_register_unregister_active_count():
    d = DrainController()
    assert d.active() == 0
    d.register(); d.register()
    assert d.active() == 2
    d.unregister()
    assert d.active() == 1


async def test_request_drain_sets_flag():
    d = DrainController()
    assert not d.is_draining()
    d.request_drain()
    assert d.is_draining()


async def test_wait_drained_returns_when_zero():
    d = DrainController()
    d.register()
    async def drop_soon():
        await asyncio.sleep(0)
        d.unregister()
    asyncio.create_task(drop_soon())
    ok = await d.wait_drained(poll=0.001, timeout=1.0)
    assert ok is True


async def test_wait_drained_times_out_if_stuck():
    d = DrainController()
    d.register()  # never unregistered
    ok = await d.wait_drained(poll=0.001, timeout=0.01)
    assert ok is False

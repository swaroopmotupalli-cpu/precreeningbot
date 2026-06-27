# agent/tara_agent/lifecycle.py
"""Reconnect grace window + exactly-once teardown for one interview session.

A candidate disconnect starts a grace timer (strictly < the Tier-1 lease) instead
of ending the interview. A rejoin within the window resumes; expiry tears down.
Every exit path funnels through teardown(), which fires on_teardown AT MOST ONCE.
"""
from __future__ import annotations
import asyncio
from typing import Awaitable, Callable


class SessionLifecycle:
    def __init__(
        self,
        *,
        candidate_id_getter: Callable[[], str | None],
        grace_seconds: float,
        on_resume: Callable[[], Awaitable[None]],
        on_teardown: Callable[[str], Awaitable[None]],
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self._candidate_id_getter = candidate_id_getter
        self._grace = grace_seconds
        self._on_resume = on_resume
        self._on_teardown = on_teardown
        self._sleep = sleep
        self._candidate_id: str | None = None
        self._grace_task: asyncio.Task | None = None
        self._torn_down = False

    def note_candidate(self, identity: str) -> None:
        if self._candidate_id is None:
            self._candidate_id = identity

    def _is_candidate(self, identity: str) -> bool:
        cid = self._candidate_id or self._candidate_id_getter()
        return cid is not None and identity == cid

    async def participant_left(self, identity: str) -> None:
        if self._torn_down or not self._is_candidate(identity):
            return
        if self._grace_task is None or self._grace_task.done():
            self._grace_task = asyncio.create_task(self._grace_then_teardown())

    async def _grace_then_teardown(self) -> None:
        try:
            await self._sleep(self._grace)
        except asyncio.CancelledError:
            return
        await self.teardown("reconnect_grace_expiry")

    async def participant_joined(self, identity: str) -> None:
        if not self._is_candidate(identity):
            return
        if self._grace_task is not None and not self._grace_task.done():
            self._grace_task.cancel()
            self._grace_task = None
            await self._on_resume()

    async def teardown(self, reason: str) -> None:
        if self._torn_down:
            return
        self._torn_down = True
        if self._grace_task is not None and not self._grace_task.done():
            self._grace_task.cancel()
        await self._on_teardown(reason)

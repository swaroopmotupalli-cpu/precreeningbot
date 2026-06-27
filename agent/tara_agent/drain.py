# agent/tara_agent/drain.py
"""Process-level drain controller. On SIGTERM the worker stops accepting new
jobs and waits for in-flight interviews to finish before exiting — so scale-down
DRAINS instead of evicting live sessions. One instance per worker process."""
from __future__ import annotations
import asyncio
import time
from typing import Awaitable, Callable


class DrainController:
    def __init__(self):
        self._active = 0
        self._draining = False

    def register(self) -> None:
        self._active += 1

    def unregister(self) -> None:
        if self._active > 0:
            self._active -= 1

    def request_drain(self) -> None:
        self._draining = True

    def is_draining(self) -> bool:
        return self._draining

    def active(self) -> int:
        return self._active

    async def wait_drained(
        self, *, poll: float = 0.2, timeout: float | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> bool:
        start = time.monotonic()
        while self._active > 0:
            if timeout is not None and (time.monotonic() - start) >= timeout:
                return False
            await sleep(poll)
        return True

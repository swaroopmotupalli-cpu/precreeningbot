"""Bounded retry with jittered exponential backoff.

Used on the calls Tara CONTROLS: the greeting generate_reply (hot path, 1 retry)
and the off-path coverage tagger + Mongo write (more generous). In-stream
STT/LLM/TTS retries are the LiveKit plugins' own concern — not wrapped here.
"""
from __future__ import annotations
import asyncio
import random
from typing import Any, Awaitable, Callable

_TRANSIENT_MARKERS = (
    "unavailable", "deadline", "timeout", "timed out", "reset",
    "temporarily", "503", "500", "502", "504", "connection",
)


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, ConnectionError, ConnectionResetError)):
        return True
    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return False
    msg = str(exc).lower()
    if any(code in msg for code in ("401", "403", "400", "404", "422", "unauthorized", "invalid")):
        return False
    return any(m in msg for m in _TRANSIENT_MARKERS)


async def retry_async(
    fn: Callable[[], Awaitable[Any]],
    *,
    attempts: int,
    base_delay: float,
    max_delay: float,
    retry_on: Callable[[BaseException], bool] = is_transient,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    rng: Callable[[], float] = random.random,
) -> Any:
    last: BaseException | None = None
    for i in range(attempts):
        try:
            return await fn()
        except BaseException as exc:  # noqa: BLE001 — re-raised below
            last = exc
            if i + 1 >= attempts or not retry_on(exc):
                raise
            delay = min(max_delay, base_delay * (2 ** i)) * (0.5 + 0.5 * rng())
            await sleep(delay)
    assert last is not None
    raise last

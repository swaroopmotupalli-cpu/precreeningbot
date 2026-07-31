import json
import time

class TranscriptStore:
    """Redis-backed transcript store for a single room.

    Designed for a single appender per room (one worker, one event loop) within Phase 1.
    No locking or transactions required; single-appender design is sufficient.
    """
    def __init__(self, redis, room: str, ttl_seconds: int = 7200):
        self._redis = redis
        self._key = f"transcript:{room}"
        self._seq_key = f"transcript:{room}:seq"
        self._ttl = ttl_seconds

    async def append(self, speaker: str, text: str, *, interrupted: bool = False) -> int:
        seq = await self._redis.incr(self._seq_key) - 1  # 0-based
        line = {"seq": seq, "speaker": speaker, "text": text, "ts": time.time()}
        if interrupted:
            # Cut off mid-sentence by a barge-in. Recorded inline, in the SAME
            # append the caller already knows this for (the framework hands
            # us the already-truncated text and the interrupted flag together
            # on one event) — NOT via a later separate search-and-overwrite
            # step. A prior version used a second `truncate_last` call for
            # this, racing against this same append as two independent
            # fire-and-forget tasks; whichever lost the race clobbered the
            # WRONG (previous) line. Setting it here removes the race by
            # construction — there is nothing left to race against.
            line["interrupted"] = True
        await self._redis.rpush(self._key, json.dumps(line))
        await self._redis.expire(self._key, self._ttl)
        await self._redis.expire(self._seq_key, self._ttl)
        return seq

    async def assemble(self) -> list[dict]:
        raw = await self._redis.lrange(self._key, 0, -1)
        return sorted((json.loads(r) for r in raw), key=lambda l: l["seq"])

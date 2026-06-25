import json
import time

class TranscriptStore:
    def __init__(self, redis, room: str, ttl_seconds: int = 7200):
        self._redis = redis
        self._key = f"transcript:{room}"
        self._seq_key = f"transcript:{room}:seq"
        self._ttl = ttl_seconds

    async def append(self, speaker: str, text: str) -> int:
        seq = await self._redis.incr(self._seq_key) - 1  # 0-based
        line = {"seq": seq, "speaker": speaker, "text": text, "ts": time.time()}
        await self._redis.rpush(self._key, json.dumps(line))
        await self._redis.expire(self._key, self._ttl)
        await self._redis.expire(self._seq_key, self._ttl)
        return seq

    async def truncate_last(self, speaker: str, spoken_text: str) -> None:
        raw = await self._redis.lrange(self._key, 0, -1)
        for idx in range(len(raw) - 1, -1, -1):
            line = json.loads(raw[idx])
            if line["speaker"] == speaker:
                line["text"] = spoken_text
                await self._redis.lset(self._key, idx, json.dumps(line))
                return

    async def assemble(self) -> list[dict]:
        raw = await self._redis.lrange(self._key, 0, -1)
        return sorted((json.loads(r) for r in raw), key=lambda l: l["seq"])

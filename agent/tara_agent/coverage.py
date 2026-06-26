from tara_agent.prompts import build_coverage_prompt

class CoverageTracker:
    def __init__(self, redis, room: str, required: list[str], classify_fn):
        self._redis = redis
        self._key = f"coverage:{room}"
        self._required = required
        self._required_lower = {s.lower(): s for s in required}
        self._classify = classify_fn

    async def tag(self, answer: str) -> None:
        raw = await self._classify(build_coverage_prompt(self._required, answer))
        for tok in (t.strip() for t in raw.split(",")):
            canonical = self._required_lower.get(tok.lower())
            if canonical:
                await self._redis.sadd(self._key, canonical)

    async def covered(self) -> set[str]:
        return set(await self._redis.smembers(self._key))

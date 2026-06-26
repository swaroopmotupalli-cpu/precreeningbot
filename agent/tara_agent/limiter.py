# agent/tara_agent/limiter.py
from dataclasses import dataclass
from pathlib import Path

_LUA_DIR = Path(__file__).resolve().parents[2] / "shared" / "limiter"

@dataclass(frozen=True)
class Caps:
    global_: int
    gemini_tpm: int
    stt_streams: int
    tts_streams: int

@dataclass(frozen=True)
class AdmitResult:
    admitted: bool
    state: str | None
    bucket: str | None
    counts: dict

@dataclass(frozen=True)
class Reclaim:
    room: str
    reason: str

class Limiter:
    def __init__(self, redis, *, prefix="{tara-limiter}", caps: Caps,
                 reservation_ttl_ms: int, heartbeat_ttl_ms: int):
        self._r = redis
        self._p = prefix
        self._caps = caps
        self._res_ttl = reservation_ttl_ms
        self._hb_ttl = heartbeat_ttl_ms
        self._admit = redis.register_script((_LUA_DIR / "admit.lua").read_text())
        self._refresh = redis.register_script((_LUA_DIR / "refresh.lua").read_text())
        self._release = redis.register_script((_LUA_DIR / "release.lua").read_text())
        self._reap = redis.register_script((_LUA_DIR / "reap.lua").read_text())

    def _keys(self):
        p = self._p
        return [f"{p}:count:global", f"{p}:count:gemini_tpm",
                f"{p}:count:stt_streams", f"{p}:count:tts_streams",
                f"{p}:reservations", f"{p}:metric:reclaim:no_show",
                f"{p}:metric:reclaim:crash", f"{p}:metric:reclaim:false"]

    async def try_admit(self, room: str, now_ms: int) -> AdmitResult:
        res = await self._admit(keys=self._keys(), args=[
            room, now_ms, self._res_ttl, self._caps.global_, self._caps.gemini_tpm,
            self._caps.stt_streams, self._caps.tts_streams, self._p])
        if res[0] == "ADMITTED":
            g, gm, st, tt = res[2], res[3], res[4], res[5]
            return AdmitResult(True, res[1], None, {
                "global": int(g), "gemini_tpm": int(gm),
                "stt_streams": int(st), "tts_streams": int(tt)})
        return AdmitResult(False, None, res[1], {})

    async def heartbeat(self, room: str, now_ms: int, *,
                        mark_heartbeat: bool = False,
                        mark_participant: bool = False) -> bool:
        res = await self._refresh(
            keys=[f"{self._p}:reservations"],
            args=[room, now_ms, self._hb_ttl,
                  1 if mark_heartbeat else 0, 1 if mark_participant else 0, self._p])
        return int(res) == 1

    async def release(self, room: str) -> bool:
        res = await self._release(
            keys=[f"{self._p}:count:global", f"{self._p}:count:gemini_tpm",
                  f"{self._p}:count:stt_streams", f"{self._p}:count:tts_streams",
                  f"{self._p}:reservations"],
            args=[room, self._p])
        return int(res) == 1

    async def reap(self, now_ms: int) -> list[Reclaim]:
        flat = await self._reap(keys=self._keys(), args=[now_ms, self._p])
        return [Reclaim(flat[i], flat[i + 1]) for i in range(0, len(flat), 2)]

    async def metrics(self) -> dict:
        async def g(k):
            return int(await self._r.get(f"{self._p}:{k}") or 0)
        return {
            "reclaim_no_show": await g("metric:reclaim:no_show"),
            "reclaim_crash": await g("metric:reclaim:crash"),
            "reclaim_false": await g("metric:reclaim:false"),
            "reject_global": await g("metric:reject:global"),
            "reject_gemini_tpm": await g("metric:reject:gemini_tpm"),
            "reject_stt_streams": await g("metric:reject:stt_streams"),
            "reject_tts_streams": await g("metric:reject:tts_streams"),
            # Live bucket counts — used by metrics_export to compute bucket_utilization
            "count_global": await g("count:global"),
            "count_gemini_tpm": await g("count:gemini_tpm"),
            "count_stt_streams": await g("count:stt_streams"),
            "count_tts_streams": await g("count:tts_streams"),
        }

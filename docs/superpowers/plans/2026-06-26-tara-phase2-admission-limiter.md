# Tara Phase 2 — Admission Limiter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A global, Redis-backed admission limiter that every session must clear before the agent joins — atomic per-provider buckets, a two-tier lease keyed by room name, guaranteed release, and reclaim instrumentation that classifies no-show / crash / **false-reclaim** so the spec's zero-false-reclaim hard gate is measurable.

**Architecture:** All atomicity lives in four shared Lua scripts (`admit`, `refresh`, `release`, `reap`) evaluated identically by the Node backend (admits at session-create) and the Python worker (heartbeat, release, reap). The lease is a Redis ZSET scored by expiry timestamp (full control + observability on expiry), not a key-TTL; bucket counts are integers; reservation metadata (heartbeat-ever, participant-joined) lives in a hash. `admit` reaps expired reservations inline before its cap check, so a no-show's slot frees exactly when the next candidate needs it; a periodic worker task reaps during idle periods.

**Tech Stack:** Python 3.12, `redis` (redis-py, real `EVAL`), `redislite` (in-process real redis-server with Lua, dev/test only), `pytest`/`pytest-asyncio`; Node 20+, `ioredis`, `redis-memory-server` (real redis for tests), `jest`/`supertest`. Lua 5.1 (Redis embedded).

> **Plan scope:** Phase 2 only. Builds on the committed Phase 1 branch `tara/phase1-single-session`. Create the Phase 2 work on a branch off it: `git checkout tara/phase1-single-session && git checkout -b tara/phase2-admission-limiter`. Per the build order, **do not begin executing Phase 2 until the Phase 1 single-stream p50<800ms gate is green** — planning now is fine. Phases 3–5 get their own plans. References spec `docs/superpowers/specs/2026-06-25-tara-cascaded-voice-interview-design.md`.

## Global Constraints

Copied verbatim from the spec (§3, §4, §6):

- **Replace any per-process semaphore** with this global Redis limiter; a session clears it BEFORE the agent joins.
- **Buckets:** `global:sessions` (cap `MAX_GLOBAL_SESSIONS`, e.g. 110), `bucket:gemini_tpm` (`GEMINI_TPM_BUDGET`, tightest = real ceiling), `bucket:stt_streams` (`STT_STREAM_BUDGET`), `bucket:tts_streams` (`TTS_STREAM_BUDGET`). A session holds one slot in EVERY bucket for its whole life.
- **Acquire / release / refresh / reap are each ONE Lua script** — atomic, all-or-nothing. Never half-admit (take some buckets, miss others). The cap is never exceeded under a read-then-write race across workers.
- **Two-tier lease, keyed by `reservation_id = room_name`:** Tier-1 reservation lease TTL = `RESERVATION_LEASE_TTL` (45s), set at admission. Tier-2 heartbeat lease: worker refreshes every `HEARTBEAT_INTERVAL` (15s), TTL `HEARTBEAT_LEASE_TTL` (60s). Reclaim windows: no-show ≤45s, crash ≤60s, normal end = immediate.
- **Reconnect idempotency:** a rejoin routes back through the SAME `try_admit(room_name)`; a live reservation is refreshed, NOT re-counted. First-join and rejoin are the same call against the same key.
- **Guaranteed release:** `release` runs in `end_interview`'s `on_finally` (Phase 1 built the finally + hard timeouts; Phase 2 wires the actual release into it). Release is downstream of nothing that can fail.
- **Reclaim instrumentation (catch #1):** passive expiry observes nothing on its own, so reclaim must be DETECTED. Detect via lazy reap inside `admit` (+ a periodic reaper). Each reservation carries a `heartbeat_ever_received` flag to classify `no_show` vs `crash`. A **false reclaim** = a lease that expired while a real LiveKit participant was present (correlate via a `participant_joined` flag set by the worker). The zero-false-reclaim gate is unmeasurable without this.
- **Coverage-tagger tokens count toward the gemini_tpm budget (catch #3):** the off-path coverage tagger fires a real Gemini classify call on EVERY user-turn (§2 of the spec). When the Phase-3 load test calibrates `GEMINI_TPM_BUDGET`, the measured per-session token figure MUST include those tagger calls (and the worker's own LLM_DIAG already exposes per-call tokens). Setting the budget from hot-path generation alone would under-size it and let the tagger silently eat headroom. The `gemini_tpm` bucket here is the single ceiling both consumers share.
- **Graceful degradation:** on rejection return a "starting shortly" / queued state — never silent over-admission, never a degraded over-admitted interview.
- **No hardcoded secrets / caps** — all caps & TTLs from env. Gemini limits are per-project not per-key (README TODO, not code).

> **Testing reality (binding):** the limiter's atomicity is Lua; `fakeredis`/`ioredis-mock` do NOT support `EVAL`. Python tests run the real scripts via **`redislite`** (in-process real redis-server). Node tests run them via **`redis-memory-server`**. There is no mocking the Lua — that would test nothing.

> **Key layout (cluster-safe):** ALL limiter keys share the hash-tag `{tara-limiter}` so they occupy one slot and a script may build per-room keys at runtime:
> `{tara-limiter}:count:global` · `:count:gemini_tpm` · `:count:stt_streams` · `:count:tts_streams` · `:reservations` (ZSET score=expiry_ms) · `:res:<room>` (HASH) · `:metric:reclaim:no_show|crash|false` · `:metric:reject:<bucket>`.

---

### Task 1: Admission config (Python Settings + Node config reader)

**Files:**
- Modify: `agent/tara_agent/config.py`
- Modify: `agent/tests/test_config.py`
- Create: `backend/src/limiterConfig.js`
- Create: `backend/test/limiterConfig.test.js`

**Interfaces:**
- Produces (Python): new `Settings` fields — `max_global_sessions: int = 110`, `gemini_tpm_budget: int = 60`, `stt_stream_budget: int = 100`, `tts_stream_budget: int = 100`, `reservation_lease_ttl: int = 45`, `heartbeat_interval: int = 15`, `heartbeat_lease_ttl: int = 60`. (Defaults are pre-load-test placeholders; the Phase-3 load test calibrates `gemini_tpm_budget` etc.)
- Produces (Node): `loadLimiterConfig(env) -> {maxGlobalSessions, geminiTpmBudget, sttStreamBudget, ttsStreamBudget, reservationLeaseTtl, heartbeatLeaseTtl}` reading the same env var names with the same defaults.

- [ ] **Step 1: Write the failing Python test**

```python
# agent/tests/test_config.py  (add these tests)
def test_admission_settings_defaults(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.max_global_sessions == 110
    assert s.reservation_lease_ttl == 45
    assert s.heartbeat_interval == 15
    assert s.heartbeat_lease_ttl == 60

def test_admission_settings_override(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
        "MAX_GLOBAL_SESSIONS": "200", "GEMINI_TPM_BUDGET": "42",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.max_global_sessions == 200
    assert s.gemini_tpm_budget == 42
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && source .venv/bin/activate && pytest tests/test_config.py -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'max_global_sessions'`

- [ ] **Step 3: Add the fields to `config.py`**

Add inside the `Settings` class (after `mongo_write_timeout_seconds`):
```python
    # admission caps / budgets (Phase 2) — load-test calibrates these (Phase 3)
    max_global_sessions: int = 110
    gemini_tpm_budget: int = 60
    stt_stream_budget: int = 100
    tts_stream_budget: int = 100
    # two-tier lease (seconds)
    reservation_lease_ttl: int = 45
    heartbeat_interval: int = 15
    heartbeat_lease_ttl: int = 60
```

- [ ] **Step 4: Run Python test to green**

Run: `cd agent && pytest tests/test_config.py -v`
Expected: PASS (all config tests)

- [ ] **Step 5: Write the failing Node test**

```javascript
// backend/test/limiterConfig.test.js
const { loadLimiterConfig } = require("../src/limiterConfig");

test("defaults", () => {
  const c = loadLimiterConfig({});
  expect(c.maxGlobalSessions).toBe(110);
  expect(c.geminiTpmBudget).toBe(60);
  expect(c.reservationLeaseTtl).toBe(45);
  expect(c.heartbeatLeaseTtl).toBe(60);
});

test("env overrides parse to numbers", () => {
  const c = loadLimiterConfig({ MAX_GLOBAL_SESSIONS: "200", STT_STREAM_BUDGET: "80" });
  expect(c.maxGlobalSessions).toBe(200);
  expect(c.sttStreamBudget).toBe(80);
});
```

- [ ] **Step 6: Run to verify it fails, then implement**

Run: `cd backend && npx jest limiterConfig` → FAIL (module not found).

```javascript
// backend/src/limiterConfig.js
function num(v, d) { return v === undefined ? d : parseInt(v, 10); }
function loadLimiterConfig(env) {
  return {
    maxGlobalSessions: num(env.MAX_GLOBAL_SESSIONS, 110),
    geminiTpmBudget: num(env.GEMINI_TPM_BUDGET, 60),
    sttStreamBudget: num(env.STT_STREAM_BUDGET, 100),
    ttsStreamBudget: num(env.TTS_STREAM_BUDGET, 100),
    reservationLeaseTtl: num(env.RESERVATION_LEASE_TTL, 45),
    heartbeatLeaseTtl: num(env.HEARTBEAT_LEASE_TTL, 60),
  };
}
module.exports = { loadLimiterConfig };
```

- [ ] **Step 7: Run Node test to green, then commit**

Run: `cd backend && npx jest limiterConfig` → PASS.
```bash
git add agent/tara_agent/config.py agent/tests/test_config.py backend/src/limiterConfig.js backend/test/limiterConfig.test.js
git commit -m "feat: admission caps and two-tier lease config (Python + Node)"
```

---

### Task 2: `admit.lua` + Python `Limiter.try_admit` (new-admit, reject, atomicity)

**Files:**
- Create: `shared/limiter/admit.lua`
- Create: `agent/tara_agent/limiter.py`
- Create: `agent/tests/conftest.py` (redislite fixture)
- Test: `agent/tests/test_limiter_admit.py`

**Interfaces:**
- Produces: `shared/limiter/admit.lua` (canonical; loaded by both services).
- Produces: `@dataclass(frozen=True) Caps(global_, gemini_tpm, stt_streams, tts_streams)`; `@dataclass(frozen=True) AdmitResult(admitted: bool, state: str, bucket: str | None, counts: dict)` where `state ∈ {"new","refreshed"}` when admitted and `bucket` names the rejecting bucket when not.
- Produces: `class Limiter(redis, *, prefix="{tara-limiter}", caps: Caps, reservation_ttl_ms: int, heartbeat_ttl_ms: int)` with `async def try_admit(self, room: str, now_ms: int) -> AdmitResult`. Loads the four `.lua` files from `shared/limiter/` via `redis.register_script`.
- Consumes: a redis-py async client (real Redis, supplied in prod by `redis.asyncio.from_url`; in tests by the redislite fixture).

- [ ] **Step 1: Write the redislite fixture**

```python
# agent/tests/conftest.py
import pytest
import redislite
import redis.asyncio as aioredis

@pytest.fixture
async def real_redis(tmp_path):
    # redislite runs a REAL redis-server (Lua/EVAL supported), per-test isolated.
    rdb = redislite.Redis(str(tmp_path / "t.rdb"))
    sock = rdb.socket_file
    client = aioredis.Redis(unix_socket_path=sock, decode_responses=True)
    yield client
    await client.aclose()
    rdb.close()
```

- [ ] **Step 2: Write the failing test**

```python
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
```

- [ ] **Step 3: Run to verify it fails**

Run: `cd agent && pip install redislite~=6.2 && pytest tests/test_limiter_admit.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tara_agent.limiter'`
(Add `redislite~=6.2` to `agent/pyproject.toml` `[project.optional-dependencies].dev`.)

- [ ] **Step 4: Write `admit.lua`**

```lua
-- shared/limiter/admit.lua
-- KEYS: 1 count:global 2 count:gemini_tpm 3 count:stt_streams 4 count:tts_streams
--       5 reservations(zset) 6 metric:reclaim:no_show 7 metric:reclaim:crash
--       8 metric:reclaim:false
-- ARGV: 1 room 2 now_ms 3 ttl_ms 4 cap_global 5 cap_gemini 6 cap_stt 7 cap_tts
--       8 keyprefix
-- returns: {"ADMITTED", "new"|"refreshed", g, gm, st, tt}  or  {"REJECTED", bucket}
local now = tonumber(ARGV[2])
local prefix = ARGV[8]

-- (1) REAP expired reservations first (frees capacity before the cap check)
local expired = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', '(' .. now)
for _, room in ipairs(expired) do
  local h = prefix .. ':res:' .. room
  redis.call('DECR', KEYS[1]); redis.call('DECR', KEYS[2])
  redis.call('DECR', KEYS[3]); redis.call('DECR', KEYS[4])
  local pj = redis.call('HGET', h, 'participant_joined')
  local hb = redis.call('HGET', h, 'heartbeat_ever')
  if pj == '1' then redis.call('INCR', KEYS[8])        -- false reclaim
  elseif hb == '1' then redis.call('INCR', KEYS[7])    -- crash
  else redis.call('INCR', KEYS[6]) end                 -- no-show
  redis.call('ZREM', KEYS[5], room)
  redis.call('DEL', h)
end

-- (2) Reconnect idempotency: live reservation exists -> refresh, no re-count
if redis.call('ZSCORE', KEYS[5], ARGV[1]) then
  redis.call('ZADD', KEYS[5], now + tonumber(ARGV[3]), ARGV[1])
  return {'ADMITTED', 'refreshed',
    redis.call('GET', KEYS[1]) or '0', redis.call('GET', KEYS[2]) or '0',
    redis.call('GET', KEYS[3]) or '0', redis.call('GET', KEYS[4]) or '0'}
end

-- (3) Capacity check (priority order = likely tightest first), all-or-nothing
local g  = tonumber(redis.call('GET', KEYS[1]) or '0')
local gm = tonumber(redis.call('GET', KEYS[2]) or '0')
local st = tonumber(redis.call('GET', KEYS[3]) or '0')
local tt = tonumber(redis.call('GET', KEYS[4]) or '0')
if gm >= tonumber(ARGV[5]) then redis.call('INCR', prefix..':metric:reject:gemini_tpm'); return {'REJECTED','gemini_tpm'} end
if g  >= tonumber(ARGV[4]) then redis.call('INCR', prefix..':metric:reject:global');     return {'REJECTED','global'} end
if st >= tonumber(ARGV[6]) then redis.call('INCR', prefix..':metric:reject:stt_streams');return {'REJECTED','stt_streams'} end
if tt >= tonumber(ARGV[7]) then redis.call('INCR', prefix..':metric:reject:tts_streams');return {'REJECTED','tts_streams'} end

-- (4) Admit: take ALL buckets atomically, create reservation with Tier-1 lease
redis.call('INCR', KEYS[1]); redis.call('INCR', KEYS[2])
redis.call('INCR', KEYS[3]); redis.call('INCR', KEYS[4])
redis.call('ZADD', KEYS[5], now + tonumber(ARGV[3]), ARGV[1])
redis.call('HSET', prefix..':res:'..ARGV[1],
  'heartbeat_ever','0','participant_joined','0','created_ms',ARGV[2])
return {'ADMITTED','new', tostring(g+1), tostring(gm+1), tostring(st+1), tostring(tt+1)}
```

- [ ] **Step 5: Write `limiter.py` (`try_admit` only for now)**

```python
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

class Limiter:
    def __init__(self, redis, *, prefix="{tara-limiter}", caps: Caps,
                 reservation_ttl_ms: int, heartbeat_ttl_ms: int):
        self._r = redis
        self._p = prefix
        self._caps = caps
        self._res_ttl = reservation_ttl_ms
        self._hb_ttl = heartbeat_ttl_ms
        self._admit = redis.register_script((_LUA_DIR / "admit.lua").read_text())
        # refresh/release/reap registered in later tasks

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
```

- [ ] **Step 6: Run tests to green**

Run: `cd agent && pytest tests/test_limiter_admit.py -v`
Expected: PASS (3 passed). The concurrency test proves atomicity (exactly cap admits, count never exceeds).

- [ ] **Step 7: Commit**

```bash
git add shared/limiter/admit.lua agent/tara_agent/limiter.py agent/tests/conftest.py agent/tests/test_limiter_admit.py agent/pyproject.toml
git commit -m "feat(limiter): atomic admit.lua + Limiter.try_admit (cap-enforced, reject names bucket)"
```

---

### Task 3: `refresh.lua` + heartbeat / participant flags + reconnect idempotency

**Files:**
- Create: `shared/limiter/refresh.lua`
- Modify: `agent/tara_agent/limiter.py`
- Test: `agent/tests/test_limiter_refresh.py`

**Interfaces:**
- Consumes: `Limiter` (Task 2), `admit.lua`'s `res:<room>` hash + `reservations` ZSET.
- Produces: `async def heartbeat(self, room: str, now_ms: int, *, mark_heartbeat: bool = False, mark_participant: bool = False) -> bool` — refreshes the lease to `now + heartbeat_ttl_ms` and optionally sets the `heartbeat_ever`/`participant_joined` flags; returns `False` if the reservation no longer exists (already reaped). Reconnect uses `try_admit` (already idempotent via Task 2's `admit.lua`).

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_limiter_refresh.py
import pytest
from tara_agent.limiter import Limiter, Caps

def make(redis):
    return Limiter(redis, caps=Caps(5,5,5,5), reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_heartbeat_extends_lease_and_sets_flags(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    ok = await lim.heartbeat("r1", now_ms=2000, mark_heartbeat=True, mark_participant=True)
    assert ok is True
    score = await real_redis.zscore("{tara-limiter}:reservations", "r1")
    assert score == 2000 + 60000                       # Tier-2 ttl applied
    assert await real_redis.hget("{tara-limiter}:res:r1", "heartbeat_ever") == "1"
    assert await real_redis.hget("{tara-limiter}:res:r1", "participant_joined") == "1"

async def test_heartbeat_missing_reservation_returns_false(real_redis):
    lim = make(real_redis)
    assert await lim.heartbeat("ghost", now_ms=2000) is False

async def test_reconnect_via_admit_refreshes_without_double_count(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    again = await lim.try_admit("r1", now_ms=5000)     # same room = rejoin
    assert again.admitted and again.state == "refreshed"
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 1   # NOT 2
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_limiter_refresh.py -v`
Expected: FAIL — `AttributeError: 'Limiter' object has no attribute 'heartbeat'`

- [ ] **Step 3: Write `refresh.lua`**

```lua
-- shared/limiter/refresh.lua
-- KEYS: 1 reservations(zset)
-- ARGV: 1 room 2 now_ms 3 hb_ttl_ms 4 mark_heartbeat(0/1) 5 mark_participant(0/1)
--       6 keyprefix
-- returns: 1 if refreshed, 0 if reservation missing
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[1])
local h = ARGV[6] .. ':res:' .. ARGV[1]
if ARGV[4] == '1' then redis.call('HSET', h, 'heartbeat_ever', '1') end
if ARGV[5] == '1' then redis.call('HSET', h, 'participant_joined', '1') end
return 1
```

- [ ] **Step 4: Add `heartbeat` to `limiter.py`**

In `__init__`, after the admit registration:
```python
        self._refresh = redis.register_script((_LUA_DIR / "refresh.lua").read_text())
```
Add the method:
```python
    async def heartbeat(self, room: str, now_ms: int, *,
                        mark_heartbeat: bool = False,
                        mark_participant: bool = False) -> bool:
        res = await self._refresh(
            keys=[f"{self._p}:reservations"],
            args=[room, now_ms, self._hb_ttl,
                  1 if mark_heartbeat else 0, 1 if mark_participant else 0, self._p])
        return int(res) == 1
```

- [ ] **Step 5: Run tests to green**

Run: `cd agent && pytest tests/test_limiter_refresh.py -v`
Expected: PASS (3 passed) — incl. reconnect refreshing without double-count.

- [ ] **Step 6: Commit**

```bash
git add shared/limiter/refresh.lua agent/tara_agent/limiter.py agent/tests/test_limiter_refresh.py
git commit -m "feat(limiter): refresh.lua heartbeat (Tier-2 lease + flags) + reconnect idempotency proven"
```

---

### Task 4: `release.lua` + `Limiter.release` (idempotent)

**Files:**
- Create: `shared/limiter/release.lua`
- Modify: `agent/tara_agent/limiter.py`
- Test: `agent/tests/test_limiter_release.py`

**Interfaces:**
- Produces: `async def release(self, room: str) -> bool` — decrements all four buckets, removes the reservation + hash; returns `True` if a live reservation was released, `False` if it was already gone (reaped/never existed). Idempotent — calling twice is safe and never drives counts negative.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_limiter_release.py
import pytest
from tara_agent.limiter import Limiter, Caps

def make(redis):
    return Limiter(redis, caps=Caps(5,5,5,5), reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_release_decrements_all_buckets(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 1
    assert await lim.release("r1") is True
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 0
    assert await real_redis.zscore("{tara-limiter}:reservations", "r1") is None
    assert await real_redis.exists("{tara-limiter}:res:r1") == 0

async def test_release_is_idempotent_no_negative_counts(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    assert await lim.release("r1") is True
    assert await lim.release("r1") is False            # already gone
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 0   # not -1
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_limiter_release.py -v`
Expected: FAIL — `AttributeError: 'Limiter' object has no attribute 'release'`

- [ ] **Step 3: Write `release.lua`**

```lua
-- shared/limiter/release.lua
-- KEYS: 1 count:global 2 count:gemini_tpm 3 count:stt_streams 4 count:tts_streams
--       5 reservations(zset)
-- ARGV: 1 room 2 keyprefix
-- returns: 1 if released, 0 if reservation was already gone
if not redis.call('ZSCORE', KEYS[5], ARGV[1]) then return 0 end
redis.call('DECR', KEYS[1]); redis.call('DECR', KEYS[2])
redis.call('DECR', KEYS[3]); redis.call('DECR', KEYS[4])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('DEL', ARGV[2] .. ':res:' .. ARGV[1])
return 1
```

- [ ] **Step 4: Add `release` to `limiter.py`**

In `__init__`:
```python
        self._release = redis.register_script((_LUA_DIR / "release.lua").read_text())
```
Method:
```python
    async def release(self, room: str) -> bool:
        res = await self._release(
            keys=[f"{self._p}:count:global", f"{self._p}:count:gemini_tpm",
                  f"{self._p}:count:stt_streams", f"{self._p}:count:tts_streams",
                  f"{self._p}:reservations"],
            args=[room, self._p])
        return int(res) == 1
```

- [ ] **Step 5: Run tests to green**

Run: `cd agent && pytest tests/test_limiter_release.py -v`
Expected: PASS (2 passed) — idempotent, no negative counts.

- [ ] **Step 6: Commit**

```bash
git add shared/limiter/release.lua agent/tara_agent/limiter.py agent/tests/test_limiter_release.py
git commit -m "feat(limiter): release.lua + idempotent Limiter.release"
```

---

### Task 5: `reap.lua` + reclaim classification (catch #1) + reject/reclaim metrics

**Files:**
- Create: `shared/limiter/reap.lua`
- Modify: `agent/tara_agent/limiter.py`
- Test: `agent/tests/test_limiter_reap.py`

**Interfaces:**
- Produces: `@dataclass(frozen=True) Reclaim(room: str, reason: str)` (`reason ∈ {"no_show","crash","false"}`); `async def reap(self, now_ms: int) -> list[Reclaim]` — reclaims every reservation whose lease score `< now_ms`, decrements its buckets, classifies it, increments the matching `metric:reclaim:*` counter, and returns the list. `async def metrics(self) -> dict` returning the reclaim + reject counters for observability. Classification: `participant_joined==1 → "false"`; elif `heartbeat_ever==1 → "crash"`; else `→ "no_show"`. (Same logic as `admit.lua`'s inline reap — `reap.lua` is the standalone periodic form.)

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_limiter_reap.py
import pytest
from tara_agent.limiter import Limiter, Caps, Reclaim

def make(redis):
    return Limiter(redis, caps=Caps(5,5,5,5), reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_reap_noshow_frees_slot_and_classifies(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)             # lease expires at 46000
    out = await lim.reap(now_ms=46001)                 # past Tier-1 ttl, no heartbeat
    assert out == [Reclaim("r1", "no_show")]
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 0
    assert int(await real_redis.get("{tara-limiter}:metric:reclaim:no_show")) == 1

async def test_reap_crash_when_heartbeat_seen_no_participant(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    await lim.heartbeat("r1", now_ms=1000, mark_heartbeat=True)   # worker took over, lease->61000
    out = await lim.reap(now_ms=61001)
    assert out == [Reclaim("r1", "crash")]
    assert int(await real_redis.get("{tara-limiter}:metric:reclaim:crash")) == 1

async def test_reap_false_reclaim_when_participant_present(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)
    await lim.heartbeat("r1", now_ms=1000, mark_heartbeat=True, mark_participant=True)
    out = await lim.reap(now_ms=61001)                 # lapsed while participant present
    assert out == [Reclaim("r1", "false")]             # the HARD-GATE signal
    assert int(await real_redis.get("{tara-limiter}:metric:reclaim:false")) == 1

async def test_reap_leaves_live_reservations(real_redis):
    lim = make(real_redis)
    await lim.try_admit("r1", now_ms=1000)             # expires 46000
    out = await lim.reap(now_ms=2000)                  # still live
    assert out == []
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_limiter_reap.py -v`
Expected: FAIL — `ImportError: cannot import name 'Reclaim'` / no `reap`.

- [ ] **Step 3: Write `reap.lua`**

```lua
-- shared/limiter/reap.lua
-- KEYS: 1 count:global 2 count:gemini_tpm 3 count:stt_streams 4 count:tts_streams
--       5 reservations(zset) 6 metric:reclaim:no_show 7 metric:reclaim:crash
--       8 metric:reclaim:false
-- ARGV: 1 now_ms 2 keyprefix
-- returns: flat list [room1, reason1, room2, reason2, ...]
local now = tonumber(ARGV[1])
local prefix = ARGV[2]
local expired = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', '(' .. now)
local out = {}
for _, room in ipairs(expired) do
  local h = prefix .. ':res:' .. room
  redis.call('DECR', KEYS[1]); redis.call('DECR', KEYS[2])
  redis.call('DECR', KEYS[3]); redis.call('DECR', KEYS[4])
  local pj = redis.call('HGET', h, 'participant_joined')
  local hb = redis.call('HGET', h, 'heartbeat_ever')
  local reason
  if pj == '1' then reason = 'false'; redis.call('INCR', KEYS[8])
  elseif hb == '1' then reason = 'crash'; redis.call('INCR', KEYS[7])
  else reason = 'no_show'; redis.call('INCR', KEYS[6]) end
  redis.call('ZREM', KEYS[5], room)
  redis.call('DEL', h)
  out[#out + 1] = room
  out[#out + 1] = reason
end
return out
```

- [ ] **Step 4: Add `Reclaim`, `reap`, `metrics` to `limiter.py`**

Add dataclass near the others:
```python
@dataclass(frozen=True)
class Reclaim:
    room: str
    reason: str
```
In `__init__`:
```python
        self._reap = redis.register_script((_LUA_DIR / "reap.lua").read_text())
```
Methods:
```python
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
        }
```

- [ ] **Step 5: Run tests to green**

Run: `cd agent && pytest tests/test_limiter_reap.py -v`
Expected: PASS (4 passed) — no_show / crash / false classification + live untouched.

- [ ] **Step 6: Commit**

```bash
git add shared/limiter/reap.lua agent/tara_agent/limiter.py agent/tests/test_limiter_reap.py
git commit -m "feat(limiter): reap.lua + reclaim classification (no_show/crash/false) + metrics"
```

---

### Task 6: Node limiter + `createSession` admission integration

**Files:**
- Create: `backend/src/limiter.js`
- Modify: `backend/src/createSession.js`
- Modify: `backend/src/server.js`
- Test: `backend/test/limiter.test.js`
- Modify: `backend/test/createSession.test.js`

**Interfaces:**
- Produces: `class Limiter(redis, {prefix, caps, reservationTtlMs})` with `async tryAdmit(room, nowMs) -> {admitted, state, bucket, counts}`, loading the SAME `shared/limiter/admit.lua` (and `release.lua` for completeness). Node only needs `tryAdmit` (it admits + sets the Tier-1 lease at session create); heartbeat/reap/release live in the Python worker.
- Modifies: `createSession(redis, tokenFactory, limiter, body)` — writes the blob, then `tryAdmit(room=sessionId, Date.now())`; on admitted returns `{sessionId, room, token, livekitUrl, status:"admitted"}`; on rejected returns `{sessionId, status:"queued", bucket}` with **no token** (graceful "starting shortly").

> **Node test infra:** add `redis-memory-server` to `backend/devDependencies`. The Node limiter test starts a real Redis via `redis-memory-server`, loads the real Lua, and mirrors the key Python cases (admit-until-cap, reject-names-bucket, reconnect-refresh-no-double-count) to prove the Node wrapper drives the identical scripts correctly. The `createSession` test injects a fake limiter (records `tryAdmit` calls; returns admitted/rejected) to test the endpoint's branching without Redis.

- [ ] **Step 1: Write the failing Node limiter test**

```javascript
// backend/test/limiter.test.js
const { RedisMemoryServer } = require("redis-memory-server");
const Redis = require("ioredis");
const { Limiter } = require("../src/limiter");

let server, redis;
beforeAll(async () => {
  server = new RedisMemoryServer();
  const host = await server.getHost(); const port = await server.getPort();
  redis = new Redis({ host, port });
});
afterAll(async () => { await redis.quit(); await server.stop(); });
beforeEach(async () => { await redis.flushall(); });

const caps = { global: 2, geminiTpm: 2, sttStreams: 2, ttsStreams: 2 };
const mk = () => new Limiter(redis, { caps, reservationTtlMs: 45000 });

test("admits until cap then rejects naming the bucket", async () => {
  const lim = mk();
  expect((await lim.tryAdmit("r1", 1000)).admitted).toBe(true);
  expect((await lim.tryAdmit("r2", 1000)).admitted).toBe(true);
  const r = await lim.tryAdmit("r3", 1000);
  expect(r.admitted).toBe(false);
  expect(r.bucket).toBe("global");
});

test("reconnect refreshes without double-count", async () => {
  const lim = mk();
  await lim.tryAdmit("r1", 1000);
  const again = await lim.tryAdmit("r1", 5000);
  expect(again.state).toBe("refreshed");
  expect(Number(await redis.get("{tara-limiter}:count:global"))).toBe(1);
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npm install redis-memory-server --save-dev && npx jest limiter.test`
Expected: FAIL — `Cannot find module '../src/limiter'`

- [ ] **Step 3: Write `limiter.js`**

```javascript
// backend/src/limiter.js
const fs = require("fs");
const path = require("path");

const LUA_DIR = path.resolve(__dirname, "..", "..", "shared", "limiter");
const ADMIT = fs.readFileSync(path.join(LUA_DIR, "admit.lua"), "utf8");
const PREFIX = "{tara-limiter}";

function keys() {
  return [`${PREFIX}:count:global`, `${PREFIX}:count:gemini_tpm`,
    `${PREFIX}:count:stt_streams`, `${PREFIX}:count:tts_streams`,
    `${PREFIX}:reservations`, `${PREFIX}:metric:reclaim:no_show`,
    `${PREFIX}:metric:reclaim:crash`, `${PREFIX}:metric:reclaim:false`];
}

class Limiter {
  constructor(redis, { prefix = PREFIX, caps, reservationTtlMs }) {
    this.redis = redis; this.caps = caps; this.ttl = reservationTtlMs; this.prefix = prefix;
  }
  async tryAdmit(room, nowMs) {
    const k = keys();
    const res = await this.redis.eval(ADMIT, k.length, ...k,
      room, nowMs, this.ttl, this.caps.global, this.caps.geminiTpm,
      this.caps.sttStreams, this.caps.ttsStreams, this.prefix);
    if (res[0] === "ADMITTED") {
      return { admitted: true, state: res[1], bucket: null, counts: {
        global: Number(res[2]), geminiTpm: Number(res[3]),
        sttStreams: Number(res[4]), ttsStreams: Number(res[5]) } };
    }
    return { admitted: false, state: null, bucket: res[1], counts: {} };
  }
}
module.exports = { Limiter };
```

- [ ] **Step 4: Run Node limiter test to green**

Run: `cd backend && npx jest limiter.test`
Expected: PASS (2 passed).

- [ ] **Step 5: Update `createSession.js` + its test for admission**

Modify `createSession` to take a `limiter` and branch on admission:
```javascript
// backend/src/createSession.js  (replace the body after the blob write)
async function createSession(redis, tokenFactory, limiter, body) {
  for (const f of REQUIRED) {
    if (body[f] === undefined) throw new Error(`field ${f} is required`);
  }
  const sessionId = crypto.randomUUID();
  const blob = {
    contestId: body.contestId, candidateId: body.candidateId,
    skills: body.skills, resumeText: body.resumeText, jdText: body.jdText,
    maxQuestions: body.maxQuestions ?? 12, status: "created",
  };
  await redis.set(`session:${sessionId}`, JSON.stringify(blob), "EX", 7200);

  const admit = await limiter.tryAdmit(sessionId, Date.now());
  if (!admit.admitted) {
    return { sessionId, status: "queued", bucket: admit.bucket };   // starting shortly, NO token
  }
  const token = await tokenFactory(sessionId, body.candidateId);
  return { sessionId, room: sessionId, token,
           livekitUrl: process.env.LIVEKIT_URL, status: "admitted" };
}
```
Update `backend/test/createSession.test.js`: the existing two tests pass a fake `limiter` `{ tryAdmit: async () => ({ admitted: true, state: "new" }) }` as the new 3rd arg, and assert `status === "admitted"`. Add a test:
```javascript
test("rejected admission returns queued with bucket and no token", async () => {
  const redis = { set: async () => {} };
  const limiter = { tryAdmit: async () => ({ admitted: false, bucket: "gemini_tpm" }) };
  const out = await createSession(redis, () => "tok", limiter, {
    contestId: "c", candidateId: "u", skills: ["x"], resumeText: "r", jdText: "j" });
  expect(out.status).toBe("queued");
  expect(out.bucket).toBe("gemini_tpm");
  expect(out.token).toBeUndefined();
});
```
Wire `server.js`: construct one `Limiter` (caps from `loadLimiterConfig(process.env)`) and pass it into the handler's `createSession(redis, mintToken, limiter, req.body)`; when `status === "queued"`, respond `503` with the JSON body.

- [ ] **Step 6: Run Node suite to green, then commit**

Run: `cd backend && npx jest`
Expected: PASS (limiterConfig + limiter + createSession).
```bash
git add backend/src/limiter.js backend/src/createSession.js backend/src/server.js backend/test/limiter.test.js backend/test/createSession.test.js backend/package.json
git commit -m "feat(backend): admission via shared admit.lua; queued/503 on reject, no token"
```

---

### Task 7: Worker integration — heartbeat loop, participant signals, release, reaper

**Files:**
- Modify: `agent/tara_agent/worker.py`
- Modify: `agent/tara_agent/interview_agent.py`
- Manual/integration verification (logic is unit-tested in Tasks 2–5; this is the wiring seam, like Phase-1 Task 10).

**Interfaces:**
- Consumes: `Limiter` (Tasks 2–5), `get_settings()` lease/cap fields (Task 1).
- Produces: worker constructs `Limiter` from settings; on agent join → `await limiter.heartbeat(room, now, mark_heartbeat=True, mark_participant=True)` (promote to Tier-2 + set both flags); a background task heartbeats every `heartbeat_interval` seconds; on candidate participant disconnect → `await limiter.release(room)`; a background reaper task calls `limiter.reap(now)` every `heartbeat_interval` seconds and logs reclaims by reason; `InterviewAgent._wrap_up`'s `on_finally` now calls `limiter.release(room)` (Phase 1 left it as `done.set` only — keep `done.set` AND add release).

> **Verify at implementation time (installed `livekit-agents==1.0.19`):** the room/participant events for "candidate participant connected" and "disconnected" (e.g. `ctx.room.on("participant_connected"/"participant_disconnected")` or `participant_*` on the session). Confirm against installed source; wire `mark_participant`/`release` to the correct event. The heartbeat + reaper loops use `asyncio.create_task` with `asyncio.sleep(settings.heartbeat_interval)`. Use a monotonic millisecond clock: `int(time.time() * 1000)` for `now_ms` (must match the Node `Date.now()` epoch-ms used at admit).

- [ ] **Step 1: Add release to `interview_agent.py` `on_finally`**

The `InterviewAgent` constructor takes a new `limiter` and `room`; `_wrap_up` passes an `on_finally` that releases then signals done:
```python
# in InterviewAgent.__init__ signature add: limiter, room
        self._limiter = limiter
        self._room = room
# replace the end_interview on_finally argument in _wrap_up:
            on_finally=self._on_end_and_release,
# add method:
    def _on_end_and_release(self):
        # release is sync-scheduled; end_interview's finally guarantees this runs
        import asyncio
        asyncio.create_task(self._limiter.release(self._room))
        self._on_end()
```
(Release is fire-and-scheduled so the `finally` is not blocked; the lease backstops it if the task is cut off. `done.set()` via `self._on_end()` still unblocks the worker.)

- [ ] **Step 2: Wire limiter, heartbeat loop, participant signals, reaper in `worker.py`**

In `entrypoint`, after building `redis` and before `session.start`:
```python
    from tara_agent.limiter import Limiter, Caps
    import time
    limiter = Limiter(redis, caps=Caps(
        global_=s.max_global_sessions, gemini_tpm=s.gemini_tpm_budget,
        stt_streams=s.stt_stream_budget, tts_streams=s.tts_stream_budget),
        reservation_ttl_ms=s.reservation_lease_ttl * 1000,
        heartbeat_ttl_ms=s.heartbeat_lease_ttl * 1000)
    now_ms = lambda: int(time.time() * 1000)

    async def _heartbeat_loop():
        while not done.is_set():
            await limiter.heartbeat(ctx.room.name, now_ms())
            await asyncio.sleep(s.heartbeat_interval)

    async def _reaper_loop():
        while not done.is_set():
            for rc in await limiter.reap(now_ms()):
                logging.getLogger("tara.limiter").info("reclaim %s reason=%s", rc.room, rc.reason)
            await asyncio.sleep(s.heartbeat_interval)
```
Pass `limiter=limiter, room=ctx.room.name` into the `InterviewAgent(...)` constructor. After `session.start(...)`, promote the lease and start the loops:
```python
    await limiter.heartbeat(ctx.room.name, now_ms(), mark_heartbeat=True, mark_participant=True)
    hb = asyncio.create_task(_heartbeat_loop())
    rp = asyncio.create_task(_reaper_loop())

    @ctx.room.on("participant_disconnected")
    def _on_part_left(p):
        asyncio.create_task(limiter.release(ctx.room.name))
        done.set()
```
After `await done.wait()`, cancel the loops:
```python
    hb.cancel(); rp.cancel()
```

- [ ] **Step 3: Verify imports + boot (no live LiveKit needed)**

Run: `cd agent && source .venv/bin/activate && python -c "from tara_agent import worker, interview_agent"` → exits 0.
Run: `python -m tara_agent.worker download-files` → completes.
Do NOT block on `worker dev` (needs live creds). Confirm against installed source that the participant-disconnect event name is correct; adjust if 1.0.19 differs and note it.

- [ ] **Step 4: Commit**

```bash
git add agent/tara_agent/worker.py agent/tara_agent/interview_agent.py
git commit -m "feat(agent): wire limiter — heartbeat loop, participant signals, release-on-end, reaper"
```

---

### Task 8: Limiter lifecycle integration test (the Phase-2 gate analog)

**Files:**
- Test: `agent/tests/test_limiter_lifecycle.py`

**Interfaces:**
- Consumes: the full `Limiter` (Tasks 2–5) against `redislite`. No new product code — this is the proof that the subsystem holds, mirroring the spec's Phase-2 acceptance checks (cap holds, no-show/crash/false reclaim, reconnect idempotency, atomicity at the boundary).

- [ ] **Step 1: Write the lifecycle/property test**

```python
# agent/tests/test_limiter_lifecycle.py
import asyncio
import pytest
from tara_agent.limiter import Limiter, Caps, Reclaim

def make(redis, cap=3):
    return Limiter(redis, caps=Caps(cap, cap, cap, cap),
                   reservation_ttl_ms=45000, heartbeat_ttl_ms=60000)

async def test_cap_holds_then_noshow_reclaim_frees_a_slot(real_redis):
    lim = make(real_redis, cap=2)
    assert (await lim.try_admit("a", 1000)).admitted
    assert (await lim.try_admit("b", 1000)).admitted
    assert not (await lim.try_admit("c", 1000)).admitted          # cap holds
    # 'a' is a no-show; next admit after its lease lapses reaps it inline and succeeds
    c2 = await lim.try_admit("c", now_ms=46001)
    assert c2.admitted                                            # slot freed by inline reap
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 2

async def test_crash_vs_false_reclaim_distinguished(real_redis):
    lim = make(real_redis, cap=5)
    await lim.try_admit("crasher", 1000)
    await lim.heartbeat("crasher", 1000, mark_heartbeat=True)      # worker alive, no participant flag
    await lim.try_admit("dropped", 1000)
    await lim.heartbeat("dropped", 1000, mark_heartbeat=True, mark_participant=True)
    out = {r.room: r.reason for r in await lim.reap(now_ms=61001)}
    assert out["crasher"] == "crash"
    assert out["dropped"] == "false"                              # hard-gate signal

async def test_concurrent_admits_never_exceed_cap(real_redis):
    lim = make(real_redis, cap=10)
    res = await asyncio.gather(*[lim.try_admit(f"r{i}", 1000) for i in range(100)])
    assert sum(1 for x in res if x.admitted) == 10
    assert int(await real_redis.get("{tara-limiter}:count:global")) == 10

async def test_full_lifecycle_admit_heartbeat_release(real_redis):
    lim = make(real_redis, cap=1)
    assert (await lim.try_admit("r", 1000)).admitted
    assert await lim.heartbeat("r", 2000, mark_heartbeat=True, mark_participant=True)
    assert await lim.release("r") is True
    assert (await lim.try_admit("r2", 3000)).admitted             # slot returned on clean release
    m = await lim.metrics()
    assert m["reclaim_false"] == 0                                # clean release != reclaim
```

- [ ] **Step 2: Run to green**

Run: `cd agent && pytest tests/test_limiter_lifecycle.py -v`
Expected: PASS (4 passed). This proves: cap holds, inline-reap frees no-show slots, crash≠false classification, atomicity at scale, and clean release produces zero false-reclaims.

- [ ] **Step 3: Commit**

```bash
git add agent/tests/test_limiter_lifecycle.py
git commit -m "test(limiter): lifecycle + property gate — cap holds, reclaim classes, atomicity"
```

---

## Self-Review

**Spec coverage (Phase 2 / §3–§4 + catch #1):**
- Global Redis limiter replacing per-process semaphore → Tasks 2–6.
- Four per-provider buckets, hold all for session life, atomic all-or-nothing acquire → `admit.lua` (Task 2); concurrency test proves cap never exceeded.
- One Lua script each for acquire/release/refresh/reap → Tasks 2/4/3/5.
- Two-tier lease keyed by room_name (Tier-1 45s at admit, Tier-2 60s heartbeat) → `admit.lua` ttl + `refresh.lua` (Tasks 2–3).
- Reconnect idempotency (refresh, no double-count) → Task 3 test + Task 2 `admit.lua` ZSCORE branch.
- Guaranteed release wired into `end_interview` finally → Task 7 (`_on_end_and_release`); the finally + timeouts were built Phase 1.
- Reclaim instrumentation + no_show/crash/false classification + participant-joined correlation → `reap.lua` + Task 5; inline reap in `admit.lua` frees slots on demand (Task 2/8).
- Reject names the (tightest-priority) bucket + per-bucket reject metrics + reclaim metrics → `admit.lua` + `Limiter.metrics` (Tasks 2/5).
- Graceful degradation: queued/503, no token on reject → Task 6.
- Caps/TTLs from env, never hardcoded → Task 1.

**Deferred to later phases (correctly out of Phase 2):** Prometheus export of the reclaim/reject/bucket-utilization metrics and the false-reclaim SLO dashboard → Phase 3 (observability/scaling); the staged load test that calibrates `GEMINI_TPM_BUDGET`/`SESSIONS_PER_POD_TARGET`/warm-pool and reports false-reclaim slope → Phase 3; barge-in + goodbye-line transcript flush + hot-path retries → Phase 4; async scoring/S3/ATS writes → Phase 5. A real queue/backoff for rejected candidates (Phase 2 returns a simple queued/503; retry re-creates a session) is a known simplification flagged here.

**Placeholder scan:** no TBD/TODO/"handle errors" — every Lua script and wrapper method is complete. The cap default values are explicit pre-calibration placeholders documented as such (Task 1) and as a README TODO (Gemini per-project quota). The Task 7 livekit event-name "verify against installed version" is an explicit verification step, not a placeholder.

**Type/name consistency:** `Caps(global_, gemini_tpm, stt_streams, tts_streams)`, `AdmitResult(admitted, state, bucket, counts)`, `Reclaim(room, reason)`, `Limiter.try_admit/heartbeat/release/reap/metrics`, the `{tara-limiter}` key layout, the `admit.lua` KEYS/ARGV order, and the Node `tryAdmit` result shape (`{admitted, state, bucket, counts}`) are consistent across Python (Tasks 2–5,7,8) and Node (Task 6). `now_ms` is epoch-milliseconds in both services (Node `Date.now()`, Python `int(time.time()*1000)`) so lease scores are comparable. `admit.lua`'s inline reap and `reap.lua` use identical classification logic.

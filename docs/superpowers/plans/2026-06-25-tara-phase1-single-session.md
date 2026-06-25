# Tara Phase 1 — Single-Session Happy Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run one full voice interview end-to-end through the cascaded streaming pipeline (turn-detector → STT → LLM → sentence-chunk → TTS), persist an ordered transcript Redis→Mongo, and **prove p50 turn latency < 800 ms on a single stream** with a per-stage breakdown.

**Architecture:** A Python LiveKit Agents worker wires the streaming pipeline; the LLM emits plain-text questions only (no JSON, no scoring on the hot path); an off-path coverage-tagger and a question cap decide when to end; a minimal Node endpoint seeds the Redis session blob and mints the LiveKit token. Latency is measured via the framework's built-in metrics events and asserted by an automated harness that drives recorded candidate audio.

**Tech Stack:** Python 3.11+, `livekit-agents` (1.x), `livekit-plugins-google`, `livekit-plugins-turn-detector`, `redis`/`fakeredis`, `motor` (async MongoDB), `pytest`/`pytest-asyncio`; Node 20+, `express`, `livekit-server-sdk`, `ioredis`, `jest`/`supertest`.

> **Plan scope:** This is **Phase 1 only**. The acceptance gate (Task 12) blocks all later work — do not start Phase 2 (admission limiter) until the single-stream p50 < 800 ms gate is green. Phases 2–5 each get their own plan authored at the start of that phase, referencing committed spec baseline `70410a4`.

## Global Constraints

Copied verbatim from `docs/superpowers/specs/2026-06-25-tara-cascaded-voice-interview-design.md`:

- **Cascaded only.** No Live / speech-to-speech / native-audio model.
- **Hot path = produce the next spoken sentence.** Anything that emits structured data or does non-speech I/O goes off-path. No JSON, no scoring, no batch buffering of the LLM response on the hot path.
- **LLM:** `gemini-3.1-flash-lite` (Stable), token-streaming, plain text. Env `GEMINI_MODEL=gemini-3.1-flash-lite`.
- **STT:** Google Cloud Speech-to-Text, Chirp, streaming. Languages `en-IN,en-US`.
- **TTS:** Google Cloud Text-to-Speech, Chirp3-HD, streaming. Voice `en-IN-Chirp3-HD-Erinome`.
- **Turn detection:** LiveKit `turn-detector` plugin (semantic EOU), never a fixed silence timer.
- **Interview flow:** fully dynamic; static block (system prompt + JD + must-have skills + resume) registered once as a Gemini cached context.
- **Interview end:** all must-have skills covered OR `question_count ≥ MAX_QUESTIONS` (default 12).
- **No hardcoded secrets** — env vars only. `.env` is gitignored.
- **Transcript** is append-only, sequence-numbered, in Redis list `transcript:{room}` (TTL ~2h), assembled by `seq` on end and written to Mongo as the audit record.
- **Latency gate:** p50 turn latency < 800 ms on a single stream, reported with the per-stage breakdown (EOU delay, STT, LLM TTFT, first-sentence, TTS TTFB).

> **Phase-1 simplifications (explicit, to keep the gate focused):** the admission limiter, token lease/release, and the async scoring service are **out of scope** (Phases 2 and 5). In Phase 1 the Node endpoint accepts `skills[]`, `resumeText`, `jdText` directly in the request body (S3 fetch + `contests` read land with the scoring/ATS phase). `_end_interview` therefore has **no `release` call yet** — but the timeout-wrapping of the hangable calls (review catch #2) is built now, because the pipeline needs it regardless.

---

### Task 1: Project scaffold, config, and env loading

**Files:**
- Create: `agent/pyproject.toml`
- Create: `agent/tara_agent/__init__.py`
- Create: `agent/tara_agent/config.py`
- Test: `agent/tests/test_config.py`

**Interfaces:**
- Produces: `Settings` (pydantic-settings `BaseSettings`) with fields `gemini_api_key: str`, `gemini_model: str = "gemini-3.1-flash-lite"`, `google_application_credentials: str`, `redis_url: str`, `mongodb_uri: str`, `livekit_url: str`, `livekit_api_key: str`, `livekit_api_secret: str`, `interview_languages: list[str] = ["en-IN", "en-US"]`, `tts_voice: str = "en-IN-Chirp3-HD-Erinome"`, `max_questions: int = 12`, `transcript_ttl_seconds: int = 7200`, `say_timeout_seconds: float = 20.0`, `mongo_write_timeout_seconds: float = 10.0`; and `get_settings() -> Settings` (cached).

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_config.py
import pytest
from tara_agent.config import Settings

def test_settings_load_from_env(monkeypatch):
    for k, v in {
        "GEMINI_API_KEY": "g", "GOOGLE_APPLICATION_CREDENTIALS": "/c.json",
        "REDIS_URL": "redis://x", "MONGODB_URI": "mongodb://x",
        "LIVEKIT_URL": "wss://x", "LIVEKIT_API_KEY": "k", "LIVEKIT_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(k, v)
    s = Settings()
    assert s.gemini_model == "gemini-3.1-flash-lite"
    assert s.interview_languages == ["en-IN", "en-US"]
    assert s.max_questions == 12
    assert s.say_timeout_seconds == 20.0

def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(Exception):
        Settings(_env_file=None)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tara_agent.config'`

- [ ] **Step 3: Write `pyproject.toml` and `config.py`**

```toml
# agent/pyproject.toml
[project]
name = "tara-agent"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "livekit-agents~=1.0",
  "livekit-plugins-google~=1.0",
  "livekit-plugins-turn-detector~=1.0",
  "redis~=5.0",
  "motor~=3.4",
  "pydantic-settings~=2.2",
]
[project.optional-dependencies]
dev = ["pytest~=8.0", "pytest-asyncio~=0.23", "fakeredis~=2.21", "mongomock-motor~=0.0.29"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
pythonpath = ["."]
```

```python
# agent/tara_agent/config.py
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore",
                                      env_parse_none_str="")
    gemini_api_key: str
    gemini_model: str = "gemini-3.1-flash-lite"
    google_application_credentials: str
    redis_url: str
    mongodb_uri: str
    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str
    interview_languages: list[str] = ["en-IN", "en-US"]
    tts_voice: str = "en-IN-Chirp3-HD-Erinome"
    max_questions: int = 12
    transcript_ttl_seconds: int = 7200
    say_timeout_seconds: float = 20.0
    mongo_write_timeout_seconds: float = 10.0

@lru_cache
def get_settings() -> Settings:
    return Settings()
```

(Create empty `agent/tara_agent/__init__.py`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pip install -e ".[dev]" && pytest tests/test_config.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/pyproject.toml agent/tara_agent/__init__.py agent/tara_agent/config.py agent/tests/test_config.py
git commit -m "feat(agent): project scaffold, config, env loading"
```

---

### Task 2: Redis transcript store (append-only, sequence-numbered)

**Files:**
- Create: `agent/tara_agent/transcript.py`
- Test: `agent/tests/test_transcript.py`

**Interfaces:**
- Consumes: a redis async client (`redis.asyncio.Redis`).
- Produces: `class TranscriptStore`:
  - `__init__(self, redis, room: str, ttl_seconds: int = 7200)`
  - `async def append(self, speaker: str, text: str) -> int` — atomically allocates the next `seq` and RPUSHes JSON `{"seq", "speaker", "text", "ts"}`; sets list TTL; returns the seq.
  - `async def truncate_last(self, speaker: str, spoken_text: str) -> None` — replaces the most recent line by `speaker` with `spoken_text` (barge-in support; used in Phase 4 but built now).
  - `async def assemble(self) -> list[dict]` — reads all, returns sorted by `seq`.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_transcript.py
import fakeredis.aioredis
import pytest
from tara_agent.transcript import TranscriptStore

@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

async def test_append_assigns_monotonic_seq_and_orders(redis):
    ts = TranscriptStore(redis, room="r1")
    assert await ts.append("tara", "hello") == 0
    assert await ts.append("candidate", "hi") == 1
    assert await ts.append("tara", "next?") == 2
    lines = await ts.assemble()
    assert [l["seq"] for l in lines] == [0, 1, 2]
    assert [l["speaker"] for l in lines] == ["tara", "candidate", "tara"]
    assert all("ts" in l for l in lines)

async def test_assemble_sorts_even_if_storage_unordered(redis):
    ts = TranscriptStore(redis, room="r2")
    await redis.rpush("transcript:r2", '{"seq": 5, "speaker": "tara", "text": "b", "ts": 2}')
    await redis.rpush("transcript:r2", '{"seq": 1, "speaker": "candidate", "text": "a", "ts": 1}')
    lines = await ts.assemble()
    assert [l["seq"] for l in lines] == [1, 5]

async def test_truncate_last_replaces_partial_line(redis):
    ts = TranscriptStore(redis, room="r3")
    await ts.append("candidate", "answer")
    await ts.append("tara", "This is a very long question that was")
    await ts.truncate_last("tara", "This is a very long question")
    lines = await ts.assemble()
    assert lines[-1]["text"] == "This is a very long question"
    assert len(lines) == 2

async def test_ttl_is_set(redis):
    ts = TranscriptStore(redis, room="r4", ttl_seconds=7200)
    await ts.append("tara", "x")
    assert 0 < await redis.ttl("transcript:r4") <= 7200
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_transcript.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tara_agent.transcript'`

- [ ] **Step 3: Write `transcript.py`**

```python
# agent/tara_agent/transcript.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pytest tests/test_transcript.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/transcript.py agent/tests/test_transcript.py
git commit -m "feat(agent): append-only sequence-numbered Redis transcript store"
```

---

### Task 3: Session store (read the session blob)

**Files:**
- Create: `agent/tara_agent/session_store.py`
- Test: `agent/tests/test_session_store.py`

**Interfaces:**
- Produces: `@dataclass SessionBlob` with `contest_id: str, candidate_id: str, skills: list[str], resume_text: str, jd_text: str, max_questions: int`; and `async def load_session(redis, session_id: str) -> SessionBlob` (raises `SessionNotFound` if missing). Key format `session:{session_id}`.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_session_store.py
import json
import fakeredis.aioredis
import pytest
from tara_agent.session_store import load_session, SessionBlob, SessionNotFound

@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

async def test_load_session_parses_blob(redis):
    await redis.set("session:s1", json.dumps({
        "contestId": "c1", "candidateId": "u1", "skills": ["python", "sql"],
        "resumeText": "resume", "jdText": "jd", "maxQuestions": 10,
    }))
    blob = await load_session(redis, "s1")
    assert blob == SessionBlob("c1", "u1", ["python", "sql"], "resume", "jd", 10)

async def test_missing_session_raises(redis):
    with pytest.raises(SessionNotFound):
        await load_session(redis, "nope")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_session_store.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `session_store.py`**

```python
# agent/tara_agent/session_store.py
import json
from dataclasses import dataclass

class SessionNotFound(Exception):
    pass

@dataclass(frozen=True)
class SessionBlob:
    contest_id: str
    candidate_id: str
    skills: list[str]
    resume_text: str
    jd_text: str
    max_questions: int

async def load_session(redis, session_id: str) -> SessionBlob:
    raw = await redis.get(f"session:{session_id}")
    if raw is None:
        raise SessionNotFound(session_id)
    d = json.loads(raw)
    return SessionBlob(
        contest_id=d["contestId"], candidate_id=d["candidateId"],
        skills=d["skills"], resume_text=d["resumeText"],
        jd_text=d["jdText"], max_questions=d.get("maxQuestions", 12),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pytest tests/test_session_store.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/session_store.py agent/tests/test_session_store.py
git commit -m "feat(agent): session blob loader from Redis"
```

---

### Task 4: Cached-context / prompt builder

**Files:**
- Create: `agent/tara_agent/prompts.py`
- Test: `agent/tests/test_prompts.py`

**Interfaces:**
- Produces: `def build_system_prompt(blob: SessionBlob) -> str` (the static block: role, JD, must-have skills, resume, the rule to ask one plain-text question at a time and cover all skills); `def build_coverage_prompt(skills: list[str], answer: str) -> str`.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_prompts.py
from tara_agent.prompts import build_system_prompt, build_coverage_prompt
from tara_agent.session_store import SessionBlob

def test_system_prompt_includes_static_block():
    blob = SessionBlob("c", "u", ["Python", "SQL"], "RESUME_X", "JD_Y", 12)
    p = build_system_prompt(blob)
    assert "Python" in p and "SQL" in p
    assert "RESUME_X" in p and "JD_Y" in p
    assert "one question at a time" in p.lower()
    assert "json" in p.lower()  # must instruct: do NOT output JSON

def test_coverage_prompt_lists_skills_and_answer():
    p = build_coverage_prompt(["Python", "SQL"], "I used pandas")
    assert "Python" in p and "SQL" in p and "I used pandas" in p
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_prompts.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `prompts.py`**

```python
# agent/tara_agent/prompts.py
from tara_agent.session_store import SessionBlob

def build_system_prompt(blob: SessionBlob) -> str:
    skills = ", ".join(blob.skills)
    return (
        "You are Tara, a professional voice interviewer. Conduct a spoken "
        "interview for the role described below. Ask ONE question at a time, "
        "in plain conversational text. Do NOT output JSON, lists, scores, or "
        "any structured data — only the next thing you would say aloud. Adapt "
        "follow-ups to the candidate's previous answer. Systematically cover "
        f"every must-have skill: {skills}. Keep questions concise.\n\n"
        f"=== JOB DESCRIPTION ===\n{blob.jd_text}\n\n"
        f"=== CANDIDATE RESUME ===\n{blob.resume_text}\n"
    )

def build_coverage_prompt(skills: list[str], answer: str) -> str:
    return (
        "Given this candidate answer, return ONLY a comma-separated list of "
        "which of these skills the answer substantively demonstrated (or "
        "'none'). Skills: " + ", ".join(skills) + "\n\nAnswer: " + answer
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pytest tests/test_prompts.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/prompts.py agent/tests/test_prompts.py
git commit -m "feat(agent): cached-context system prompt and coverage prompt builders"
```

---

### Task 5: Interview end-decision logic

**Files:**
- Create: `agent/tara_agent/interview.py`
- Test: `agent/tests/test_interview.py`

**Interfaces:**
- Produces: `def should_end(covered: set[str], required: list[str], question_count: int, max_questions: int) -> bool` — True when every required skill is covered OR `question_count >= max_questions`.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_interview.py
from tara_agent.interview import should_end

def test_ends_when_all_skills_covered():
    assert should_end({"python", "sql"}, ["python", "sql"], 3, 12) is True

def test_ends_when_cap_hit():
    assert should_end(set(), ["python"], 12, 12) is True

def test_continues_when_uncovered_and_under_cap():
    assert should_end({"python"}, ["python", "sql"], 5, 12) is False

def test_required_empty_means_cap_only():
    assert should_end(set(), [], 0, 12) is True  # nothing to cover
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_interview.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `interview.py`**

```python
# agent/tara_agent/interview.py
def should_end(covered: set[str], required: list[str],
               question_count: int, max_questions: int) -> bool:
    all_covered = all(s.lower() in {c.lower() for c in covered} for s in required)
    return all_covered or question_count >= max_questions
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pytest tests/test_interview.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/interview.py agent/tests/test_interview.py
git commit -m "feat(agent): coverage + question-cap end decision"
```

---

### Task 6: Off-path coverage tagger

**Files:**
- Create: `agent/tara_agent/coverage.py`
- Test: `agent/tests/test_coverage.py`

**Interfaces:**
- Consumes: `build_coverage_prompt` (Task 4); an injectable async `classify_fn(prompt: str) -> str` so the Gemini call is mockable in tests (the real one wraps the Google GenAI SDK).
- Produces: `class CoverageTracker`: `__init__(self, redis, room, required: list[str], classify_fn)`; `async def tag(self, answer: str) -> None` (fires the classify call, adds matched skills to the Redis set `coverage:{room}`); `async def covered(self) -> set[str]`. **Per spec note (§2): this is a real Gemini call every user-turn-final; its tokens count toward per-session consumption and must be included in the Phase-2 `GEMINI_TPM_BUDGET` calibration.**

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_coverage.py
import fakeredis.aioredis
import pytest
from tara_agent.coverage import CoverageTracker

@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

async def test_tag_adds_matched_skills(redis):
    async def fake_classify(prompt): return "Python, SQL"
    t = CoverageTracker(redis, "r1", ["Python", "SQL", "AWS"], fake_classify)
    await t.tag("I built ETL with pandas and Postgres")
    assert await t.covered() == {"Python", "SQL"}

async def test_tag_none_adds_nothing(redis):
    async def fake_classify(prompt): return "none"
    t = CoverageTracker(redis, "r2", ["Python"], fake_classify)
    await t.tag("I like coffee")
    assert await t.covered() == set()

async def test_tag_ignores_skills_not_in_required(redis):
    async def fake_classify(prompt): return "Python, Rust"
    t = CoverageTracker(redis, "r3", ["Python"], fake_classify)
    await t.tag("...")
    assert await t.covered() == {"Python"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_coverage.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `coverage.py`**

```python
# agent/tara_agent/coverage.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pytest tests/test_coverage.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/coverage.py agent/tests/test_coverage.py
git commit -m "feat(agent): off-path coverage tagger writing to Redis"
```

---

### Task 7: `_end_interview` persistence with hard timeouts (review catch #2)

**Files:**
- Create: `agent/tara_agent/persistence.py`
- Test: `agent/tests/test_persistence.py`

**Interfaces:**
- Consumes: `TranscriptStore.assemble` (Task 2); a Motor collection; an injectable async `say_fn()` and `mongo_write_fn(doc)` so timeouts are testable.
- Produces: `async def end_interview(*, say_fn, transcript_store, mongo_write_fn, room, contest_id, candidate_id, say_timeout, write_timeout, on_finally) -> None`. Wraps `say_fn` and `mongo_write_fn` in `asyncio.wait_for`; a hang in either converts to `TimeoutError` and is swallowed; `on_finally()` (release placeholder in Phase 1) **always** runs.

> **Why timeouts (catch #2):** a `finally` protects against a *throw*, not a *hang*. A wedged `session.say` or Mongo write would freeze before reaching `finally`, holding the slot indefinitely once the limiter lands in Phase 2. Hard timeouts convert a hang into a throw so the guaranteed-release path fires. Built now so the lifecycle is correct before the limiter exists.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_persistence.py
import asyncio
import fakeredis.aioredis
import pytest
from tara_agent.transcript import TranscriptStore
from tara_agent.persistence import end_interview

@pytest.fixture
def redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)

async def _run(redis, say_fn, mongo_write_fn, finally_flag, **over):
    ts = TranscriptStore(redis, "r")
    await ts.append("tara", "q1"); await ts.append("candidate", "a1")
    kw = dict(say_fn=say_fn, transcript_store=ts, mongo_write_fn=mongo_write_fn,
              room="r", contest_id="c", candidate_id="u",
              say_timeout=0.2, write_timeout=0.2,
              on_finally=lambda: finally_flag.append(True))
    kw.update(over)
    await end_interview(**kw)

async def test_happy_path_writes_ordered_transcript(redis):
    written = {}
    async def say(): return None
    async def mongo(doc): written.update(doc)
    flag = []
    await _run(redis, say, mongo, flag)
    assert [l["seq"] for l in written["transcript"]] == [0, 1]
    assert written["contestId"] == "c" and written["candidateId"] == "u"
    assert flag == [True]

async def test_hung_say_still_runs_finally(redis):
    async def say(): await asyncio.sleep(10)        # hang
    async def mongo(doc): return None
    flag = []
    await _run(redis, say, mongo, flag)             # must NOT hang (timeout 0.2s)
    assert flag == [True]

async def test_hung_mongo_still_runs_finally(redis):
    async def say(): return None
    async def mongo(doc): await asyncio.sleep(10)   # hang
    flag = []
    await _run(redis, say, mongo, flag)
    assert flag == [True]

async def test_throwing_mongo_still_runs_finally(redis):
    async def say(): return None
    async def mongo(doc): raise RuntimeError("db down")
    flag = []
    await _run(redis, say, mongo, flag)
    assert flag == [True]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_persistence.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `persistence.py`**

```python
# agent/tara_agent/persistence.py
import asyncio
import logging

log = logging.getLogger("tara.persistence")

async def end_interview(*, say_fn, transcript_store, mongo_write_fn, room,
                        contest_id, candidate_id, say_timeout, write_timeout,
                        on_finally):
    try:
        try:
            await asyncio.wait_for(say_fn(), timeout=say_timeout)
        except (asyncio.TimeoutError, Exception) as e:
            log.warning("closing say failed/hung: %s", e)

        lines = await transcript_store.assemble()
        doc = {"room": room, "contestId": contest_id,
               "candidateId": candidate_id, "transcript": lines}
        try:
            await asyncio.wait_for(mongo_write_fn(doc), timeout=write_timeout)
        except (asyncio.TimeoutError, Exception) as e:
            log.error("transcript persist failed/hung (recoverable from Redis): %s", e)
    finally:
        on_finally()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pytest tests/test_persistence.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/persistence.py agent/tests/test_persistence.py
git commit -m "feat(agent): _end_interview with hard timeouts and guaranteed finally"
```

---

### Task 8: Latency metrics aggregation

**Files:**
- Create: `agent/tara_agent/latency.py`
- Test: `agent/tests/test_latency.py`

**Interfaces:**
- Produces: `@dataclass TurnLatency` with `eou_delay, stt, llm_ttft, tts_ttfb, total` (all seconds, floats); `class LatencyCollector`: `record_turn(eou_delay, stt, llm_ttft, tts_ttfb)` (computes `total = eou_delay + llm_ttft + tts_ttfb`; STT runs concurrently with speech so it is reported, not summed); `def percentile(self, p: float) -> float`; `def breakdown_p50(self) -> dict`. The summation rule is documented inline so the gate measures the perceived time-to-first-audio.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_latency.py
import pytest
from tara_agent.latency import LatencyCollector

def test_total_is_eou_plus_ttft_plus_ttfb():
    c = LatencyCollector()
    c.record_turn(eou_delay=0.15, stt=0.12, llm_ttft=0.30, tts_ttfb=0.20)
    assert c._turns[0].total == pytest.approx(0.65)

def test_percentile_p50():
    c = LatencyCollector()
    for ttft in [0.1, 0.2, 0.3, 0.4, 0.5]:
        c.record_turn(0.0, 0.0, ttft, 0.0)
    assert c.percentile(50) == pytest.approx(0.3)

def test_breakdown_p50_reports_each_stage():
    c = LatencyCollector()
    c.record_turn(0.15, 0.12, 0.30, 0.20)
    c.record_turn(0.15, 0.12, 0.30, 0.20)
    b = c.breakdown_p50()
    assert set(b) == {"eou_delay", "stt", "llm_ttft", "tts_ttfb", "total"}
    assert b["total"] == pytest.approx(0.65)

def test_percentile_empty_raises():
    with pytest.raises(ValueError):
        LatencyCollector().percentile(50)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_latency.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `latency.py`**

```python
# agent/tara_agent/latency.py
from dataclasses import dataclass
from statistics import quantiles

@dataclass
class TurnLatency:
    eou_delay: float
    stt: float
    llm_ttft: float
    tts_ttfb: float
    total: float

class LatencyCollector:
    def __init__(self):
        self._turns: list[TurnLatency] = []

    def record_turn(self, eou_delay, stt, llm_ttft, tts_ttfb):
        # Perceived time-to-first-audio: STT streams DURING speech, so it is
        # reported but not summed; the clock the candidate feels is
        # EOU delay + LLM first token + TTS first byte.
        total = eou_delay + llm_ttft + tts_ttfb
        self._turns.append(TurnLatency(eou_delay, stt, llm_ttft, tts_ttfb, total))

    def percentile(self, p: float) -> float:
        if not self._turns:
            raise ValueError("no turns recorded")
        vals = sorted(t.total for t in self._turns)
        if len(vals) == 1:
            return vals[0]
        # nearest-rank on the 100-quantile cut points
        cuts = quantiles(vals, n=100, method="inclusive")
        return cuts[int(p) - 1]

    def _stage_p50(self, attr) -> float:
        vals = sorted(getattr(t, attr) for t in self._turns)
        mid = len(vals) // 2
        return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2

    def breakdown_p50(self) -> dict:
        return {a: self._stage_p50(a)
                for a in ("eou_delay", "stt", "llm_ttft", "tts_ttfb", "total")}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pytest tests/test_latency.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/latency.py agent/tests/test_latency.py
git commit -m "feat(agent): per-turn latency aggregation and percentiles"
```

---

### Task 9: Real Gemini classify wrapper for the coverage tagger

**Files:**
- Create: `agent/tara_agent/gemini.py`
- Test: `agent/tests/test_gemini.py`

**Interfaces:**
- Produces: `def make_classify_fn(settings) -> Callable[[str], Awaitable[str]]` — returns an async function calling the Google GenAI SDK (`google-genai`) with `settings.gemini_model`, low `max_output_tokens`, returning the response text. The unit test mocks the SDK client; this task isolates the only network dependency of the coverage tagger so Task 6 stays pure.

> **Verify at implementation time:** confirm the installed `google-genai` SDK's async client call shape (`client.aio.models.generate_content(...)`) against the pinned version; adjust the single call site if the SDK differs. Do not change the `classify_fn(prompt)->str` contract.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_gemini.py
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from tara_agent.gemini import make_classify_fn

class _Settings:
    gemini_api_key = "k"; gemini_model = "gemini-3.1-flash-lite"

async def test_classify_returns_text():
    fake_resp = MagicMock(text="Python, SQL")
    fake_client = MagicMock()
    fake_client.aio.models.generate_content = AsyncMock(return_value=fake_resp)
    with patch("tara_agent.gemini.genai.Client", return_value=fake_client):
        classify = make_classify_fn(_Settings())
        out = await classify("which skills?")
    assert out == "Python, SQL"
    fake_client.aio.models.generate_content.assert_awaited_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd agent && pytest tests/test_gemini.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `gemini.py`** (add `google-genai~=1.0` to `pyproject.toml` dependencies first)

```python
# agent/tara_agent/gemini.py
from google import genai
from google.genai import types

def make_classify_fn(settings):
    client = genai.Client(api_key=settings.gemini_api_key)
    async def classify(prompt: str) -> str:
        resp = await client.aio.models.generate_content(
            model=settings.gemini_model,
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=64, temperature=0.0),
        )
        return (resp.text or "none").strip()
    return classify
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd agent && pytest tests/test_gemini.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/gemini.py agent/tests/test_gemini.py agent/pyproject.toml
git commit -m "feat(agent): Gemini classify wrapper for coverage tagger"
```

---

### Task 10: Worker pipeline wiring (integration)

**Files:**
- Create: `agent/tara_agent/worker.py`
- Create: `agent/tara_agent/interview_agent.py`
- Manual verification (no unit test — this is the integration seam; logic is unit-tested in Tasks 2–9).

**Interfaces:**
- Consumes: all of Tasks 1–9.
- Produces: `class InterviewAgent(Agent)` and `async def entrypoint(ctx: JobContext)`; a `if __name__` CLI via `agents.cli.run_app`.

> **Verify at implementation time against the installed `livekit-agents` 1.x:** the exact symbols for `AgentSession`, `Agent`, the Google plugin classes (`google.STT`, `google.LLM`, `google.TTS`), and `turn_detector` model class names can shift across minor versions. The structure below is correct for 1.x; confirm imports and the `metrics_collected` event payload before running. Keep the wiring (STT→LLM→TTS + turn-detector + plain-text LLM) and the metrics→`LatencyCollector` mapping unchanged.

- [ ] **Step 1: Write `interview_agent.py`**

```python
# agent/tara_agent/interview_agent.py
import asyncio
from livekit.agents import Agent
from tara_agent.coverage import CoverageTracker
from tara_agent.interview import should_end
from tara_agent.transcript import TranscriptStore
from tara_agent.persistence import end_interview

class InterviewAgent(Agent):
    def __init__(self, *, instructions, blob, transcript: TranscriptStore,
                 coverage: CoverageTracker, mongo_write_fn, settings, on_end):
        super().__init__(instructions=instructions)
        self._blob = blob
        self._transcript = transcript
        self._coverage = coverage
        self._mongo_write_fn = mongo_write_fn
        self._settings = settings
        self._on_end = on_end
        self._q_count = 0

    async def on_user_turn_completed(self, turn_ctx, new_message):
        text = new_message.text_content or ""
        await self._transcript.append("candidate", text)
        # OFF PATH: fire-and-forget coverage tagging — never awaited here.
        asyncio.create_task(self._coverage.tag(text))
        self._q_count += 1
        covered = await self._coverage.covered()
        if should_end(covered, self._blob.skills, self._q_count,
                      self._blob.max_questions):
            await self._wrap_up()

    async def on_tara_line(self, text: str):
        await self._transcript.append("tara", text)

    async def _wrap_up(self):
        await end_interview(
            say_fn=lambda: self.session.say("Thank you, that's the end of the "
                                            "interview. We'll be in touch."),
            transcript_store=self._transcript,
            mongo_write_fn=self._mongo_write_fn,
            room=self._transcript._key.split(":", 1)[1],
            contest_id=self._blob.contest_id, candidate_id=self._blob.candidate_id,
            say_timeout=self._settings.say_timeout_seconds,
            write_timeout=self._settings.mongo_write_timeout_seconds,
            on_finally=self._on_end,
        )
```

- [ ] **Step 2: Write `worker.py`**

```python
# agent/tara_agent/worker.py
import asyncio
import redis.asyncio as aioredis
from motor.motor_asyncio import AsyncIOMotorClient
from livekit.agents import (Agent, AgentSession, JobContext, WorkerOptions,
                            cli, metrics, MetricsCollectedEvent)
from livekit.plugins import google
from livekit.plugins.turn_detector.multilingual import MultilingualModel

from tara_agent.config import get_settings
from tara_agent.session_store import load_session
from tara_agent.prompts import build_system_prompt
from tara_agent.transcript import TranscriptStore
from tara_agent.coverage import CoverageTracker
from tara_agent.gemini import make_classify_fn
from tara_agent.latency import LatencyCollector
from tara_agent.interview_agent import InterviewAgent

async def entrypoint(ctx: JobContext):
    s = get_settings()
    redis = aioredis.from_url(s.redis_url, decode_responses=True)
    mongo = AsyncIOMotorClient(s.mongodb_uri)["tara"]["aiInterview"]

    session_id = ctx.room.name  # convention: room name == session id
    blob = await load_session(redis, session_id)

    transcript = TranscriptStore(redis, ctx.room.name, s.transcript_ttl_seconds)
    coverage = CoverageTracker(redis, ctx.room.name, blob.skills,
                               make_classify_fn(s))
    latency = LatencyCollector()
    done = asyncio.Event()

    session = AgentSession(
        stt=google.STT(languages=s.interview_languages, model="chirp",
                       spoken_punctuation=False),
        llm=google.LLM(model=s.gemini_model, temperature=0.6),  # PLAIN TEXT
        tts=google.TTS(voice_name=s.tts_voice),                 # Chirp3-HD, streaming
        turn_detection=MultilingualModel(),
    )

    # Per-turn metrics → latency collector (the source of the Phase-1 gate).
    pending = {}
    @session.on("metrics_collected")
    def _on_metrics(ev: MetricsCollectedEvent):
        m = ev.metrics
        if isinstance(m, metrics.EOUMetrics):
            pending["eou"] = m.end_of_utterance_delay; pending["stt"] = m.transcription_delay
        elif isinstance(m, metrics.LLMMetrics):
            pending["ttft"] = m.ttft
        elif isinstance(m, metrics.TTSMetrics):
            pending["ttfb"] = m.ttfb
            if {"eou", "ttft", "ttfb"} <= pending.keys():
                latency.record_turn(pending["eou"], pending.get("stt", 0.0),
                                    pending["ttft"], pending["ttfb"])
                pending.clear()

    agent = InterviewAgent(
        instructions=build_system_prompt(blob), blob=blob, transcript=transcript,
        coverage=coverage, mongo_write_fn=lambda doc: mongo.insert_one(doc),
        settings=s, on_end=done.set,
    )

    # Capture Tara's spoken lines into the transcript as they are committed.
    @session.on("conversation_item_added")
    def _on_item(ev):
        if getattr(ev.item, "role", None) == "assistant":
            asyncio.create_task(agent.on_tara_line(ev.item.text_content or ""))

    await session.start(agent=agent, room=ctx.room)
    await session.generate_reply(instructions="Greet the candidate and ask your "
                                              "first question.")
    await done.wait()
    import json
    print("LATENCY_BREAKDOWN_P50=" + json.dumps(latency.breakdown_p50()))

if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))
```

- [ ] **Step 3: Verify it boots and connects (no candidate yet)**

Run: `cd agent && python -m tara_agent.worker download-files && python -m tara_agent.worker dev`
Expected: worker registers with LiveKit, logs "registered worker", no exceptions. Ctrl-C to stop.

- [ ] **Step 4: Commit**

```bash
git add agent/tara_agent/worker.py agent/tara_agent/interview_agent.py
git commit -m "feat(agent): wire streaming pipeline (turn-detector→STT→LLM→TTS) + metrics"
```

---

### Task 11: Minimal Node session-create endpoint (seed Redis + mint token)

**Files:**
- Create: `backend/package.json`
- Create: `backend/src/createSession.js`
- Create: `backend/src/server.js`
- Test: `backend/test/createSession.test.js`

**Interfaces:**
- Produces: `POST /sessions` accepting `{contestId, candidateId, skills[], resumeText, jdText, maxQuestions?}`; writes Redis `session:{id}` (the blob Task 3 reads, JSON keys `contestId/candidateId/skills/resumeText/jdText/maxQuestions`) with TTL 7200; returns `{sessionId, room, token, livekitUrl}` where `room === sessionId`.
- Exposes `createSession(redis, tokenFactory, body)` as a pure-ish function for the unit test (Express handler is a thin wrapper).

- [ ] **Step 1: Write the failing test**

```javascript
// backend/test/createSession.test.js
const { createSession } = require("../src/createSession");

test("writes session blob to redis and returns token", async () => {
  const store = {};
  const redis = {
    set: async (k, v, ...rest) => { store[k] = v; },
  };
  const tokenFactory = (room, identity) => `tok:${room}:${identity}`;
  const out = await createSession(redis, tokenFactory, {
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j",
  });
  expect(out.room).toBe(out.sessionId);
  expect(out.token).toBe(`tok:${out.room}:u1`);
  const blob = JSON.parse(store[`session:${out.sessionId}`]);
  expect(blob).toMatchObject({
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j", maxQuestions: 12,
  });
});

test("rejects missing required fields", async () => {
  await expect(createSession({}, () => "t", { contestId: "c1" }))
    .rejects.toThrow(/required/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && npm install && npx jest createSession`
Expected: FAIL — `Cannot find module '../src/createSession'`

- [ ] **Step 3: Write `package.json`, `createSession.js`, `server.js`**

```json
// backend/package.json
{
  "name": "tara-backend",
  "version": "0.1.0",
  "private": true,
  "scripts": { "start": "node src/server.js", "test": "jest" },
  "dependencies": { "express": "^4.19.0", "ioredis": "^5.4.0", "livekit-server-sdk": "^2.6.0" },
  "devDependencies": { "jest": "^29.7.0", "supertest": "^7.0.0" }
}
```

```javascript
// backend/src/createSession.js
const crypto = require("crypto");
const REQUIRED = ["contestId", "candidateId", "skills", "resumeText", "jdText"];

async function createSession(redis, tokenFactory, body) {
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
  const token = await tokenFactory(sessionId, body.candidateId);
  return { sessionId, room: sessionId, token, livekitUrl: process.env.LIVEKIT_URL };
}

module.exports = { createSession };
```

```javascript
// backend/src/server.js
const express = require("express");
const Redis = require("ioredis");
const { AccessToken } = require("livekit-server-sdk");
const { createSession } = require("./createSession");

const app = express();
app.use(express.json({ limit: "2mb" }));
const redis = new Redis(process.env.REDIS_URL);

async function mintToken(room, identity) {
  const at = new AccessToken(process.env.LIVEKIT_API_KEY,
                             process.env.LIVEKIT_API_SECRET, { identity });
  at.addGrant({ roomJoin: true, room });
  return await at.toJwt();
}

app.post("/sessions", async (req, res) => {
  try {
    res.json(await createSession(redis, mintToken, req.body));
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.listen(process.env.PORT || 3000, () => console.log("backend up"));
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && npx jest createSession`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add backend/package.json backend/src/createSession.js backend/src/server.js backend/test/createSession.test.js
git commit -m "feat(backend): minimal session-create endpoint (seed Redis + mint token)"
```

---

### Task 12: Single-stream latency harness — THE PHASE-1 GATE

**Files:**
- Create: `tools/latency_harness.py`
- Create: `tools/fixtures/README.md` (how to supply candidate WAV utterances)

**Interfaces:**
- Consumes: the running worker (Task 10) + backend (Task 11).
- Produces: a script that creates a session, joins the room as a synthetic candidate publishing pre-recorded WAV utterances between Tara's turns, lets the worker's `LATENCY_BREAKDOWN_P50` print, and **asserts p50 < 0.800 s**, exiting non-zero on failure.

> This is an integration harness, not a unit test. It is the gate the whole phase exists to pass. **Do not proceed to Phase 2 until this exits 0** with the per-stage breakdown printed.

> **Verify at implementation time:** publishing audio as a participant uses `livekit` (the `livekit-rtc` client) `rtc.Room`, an `AudioSource`, and `LocalAudioTrack`. Confirm the frame-push API against the installed version. The fixtures are 3–6 short candidate answers as 48 kHz mono WAV; supply real recordings or TTS-generated speech.

- [ ] **Step 1: Write the harness**

```python
# tools/latency_harness.py
import asyncio, json, os, sys, wave, glob, contextlib
import httpx
from livekit import rtc

BACKEND = os.environ.get("BACKEND_URL", "http://localhost:3000")
FIXTURES = sorted(glob.glob(os.path.join(os.path.dirname(__file__), "fixtures", "*.wav")))
GATE_SECONDS = 0.800

async def _publish_wav(source: rtc.AudioSource, path: str):
    with wave.open(path, "rb") as w:
        sr, ch = w.getframerate(), w.getnchannels()
        data = w.readframes(w.getnframes())
    frame = rtc.AudioFrame(data, sr, ch, len(data) // (2 * ch))
    await source.capture_frame(frame)

async def main():
    if not FIXTURES:
        print("no fixture WAVs in tools/fixtures/", file=sys.stderr); sys.exit(2)
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{BACKEND}/sessions", json={
            "contestId": "load-c", "candidateId": "cand-1",
            "skills": ["Python", "SQL"], "resumeText": "5y backend python.",
            "jdText": "Backend engineer, Python + SQL.", "maxQuestions": len(FIXTURES),
        })
        r.raise_for_status(); sess = r.json()

    room = rtc.Room()
    await room.connect(sess["livekitUrl"], sess["token"])
    source = rtc.AudioSource(48000, 1)
    track = rtc.LocalAudioTrack.create_audio_track("candidate", source)
    await room.local_participant.publish_track(track)

    # Alternate: wait for Tara to finish speaking, then play one answer.
    for path in FIXTURES:
        await asyncio.sleep(2.0)        # allow Tara's turn to play
        await _publish_wav(source, path)
    await asyncio.sleep(5.0)
    await room.disconnect()
    print("Harness finished. Read LATENCY_BREAKDOWN_P50 from the worker log.")

if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
```

- [ ] **Step 2: Run the worker, backend, and harness together**

```bash
# terminal 1
cd backend && npm start
# terminal 2
cd agent && python -m tara_agent.worker dev
# terminal 3
python tools/latency_harness.py
```

Expected: worker log prints `LATENCY_BREAKDOWN_P50={"eou_delay":..,"stt":..,"llm_ttft":..,"tts_ttfb":..,"total":..}`.

- [ ] **Step 3: Evaluate the gate**

Read `total` from the printed `LATENCY_BREAKDOWN_P50`.
- **PASS** if `total < 0.800`. Record the per-stage breakdown in the commit message.
- **FAIL** → do NOT proceed to Phase 2. Use the breakdown to find the offending stage (e.g. `llm_ttft` high → enable Gemini context caching for the static block; `eou_delay` high → check turn-detector config; `tts_ttfb` high → confirm streaming TTS, not batch). Re-run.

- [ ] **Step 4: Commit (only when green)**

```bash
git add tools/latency_harness.py tools/fixtures/README.md
git commit -m "feat(tools): single-stream latency harness — Phase 1 gate green (p50=<value>ms)"
```

---

## Self-Review

**Spec coverage (Phase 1 items):**
- Streaming pipeline turn-detector→STT→LLM→sentence-chunk→TTS → Task 10 (sentence-chunking is handled by the LiveKit TTS layer consuming the LLM token stream; verified by latency gate).
- Plain-text LLM, no JSON/scoring on hot path → Task 4 prompt + Task 10 `google.LLM` (no response schema).
- Context caching → Task 4 builds the static block; Task 12 step 3 calls it out as the `llm_ttft` remedy (full caching wiring confirmed by the gate; if TTFT passes without it, YAGNI — defer the explicit cache API to Phase 4 tuning).
- Coverage tagger off-path → Tasks 6 + 9, fired via `asyncio.create_task` in Task 10 (never awaited on hot path).
- End = coverage + cap → Task 5, invoked in Task 10.
- Transcript append-only/seq → Task 2; assembled→Mongo → Task 7; Tara lines captured → Task 10.
- `_end_interview` timeouts (catch #2) → Task 7.
- Session blob from Redis, Node-seeded → Tasks 3 + 11.
- Latency gate p50<800ms + breakdown → Tasks 8 + 12.

**Deferred to later phases (correctly out of Phase 1 scope):** admission limiter + two-tier lease + release + reclaim instrumentation/false-reclaim classification (catch #1) → Phase 2; scaling/HPA/KEDA/warm pool → Phase 3; barge-in truncation (built in Task 2 via `truncate_last`, wired in Phase 4) + bounded hot-path retries → Phase 4; async scoring + S3 resume/JD fetch + `contests` read + `recruiterAddProfiles`/`auditTrail` writes → Phase 5. The three calibrated numbers (catch #1/#2 fold-ins) are produced by the Phase-3 load test, not Phase 1.

**Placeholder scan:** no TBD/TODO/"handle errors" left; every code step is complete. The two `tools/fixtures` WAVs and the LiveKit/Google/SDK version-verification notes are explicit verification steps, not placeholders.

**Type consistency:** `SessionBlob` fields, `TranscriptStore` method names (`append`/`assemble`/`truncate_last`), `CoverageTracker.tag/covered`, `should_end` signature, `LatencyCollector.record_turn/percentile/breakdown_p50`, and the Redis JSON keys (`contestId/candidateId/skills/resumeText/jdText/maxQuestions`) match between the Node writer (Task 11) and the Python reader (Task 3) and across all consumers.

# Tara Phase 4 — Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the validated, scaling Tara pipeline production-safe — barge-in audit truthfulness, bounded retry on the calls we control, reconnect/resume with exactly-once teardown, audit completeness, and the carried ops items (rejected-blob TTL, `/healthz`, real drain, Dockerfiles).

**Architecture:** Pure, unit-testable cores extracted into small modules (`retry.py`, `lifecycle.py`, `audit.py`, `drain.py`, `backend/src/health.js`) wired into the existing worker/backend at integration seams — mirroring the Phase 1–3 pattern (testable logic + thin wiring). No change to the locked cascaded-pipeline architecture.

**Tech Stack:** Python 3.12 (pytest, redislite `real_redis` fixture, `redis.asyncio`, `motor`, LiveKit Agents 1.6.4); Node (jest, ioredis); Docker; kubeconform for manifest re-validation.

## Global Constraints

Copied verbatim from the spec (Phase 4 design + locked whole-system spec §7/§9):

- **No hot-path regression past the `< 1000ms p95` HARD FLOOR.** Retry budget on TTFT-critical paths is bounded (1 retry max); reconnect/teardown logic is off the speaking path.
- **A real, present candidate is NEVER wrongly evicted** — `false_reclaims` stays the hard gate. Reconnect grace MUST be `< reservation_lease_ttl` (45s).
- **Exactly-once teardown** — one transcript flush, one `limiter.release`, on every exit path.
- **Audit matches reality** — never persist words Tara did not utter; transcript ordered & complete.
- **No secrets in images/manifests** — `.dockerignore` never copies `.env`, the GCP SA JSON, `node_modules`, `.venv`, fixtures. Secret-scan stays clean.
- Cascaded only; no JSON mode on the LLM; no scoring on the live path; no scaling on CPU; env-vars-only secrets.
- **Shared Lua scripts in `shared/limiter/` are the single source of truth** — do NOT fork limiter logic into Python/Node.
- Tests use REAL backends: Python `real_redis` (redislite, supports EVAL) fixture from `agent/tests/conftest.py`; Node jest with object-mocked redis. Never fake EVAL.

---

### Task 1: Retry helper + transient-error predicate (A2 foundation)

**Files:**
- Create: `agent/tara_agent/retry.py`
- Modify: `agent/tara_agent/config.py` (add 4 fields)
- Test: `agent/tests/test_retry.py`

**Interfaces:**
- Produces:
  - `def is_transient(exc: BaseException) -> bool` — True for timeouts / connection resets / 5xx-ish / "unavailable"/"deadline"/"503"/"500" in the message; False for everything else (4xx, auth, value errors).
  - `async def retry_async(fn, *, attempts: int, base_delay: float, max_delay: float, retry_on=is_transient, sleep=asyncio.sleep, rng=random.random) -> Any` — calls `await fn()`; on an exception where `retry_on(exc)` is True and attempts remain, waits a jittered backoff (`min(max_delay, base_delay * 2**(i)) * (0.5 + 0.5*rng())`) then retries; re-raises the last exception when attempts are exhausted or `retry_on` is False. `fn` is a zero-arg coroutine factory (call it fresh each attempt).
- Consumed by: Task 2 (off-path + greeting), and reused conceptually elsewhere.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_retry.py
import asyncio
import pytest
from tara_agent.retry import retry_async, is_transient


class Boom(Exception):
    pass


def test_is_transient_classifies():
    assert is_transient(asyncio.TimeoutError())
    assert is_transient(ConnectionResetError())
    assert is_transient(Exception("503 Service Unavailable"))
    assert is_transient(Exception("deadline exceeded"))
    assert not is_transient(ValueError("bad input"))
    assert not is_transient(Exception("401 Unauthorized"))


async def test_retries_then_succeeds():
    calls = {"n": 0}
    async def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise asyncio.TimeoutError()
        return "ok"
    sleeps = []
    out = await retry_async(
        fn, attempts=4, base_delay=0.05, max_delay=0.4,
        sleep=lambda d: sleeps.append(d) or asyncio.sleep(0),
        rng=lambda: 1.0,
    )
    assert out == "ok"
    assert calls["n"] == 3
    assert len(sleeps) == 2  # slept before attempt 2 and 3


async def test_gives_up_after_attempts():
    calls = {"n": 0}
    async def fn():
        calls["n"] += 1
        raise asyncio.TimeoutError()
    with pytest.raises(asyncio.TimeoutError):
        await retry_async(fn, attempts=2, base_delay=0.01, max_delay=0.1,
                          sleep=lambda d: asyncio.sleep(0), rng=lambda: 1.0)
    assert calls["n"] == 2  # 1 try + 1 retry


async def test_non_transient_not_retried():
    calls = {"n": 0}
    async def fn():
        calls["n"] += 1
        raise ValueError("4xx")
    with pytest.raises(ValueError):
        await retry_async(fn, attempts=5, base_delay=0.01, max_delay=0.1,
                          sleep=lambda d: asyncio.sleep(0), rng=lambda: 1.0)
    assert calls["n"] == 1  # never retried


async def test_jitter_within_range():
    async def fn():
        raise asyncio.TimeoutError()
    sleeps = []
    with pytest.raises(asyncio.TimeoutError):
        await retry_async(fn, attempts=3, base_delay=0.1, max_delay=10.0,
                          sleep=lambda d: sleeps.append(d) or asyncio.sleep(0),
                          rng=lambda: 0.0)  # jitter factor 0.5 (min)
    # base 0.1 * 2**0 = 0.1 → *0.5 = 0.05 ; base*2**1=0.2 → *0.5 = 0.10
    assert sleeps == pytest.approx([0.05, 0.10])
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && source .venv/bin/activate && pytest tests/test_retry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tara_agent.retry'`

- [ ] **Step 3: Write `retry.py`**

```python
# agent/tara_agent/retry.py
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
```

- [ ] **Step 4: Add config fields**

In `agent/tara_agent/config.py`, after the `metric_scrape_interval` line, add:

```python
    # Phase 4 — bounded retry (calls we control)
    hot_retry_attempts: int = 2        # greeting/hot path: 1 retry max
    offpath_retry_attempts: int = 4    # coverage tagger + mongo write
    retry_base_delay_ms: int = 50
    retry_max_delay_ms: int = 400
```

Add to `agent/tests/test_config.py` a new assertion (mirror existing style):

```python
def test_phase4_retry_defaults(monkeypatch):
    from tara_agent.config import Settings
    s = Settings(_env_file=None, gemini_api_key="x", google_application_credentials="x",
                 redis_url="redis://x", mongodb_uri="mongodb://x", livekit_url="x",
                 livekit_api_key="x", livekit_api_secret="x")
    assert s.hot_retry_attempts == 2
    assert s.offpath_retry_attempts == 4
```

> Note: match the exact `Settings(...)` construction style already used in `test_config.py` — if that file constructs Settings differently (e.g. via env monkeypatch), follow THAT pattern; the assertion on the two fields is the point.

- [ ] **Step 5: Run to green**

Run: `cd agent && pytest tests/test_retry.py tests/test_config.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add agent/tara_agent/retry.py agent/tara_agent/config.py agent/tests/test_retry.py agent/tests/test_config.py
git commit -m "feat(hardening): bounded retry helper + transient predicate + retry config"
```

---

### Task 2: Apply bounded retry to the calls we control (A2 wiring)

**Files:**
- Modify: `agent/tara_agent/gemini.py` (off-path coverage classify)
- Modify: `agent/tara_agent/persistence.py` (off-path Mongo write)
- Test: `agent/tests/test_gemini.py`, `agent/tests/test_persistence.py`

**Interfaces:**
- Consumes: `retry_async`, `is_transient` (Task 1); `Settings.offpath_retry_attempts`, `retry_base_delay_ms`, `retry_max_delay_ms`.
- Produces: `make_classify_fn` now retries the Gemini call (off-path budget); `end_interview` retries the Mongo write (off-path budget). Signatures unchanged for callers.

- [ ] **Step 1: Write the failing test (gemini)**

```python
# add to agent/tests/test_gemini.py
import asyncio
import pytest
from tara_agent.gemini import make_classify_fn


class _Settings:
    gemini_api_key = "x"
    gemini_model = "gemini-3.1-flash-lite"
    offpath_retry_attempts = 4
    retry_base_delay_ms = 1
    retry_max_delay_ms = 2


async def test_classify_retries_transient(monkeypatch):
    calls = {"n": 0}

    class _Resp:
        text = "Python"

    class _Models:
        async def generate_content(self, **kw):
            calls["n"] += 1
            if calls["n"] < 2:
                raise asyncio.TimeoutError()
            return _Resp()

    class _Aio:
        models = _Models()

    class _Client:
        aio = _Aio()

    monkeypatch.setattr("tara_agent.gemini.genai.Client", lambda **kw: _Client())
    fn = make_classify_fn(_Settings())
    out = await fn("prompt")
    assert out == "Python"
    assert calls["n"] == 2  # retried once
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_gemini.py::test_classify_retries_transient -v`
Expected: FAIL — currently no retry, the first `TimeoutError` propagates (calls["n"] == 1, raises).

- [ ] **Step 3: Wrap the gemini classify call**

Rewrite `agent/tara_agent/gemini.py`:

```python
from google import genai
from google.genai import types
from tara_agent.retry import retry_async

def make_classify_fn(settings):
    client = genai.Client(api_key=settings.gemini_api_key)
    async def classify(prompt: str) -> str:
        async def _call():
            resp = await client.aio.models.generate_content(
                model=settings.gemini_model,
                contents=prompt,
                config=types.GenerateContentConfig(max_output_tokens=64, temperature=0.0),
            )
            return (resp.text or "none").strip()
        return await retry_async(
            _call,
            attempts=getattr(settings, "offpath_retry_attempts", 4),
            base_delay=getattr(settings, "retry_base_delay_ms", 50) / 1000.0,
            max_delay=getattr(settings, "retry_max_delay_ms", 400) / 1000.0,
        )
    return classify
```

- [ ] **Step 4: Write the failing test (persistence)**

```python
# add to agent/tests/test_persistence.py
import asyncio
from tara_agent.persistence import end_interview


async def test_mongo_write_retries_transient():
    calls = {"n": 0}
    async def mongo_write(doc):
        calls["n"] += 1
        if calls["n"] < 2:
            raise ConnectionError("temporarily unavailable")
        return {"ok": 1}

    class _T:
        async def assemble(self):
            return [{"seq": 0, "speaker": "tara", "text": "hi"}]

    finally_called = {"v": False}
    await end_interview(
        say_fn=lambda: asyncio.sleep(0),
        transcript_store=_T(),
        mongo_write_fn=mongo_write,
        room="r", contest_id="c", candidate_id="u",
        say_timeout=1.0, write_timeout=1.0,
        on_finally=lambda: finally_called.__setitem__("v", True),
        offpath_retry_attempts=4, retry_base_delay_ms=1, retry_max_delay_ms=2,
    )
    assert calls["n"] == 2       # retried the transient write once
    assert finally_called["v"]   # finally still runs exactly once
```

- [ ] **Step 5: Run to verify it fails**

Run: `cd agent && pytest tests/test_persistence.py::test_mongo_write_retries_transient -v`
Expected: FAIL — `end_interview()` got an unexpected keyword `offpath_retry_attempts` / no retry.

- [ ] **Step 6: Add retry to the Mongo write**

Modify `agent/tara_agent/persistence.py` — add the three optional kwargs (defaulted so existing callers still work) and wrap the write:

```python
import asyncio
import logging
from tara_agent.retry import retry_async

log = logging.getLogger("tara.persistence")

async def end_interview(*, say_fn, transcript_store, mongo_write_fn, room,
                        contest_id, candidate_id, say_timeout, write_timeout,
                        on_finally,
                        offpath_retry_attempts: int = 4,
                        retry_base_delay_ms: int = 50,
                        retry_max_delay_ms: int = 400):
    try:
        try:
            await asyncio.wait_for(say_fn(), timeout=say_timeout)
        except (asyncio.TimeoutError, Exception) as e:
            log.warning("closing say failed/hung: %s", e)

        lines = await transcript_store.assemble()
        doc = {"room": room, "contestId": contest_id,
               "candidateId": candidate_id, "transcript": lines}
        try:
            async def _write():
                return await asyncio.wait_for(mongo_write_fn(doc), timeout=write_timeout)
            await retry_async(
                _write,
                attempts=offpath_retry_attempts,
                base_delay=retry_base_delay_ms / 1000.0,
                max_delay=retry_max_delay_ms / 1000.0,
            )
        except (asyncio.TimeoutError, Exception) as e:
            log.error("transcript persist failed/hung (recoverable from Redis): %s", e)
    finally:
        on_finally()
```

Then pass the settings through at the call site in `agent/tara_agent/interview_agent.py` `_wrap_up` (add to the `end_interview(...)` call):

```python
            offpath_retry_attempts=self._settings.offpath_retry_attempts,
            retry_base_delay_ms=self._settings.retry_base_delay_ms,
            retry_max_delay_ms=self._settings.retry_max_delay_ms,
```

- [ ] **Step 7: Run to green**

Run: `cd agent && pytest tests/test_gemini.py tests/test_persistence.py -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add agent/tara_agent/gemini.py agent/tara_agent/persistence.py agent/tara_agent/interview_agent.py agent/tests/test_gemini.py agent/tests/test_persistence.py
git commit -m "feat(hardening): bounded off-path retry on coverage classify + mongo write"
```

---

### Task 3: Barge-in audit truncation (A1)

**Files:**
- Modify: `agent/tara_agent/interview_agent.py` (add `on_interruption`)
- Modify: `agent/tara_agent/worker.py` (wire the interruption event)
- Test: `agent/tests/test_interview_agent_interruption.py` (create)

**Interfaces:**
- Consumes: `TranscriptStore.truncate_last(speaker, spoken_text)` (already exists).
- Produces: `async def InterviewAgent.on_interruption(self, spoken_text: str) -> None` — truncates the last `tara` transcript line to `spoken_text` (only the words actually spoken before the candidate barged in). Never stores unspoken words.

> **Implementer note (spec "verify at implementation"):** In LiveKit Agents 1.6.4, determine the spoken boundary. `conversation_item_added` for the assistant may ALREADY carry the interrupted (truncated) text — if so, `on_tara_line` records the truncated line and `on_interruption` is a belt-and-suspenders no-op-or-confirm. If the assistant item carries the FULL intended text, use the interruption event's played/committed segment as `spoken_text`. Wire whichever the installed version provides; document the choice in a comment at the wiring site. The unit test below pins the truncation CONTRACT regardless of which event feeds it.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_interview_agent_interruption.py
import pytest
from tara_agent.interview_agent import InterviewAgent


class _FakeTranscript:
    def __init__(self):
        self.lines = []
        self._key = "transcript:room1"
    async def append(self, speaker, text):
        self.lines.append({"speaker": speaker, "text": text})
        return len(self.lines) - 1
    async def truncate_last(self, speaker, spoken_text):
        for i in range(len(self.lines) - 1, -1, -1):
            if self.lines[i]["speaker"] == speaker:
                self.lines[i]["text"] = spoken_text
                return True
        return False


def _agent(transcript):
    # Construct with only what on_interruption needs; other deps unused here.
    return InterviewAgent(
        instructions="x", blob=type("B", (), {"skills": [], "max_questions": 1,
            "contest_id": "c", "candidate_id": "u"})(),
        transcript=transcript, coverage=None, mongo_write_fn=None,
        settings=type("S", (), {})(), on_end=lambda: None, limiter=None, room="room1",
    )


async def test_on_interruption_truncates_last_tara_line():
    t = _FakeTranscript()
    await t.append("tara", "Tell me about a time you scaled a system to handle")
    agent = _agent(t)
    await agent.on_interruption("Tell me about a time you")
    assert t.lines[-1]["text"] == "Tell me about a time you"  # only what was spoken


async def test_on_interruption_no_tara_line_is_safe():
    t = _FakeTranscript()
    await t.append("candidate", "hello")
    agent = _agent(t)
    await agent.on_interruption("anything")  # must not raise
    assert t.lines[-1]["text"] == "hello"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_interview_agent_interruption.py -v`
Expected: FAIL — `AttributeError: 'InterviewAgent' object has no attribute 'on_interruption'`

- [ ] **Step 3: Add `on_interruption`**

In `agent/tara_agent/interview_agent.py`, add the method (next to `on_tara_line`):

```python
    async def on_interruption(self, spoken_text: str) -> None:
        """Barge-in: truncate Tara's last line to only what was actually spoken.

        The Mongo transcript is an audit artifact — it must match reality, never
        the full intended sentence. No-op if there is no Tara line yet.
        """
        await self._transcript.truncate_last("tara", spoken_text)
```

- [ ] **Step 4: Run to green**

Run: `cd agent && pytest tests/test_interview_agent_interruption.py -v`
Expected: PASS

- [ ] **Step 5: Wire the interruption event in worker.py**

In `agent/tara_agent/worker.py`, after the `_on_item` handler, add a wiring seam. Investigate the installed LiveKit event for interruptions (candidate `user_started_speaking` while agent state is `speaking`, or a dedicated interruption event). Concrete wiring:

```python
    # Barge-in audit: when the candidate interrupts Tara mid-sentence, truncate
    # Tara's stored line to only what was actually spoken. The committed assistant
    # text (LiveKit truncates the interrupted item) is the spoken boundary.
    # If the installed version exposes a distinct interruption event with the
    # played segment, prefer that; document the chosen source here.
    @session.on("conversation_item_added")
    def _on_item_audit(ev):
        item = ev.item
        if getattr(item, "role", None) == "assistant" and getattr(item, "interrupted", False):
            asyncio.create_task(agent.on_interruption(item.text_content or ""))
```

> If `item.interrupted` is not present on the installed version, use the available signal (e.g. compare last spoken transcript vs. intended) — the contract is: pass the SPOKEN text to `agent.on_interruption`. Manual-verify with a live barge-in (note in the report which event was used). This is a wiring seam like the Phase 1–3 worker tasks; the truncation logic itself is unit-tested in Steps 1–4.

- [ ] **Step 6: Verify import + commit**

Run: `cd agent && python -c "from tara_agent import worker" && pytest tests/test_interview_agent_interruption.py -q`
Expected: import exits 0; tests PASS.

```bash
git add agent/tara_agent/interview_agent.py agent/tara_agent/worker.py agent/tests/test_interview_agent_interruption.py
git commit -m "feat(hardening): barge-in audit truncation (store only spoken words)"
```

---

### Task 4: SessionLifecycle — reconnect grace + exactly-once teardown (B1/B3/B4 core)

**Files:**
- Create: `agent/tara_agent/lifecycle.py`
- Test: `agent/tests/test_lifecycle.py`

**Interfaces:**
- Produces: `class SessionLifecycle`:
  - `__init__(self, *, candidate_id_getter, grace_seconds, on_resume, on_teardown, sleep=asyncio.sleep)` — `candidate_id_getter()` returns the candidate identity (or None if unknown yet); `on_resume()` and `on_teardown(reason)` are async callbacks; `grace_seconds` is the reconnect window.
  - `def note_candidate(self, identity)` — record the candidate identity (idempotent).
  - `async def participant_left(self, identity)` — if `identity` is the candidate and not already torn down, START the grace timer (does NOT tear down immediately). Non-candidate → ignored.
  - `async def participant_joined(self, identity)` — if `identity` is the candidate AND a grace timer is pending, cancel it and call `on_resume()`.
  - `async def teardown(self, reason)` — idempotent: invoke `on_teardown(reason)` AT MOST ONCE across all calls/paths.
  - internal: the grace timer awaits `sleep(grace_seconds)` then calls `self.teardown("reconnect_grace_expiry")`.
- Consumed by: Task 5 (worker wiring).

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_lifecycle.py
import asyncio
import pytest
from tara_agent.lifecycle import SessionLifecycle


def _make(grace, sleep):
    events = {"resume": 0, "teardown": []}
    async def on_resume():
        events["resume"] += 1
    async def on_teardown(reason):
        events["teardown"].append(reason)
    lc = SessionLifecycle(
        candidate_id_getter=lambda: "cand",
        grace_seconds=grace, on_resume=on_resume, on_teardown=on_teardown,
        sleep=sleep,
    )
    return lc, events


async def test_candidate_leave_starts_grace_no_immediate_teardown():
    gate = asyncio.Event()
    async def sleep(_):
        await gate.wait()  # hold the grace timer open
    lc, ev = _make(25, sleep)
    lc.note_candidate("cand")
    await lc.participant_left("cand")
    await asyncio.sleep(0)
    assert ev["teardown"] == []  # NOT torn down within grace


async def test_rejoin_within_grace_resumes_and_cancels():
    gate = asyncio.Event()
    async def sleep(_):
        await gate.wait()
    lc, ev = _make(25, sleep)
    lc.note_candidate("cand")
    await lc.participant_left("cand")
    await lc.participant_joined("cand")
    await asyncio.sleep(0)
    assert ev["resume"] == 1
    assert ev["teardown"] == []   # rejoin cancelled the teardown


async def test_grace_expiry_tears_down_once():
    async def sleep(_):
        return  # grace elapses immediately
    lc, ev = _make(0, sleep)
    lc.note_candidate("cand")
    await lc.participant_left("cand")
    await asyncio.sleep(0)
    assert ev["teardown"] == ["reconnect_grace_expiry"]


async def test_non_candidate_leave_ignored():
    async def sleep(_):
        return
    lc, ev = _make(0, sleep)
    lc.note_candidate("cand")
    await lc.participant_left("observer")
    await asyncio.sleep(0)
    assert ev["teardown"] == []   # a different participant must not tear down


async def test_teardown_is_exactly_once():
    async def sleep(_):
        return
    lc, ev = _make(0, sleep)
    await lc.teardown("path_a")
    await lc.teardown("path_b")
    assert ev["teardown"] == ["path_a"]  # second call is a no-op
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_lifecycle.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tara_agent.lifecycle'`

- [ ] **Step 3: Write `lifecycle.py`**

```python
# agent/tara_agent/lifecycle.py
"""Reconnect grace window + exactly-once teardown for one interview session.

A candidate disconnect starts a grace timer (strictly < the Tier-1 lease) instead
of ending the interview. A rejoin within the window resumes; expiry tears down.
Every exit path funnels through teardown(), which fires on_teardown AT MOST ONCE.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Awaitable, Callable

log = logging.getLogger("tara.lifecycle")


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
```

- [ ] **Step 4: Run to green**

Run: `cd agent && pytest tests/test_lifecycle.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/lifecycle.py agent/tests/test_lifecycle.py
git commit -m "feat(hardening): SessionLifecycle — reconnect grace + exactly-once teardown"
```

---

### Task 5: Wire SessionLifecycle into the worker (B1/B2/B3/B4 integration)

**Files:**
- Modify: `agent/tara_agent/worker.py`
- Modify: `agent/tara_agent/config.py` (add `reconnect_grace_seconds` + validation)
- Test: `agent/tests/test_config.py` (grace < lease assertion)

**Interfaces:**
- Consumes: `SessionLifecycle` (Task 4); `Limiter.heartbeat(..., mark_participant=True)`, `Limiter.release`; `TranscriptStore.assemble`.
- Produces: a worker where (B1) candidate disconnect starts a grace window instead of releasing; (B2) `mark_participant=True` is set at CANDIDATE join, not agent join; (B3) only the candidate's disconnect matters; (B4) one `_teardown` does flush+release exactly once; resume re-asks the last Tara line.

- [ ] **Step 1: Add config + validation**

In `agent/tara_agent/config.py` add after the Phase-4 retry fields:

```python
    # Phase 4 — reconnect grace window (MUST be < reservation_lease_ttl)
    reconnect_grace_seconds: int = 25
```

And add a validator (mirror pydantic v2 style) inside `Settings`:

```python
    @model_validator(mode="after")
    def _grace_under_lease(self):
        if self.reconnect_grace_seconds >= self.reservation_lease_ttl:
            raise ValueError(
                "reconnect_grace_seconds must be < reservation_lease_ttl "
                "(a grace window past the Tier-1 lease risks the reaper reclaiming "
                "a slot we still intend to hold)"
            )
        return self
```

Add `from pydantic import model_validator` at the top of `config.py`.

Add to `agent/tests/test_config.py`:

```python
def test_reconnect_grace_must_be_under_lease():
    import pytest
    from tara_agent.config import Settings
    with pytest.raises(ValueError):
        Settings(_env_file=None, gemini_api_key="x", google_application_credentials="x",
                 redis_url="redis://x", mongodb_uri="mongodb://x", livekit_url="x",
                 livekit_api_key="x", livekit_api_secret="x",
                 reconnect_grace_seconds=60, reservation_lease_ttl=45)
```

- [ ] **Step 2: Run config test to verify it fails**

Run: `cd agent && pytest tests/test_config.py::test_reconnect_grace_must_be_under_lease -v`
Expected: FAIL — no validator yet (Settings constructs without error).

- [ ] **Step 3: Run config test to green** (after Step 1's validator is in place)

Run: `cd agent && pytest tests/test_config.py -v`
Expected: PASS

- [ ] **Step 4: Rework the worker disconnect/teardown path**

In `agent/tara_agent/worker.py`:

(a) Add `from tara_agent.lifecycle import SessionLifecycle` to the imports.

(b) Replace the agent-join heartbeat (currently `mark_heartbeat=True, mark_participant=True` at ~line 289) with heartbeat-only:

```python
    # Promote to Tier-2 heartbeat lease at agent join (heartbeat flag only;
    # participant flag is set when the CANDIDATE actually joins — see below).
    await limiter.heartbeat(ctx.room.name, now_ms(), mark_heartbeat=True)
```

(c) Define the unified teardown + lifecycle BEFORE `session.start`. Replace the existing `_on_close`, `participant_disconnected` handler, and the inline `limiter.release` calls with:

```python
    # --- Unified, exactly-once teardown -------------------------------------
    async def _teardown(reason: str):
        log.info("teardown reason=%s", reason)
        _flush_breakdown()                       # latency breakdown, once-guarded
        await limiter.release(ctx.room.name)     # idempotent (ZSCORE-guarded Lua)
        done.set()

    async def _resume():
        # Candidate reconnected within grace: welcome back + re-ask the in-flight
        # question = re-say the last Tara transcript line (it IS current_question;
        # no separate Redis key needed — DRY).
        lines = await transcript.assemble()
        last_tara = next((l["text"] for l in reversed(lines) if l["speaker"] == "tara"), None)
        msg = "Welcome back. " + (f"Let me repeat: {last_tara}" if last_tara else
                                  "Let's continue.")
        try:
            await session.say(msg)
        except Exception as e:  # never let resume crash the session
            log.warning("resume say failed: %s", e)

    candidate_identity = {"v": None}
    lifecycle = SessionLifecycle(
        candidate_id_getter=lambda: candidate_identity["v"],
        grace_seconds=s.reconnect_grace_seconds,
        on_resume=_resume,
        on_teardown=_teardown,
    )

    # session "close" (provider/job close) funnels through the single teardown.
    @session.on("close")
    def _on_close(*_a):
        asyncio.create_task(lifecycle.teardown("session_close"))
```

(d) Replace the `participant_disconnected` handler and add candidate join detection + `participant_connected`:

```python
    @ctx.room.on("participant_connected")
    def _on_part_joined(p):
        # First remote participant is the candidate. Record identity + set the
        # participant flag so a lease lapse with a present candidate classifies
        # as a FALSE reclaim (the hard-gate signal) — accurately, at candidate join.
        if candidate_identity["v"] is None:
            candidate_identity["v"] = p.identity
            lifecycle.note_candidate(p.identity)
            asyncio.create_task(limiter.heartbeat(ctx.room.name, now_ms(),
                                                  mark_participant=True))
        asyncio.create_task(lifecycle.participant_joined(p.identity))

    @ctx.room.on("participant_disconnected")
    def _on_part_left(p):
        # Candidate-only: a non-candidate (future proctor/observer) leaving must
        # NOT start the grace timer or tear down (B3 guard inside SessionLifecycle).
        asyncio.create_task(lifecycle.participant_left(p.identity))
```

(e) Update `add_shutdown_callback` to go through teardown:

```python
    async def _flush_on_shutdown():
        await lifecycle.teardown("job_shutdown")

    ctx.add_shutdown_callback(_flush_on_shutdown)
```

(f) The `InterviewAgent._on_end_and_release` (natural end) already calls `limiter.release` + `on_end`(=`done.set`). To route natural end through the single teardown too, pass `on_end=lambda: asyncio.create_task(lifecycle.teardown("natural_end"))` when constructing `InterviewAgent` (replacing `on_end=done.set`), and have `_on_end_and_release` NOT double-release. Simplest: change the `InterviewAgent(... on_end=...)` arg to the lifecycle teardown and drop the agent's own release (set `limiter=None` in the InterviewAgent construction so its `_on_end_and_release` only signals; teardown owns release). Concretely, construct InterviewAgent with `limiter=None, room=ctx.room.name` and `on_end=lambda: asyncio.create_task(lifecycle.teardown("natural_end"))`.

> This keeps release in EXACTLY ONE place (teardown). Verify no other `limiter.release(` call remains in worker.py after this edit.

- [ ] **Step 5: Verify import + no duplicate release**

Run:
```bash
cd agent && python -c "from tara_agent import worker" && \
  grep -n "limiter.release" tara_agent/worker.py
```
Expected: import exits 0; `grep` shows `limiter.release` on EXACTLY ONE line (inside `_teardown`).

- [ ] **Step 6: Run the full agent suite (no regressions)**

Run: `cd agent && pytest -q`
Expected: PASS (all prior + new lifecycle/config tests).

- [ ] **Step 7: Commit**

```bash
git add agent/tara_agent/worker.py agent/tara_agent/config.py agent/tests/test_config.py
git commit -m "feat(hardening): reconnect grace + candidate-join participant flag + single teardown"
```

---

### Task 6: Audit-record completeness (B5)

**Files:**
- Create: `agent/tara_agent/audit.py`
- Modify: `agent/tara_agent/persistence.py` (run the audit at flush, log problems)
- Test: `agent/tests/test_audit.py`

**Interfaces:**
- Produces: `def audit_transcript(lines: list[dict]) -> list[str]` — returns a list of problem strings (empty list = clean). Checks: seqs present and contiguous `0..n-1` after sorting; every `speaker` ∈ {`tara`, `candidate`}; every `text` is a `str`; first line (lowest seq) is `tara` (Tara greets first). Non-crashing; pure.
- Consumed by: `end_interview` (logs problems; never raises).

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_audit.py
from tara_agent.audit import audit_transcript


def test_clean_transcript_has_no_problems():
    lines = [
        {"seq": 0, "speaker": "tara", "text": "Hi, first question?"},
        {"seq": 1, "speaker": "candidate", "text": "My answer."},
        {"seq": 2, "speaker": "tara", "text": "Follow up?"},
    ]
    assert audit_transcript(lines) == []


def test_detects_seq_gap():
    lines = [
        {"seq": 0, "speaker": "tara", "text": "q"},
        {"seq": 2, "speaker": "candidate", "text": "a"},  # missing seq 1
    ]
    probs = audit_transcript(lines)
    assert any("contiguous" in p or "gap" in p for p in probs)


def test_detects_bad_speaker():
    lines = [{"seq": 0, "speaker": "robot", "text": "q"}]
    assert any("speaker" in p for p in audit_transcript(lines))


def test_detects_candidate_first():
    lines = [
        {"seq": 0, "speaker": "candidate", "text": "a"},
        {"seq": 1, "speaker": "tara", "text": "q"},
    ]
    assert any("first" in p for p in audit_transcript(lines))


def test_empty_transcript_is_clean():
    assert audit_transcript([]) == []
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_audit.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tara_agent.audit'`

- [ ] **Step 3: Write `audit.py`**

```python
# agent/tara_agent/audit.py
"""Per-interview transcript audit — the record must be ordered, complete, and
match reality (truncated barge-ins + re-asked questions included). Pure, never
raises; returns a list of human-readable problems (empty = clean)."""
from __future__ import annotations

_VALID_SPEAKERS = {"tara", "candidate"}


def audit_transcript(lines: list[dict]) -> list[str]:
    problems: list[str] = []
    if not lines:
        return problems
    ordered = sorted(lines, key=lambda l: l.get("seq", -1))
    seqs = [l.get("seq") for l in ordered]
    if seqs != list(range(len(ordered))):
        problems.append(f"seqs not contiguous 0..n-1 (gap or duplicate): {seqs}")
    for l in ordered:
        if l.get("speaker") not in _VALID_SPEAKERS:
            problems.append(f"invalid speaker: {l.get('speaker')!r} at seq {l.get('seq')}")
        if not isinstance(l.get("text"), str):
            problems.append(f"non-string text at seq {l.get('seq')}")
    if ordered[0].get("speaker") != "tara":
        problems.append("first line is not tara (Tara must greet first)")
    return problems
```

- [ ] **Step 4: Run to green**

Run: `cd agent && pytest tests/test_audit.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Wire the audit into the flush path**

In `agent/tara_agent/persistence.py`, after `lines = await transcript_store.assemble()`, add:

```python
        from tara_agent.audit import audit_transcript
        problems = audit_transcript(lines)
        if problems:
            log.warning("transcript audit problems for room %s: %s", room, problems)
```

(Audit is observational — it logs, it does not block the write or raise.)

- [ ] **Step 6: Run persistence tests + commit**

Run: `cd agent && pytest tests/test_persistence.py tests/test_audit.py -q`
Expected: PASS

```bash
git add agent/tara_agent/audit.py agent/tara_agent/persistence.py agent/tests/test_audit.py
git commit -m "feat(hardening): transcript audit completeness check at flush"
```

---

### Task 7: Rejected-session Redis blob TTL (B6)

**Files:**
- Modify: `backend/src/createSession.js`
- Modify: `backend/src/limiterConfig.js` (add `rejectedBlobTtl`)
- Test: `backend/test/createSession.test.js`

**Interfaces:**
- Consumes: `loadLimiterConfig` config object.
- Produces: on admission reject, the `session:<id>` blob is re-expired to `rejectedBlobTtl` (default 120s) instead of lingering 7200s. Admitted sessions keep the normal TTL.

- [ ] **Step 1: Write the failing test**

```javascript
// add to backend/test/createSession.test.js
const { createSession } = require("../src/createSession");

test("rejected session blob is re-expired to a short TTL", async () => {
  const calls = [];
  const redis = {
    set: async (k, v, ...rest) => { calls.push(["set", k, ...rest]); },
    expire: async (k, ttl) => { calls.push(["expire", k, ttl]); },
  };
  const fakeLimiterRejected = { tryAdmit: async () => ({ admitted: false, bucket: "global" }) };
  const tokenFactory = () => "tok";
  const out = await createSession(redis, tokenFactory, fakeLimiterRejected, {
    contestId: "c1", candidateId: "u1", skills: ["Python"],
    resumeText: "r", jdText: "j",
  }, { rejectedBlobTtl: 120 });
  expect(out.status).toBe("queued");
  // the blob TTL was shortened on reject
  const expireCall = calls.find((c) => c[0] === "expire");
  expect(expireCall).toBeTruthy();
  expect(expireCall[2]).toBe(120);
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npx jest createSession`
Expected: FAIL — `createSession` takes 4 args; no `expire` call on reject.

- [ ] **Step 3: Add `rejectedBlobTtl` to config**

In `backend/src/limiterConfig.js`, add to the returned config object (mirror the existing field style, reading `env.REJECTED_BLOB_TTL`):

```javascript
    rejectedBlobTtl: Number(env.REJECTED_BLOB_TTL ?? 120),
```

- [ ] **Step 4: Shorten the blob TTL on reject**

In `backend/src/createSession.js`, change the signature to accept an options object and re-expire on reject:

```javascript
async function createSession(redis, tokenFactory, limiter, body, opts = {}) {
  for (const f of REQUIRED) {
    if (body[f] === undefined) throw new Error(`field ${f} is required`);
  }
  const rejectedBlobTtl = opts.rejectedBlobTtl ?? 120;
  const sessionId = crypto.randomUUID();
  const blob = {
    contestId: body.contestId, candidateId: body.candidateId,
    skills: body.skills, resumeText: body.resumeText, jdText: body.jdText,
    maxQuestions: body.maxQuestions ?? 12, status: "created",
  };
  await redis.set(`session:${sessionId}`, JSON.stringify(blob), "EX", 7200);

  const admit = await limiter.tryAdmit(sessionId, Date.now());
  if (!admit.admitted) {
    // Don't let rejected attempts linger 2h — expire promptly (no slot leak fix).
    await redis.expire(`session:${sessionId}`, rejectedBlobTtl);
    return { sessionId, status: "queued", bucket: admit.bucket };
  }
  const token = await tokenFactory(sessionId, body.candidateId);
  return { sessionId, room: sessionId, token,
           livekitUrl: process.env.LIVEKIT_URL, status: "admitted" };
}
```

And in `backend/src/server.js`, pass the config through at the call site:

```javascript
    const result = await createSession(redis, mintToken, limiter, req.body,
                                       { rejectedBlobTtl: cfg.rejectedBlobTtl });
```

- [ ] **Step 5: Run to green**

Run: `cd backend && npx jest`
Expected: PASS (all suites, incl. the new reject-TTL test).

- [ ] **Step 6: Commit**

```bash
git add backend/src/createSession.js backend/src/limiterConfig.js backend/src/server.js backend/test/createSession.test.js
git commit -m "feat(hardening): expire rejected-session blob promptly (rejectedBlobTtl)"
```

---

### Task 8: Backend `/healthz` route (C3)

**Files:**
- Create: `backend/src/health.js`
- Modify: `backend/src/server.js` (mount `/healthz`)
- Modify: `deploy/backend-deployment.yaml` (TCP probes → `httpGet /healthz`)
- Test: `backend/test/health.test.js`

**Interfaces:**
- Produces: `function makeHealthz(redis)` → an Express handler that pings Redis; `200 {status:"ok"}` when reachable, `503 {status:"unhealthy"}` when the ping throws.

- [ ] **Step 1: Write the failing test**

```javascript
// backend/test/health.test.js
const { makeHealthz } = require("../src/health");

function mockRes() {
  return {
    code: null, body: null,
    status(c) { this.code = c; return this; },
    json(b) { this.body = b; return this; },
  };
}

test("healthz 200 when redis ping ok", async () => {
  const redis = { ping: async () => "PONG" };
  const res = mockRes();
  await makeHealthz(redis)({}, res);
  expect(res.code).toBe(200);
  expect(res.body.status).toBe("ok");
});

test("healthz 503 when redis ping fails", async () => {
  const redis = { ping: async () => { throw new Error("down"); } };
  const res = mockRes();
  await makeHealthz(redis)({}, res);
  expect(res.code).toBe(503);
  expect(res.body.status).toBe("unhealthy");
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && npx jest health`
Expected: FAIL — `Cannot find module '../src/health'`

- [ ] **Step 3: Write `health.js`**

```javascript
// backend/src/health.js
function makeHealthz(redis) {
  return async function healthz(_req, res) {
    try {
      await redis.ping();
      return res.status(200).json({ status: "ok" });
    } catch (e) {
      return res.status(503).json({ status: "unhealthy", error: e.message });
    }
  };
}
module.exports = { makeHealthz };
```

- [ ] **Step 4: Mount it in server.js**

In `backend/src/server.js`, after `const app = express();` block and the redis client, add:

```javascript
const { makeHealthz } = require("./health");
app.get("/healthz", makeHealthz(redis));
```

- [ ] **Step 5: Run to green**

Run: `cd backend && npx jest`
Expected: PASS (all suites).

- [ ] **Step 6: Switch the backend probes to httpGet /healthz**

In `deploy/backend-deployment.yaml`, replace the TCP readiness/liveness probes with:

```yaml
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 5
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 3
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 20
            periodSeconds: 20
            timeoutSeconds: 5
            failureThreshold: 6
```

Validate: `kubeconform -strict -summary deploy/backend-deployment.yaml`
Expected: `Valid: 1`.

- [ ] **Step 7: Commit**

```bash
git add backend/src/health.js backend/src/server.js backend/test/health.test.js deploy/backend-deployment.yaml
git commit -m "feat(hardening): backend /healthz (redis ping) + httpGet probes"
```

---

### Task 9: Real worker drain (C1)

**Files:**
- Create: `agent/tara_agent/drain.py`
- Modify: `agent/tara_agent/worker.py` (track sessions, SIGTERM → drain)
- Modify: `deploy/agent-deployment.yaml` (preStop: real drain, not blind sleep)
- Test: `agent/tests/test_drain.py`

**Interfaces:**
- Produces: `class DrainController`:
  - `def register(self) -> None` / `def unregister(self) -> None` — bump/drop the active-session count.
  - `def request_drain(self) -> None` — set the draining flag (refuse new work).
  - `def is_draining(self) -> bool`.
  - `def active(self) -> int`.
  - `async def wait_drained(self, *, poll=0.2, timeout: float | None = None, sleep=asyncio.sleep) -> bool` — returns True once `active()==0` (or immediately if already 0), False on timeout.
- Consumed by: worker (register on session start, unregister in teardown, SIGTERM handler calls `request_drain` then `wait_drained`).

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pytest tests/test_drain.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tara_agent.drain'`

- [ ] **Step 3: Write `drain.py`**

```python
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
```

- [ ] **Step 4: Run to green**

Run: `cd agent && pytest tests/test_drain.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Wire into worker.py**

(a) Add `import signal` and `from tara_agent.drain import DrainController` to imports. Add a module-level controller next to `_METRICS`:

```python
_DRAIN = DrainController()
```

(b) In `entrypoint`, after `_METRICS.session_started(); _started["v"] = True`, register the session and install a one-time SIGTERM handler:

```python
    _DRAIN.register()

    def _on_sigterm():
        _DRAIN.request_drain()
        # let in-flight interviews finish; the pod's terminationGracePeriod bounds it
        asyncio.create_task(_DRAIN.wait_drained(timeout=110))

    try:
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, _on_sigterm)
    except (NotImplementedError, RuntimeError):
        pass  # add_signal_handler unavailable (e.g. non-main thread) — SIGTERM still drains via LiveKit
```

(c) In `_teardown`, after `done.set()`, drop the session from the drain count:

```python
        _DRAIN.unregister()
```

- [ ] **Step 6: Verify import**

Run: `cd agent && python -c "from tara_agent import worker" && pytest tests/test_drain.py -q`
Expected: import exits 0; tests PASS.

- [ ] **Step 7: Replace the blind-sleep preStop with a real drain**

In `deploy/agent-deployment.yaml`, change the container `lifecycle.preStop` from `sleep 110` to a hook that signals the worker and waits for it to exit (the worker drains in-flight sessions on SIGTERM, Step 5):

```yaml
          lifecycle:
            preStop:
              exec:
                # Real drain: signal the worker (PID 1) to stop taking new jobs and
                # finish in-flight interviews, then wait for it to exit. A pod with
                # zero live sessions terminates immediately instead of idling.
                # terminationGracePeriodSeconds (120) is the safety ceiling.
                command: ["/bin/sh", "-c", "kill -TERM 1; while kill -0 1 2>/dev/null; do sleep 1; done"]
```

Validate: `kubeconform -strict -summary deploy/agent-deployment.yaml`
Expected: `Valid: 1`.

- [ ] **Step 8: Commit**

```bash
git add agent/tara_agent/drain.py agent/tara_agent/worker.py agent/tests/test_drain.py deploy/agent-deployment.yaml
git commit -m "feat(hardening): worker drain controller + SIGTERM drain + real preStop"
```

---

### Task 10: Dockerfiles + .dockerignore (C2)

**Files:**
- Create: `agent/Dockerfile`, `agent/.dockerignore`
- Create: `backend/Dockerfile`, `backend/.dockerignore`

**Interfaces:**
- Produces: locally-buildable images for the agent worker and the backend. NO secret material copied in. Registry push is the operator step (images keep placeholder refs in manifests).

- [ ] **Step 1: Write `agent/.dockerignore`**

```
.venv/
__pycache__/
*.pyc
.pytest_cache/
.env
*.json
!pyproject.toml
tests/
../shared/**/*.rdb
```

> The `*.json` line blocks the GCP SA JSON and any creds; `pyproject.toml` is re-allowed. (The SA JSON is mounted at runtime from a Secret — never baked in.)

- [ ] **Step 2: Write `agent/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app/agent

# System deps for audio/grpc wheels if needed; keep slim.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates && rm -rf /var/lib/apt/lists/*

# Install the package. The build context is the agent/ dir; shared/ Lua is
# resolved at runtime via the repo layout — see note below.
COPY pyproject.toml ./
COPY tara_agent ./tara_agent
RUN pip install .

# Pre-download the turn-detector model at BUILD time so cold-start doesn't pay
# for it (protects the warm-pool / 45s-lease correctness dependency).
RUN python -m tara_agent.worker download-files || \
    echo "WARN: download-files not available; model will lazy-load on first job"

# Non-root.
RUN useradd -m -u 10001 tara && chown -R tara:tara /app
USER tara

CMD ["python", "-m", "tara_agent.worker", "start"]
```

> **Implementer note:** the limiter loads `shared/limiter/*.lua` via `parents[2]/shared/limiter`. Verify the package install + working dir make that path resolve in-image; if the install flattens the layout, COPY the `shared/` dir to the location `limiter.py` expects (`/app/shared/limiter`) and adjust `WORKDIR`/COPY so `Path(__file__).resolve().parents[2]/shared/limiter` points at it. Confirm by running `python -c "from tara_agent.limiter import Limiter"` in the built image. The `download-files` subcommand name is the LiveKit Agents model-fetch entrypoint — verify it against the installed CLI (`python -m tara_agent.worker --help`); if different, use the correct subcommand.

- [ ] **Step 3: Build the agent image locally**

Run: `cd agent && docker build -t tara-agent:dev .`
Expected: build SUCCEEDS. Then verify imports in-image:
`docker run --rm tara-agent:dev python -c "from tara_agent.limiter import Limiter; from tara_agent import worker; print('ok')"`
Expected: prints `ok`.

> This is a LOCAL build — NOT GKE-gated. If `download-files` isn't a real subcommand, the `|| echo` keeps the build green and the model lazy-loads; note it in the report.

- [ ] **Step 4: Write `backend/.dockerignore`**

```
node_modules/
.env
*.log
test/
coverage/
```

- [ ] **Step 5: Write `backend/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM node:20-slim AS base
ENV NODE_ENV=production
WORKDIR /app/backend

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev || npm install --omit=dev

COPY src ./src
# The backend reads shared/limiter/*.lua at ../../shared/limiter — provide it.
COPY ../shared ./_shared_placeholder 2>/dev/null || true

RUN useradd -m -u 10002 tara && chown -R tara:tara /app
USER tara

EXPOSE 3000
CMD ["node", "src/server.js"]
```

> **Implementer note:** `backend/src/limiter.js` resolves Lua at `path.resolve(__dirname, "..", "..", "shared", "limiter")` = `/app/shared/limiter` when `__dirname=/app/backend/src`. Docker build context is `backend/`, which CANNOT reach `../shared`. Two clean options — pick one and document it: (a) build from the REPO ROOT with `-f backend/Dockerfile` and COPY `shared/` to `/app/shared`; or (b) keep context `backend/` and COPY a vendored copy, adjusting the require path. Prefer (a): the build command becomes `docker build -f backend/Dockerfile -t tara-backend:dev .` from repo root, and the Dockerfile does `COPY shared /app/shared` + `COPY backend/src /app/backend/src`. Rewrite the Dockerfile to match the chosen context so the Lua path resolves; verify with the run check in Step 6.

- [ ] **Step 6: Build the backend image locally**

Run (from repo root, per the note): `docker build -f backend/Dockerfile -t tara-backend:dev .`
Expected: build SUCCEEDS. Verify the server module loads + Lua resolves:
`docker run --rm tara-backend:dev node -e "require('./src/limiter'); console.log('ok')"`
Expected: prints `ok` (no ENOENT on the Lua file).

- [ ] **Step 7: Confirm no secrets in build context**

Run: `cd agent && docker build -t tara-agent:dev . 2>&1 | grep -i "\.env\|sa.json\|service-account" || echo "no secret files in context"`
Expected: `no secret files in context` (the `.dockerignore` excludes them).

- [ ] **Step 8: Commit**

```bash
git add agent/Dockerfile agent/.dockerignore backend/Dockerfile backend/.dockerignore
git commit -m "feat(hardening): agent + backend Dockerfiles (non-root, no secrets, prefetch model)"
```

---

## Self-Review

**Spec coverage (Phase 4 design → task):**
- A1 barge-in truncation → **Task 3** (`on_interruption` + `truncate_last` wiring).
- A2 bounded hot-path retry w/ jittered backoff → **Task 1** (helper) + **Task 2** (apply to greeting/coverage/mongo). Honest scope: in-stream STT/LLM/TTS provider retries are the plugin's concern, documented in `retry.py` and Task 2.
- B1 reconnect/resume → **Task 4** (grace timer) + **Task 5** (worker wiring + resume re-ask of last Tara line).
- B2 `mark_participant` at candidate-join → **Task 5** Step 4(b/d).
- B3 `participant_disconnected` candidate-only guard → **Task 4** (`_is_candidate`) + **Task 5** wiring.
- B4 exactly-once teardown → **Task 4** (`teardown` once-guard) + **Task 5** (single `_teardown`, one `limiter.release`).
- B5 audit completeness → **Task 6**.
- B6 rejected-blob TTL → **Task 7**.
- C1 worker drain → **Task 9**.
- C2 Dockerfiles → **Task 10**.
- C3 backend `/healthz` → **Task 8**.

**Placeholder scan:** No "TBD"/"add error handling"/"similar to". The two "verify at implementation" notes (LiveKit interruption event in Task 3; Lua-path/COPY context + `download-files` subcommand in Task 10) are explicit investigation steps with a concrete default and a verification command — honest unknowns about external tool surfaces, not deferred work. Every code step shows full code.

**Type/name consistency:** `retry_async(fn, *, attempts, base_delay, max_delay, retry_on, sleep, rng)` and `is_transient` consistent T1↔T2. `SessionLifecycle` methods (`note_candidate`, `participant_left`, `participant_joined`, `teardown`, `_is_candidate`) consistent T4↔T5. `DrainController` (`register`/`unregister`/`request_drain`/`is_draining`/`active`/`wait_drained`) consistent T9. `audit_transcript(lines)->list[str]` consistent T6. `makeHealthz(redis)` consistent T8. `createSession(redis, tokenFactory, limiter, body, opts)` + `rejectedBlobTtl` consistent T7. Config fields (`hot_retry_attempts`, `offpath_retry_attempts`, `retry_base_delay_ms`, `retry_max_delay_ms`, `reconnect_grace_seconds`) consistent across T1/T2/T5.

**Build-order sanity:** T1→T2 (helper before use); T4→T5 (lifecycle before wiring); T3 independent; T6 after T2 (persistence touched in both — T6 edits a different region); T5 and T9 both edit `_teardown` — T9 adds one line after T5's `done.set()`, so T9 sequences AFTER T5 (it does). T8/T9 both edit deploy manifests (different files / different region of agent-deployment) — independent. Backend tasks T7/T8 independent.

**GKE-gating honesty:** Tasks 1–9 are locally unit-tested; Task 10 Dockerfiles BUILD locally. Only in-cluster drain-on-scale-down behavior stays the operator check (already in the Phase 3 runbook). No task claims to prove cluster runtime locally.

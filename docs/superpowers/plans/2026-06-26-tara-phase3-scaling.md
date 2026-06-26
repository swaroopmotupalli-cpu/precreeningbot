# Tara Phase 3 — Scaling & Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scale the agent worker on **active-session count per pod** (never CPU) with a warm-headroom buffer, export the limiter + latency metrics to Prometheus, and run the staged load test that calibrates `GEMINI_TPM_BUDGET` / `SESSIONS_PER_POD_TARGET` / warm-pool, measures p95 + the false-reclaim slope, and closes the carried-open GKE single-stream p50 re-measure.

**Architecture:** The Python worker exposes a Prometheus `/metrics` endpoint (active sessions, warm pods, per-bucket utilization, reclaim/reject + false-reclaim counters read from the limiter, turn-latency histogram). KEDA scales the worker Deployment off the summed `active_sessions` custom metric via its Prometheus scaler; a warm-pool floor + PodDisruptionBudget + preStop drain protect the 45s lease and in-flight interviews. A staged load-test harness (built on the Phase-1 single-stream harness) drives 50→120→200 concurrent synthetic candidates and emits the headline numbers.

**Tech Stack:** Python 3.12, `prometheus-client`; Kubernetes (GKE), KEDA (Prometheus scaler), Prometheus, PodDisruptionBudget; `kubeconform`/`kubectl --dry-run=client` for manifest validation.

> **Plan scope & GKE-gating (read first):** Phase 3 only; builds on merged `main` (Phase 1 + Phase 2). Branch: `tara/phase3-scaling` (already created off `main`). **Tasks 1–3 are locally testable (TDD) — exporter metric logic + load-test aggregation math.** **Tasks 4–7 are GKE-gated:** the manifests/KEDA/PDB are *written and syntactically validated here* (`kubeconform`/dry-run), but their runtime behavior (HPA actually scaling, the calibrated numbers, p95, false-reclaim slope, GKE p50) can ONLY be validated on a real GKE cluster with live provider quota. Those acceptance criteria are explicitly the cluster operator's step — this plan produces the artifacts and the runbook, not the cluster run. Per the build order, Phase 3's load test is also where the **carried-open Phase-1 single-stream p50** is finally re-measured in the production environment.

## Global Constraints

Copied verbatim from the spec (§5, §6, §7):

- **Scale signal = active-session count per pod. NEVER CPU** (CPU is a lagging signal for these I/O-bound agents). Target **~10–15 sessions/pod** (`SESSIONS_PER_POD_TARGET=12` default, load-test-calibrated).
- **Warm headroom buffer ~20% spare pods** (`WARM_POOL_PERCENT=20`); it is a **correctness dependency** for the 45s Tier-1 lease (cold-start dispatch >30s fires false no-shows). `WARM_POOL_MIN_PODS` absolute floor set by the load test.
- **Metric freshness is a reliability parameter**, not a dashboard preference: `METRIC_SCRAPE_INTERVAL=5s`, `KEDA_POLL_INTERVAL=10s`; small scale-up stabilization (react fast), larger scale-down (drain calm). `T_react < T_drain` must hold.
- **Scale-down drains, never evicts**: PodDisruptionBudget + `preStop` drain hook + KEDA cooldown; never kill a pod with live sessions.
- **Exported signals**: `active_sessions{pod}`, `warm_pods_available` (paged), `bucket_utilization{bucket}` + `bucket_rejections{bucket}`, `turn_latency` histogram (p50/p95) + `llm_ttft` + per-stage, `lease_reclaims{reason}`, **`false_reclaims`** (hard gate).
- **Staged load test 50 → 120 → 200**; two headline metrics per stage: **p95 turn latency** (admitted sessions) + **bucket rejection rate by bucket**.
- **Hard gates the load test must prove**: cap never exceeded; over-cap queues/rejects gracefully; no-show reclaim ≤45s; crash reclaim ≤60s; reconnect does not increment the cap; **`false_reclaims` == 0 across all three stages** (a real candidate evicted mid-join is a hard fail even if p95 passes); false-reclaim **slope** flat across 50→120→200.
- **Three calibrated numbers the load test MUST emit**: (1) measured **TOTAL tokens-per-session including the off-path coverage-tagger calls** → sets `GEMINI_TPM_BUDGET`; (2) measured sessions-per-pod before latency degrades → confirms/corrects `SESSIONS_PER_POD_TARGET`; (3) warm-buffer behavior under the sharpest ramp → confirms `WARM_POOL_PERCENT` and whether `WARM_POOL_MIN_PODS` is needed.
- **Revised single-stream p50** (from Phase 1, to confirm on GKE): goal < 1.2s / hard floor < 1.5s.
- Gemini limits are per-project not per-key (README TODO).

---

### Task 1: Prometheus metrics registry

**Files:**
- Create: `agent/tara_agent/metrics_export.py`
- Test: `agent/tests/test_metrics_export.py`

**Interfaces:**
- Produces: `class TaraMetrics`:
  - `__init__(self, registry=None)` — builds a `prometheus_client.CollectorRegistry` (own registry, not the global default, for test isolation) with: `active_sessions` (Gauge), `warm_pods_available` (Gauge), `bucket_utilization` (Gauge, label `bucket`), `bucket_rejections` (Counter, label `bucket`), `lease_reclaims` (Counter, label `reason`), `false_reclaims` (Counter), `turn_latency_seconds` (Histogram, buckets `[0.3,0.5,0.8,1.0,1.2,1.5,2.0,3.0,5.0]`), `llm_ttft_seconds` (Histogram, same-ish buckets).
  - `session_started()` / `session_ended()` → inc/dec `active_sessions`.
  - `observe_turn(total_s: float, llm_ttft_s: float)` → observes both histograms.
  - `sync_limiter(metrics: dict, caps: dict)` — takes the dict from `Limiter.metrics()` (keys `reclaim_no_show/crash/false`, `reject_<bucket>`) plus current bucket counts/caps, and sets the gauges/counters. Counters are set via the absolute-value pattern (track last value, `inc` by delta) since Redis holds the source of truth.
  - `render() -> bytes` — `prometheus_client.generate_latest(self.registry)`.

- [ ] **Step 1: Write the failing test**

```python
# agent/tests/test_metrics_export.py
from tara_agent.metrics_export import TaraMetrics

def test_active_sessions_inc_dec():
    m = TaraMetrics()
    m.session_started(); m.session_started(); m.session_ended()
    body = m.render().decode()
    assert "active_sessions 1.0" in body

def test_turn_latency_observed():
    m = TaraMetrics()
    m.observe_turn(total_s=0.7, llm_ttft_s=0.3)
    body = m.render().decode()
    assert "turn_latency_seconds_count 1.0" in body
    assert "llm_ttft_seconds_count 1.0" in body

def test_sync_limiter_sets_reclaim_and_reject():
    m = TaraMetrics()
    m.sync_limiter(
        {"reclaim_no_show": 2, "reclaim_crash": 1, "reclaim_false": 0,
         "reject_global": 5, "reject_gemini_tpm": 3,
         "reject_stt_streams": 0, "reject_tts_streams": 0},
        caps={"global": 10, "gemini_tpm": 6, "stt_streams": 10, "tts_streams": 10},
    )
    body = m.render().decode()
    assert 'lease_reclaims_total{reason="no_show"} 2.0' in body
    assert 'false_reclaims_total 0.0' in body
    assert 'bucket_rejections_total{bucket="gemini_tpm"} 3.0' in body

def test_false_reclaims_is_distinct_counter():
    m = TaraMetrics()
    m.sync_limiter({"reclaim_no_show": 0, "reclaim_crash": 0, "reclaim_false": 4,
                    "reject_global": 0, "reject_gemini_tpm": 0,
                    "reject_stt_streams": 0, "reject_tts_streams": 0}, caps={})
    body = m.render().decode()
    assert "false_reclaims_total 4.0" in body
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd agent && pip install 'prometheus-client~=0.20' && pytest tests/test_metrics_export.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tara_agent.metrics_export'`
(Add `prometheus-client~=0.20` to `agent/pyproject.toml` dependencies.)

- [ ] **Step 3: Write `metrics_export.py`**

```python
# agent/tara_agent/metrics_export.py
from prometheus_client import (
    CollectorRegistry, Gauge, Counter, Histogram, generate_latest,
)

_LAT_BUCKETS = (0.3, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 3.0, 5.0)

class TaraMetrics:
    def __init__(self, registry: CollectorRegistry | None = None):
        self.registry = registry or CollectorRegistry()
        self.active_sessions = Gauge("active_sessions", "Live interviews on this pod", registry=self.registry)
        self.warm_pods_available = Gauge("warm_pods_available", "Warm spare pods", registry=self.registry)
        self.bucket_utilization = Gauge("bucket_utilization", "count/cap per bucket", ["bucket"], registry=self.registry)
        self.bucket_rejections = Counter("bucket_rejections", "Admission rejections by bucket", ["bucket"], registry=self.registry)
        self.lease_reclaims = Counter("lease_reclaims", "Lease reclaims by reason", ["reason"], registry=self.registry)
        self.false_reclaims = Counter("false_reclaims", "Real participants evicted mid-join (HARD GATE)", registry=self.registry)
        self.turn_latency = Histogram("turn_latency_seconds", "End-of-utterance to first audio", buckets=_LAT_BUCKETS, registry=self.registry)
        self.llm_ttft = Histogram("llm_ttft_seconds", "LLM time-to-first-token", buckets=_LAT_BUCKETS, registry=self.registry)
        self._last = {}  # absolute-value tracking for Redis-sourced counters

    def session_started(self): self.active_sessions.inc()
    def session_ended(self): self.active_sessions.dec()

    def observe_turn(self, total_s: float, llm_ttft_s: float):
        self.turn_latency.observe(total_s)
        self.llm_ttft.observe(llm_ttft_s)

    def _bump(self, counter, key, new):
        delta = new - self._last.get(key, 0)
        if delta > 0:
            counter.inc(delta)
        self._last[key] = new

    def sync_limiter(self, metrics: dict, caps: dict):
        for reason in ("no_show", "crash", "false"):
            self._bump(self.lease_reclaims.labels(reason=reason), f"rc_{reason}",
                       metrics.get(f"reclaim_{reason}", 0))
        self.false_reclaims  # ensure registered
        self._bump(self.false_reclaims, "false", metrics.get("reclaim_false", 0))
        for b in ("global", "gemini_tpm", "stt_streams", "tts_streams"):
            self._bump(self.bucket_rejections.labels(bucket=b), f"rej_{b}",
                       metrics.get(f"reject_{b}", 0))
            cap = (caps or {}).get(b, 0)
            if cap:
                self.bucket_utilization.labels(bucket=b).set(min(1.0, self._last.get(f"cnt_{b}", 0) / cap))

    def render(self) -> bytes:
        return generate_latest(self.registry)
```

- [ ] **Step 4: Run to green**

Run: `cd agent && pytest tests/test_metrics_export.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add agent/tara_agent/metrics_export.py agent/tests/test_metrics_export.py agent/pyproject.toml
git commit -m "feat(metrics): Prometheus registry for sessions/latency/limiter signals"
```

---

### Task 2: Wire the exporter into the worker (active sessions, turn latency, limiter sync, /metrics server)

**Files:**
- Modify: `agent/tara_agent/worker.py`
- Manual/integration verification (the metric *logic* is unit-tested in Task 1; this is the wiring seam, like Phase 1/2 worker wiring).

**Interfaces:**
- Consumes: `TaraMetrics` (Task 1), `Limiter.metrics()` (Phase 2).
- Produces: a module-level `TaraMetrics` started once per worker process via `prometheus_client.start_http_server(port, registry=metrics.registry)` on `METRICS_PORT` (default 9091); `session_started()` after `session.start`, `session_ended()` in the same flush/cleanup path; `observe_turn(...)` from the existing `metrics_collected` handler (reuse the already-computed `eou+ttft+ttfb` total and `llm_ttft`); a periodic task that calls `metrics.sync_limiter(await limiter.metrics(), caps_dict)` every `METRIC_SCRAPE_INTERVAL`.

> **Verify at implementation time:** `prometheus_client.start_http_server` must be called once per *process* (the LiveKit worker process may run multiple jobs); guard with a module-level flag so re-entry is a no-op. Confirm the port doesn't collide with the worker's own HTTP server. Add `METRICS_PORT` + `metric_scrape_interval` to `config.py` Settings (default 9091, 5).

- [ ] **Step 1: Add config fields** (in `config.py` Settings): `metrics_port: int = 9091`, `metric_scrape_interval: int = 5`. (Add a test assertion to `test_config.py` mirroring Phase 2 Task 1's pattern.)

- [ ] **Step 2: Wire into `worker.py`** — at module load, create `_METRICS = TaraMetrics()` and start the HTTP server once (guarded). In `entrypoint`: `_METRICS.session_started()` after `session.start`; in the `metrics_collected` TTSMetrics branch where `record_turn` already fires, also call `_METRICS.observe_turn(total, pending["ttft"])`; add a `_metrics_sync_loop` task (every `s.metric_scrape_interval`s: `_METRICS.sync_limiter(await limiter.metrics(), caps_dict)`); call `_METRICS.session_ended()` in `_flush_breakdown` (once-guarded alongside the existing flush). Cancel the sync loop after `done`.

```python
# additions to worker.py (sketch — integrate with existing structure)
from tara_agent.metrics_export import TaraMetrics
import prometheus_client

_METRICS = TaraMetrics()
_metrics_server_started = False
def _ensure_metrics_server(port):
    global _metrics_server_started
    if not _metrics_server_started:
        prometheus_client.start_http_server(port, registry=_METRICS.registry)
        _metrics_server_started = True

# in entrypoint, after settings:
_ensure_metrics_server(s.metrics_port)
caps_dict = {"global": s.max_global_sessions, "gemini_tpm": s.gemini_tpm_budget,
             "stt_streams": s.stt_stream_budget, "tts_streams": s.tts_stream_budget}
async def _metrics_sync_loop():
    while not done.is_set():
        await _METRICS.sync_limiter(  # sync_limiter is sync; wrap the await on limiter.metrics()
            *( (await limiter.metrics()), caps_dict))
        await asyncio.sleep(s.metric_scrape_interval)
# session_started() after session.start; observe_turn(total, ttft) in the metrics handler;
# session_ended() inside _flush_breakdown; ms = asyncio.create_task(_metrics_sync_loop()); ms.cancel() after done.
```

- [ ] **Step 3: Verify** — `python -c "from tara_agent import worker"` exits 0; `python - <<'PY'` snippet importing `TaraMetrics`, calling `session_started`/`observe_turn`/`sync_limiter`, and asserting `b"active_sessions"` in `render()` (proves the wiring objects line up). Do NOT block on `worker dev`.

- [ ] **Step 4: Commit**

```bash
git add agent/tara_agent/worker.py agent/tara_agent/config.py agent/tests/test_config.py
git commit -m "feat(metrics): export active_sessions/turn_latency/limiter metrics from the worker"
```

---

### Task 3: Load-test aggregation & report (the math that emits the headline + calibrated numbers)

**Files:**
- Create: `tools/loadtest/aggregate.py`
- Test: `tools/loadtest/test_aggregate.py`

**Interfaces:**
- Produces: `@dataclass SessionOutcome(admitted: bool, rejected_bucket: str | None, turn_latencies_s: list[float], total_tokens: int, was_injected_noshow: bool, was_real_participant: bool, reclaimed_reason: str | None)`; and:
  - `def p95(latencies: list[float]) -> float`
  - `def stage_report(stage_name: str, concurrency: int, outcomes: list[SessionOutcome]) -> dict` → `{stage, concurrency, p95_turn_latency, rejection_rate, rejections_by_bucket, false_reclaims, tokens_per_session_p50}` where **false_reclaims counts outcomes that were real participants but got reclaimed** (NOT injected no-shows).
  - `def false_reclaim_slope(stage_reports: list[dict]) -> float` — linear slope of `false_reclaims` across the staged concurrencies (flat/zero = healthy).
  - `def calibrated_budgets(stage_reports: list[dict], target_pod_sessions: int) -> dict` → emits the three numbers: `gemini_tpm_budget` (from measured TOTAL tokens-per-session incl. tagger × target throughput), `sessions_per_pod` (highest concurrency/pod before p95 degraded past the floor), `warm_pool_note`.

- [ ] **Step 1: Write the failing test**

```python
# tools/loadtest/test_aggregate.py
from aggregate import SessionOutcome, p95, stage_report, false_reclaim_slope

def _ok(lat, tokens=1000): 
    return SessionOutcome(True, None, lat, tokens, False, True, None)

def test_p95():
    assert p95([0.1*i for i in range(1, 101)]) == 9.5  # 95th of 0.1..10.0

def test_stage_report_rejection_and_false_reclaim():
    outs = [_ok([0.7, 0.8]) for _ in range(8)]
    outs.append(SessionOutcome(False, "gemini_tpm", [], 0, False, False, None))  # rejected
    # a REAL participant that got reclaimed = false reclaim (hard gate)
    outs.append(SessionOutcome(True, None, [0.9], 1000, False, True, "false"))
    r = stage_report("stage2", 10, outs)
    assert r["rejections_by_bucket"]["gemini_tpm"] == 1
    assert r["false_reclaims"] == 1
    assert 0.0 < r["rejection_rate"] < 1.0

def test_injected_noshow_is_not_a_false_reclaim():
    outs = [SessionOutcome(True, None, [], 0, True, False, "no_show")]  # injected no-show
    r = stage_report("s", 1, outs)
    assert r["false_reclaims"] == 0

def test_false_reclaim_slope_flat_is_zero():
    reports = [{"concurrency": 50, "false_reclaims": 0},
               {"concurrency": 120, "false_reclaims": 0},
               {"concurrency": 200, "false_reclaims": 0}]
    assert false_reclaim_slope(reports) == 0.0
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd tools/loadtest && python -m pytest test_aggregate.py -v` (uses the agent venv: `source ../../agent/.venv/bin/activate`)
Expected: FAIL — `ModuleNotFoundError: No module named 'aggregate'`

- [ ] **Step 3: Write `aggregate.py`**

```python
# tools/loadtest/aggregate.py
from dataclasses import dataclass

@dataclass
class SessionOutcome:
    admitted: bool
    rejected_bucket: str | None
    turn_latencies_s: list
    total_tokens: int
    was_injected_noshow: bool
    was_real_participant: bool
    reclaimed_reason: str | None

def p95(latencies):
    if not latencies:
        return 0.0
    s = sorted(latencies)
    # nearest-rank
    import math
    rank = max(1, math.ceil(0.95 * len(s)))
    return s[rank - 1]

def stage_report(stage_name, concurrency, outcomes):
    admitted = [o for o in outcomes if o.admitted]
    rejected = [o for o in outcomes if not o.admitted]
    rej_by_bucket = {}
    for o in rejected:
        rej_by_bucket[o.rejected_bucket] = rej_by_bucket.get(o.rejected_bucket, 0) + 1
    all_lat = [l for o in admitted for l in o.turn_latencies_s]
    # false reclaim = a REAL participant that got reclaimed (not an injected no-show)
    false_reclaims = sum(
        1 for o in outcomes
        if o.was_real_participant and o.reclaimed_reason == "false" and not o.was_injected_noshow
    )
    tps = sorted(o.total_tokens for o in admitted if o.total_tokens > 0)
    tps_p50 = tps[len(tps) // 2] if tps else 0
    return {
        "stage": stage_name, "concurrency": concurrency,
        "p95_turn_latency": p95(all_lat),
        "rejection_rate": (len(rejected) / len(outcomes)) if outcomes else 0.0,
        "rejections_by_bucket": rej_by_bucket,
        "false_reclaims": false_reclaims,
        "tokens_per_session_p50": tps_p50,
    }

def false_reclaim_slope(stage_reports):
    pts = [(r["concurrency"], r["false_reclaims"]) for r in stage_reports]
    n = len(pts)
    if n < 2:
        return 0.0
    sx = sum(x for x, _ in pts); sy = sum(y for _, y in pts)
    sxy = sum(x * y for x, y in pts); sxx = sum(x * x for x, _ in pts)
    denom = n * sxx - sx * sx
    return 0.0 if denom == 0 else (n * sxy - sx * sy) / denom

def calibrated_budgets(stage_reports, target_pod_sessions):
    # tokens-per-session p50 across stages × target throughput → TPM budget headroom.
    tps = max((r["tokens_per_session_p50"] for r in stage_reports), default=0)
    healthy = [r for r in stage_reports if r["p95_turn_latency"] <= 1.5 and r["false_reclaims"] == 0]
    max_ok_conc = max((r["concurrency"] for r in healthy), default=0)
    return {
        "gemini_tpm_budget_basis_tokens_per_session": tps,
        "sessions_per_pod_max_before_degrade": max_ok_conc,
        "target_pod_sessions_assumed": target_pod_sessions,
        "warm_pool_note": "set WARM_POOL_MIN_PODS if false_reclaim_slope climbs across stages",
    }
```

- [ ] **Step 4: Run to green**

Run: `cd tools/loadtest && python -m pytest test_aggregate.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add tools/loadtest/aggregate.py tools/loadtest/test_aggregate.py
git commit -m "feat(loadtest): stage aggregation — p95, rejection-by-bucket, false-reclaim slope, calibration"
```

---

### Task 4: Load-test driver (GKE/live — orchestrates concurrent synthetic candidates)

**Files:**
- Create: `tools/loadtest/run_loadtest.py`
- Create: `tools/loadtest/README.md`

**Interfaces:**
- Consumes: the Phase-1 single-stream harness primitives (`tools/latency_harness.py` — session create, room join, WAV publish, `tara_done` gating) and `aggregate.py` (Task 3).
- Produces: a driver that, per stage in `[50, 120, 200]`, launches that many concurrent synthetic candidates (each: POST /sessions, join, publish WAVs gated on `tara_done`, record turn latencies + tokens), **injects** a configurable fraction of no-shows (token issued, never joins) and worker-crash/reconnect scenarios, collects `SessionOutcome`s, and prints `stage_report` per stage + the `false_reclaim_slope` + `calibrated_budgets`. **Asserts the hard gates**: cap never exceeded, `false_reclaims == 0` every stage, slope flat.

> **GKE-gated — this is the cluster operator's run, not a local unit test.** It requires the deployed stack (Tasks 5–6) on GKE, live LiveKit/Google quota, and real load capacity. The DRIVER LOGIC (staging, injection, outcome collection, calling aggregate) is what this task delivers; its *execution* produces the numbers in Task 7. Local verification here = `python -c "import ast; ast.parse(open('tools/loadtest/run_loadtest.py').read())"` + a `--dry-run` mode that runs 2 synthetic sessions against a local worker+backend to prove the driver wiring (no full 200-concurrent run locally).

- [ ] **Step 1: Write the driver** (full `run_loadtest.py` with `--stages 50,120,200`, `--noshow-fraction`, `--dry-run`; per-stage asyncio.gather of N candidate coroutines reusing the Phase-1 harness; collect outcomes; print reports + assert hard gates) — *(full code: build on `latency_harness.py`'s session/join/publish functions; ~150 lines)*.

- [ ] **Step 2: Verify** — `ast.parse` clean; `python tools/loadtest/run_loadtest.py --dry-run` (2 sessions against local backend+worker) prints two `stage_report`s and exits 0. Document in README that the real staged run is a GKE step.

- [ ] **Step 3: Commit**

```bash
git add tools/loadtest/run_loadtest.py tools/loadtest/README.md
git commit -m "feat(loadtest): staged driver (50/120/200) with no-show injection + hard-gate asserts"
```

---

### Task 5: Kubernetes manifests (agent worker + backend Deployments, Services, config)

**Files:**
- Create: `deploy/agent-deployment.yaml`, `deploy/backend-deployment.yaml`, `deploy/services.yaml`, `deploy/configmap.yaml`, `deploy/README.md`

**Interfaces:**
- Produces: a `tara-agent` Deployment (Python worker; container exposes `METRICS_PORT` 9091; env from ConfigMap + Secret; `readinessProbe` on the metrics port; resources requests/limits sized for I/O-bound work — modest CPU, headroom RAM), a `tara-backend` Deployment + Service (Node `/sessions`), a ConfigMap for non-secret config (caps, lease TTLs, `SESSIONS_PER_POD_TARGET`, `WARM_POOL_PERCENT`, `METRIC_SCRAPE_INTERVAL`), and Secret *references* (never inline secrets — `GEMINI_API_KEY`, `GOOGLE_APPLICATION_CREDENTIALS` via mounted secret, `LIVEKIT_*`, `MONGODB_URI`, `REDIS_URL`).

> **GKE-gated for runtime; locally validatable for SYNTAX.** Validate every manifest with `kubeconform -strict` (or `kubectl apply --dry-run=client -f`). No secret values in any file (CI/secret-scanner clean). Runtime behavior (pods scheduling, probes passing) is the cluster step.

- [ ] **Step 1: Write the manifests** (concrete YAML — agent Deployment with `containerPort: 9091` named `metrics`, env, probes, resources; backend Deployment+Service; ConfigMap with the env keys; Secret refs via `secretKeyRef`/volume mount for the GCP creds JSON).

- [ ] **Step 2: Validate** — `kubeconform -strict -summary deploy/*.yaml` (install kubeconform, or `kubectl apply --dry-run=client -f deploy/`). Expected: all valid, 0 errors. Run `gitleaks detect`/grep to confirm NO secret values in `deploy/`.

- [ ] **Step 3: Commit**

```bash
git add deploy/agent-deployment.yaml deploy/backend-deployment.yaml deploy/services.yaml deploy/configmap.yaml deploy/README.md
git commit -m "feat(deploy): k8s Deployments/Service/ConfigMap (metrics port, secret refs, no inline secrets)"
```

---

### Task 6: KEDA scaling on active-session count + warm pool + drain safety

**Files:**
- Create: `deploy/keda-scaledobject.yaml`, `deploy/pdb.yaml`, `deploy/prometheus-scrape.yaml`
- Modify: `deploy/agent-deployment.yaml` (add `preStop` drain hook + `terminationGracePeriodSeconds`)

**Interfaces:**
- Produces: a KEDA `ScaledObject` targeting the `tara-agent` Deployment, **Prometheus scaler** querying `sum(active_sessions)` with `threshold` = `SESSIONS_PER_POD_TARGET` (scales replicas so avg sessions/pod ≈ target), `minReplicaCount` set for the ~20% warm floor, `pollingInterval: 10` (`KEDA_POLL_INTERVAL`), `cooldownPeriod` larger than polling (calm scale-down), and `advanced.horizontalPodAutoscalerConfig.behavior` with **fast scale-up / slow scale-down** stabilization windows; a `PodDisruptionBudget` (`minAvailable`) so live-session pods aren't evicted; a `preStop` hook that flips the worker to stop accepting new jobs and waits for in-flight interviews to drain (within `terminationGracePeriodSeconds`); a Prometheus scrape config/ServiceMonitor with `scrape_interval: 5s` (`METRIC_SCRAPE_INTERVAL`) for the agent metrics port.

> **GKE-gated for runtime; locally validatable for SYNTAX.** `kubeconform`/dry-run validate. The `T_react < T_drain` constraint, actual scale-up-leads-demand behavior, and warm-pool-never-floors are the cluster step (Task 7). Document inline that scrape/poll cadence are *in-path reliability params*, not monitoring taste.

- [ ] **Step 1: Write the manifests** (KEDA ScaledObject with prometheus trigger `serverAddress`, `query: sum(active_sessions)`, `threshold: "12"`, `minReplicaCount` ≈ ceil(target_pods × 1.2); PDB `minAvailable`; preStop `exec`/`httpGet` drain; ServiceMonitor scrape_interval 5s).

- [ ] **Step 2: Validate** — `kubeconform -strict deploy/keda-scaledobject.yaml deploy/pdb.yaml deploy/prometheus-scrape.yaml` (KEDA CRD schema may need `-schema-location` for the KEDA CRD; document the flag). Confirm no secrets.

- [ ] **Step 3: Commit**

```bash
git add deploy/keda-scaledobject.yaml deploy/pdb.yaml deploy/prometheus-scrape.yaml deploy/agent-deployment.yaml
git commit -m "feat(deploy): KEDA scale-on-active-sessions + warm floor + PDB/drain + 5s scrape"
```

---

### Task 7: Staged load-test runbook & GKE acceptance (the cluster validation that emits the numbers)

**Files:**
- Create: `docs/superpowers/runbooks/2026-06-26-phase3-loadtest-runbook.md`

**Interfaces:**
- Consumes: the deployed stack (Tasks 5–6), the driver (Task 4), the exporter (Tasks 1–2).
- Produces: a step-by-step runbook the cluster operator follows to: deploy to GKE, raise the Google STT/Gemini quotas, run `run_loadtest.py --stages 50,120,200`, and record the **acceptance results** — the two headline metrics per stage (p95 + rejection-rate-by-bucket), the hard-gate outcomes (cap held, `false_reclaims == 0` every stage, slope flat, no-show ≤45s, crash ≤60s, reconnect no double-count, `T_react < T_drain`), the **three calibrated numbers** (`GEMINI_TPM_BUDGET` from total tokens/session incl. tagger, `SESSIONS_PER_POD_TARGET`, warm-pool sizing + whether `WARM_POOL_MIN_PODS` is needed), and the **carried-open single-stream GKE p50** (goal <1.2s / floor <1.5s).

> This task is a DOCUMENT (the runbook) + the acceptance checklist. The actual run is the operator's on GKE — this plan cannot execute it. The runbook makes the run reproducible and the pass/fail explicit. Mark every number as "to be filled by the GKE run."

- [ ] **Step 1: Write the runbook** (deploy steps; quota-raise checklist; the staged run command; a results table with each acceptance row and a blank to fill; the explicit hard-fail conditions — any real candidate evicted mid-join at any stage = hard fail even if p95 passes; back-out via the `phase-2-merged` tag).

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/runbooks/2026-06-26-phase3-loadtest-runbook.md
git commit -m "docs(loadtest): Phase-3 GKE load-test runbook + acceptance checklist (operator-run)"
```

---

## Self-Review

**Spec coverage (Phase 3 / §5–§7):**
- Scale on active-session count, not CPU; target ~12/pod → Task 6 KEDA `query: sum(active_sessions)`, `threshold: 12`; the `active_sessions` gauge → Tasks 1–2.
- Warm headroom ~20% (correctness dep) + `WARM_POOL_MIN_PODS` → Task 6 `minReplicaCount`; sizing confirmed by Task 7 run.
- Metric freshness as reliability param (5s scrape, 10s poll, fast-up/slow-down) → Task 6.
- Scale-down drains never evicts (PDB + preStop) → Task 6.
- Exported signals incl. `false_reclaims` hard-gate counter → Tasks 1–2.
- Staged load test 50→120→200, headline p95 + rejection-by-bucket → Tasks 3–4, run in Task 7.
- Hard gates (cap, queue/reject, no-show ≤45s, crash ≤60s, reconnect, `false_reclaims==0`, slope flat, `T_react<T_drain`) → asserted in Task 4 driver, validated in Task 7.
- Three calibrated numbers incl. **tokens-per-session WITH the coverage-tagger** (catch #3) → Task 3 `calibrated_budgets` + Task 7 run.
- Carried-open Phase-1 GKE single-stream p50 re-measure → Task 7.

**GKE-gating honesty:** Tasks 1–3 are fully locally TDD'd; Task 4 delivers driver logic + a 2-session `--dry-run` (full run is GKE); Tasks 5–6 are syntactically validated (`kubeconform`/dry-run) with runtime behavior on the cluster; Task 7 is the operator runbook + acceptance. No task claims to *run* the cluster load test locally — that would be dishonest.

**Placeholder scan:** Task 4 Step 1 says "full code: ~150 lines" rather than inlining the whole driver — that is a deliberate pointer to build on the existing `latency_harness.py` primitives (DRY), not a TODO; the interfaces + behavior + verification are fully specified. All other code steps are complete. No "add error handling"/"TBD".

**Type/name consistency:** `TaraMetrics` methods (`session_started/ended`, `observe_turn`, `sync_limiter`, `render`) consistent across Tasks 1–2; `SessionOutcome` fields + `stage_report`/`p95`/`false_reclaim_slope`/`calibrated_budgets` consistent across Tasks 3–4; the metric names (`active_sessions`, `false_reclaims`, `bucket_rejections`, `lease_reclaims`) match between the exporter (Task 1), the KEDA query (Task 6), and the load-test asserts (Task 4). `Limiter.metrics()` keys (`reclaim_*`, `reject_*`) match what Task 1 `sync_limiter` consumes.

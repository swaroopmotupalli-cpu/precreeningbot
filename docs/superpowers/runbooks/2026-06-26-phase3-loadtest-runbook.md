# Tara Phase 3 — GKE Staged Load-Test Runbook & Acceptance

> **This is the cluster operator's runbook.** Phase 3's code (exporter, driver, manifests,
> KEDA/PDB/scrape) is written and statically validated, but its runtime behavior — HPA actually
> scaling, the calibrated numbers, p95, the false-reclaim slope, and the carried-open single-stream
> p50 — can ONLY be established on a real GKE cluster with live provider quota. Follow this runbook to
> deploy, run the staged load test (`50 → 120 → 200`), and fill in the acceptance results. **Every
> number below marked `__TBD (GKE run)__` is filled by the operator from this run.** Nothing in CI or
> on a laptop produces these numbers.

Branch under test: `tara/phase3-scaling`. Back-out target if acceptance fails: tag **`phase-2-merged`**
(the last known-good integrated `main` before Phase 3).

---

## 0. Prerequisites (cluster-side, installed out-of-band — NOT by our manifests)

- [ ] GKE cluster reachable; `kubectl` context points at it (`kubectl config current-context`).
- [ ] **KEDA operator** installed (`keda.sh`) — provides the `ScaledObject` CRD + metrics adapter.
- [ ] **Prometheus** (Prometheus Operator preferred — provides the `ServiceMonitor` CRD) installed and
      reachable in-cluster at the address in `deploy/keda-scaledobject.yaml` (`serverAddress`,
      default `http://prometheus.monitoring.svc.cluster.local:9090` — **adjust to your stack**).
- [ ] Container images **built and pushed**: replace the placeholder `IMAGE_REGISTRY/tara-agent:TAG`
      and `IMAGE_REGISTRY/tara-backend:TAG` in `deploy/{agent,backend}-deployment.yaml` with the real
      pushed images. (Dockerfiles are a Phase-4 / operator follow-up — they do not exist in-repo yet.)
- [ ] **Provider quota raised** (see §1) — the staged run drives up to 200 concurrent streams.

## 1. Quota-raise checklist (do this BEFORE the 120/200 stages)

The single-stream gate ran on a laptop against the Developer API and India→Gemini latency; at 200
concurrent the binding limits are provider quotas, not pod CPU. Confirm headroom for the **peak** stage:

- [ ] **Google Cloud STT** — streaming-recognize concurrent-stream quota ≥ 200 (× languages if
      multi-language adds streams). Region: `global` (the worker uses `latest_long`/`global`).
- [ ] **Gemini (Developer API)** — per-**project** TPM/RPM (limits are per-project, not per-key) sized
      for `200 × tokens-per-session` **including the off-path coverage-tagger calls** (that total is one
      of the three numbers this run measures — see §5). Vertex AI is NOT used (unavailable in this env).
- [ ] **Google Cloud TTS** — Chirp3-HD concurrent synthesis quota ≥ 200.
- [ ] **LiveKit** — room/participant concurrency for ≥ 200 simultaneous rooms on the deployment plan.
- [ ] **Redis** — connection ceiling ≥ (pods × per-pod pool) + backend; the `{tara-limiter}` caps
      (`MAX_GLOBAL_SESSIONS=110` default) will REJECT past the global cap by design — see the cap gate
      in §4. If you intend 200 admitted, raise `MAX_GLOBAL_SESSIONS` in `deploy/configmap.yaml`
      accordingly; otherwise expect graceful rejection above 110 (that is itself an acceptance row).

> If any quota cannot be raised, record the ceiling reached and run the highest stage the quota allows.
> Do NOT mock/stub a provider to hit a number — a stubbed run is not an acceptance run.

## 2. Deploy

```bash
# 1. Secrets (out-of-band — never in git). Placeholder values shown.
kubectl create secret generic tara-secrets \
  --from-literal=GEMINI_API_KEY=REPLACE_ME \
  --from-literal=REDIS_URL=redis://REPLACE_ME \
  --from-literal=MONGODB_URI=mongodb+srv://REPLACE_ME \
  --from-literal=LIVEKIT_URL=wss://REPLACE_ME \
  --from-literal=LIVEKIT_API_KEY=REPLACE_ME \
  --from-literal=LIVEKIT_API_SECRET=REPLACE_ME
kubectl create secret generic tara-gcp-sa --from-file=sa.json=/path/to/service-account.json

# 2. Config + workloads + services (apply order).
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/backend-deployment.yaml
kubectl apply -f deploy/agent-deployment.yaml
kubectl apply -f deploy/services.yaml

# 3. Scaling + safety (requires KEDA + Prometheus-operator CRDs present).
kubectl apply -f deploy/prometheus-scrape.yaml   # ServiceMonitor (5s scrape)
kubectl apply -f deploy/pdb.yaml                  # minAvailable: 1
kubectl apply -f deploy/keda-scaledobject.yaml    # scale on sum(active_sessions), threshold 12
```

- [ ] All pods `Ready`; `kubectl get scaledobject tara-agent` shows `READY=True`, `ACTIVE` toggling.
- [ ] Prometheus is scraping the agent: query `active_sessions` returns a series per agent pod.
- [ ] Backend `/sessions` reachable from the load-test origin (port-forward or ingress).

## 3. Run the staged load test

The driver (`tools/loadtest/run_loadtest.py`) drives the three stages, injects no-shows, collects
client-side `SessionOutcome`s, and prints `stage_report` + `false_reclaim_slope` + `calibrated_budgets`.
Point it at the deployed backend.

```bash
source agent/.venv/bin/activate
export BACKEND_URL="https://<your-backend-ingress>"      # or http://localhost:<port-forward>
export LIVEKIT_URL="wss://<your-livekit>"                # if not returned in the /sessions blob
export GLOBAL_CAP="110"                                   # = MAX_GLOBAL_SESSIONS, enables the cap gate
python tools/loadtest/run_loadtest.py --stages 50,120,200 --noshow-fraction 0.1
```

> **Authoritative vs. client-side — read this.** The driver's in-process gate asserts are a
> **client-side smoke gate**: it cannot see server-side reclaim classification, per-session tokens, or
> the true cap from outside, so its `false_reclaims` reads 0 *by construction* and per-turn latency is a
> wall-clock proxy. The **authoritative** acceptance numbers come from the **worker's Prometheus
> metrics** captured DURING the run (§4–§5). Record both; on any disagreement, Prometheus is the source
> of truth.

## 4. Acceptance — hard gates (ALL must pass; any failure = back out via `phase-2-merged`)

Capture these from Prometheus during/just after each stage. **A real candidate evicted mid-join at ANY
stage is a HARD FAIL even if p95 passes.**

| # | Hard gate | How to verify (PromQL / observation) | 50 | 120 | 200 |
|---|-----------|--------------------------------------|----|-----|-----|
| 1 | Cap never exceeded | `max_over_time(sum(active_sessions)[stage:5s])` ≤ `MAX_GLOBAL_SESSIONS`; limiter `count:global` never > cap | `__TBD__` | `__TBD__` | `__TBD__` |
| 2 | Over-cap queues/rejects gracefully | backend returns 503 `queued`+`bucket` (no token) past cap; `bucket_rejections_total{bucket=...}` increments; no 5xx crash | `__TBD__` | `__TBD__` | `__TBD__` |
| 3 | No-show reclaim ≤ 45s | injected no-shows reclaimed within Tier-1 lease; `lease_reclaims_total{reason="no_show"}` rises ≤45s after token | `__TBD__` | `__TBD__` | `__TBD__` |
| 4 | Crash reclaim ≤ 60s | killed-worker sessions reclaimed within Tier-2; `lease_reclaims_total{reason="crash"}` ≤60s | `__TBD__` | `__TBD__` | `__TBD__` |
| 5 | Reconnect does NOT increment cap | drop+rejoin a live candidate; `count:global` unchanged across the reconnect | `__TBD__` | `__TBD__` | `__TBD__` |
| 6 | **`false_reclaims == 0`** (THE hard gate) | `false_reclaims_total` stays **0** for the whole stage — a present candidate must never be evicted | `__TBD__` | `__TBD__` | `__TBD__` |
| 7 | False-reclaim **slope** flat | `false_reclaim_slope` across 50→120→200 ≈ 0 (driver prints it; cross-check `false_reclaims_total` deltas) | — | — | `__TBD__` (single slope) |
| 8 | `T_react < T_drain` | scale-up reacts before lease window closes (scaleUp stabilization 0s); scale-down drains (300s window + preStop) — no pod with live sessions killed | `__TBD__` | `__TBD__` | `__TBD__` |

> Gate 6 expanded: `false_reclaims` counts a **real participant** whose lease lapsed **while
> `participant_joined==1`** (the limiter's `reap`/`admit`-inline classification → reason `"false"`).
> This is the warm-pool correctness signal: if cold-start dispatch exceeds the 45s lease, present
> candidates get wrongly evicted and this counter rises. **Any non-zero value at any stage fails the
> phase** regardless of latency.

## 5. Acceptance — headline metrics (two per stage)

| Metric | How | 50 | 120 | 200 |
|--------|-----|----|-----|-----|
| **p95 turn latency** (admitted sessions) | `histogram_quantile(0.95, sum(rate(turn_latency_seconds_bucket[stage])) by (le))` | `__TBD__` | `__TBD__` | `__TBD__` |
| **Bucket rejection rate by bucket** | `rate(bucket_rejections_total{bucket=...}[stage])` per `global/gemini_tpm/stt_streams/tts_streams` | `__TBD__` | `__TBD__` | `__TBD__` |

## 6. Acceptance — the three calibrated numbers (the point of the run)

| # | Number | Source | Result |
|---|--------|--------|--------|
| 1 | **`GEMINI_TPM_BUDGET`** basis | **TOTAL tokens-per-session INCLUDING the off-path coverage-tagger calls** × target throughput. Measure tokens/session from the worker (LLM_DIAG / a tokens metric), NOT the client driver (which reports 0). Feeds `GEMINI_TPM_BUDGET`. | `__TBD__` |
| 2 | **`SESSIONS_PER_POD_TARGET`** | highest sessions/pod before p95 degrades past the floor (driver `calibrated_budgets.sessions_per_pod_max_before_degrade`, cross-checked against per-pod `active_sessions` vs p95). Confirms/corrects the `12` default. | `__TBD__` |
| 3 | **Warm-pool sizing** | warm-buffer behavior under the sharpest 120→200 ramp: did `minReplicaCount: 2` (≈20%) keep dispatch < 30s and `false_reclaims` at 0? If the slope climbed, set an explicit **`WARM_POOL_MIN_PODS`** floor. | `__TBD__` (and: `WARM_POOL_MIN_PODS` needed? Y/N) |

## 7. Acceptance — carried-open single-stream p50 (re-measure on GKE)

The Phase-1 latency gate was measured on a laptop (India→Gemini, Developer API) and is pessimistic.
Re-measure the **single-stream** p50 on the cluster, in-region, with one candidate (the Phase-1 harness
or a 1-concurrency driver run):

- [ ] Single-stream p50 (time-to-first-audio = `eou_delay + llm_ttft + tts_ttfb`): `__TBD (GKE run)__`
- Goal **< 1.2s**, hard floor **< 1.5s** (revised target; see the design doc §7). Record pass/fail.
- This **closes the carried-open Phase-1 item.**

## 8. Pass/fail decision & back-out

- **PASS** iff: every §4 hard gate passes at all three stages (especially `false_reclaims == 0`), the
  §5 headlines are recorded, the §6 three numbers are captured (and any `WARM_POOL_MIN_PODS` applied),
  and the §7 p50 meets at least the hard floor.
- **HARD FAIL** conditions (back out regardless of other numbers): any `false_reclaims > 0` at any
  stage; cap exceeded; a pod with live sessions evicted (drain-not-evict violated); over-cap returning
  5xx instead of graceful 503.
- **Back-out:** redeploy the last known-good integrated state —
  `git checkout phase-2-merged` and re-apply (Phase 3 added only `deploy/*` + the metrics exporter; the
  limiter/pipeline are unchanged from `phase-2-merged`). Then triage with the recorded numbers before
  re-attempting.

## 9. Record-keeping

Paste the driver's full stdout (`stage_report`×3 + `false_reclaim_slope` + `calibrated_budgets`) and the
Prometheus query results into this file under each `__TBD__`, date the run, and note the cluster
region + the image tags used. Commit the filled runbook on a follow-up branch.

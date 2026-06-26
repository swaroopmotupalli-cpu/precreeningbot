# Tara — Kubernetes deploy manifests

Base manifests for running Tara on GKE: the Python LiveKit agent worker, the
Node backend, their Services, and non-secret config — plus the Phase-3 scaling,
drain-safety, and metrics-scrape wiring (KEDA ScaledObject, PodDisruptionBudget,
preStop drain hook, and Prometheus ServiceMonitor).

## Manifests

| File | What it is |
|------|------------|
| `configmap.yaml` | `tara-config` ConfigMap — all **non-secret** config for both workloads (admission caps, lease TTLs, metrics port, model/voice, backend `PORT`) plus the KEDA/operator scaling knobs. |
| `agent-deployment.yaml` | `tara-agent` Deployment — the Python LiveKit worker (`python -m tara_agent.worker start`). Exposes Prometheus metrics on port 9091 (`metrics`). |
| `backend-deployment.yaml` | `tara-backend` Deployment — the Node backend (`POST /sessions`) on port 3000. |
| `services.yaml` | `tara-backend` ClusterIP Service (port 3000) + `tara-agent-metrics` headless Service (port 9091) for per-pod metric scraping. |
| `keda-scaledobject.yaml` | KEDA `ScaledObject` targeting the `tara-agent` Deployment by name. Scales on `sum(active_sessions)` (NEVER CPU) via the Prometheus scaler, `threshold: 12` (= `SESSIONS_PER_POD_TARGET`). Warm floor `minReplicaCount: 2`, guardrail `maxReplicaCount: 20`, `pollingInterval: 10`, `cooldownPeriod: 300`, fast scale-up / slow scale-down behavior (`T_react < T_drain`). |
| `pdb.yaml` | `PodDisruptionBudget` (`minAvailable: 1`, selects `app: tara-agent`) — voluntary disruptions can't drop live-session pods below the floor. |
| `prometheus-scrape.yaml` | Prometheus-Operator `ServiceMonitor` selecting `tara-agent-metrics`, scraping port `metrics` path `/` every `5s` (= `METRIC_SCRAPE_INTERVAL`). Includes a commented raw `prometheus.yml` `scrape_config` for operator-less clusters. |

## Apply order

Config first, then workloads, then services:

```bash
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/agent-deployment.yaml
kubectl apply -f deploy/backend-deployment.yaml
kubectl apply -f deploy/services.yaml
# Scaling / drain / scrape (require operators — see prerequisites below):
kubectl apply -f deploy/keda-scaledobject.yaml
kubectl apply -f deploy/pdb.yaml
kubectl apply -f deploy/prometheus-scrape.yaml
```

(Order is not strictly enforced by Kubernetes, but applying the ConfigMap
first avoids pods crash-looping on missing config. Apply the `Service`
before the `ServiceMonitor` so the scrape target exists.)

### Cluster prerequisites for the Phase-3 manifests

- **KEDA operator** (`keda.sh`) installed — required for `keda-scaledobject.yaml`
  (the `ScaledObject` CRD). KEDA manages an HPA for the `tara-agent` Deployment.
- **Prometheus Operator** (`monitoring.coreos.com`) installed, with a Prometheus
  whose `serviceMonitorSelector` matches the ServiceMonitor's `release:` label —
  required for `prometheus-scrape.yaml`. That Prometheus must be reachable at the
  `serverAddress` in the ScaledObject's Prometheus trigger.
- **`PodDisruptionBudget`** (`policy/v1`) is a built-in — no operator needed.

## Secrets — create out-of-band (NEVER committed)

No secret values live in these manifests; everything sensitive is referenced
via `secretKeyRef` / a mounted Secret volume. Create the two Secrets manually
(or via your secrets manager / sealed-secrets / External Secrets) **before**
applying the Deployments. The values below are PLACEHOLDERS — substitute real
values from your `.env` and SA JSON (both already gitignored).

```bash
# Application secrets consumed via secretKeyRef
kubectl create secret generic tara-secrets \
  --from-literal=GEMINI_API_KEY=REPLACE_ME \
  --from-literal=REDIS_URL=redis://REPLACE_ME \
  --from-literal=MONGODB_URI=mongodb+srv://REPLACE_ME \
  --from-literal=LIVEKIT_URL=wss://REPLACE_ME \
  --from-literal=LIVEKIT_API_KEY=REPLACE_ME \
  --from-literal=LIVEKIT_API_SECRET=REPLACE_ME

# GCP service-account JSON, mounted as a file at /var/secrets/gcp/sa.json
kubectl create secret generic tara-gcp-sa \
  --from-file=sa.json=/path/to/your/service-account.json
```

`GOOGLE_APPLICATION_CREDENTIALS` is a **file path** (`/var/secrets/gcp/sa.json`),
not a value — the agent Deployment mounts the `tara-gcp-sa` Secret as a volume
and points the env var at the mounted file.

## Images — placeholders

No Dockerfiles exist yet; building and pushing images is the operator's GKE
step (Phase 4 / operator follow-up). The Deployments reference clearly-fake
images:

- `IMAGE_REGISTRY/tara-agent:TAG`
- `IMAGE_REGISTRY/tara-backend:TAG`

Replace these (e.g. `gcr.io/PROJECT_ID/tara-agent:v1`) with your built+pushed
images before applying. `imagePullPolicy` is `IfNotPresent`. The agent image
must contain the `agent/` tree (the container runs with `workingDir: /app/agent`).

## Scaling knobs in the ConfigMap

`SESSIONS_PER_POD_TARGET` ("12") and `WARM_POOL_PERCENT` ("20") are **operator /
KEDA inputs**, not read by `agent/tara_agent/config.py` today. They are
co-located in `tara-config` deliberately so all tuning lives in one place; the
KEDA ScaledObject (`keda-scaledobject.yaml`) consumes `SESSIONS_PER_POD_TARGET`
as its Prometheus-trigger `threshold`, and `WARM_POOL_PERCENT` documents the
`minReplicaCount` warm-floor formula
(`floor = ceil(baseline_pods × (1 + WARM_POOL_PERCENT/100))`).

## What comes later (Task 7)

- **Task 7** calibrates from load-test data: resource requests/limits, replica
  counts, the warm-floor `minReplicaCount`, `terminationGracePeriodSeconds`, and
  the KEDA `threshold`. The values present today are reasoned starting points,
  not load-proven numbers. kubeconform proves SCHEMA only — the runtime
  invariants (`T_react < T_drain`, scale-up leads demand, warm pool never floors,
  scale-down drains rather than evicts) are GKE-runtime properties verified there.
- A backend `/healthz` route does not exist yet — the backend readinessProbe is
  a TCP check on port 3000 for now; switch to `httpGet: /healthz` once added.

## Validation

`kubectl apply --dry-run=client` validates schema/structure locally for these
core types without contacting a cluster. It proves the YAML is well-formed and
schema-valid for the resource kinds — it does **not** prove runtime behavior
(image pulls, scheduling, probe success, secret existence). Full CRD-aware
schema validation in CI should use `kubeconform` (not installed locally here).

```bash
kubectl apply --dry-run=client \
  -f deploy/agent-deployment.yaml \
  -f deploy/backend-deployment.yaml \
  -f deploy/services.yaml \
  -f deploy/configmap.yaml
```

Runtime behavior (scheduling, probes passing, real scrape) is the GKE operator
step and is verified on-cluster, not by this dry-run.

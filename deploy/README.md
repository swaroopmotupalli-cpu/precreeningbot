# Tara — Kubernetes deploy manifests

Base manifests for running Tara on GKE: the Python LiveKit agent worker, the
Node backend, their Services, and non-secret config. This is the Phase-3
"Task 5" baseline — scaling, drain, and scrape wiring come in Task 6.

## Manifests

| File | What it is |
|------|------------|
| `configmap.yaml` | `tara-config` ConfigMap — all **non-secret** config for both workloads (admission caps, lease TTLs, metrics port, model/voice, backend `PORT`) plus the KEDA/operator scaling knobs. |
| `agent-deployment.yaml` | `tara-agent` Deployment — the Python LiveKit worker (`python -m tara_agent.worker start`). Exposes Prometheus metrics on port 9091 (`metrics`). |
| `backend-deployment.yaml` | `tara-backend` Deployment — the Node backend (`POST /sessions`) on port 3000. |
| `services.yaml` | `tara-backend` ClusterIP Service (port 3000) + `tara-agent-metrics` headless Service (port 9091) for per-pod metric scraping. |

## Apply order

Config first, then workloads, then services:

```bash
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/agent-deployment.yaml
kubectl apply -f deploy/backend-deployment.yaml
kubectl apply -f deploy/services.yaml
```

(Order is not strictly enforced by Kubernetes, but applying the ConfigMap
first avoids pods crash-looping on missing config.)

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
Task 6 KEDA ScaledObject / warm-pool logic consumes them.

## What comes later (Task 6 / Task 7)

- **Task 6** adds, by modifying `agent-deployment.yaml` and new manifests:
  `terminationGracePeriodSeconds` + a `preStop` drain hook on the agent,
  a KEDA `ScaledObject` targeting the `tara-agent` Deployment by name, a
  `PodDisruptionBudget`, and a Prometheus `ServiceMonitor` selecting the
  `tara-agent-metrics` Service. None of those are present here yet.
- **Task 7** calibrates resource requests/limits and replica counts from
  load-test data. The CPU/memory values here are starting points only.
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

# Tara — AI Voice Interview

Cascaded voice interview agent (STT → LLM → TTS) with async scoring. A candidate
joins a LiveKit room, **Tara** conducts a dynamic technical interview, and after it
ends the transcript is scored into an ATS-compatible report.

## Architecture — 3 processes

| Process | Path | Role |
|---|---|---|
| **Backend API** | `backend/` (`server.js`) | `POST /sessions` (fetch JD/resume from Marketplace, admit, mint LiveKit token) + serves the test UI |
| **Agent worker** | `agent/` (Python, LiveKit agent `tara_agent`) | Tara herself: STT → Gemini → TTS, live in the room |
| **Scorer** | `backend/src/scorer/` (`index.js`) | Redis-queue consumer: scores finished interviews → writes the report to Mongo |

Shared infra: **Redis** (admission limiter + score queue) and **MongoDB** (`Marketplace` DB: `contests`, `jobSeekerProfile`, `aiInterview`, `recruiterAddProfiles`, `auditTrail`).

## Prerequisites

- **Node 20+** and **Python 3.12**
- **Redis** (`:6379`) and **MongoDB** reachable via `MONGODB_URI`
- A repo-root **`.env`** with: `MONGODB_URI`, `REDIS_URL`, `GEMINI_API_KEY`,
  `GOOGLE_APPLICATION_CREDENTIALS` (absolute path to the GCP service-account JSON),
  `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, `PORT` (backend, e.g. 8085),
  optional `AGENT_NAME` (default `tara_agent`).
- All three processes **auto-load the repo-root `.env`** — no manual `source` needed.

### One-time setup

```bash
# Backend + scorer deps
cd backend && npm install

# Agent (Python) venv + deps
cd agent && python3.12 -m venv .venv && source .venv/bin/activate && pip install -e .
```

## Run the project (3 terminals)

```bash
# Terminal 1 — Backend API (serves the test UI + POST /sessions)
cd backend && npm start
# → http://localhost:8085

# Terminal 2 — Agent worker (Tara). Registers as agent_name "tara_agent"
cd agent && source .venv/bin/activate && python -m tara_agent.worker dev

# Terminal 3 — Scorer (scores interviews after they end)
cd backend && node src/scorer/index.js
```

Then open **http://localhost:8085**, grant microphone access, and click **Start
interview** (contestId / jsId / recruiterId are pre-filled with a test triple).

> The worker uses **explicit dispatch** (`agent_name=tara_agent`); the backend embeds
> that dispatch in the candidate token. Run the updated worker **and** backend together.

### Start an interview via the API (instead of the test UI)

```bash
curl -X POST http://localhost:8085/sessions -H 'Content-Type: application/json' -d '{
  "contestId": "<Marketplace contestId ObjectId>",
  "jsId":      "<jobSeekerProfile _id ObjectId>",
  "recruiterId": "<recruiterId ObjectId>",
  "maxQuestions": 4
}'
# → { sessionId, room, token, livekitUrl, jobTitle, candidateName, status:"admitted" }
```

Join the returned `room` on `livekitUrl` with the `token` to start the interview.

## Verify a scored report (after an interview ends)

```bash
mongosh "$MONGODB_URI" --eval '
  db.getSiblingDB("Marketplace").aiInterview.find().sort({_id:-1}).limit(1)
    .next().report.prescreeningreport'
```

## Tests

```bash
# Node backend + scorer
cd backend && npx jest

# Python agent
cd agent && source .venv/bin/activate && python -m pytest -q

# Load-test aggregation
cd tools/loadtest && python -m pytest test_aggregate.py -q
```

## Deploy (Kubernetes)

Manifests in `deploy/` — `agent-deployment.yaml`, `backend-deployment.yaml`,
`scorer-deployment.yaml`, `configmap.yaml`, KEDA/PDB, and `README.md`. Build images
with `agent/Dockerfile` and `backend/Dockerfile` (built from the repo root).

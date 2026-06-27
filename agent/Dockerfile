# Build from REPO ROOT:  docker build -f agent/Dockerfile -t tara-agent:dev .
# Rationale: shared/limiter/*.lua must be present at /app/shared/limiter so
# that tara_agent/limiter.py::_LUA_DIR (Path(__file__).resolve().parents[2] /
# "shared" / "limiter") resolves correctly.  With an editable install the
# package's __file__ stays at /app/agent/tara_agent/limiter.py, so
# parents[2] == /app — and we COPY shared/ to /app/shared at build time.

FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app/agent

# System deps (ca-certs for outbound TLS; keep slim).
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates && rm -rf /var/lib/apt/lists/*

# shared/limiter Lua scripts — copied from repo root into /app/shared so
# parents[2]/shared/limiter resolves to /app/shared/limiter.
COPY shared /app/shared

# Install the agent package in editable mode so __file__ stays in /app/agent/
# (i.e. /app/agent/tara_agent/limiter.py) rather than being flattened into
# site-packages, which would break the parents[2] Lua-path resolution.
COPY agent/pyproject.toml ./
COPY agent/tara_agent ./tara_agent
RUN pip install -e .

# Pre-download the turn-detector model at BUILD time so cold-start does not pay
# the download penalty (protects the warm-pool / 45s-lease correctness dependency).
# The download-files subcommand is real (verified: python -m tara_agent.worker --help
# lists it via the legacy livekit.agents CLI).  The || fallback keeps the build
# green if the HF Hub is unreachable in CI (model will lazy-load on first job).
RUN python -m tara_agent.worker download-files || \
    echo "WARN: download-files failed; model will lazy-load on first job"

# Non-root user.
RUN useradd -m -u 10001 tara && chown -R tara:tara /app
USER tara

CMD ["python", "-m", "tara_agent.worker", "start"]

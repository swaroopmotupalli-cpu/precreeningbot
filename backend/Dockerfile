# Build from REPO ROOT:  docker build -f backend/Dockerfile -t tara-backend:dev .
# Rationale: backend/src/limiter.js resolves Lua at
#   path.resolve(__dirname, "..", "..", "shared", "limiter")
# = /app/shared/limiter  when __dirname == /app/backend/src.
# A build context of backend/ alone cannot reach ../shared, so we build from
# repo root and COPY both shared/ and backend/ into /app/.

FROM node:20-slim AS base
ENV NODE_ENV=production
WORKDIR /app/backend

# Install production dependencies from backend/package*.json.
COPY backend/package.json backend/package-lock.json* ./
RUN npm ci --omit=dev || npm install --omit=dev

# Application source.
COPY backend/src ./src

# Lua scripts — shared/limiter/*.lua must live at /app/shared/limiter so that
# path.resolve('/app/backend/src', '..', '..', 'shared', 'limiter') resolves.
COPY shared /app/shared

# Non-root user.
RUN useradd -m -u 10002 tara && chown -R tara:tara /app
USER tara

EXPOSE 3000
CMD ["node", "src/server.js"]

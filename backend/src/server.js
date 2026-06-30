// Load repo-root .env for local runs (no-op in k8s where env comes from
// ConfigMap/Secret; never overrides already-set vars). Must run before any
// process.env read below (e.g. the Redis client + token minting).
require("dotenv").config({ path: require("path").join(__dirname, "..", "..", ".env") });
const path = require("path");
const express = require("express");
const Redis = require("ioredis");
const { MongoClient } = require("mongodb");
const { AccessToken } = require("livekit-server-sdk");
const { createSession } = require("./createSession");
const { Limiter } = require("./limiter");
const { loadLimiterConfig } = require("./limiterConfig");
const { makeHealthz } = require("./health");

const app = express();
app.use(express.json({ limit: "2mb" }));
const redis = new Redis(process.env.REDIS_URL);
app.get("/healthz", makeHealthz(redis));

// Marketplace DB — source of JD (contests) + candidate (jobSeekerProfile).
const mongo = new MongoClient(process.env.MONGODB_URI);
let marketplaceDb = null;
mongo.connect()
  .then(() => { marketplaceDb = mongo.db("Marketplace"); console.log("connected to Marketplace DB"); })
  .catch((e) => console.error("Mongo connect failed:", e.message));

// Map loadContext NotFoundError codes to HTTP status.
const NOT_FOUND_CODES = new Set(["CONTEST_NOT_FOUND", "JOBSEEKER_NOT_FOUND", "INVALID_CONTEST_ID", "INVALID_JS_ID"]);

// Static test UI (candidate-side flow tester) — served same-origin so the
// /sessions fetch needs no CORS. Open http://localhost:3000/ in a browser.
app.use(express.static(path.join(__dirname, "..", "public")));

const cfg = loadLimiterConfig(process.env);
const limiter = new Limiter(redis, {
  caps: {
    global: cfg.maxGlobalSessions,
    geminiTpm: cfg.geminiTpmBudget,
    sttStreams: cfg.sttStreamBudget,
    ttsStreams: cfg.ttsStreamBudget,
  },
  reservationTtlMs: cfg.reservationLeaseTtl * 1000,
});

async function mintToken(room, identity) {
  const at = new AccessToken(process.env.LIVEKIT_API_KEY,
                             process.env.LIVEKIT_API_SECRET, { identity });
  at.addGrant({ roomJoin: true, room });
  return await at.toJwt();
}

app.post("/sessions", async (req, res) => {
  try {
    if (!marketplaceDb) return res.status(503).json({ error: "Marketplace DB not connected yet" });
    const result = await createSession(redis, mintToken, limiter, req.body,
                                       { rejectedBlobTtl: cfg.rejectedBlobTtl, db: marketplaceDb });
    if (result.status === "queued") {
      return res.status(503).json(result);
    }
    res.json(result);
  } catch (e) {
    if (NOT_FOUND_CODES.has(e.code)) return res.status(404).json({ error: e.message, code: e.code });
    res.status(400).json({ error: e.message });
  }
});

app.listen(process.env.PORT || 3000, () => console.log("backend up"));

const express = require("express");
const Redis = require("ioredis");
const { AccessToken } = require("livekit-server-sdk");
const { createSession } = require("./createSession");
const { Limiter } = require("./limiter");
const { loadLimiterConfig } = require("./limiterConfig");

const app = express();
app.use(express.json({ limit: "2mb" }));
const redis = new Redis(process.env.REDIS_URL);

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
    const result = await createSession(redis, mintToken, limiter, req.body,
                                       { rejectedBlobTtl: cfg.rejectedBlobTtl });
    if (result.status === "queued") {
      return res.status(503).json(result);
    }
    res.json(result);
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.listen(process.env.PORT || 3000, () => console.log("backend up"));

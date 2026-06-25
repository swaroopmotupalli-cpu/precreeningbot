const express = require("express");
const Redis = require("ioredis");
const { AccessToken } = require("livekit-server-sdk");
const { createSession } = require("./createSession");

const app = express();
app.use(express.json({ limit: "2mb" }));
const redis = new Redis(process.env.REDIS_URL);

async function mintToken(room, identity) {
  const at = new AccessToken(process.env.LIVEKIT_API_KEY,
                             process.env.LIVEKIT_API_SECRET, { identity });
  at.addGrant({ roomJoin: true, room });
  return await at.toJwt();
}

app.post("/sessions", async (req, res) => {
  try {
    res.json(await createSession(redis, mintToken, req.body));
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.listen(process.env.PORT || 3000, () => console.log("backend up"));

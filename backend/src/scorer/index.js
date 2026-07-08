// backend/src/scorer/index.js
// Load repo-root .env for local runs (no-op in k8s; never overrides set vars).
require("dotenv").config({ path: require("path").join(__dirname, "..", "..", "..", ".env") });
const { MongoClient } = require("mongodb");
const Redis = require("ioredis");
const { runConsumer } = require("./queue");
const { scoreSession } = require("./pipeline");

const MODEL = process.env.GEMINI_MODEL || "gemini-3.1-flash-lite";
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:generateContent`;

async function geminiCall(prompt) {
  const r = await fetch(GEMINI_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-goog-api-key": process.env.GEMINI_API_KEY },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: { temperature: 0.2, responseMimeType: "application/json" },
    }),
  });
  const data = await r.json();
  return data?.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
}

async function main() {
  const mongo = new MongoClient(process.env.MONGODB_URI);
  await mongo.connect();
  // Marketplace ATS DB: reads aiInterview transcripts (written by the worker) +
  // contests, and writes the report to aiInterview/recruiterAddProfiles/auditTrail
  // so scores reach the recruiter's ATS (the "tara" db was a dead end).
  const db = mongo.db("Marketplace");
  const redis = new Redis(process.env.REDIS_URL);
  let stopping = false;
  const stop = () => stopping;
  process.on("SIGTERM", () => { stopping = true; });
  console.log("scorer up");
  await runConsumer(redis, (id) => scoreSession({ db, geminiCall }, id),
    { queue: "tara:score:queue", dlq: "tara:score:dlq", maxAttempts: 3, stop });
  await redis.quit();
  await mongo.close();
}

if (require.main === module) main().catch((e) => { console.error(e); process.exit(1); });
module.exports = { geminiCall };

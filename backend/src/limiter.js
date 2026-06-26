// backend/src/limiter.js
const fs = require("fs");
const path = require("path");

const LUA_DIR = path.resolve(__dirname, "..", "..", "shared", "limiter");
const ADMIT = fs.readFileSync(path.join(LUA_DIR, "admit.lua"), "utf8");
const PREFIX = "{tara-limiter}";

function keys(prefix = PREFIX) {
  return [`${prefix}:count:global`, `${prefix}:count:gemini_tpm`,
    `${prefix}:count:stt_streams`, `${prefix}:count:tts_streams`,
    `${prefix}:reservations`, `${prefix}:metric:reclaim:no_show`,
    `${prefix}:metric:reclaim:crash`, `${prefix}:metric:reclaim:false`];
}

class Limiter {
  constructor(redis, { prefix = PREFIX, caps, reservationTtlMs }) {
    this.redis = redis; this.caps = caps; this.ttl = reservationTtlMs; this.prefix = prefix;
  }
  async tryAdmit(room, nowMs) {
    const k = keys(this.prefix);
    const res = await this.redis.eval(ADMIT, k.length, ...k,
      room, nowMs, this.ttl, this.caps.global, this.caps.geminiTpm,
      this.caps.sttStreams, this.caps.ttsStreams, this.prefix);
    if (res[0] === "ADMITTED") {
      return { admitted: true, state: res[1], bucket: null, counts: {
        global: Number(res[2]), geminiTpm: Number(res[3]),
        sttStreams: Number(res[4]), ttsStreams: Number(res[5]) } };
    }
    return { admitted: false, state: null, bucket: res[1], counts: {} };
  }
}
module.exports = { Limiter };

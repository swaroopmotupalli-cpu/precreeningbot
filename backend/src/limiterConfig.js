function num(v, d) { return v === undefined ? d : parseInt(v, 10); }
function loadLimiterConfig(env) {
  return {
    maxGlobalSessions: num(env.MAX_GLOBAL_SESSIONS, 110),
    geminiTpmBudget: num(env.GEMINI_TPM_BUDGET, 60),
    sttStreamBudget: num(env.STT_STREAM_BUDGET, 100),
    ttsStreamBudget: num(env.TTS_STREAM_BUDGET, 100),
    reservationLeaseTtl: num(env.RESERVATION_LEASE_TTL, 45),
    heartbeatLeaseTtl: num(env.HEARTBEAT_LEASE_TTL, 60),
    rejectedBlobTtl: num(env.REJECTED_BLOB_TTL, 120),
  };
}
module.exports = { loadLimiterConfig };

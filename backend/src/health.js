function makeHealthz(redis) {
  return async function healthz(_req, res) {
    try {
      await redis.ping();
      return res.status(200).json({ status: "ok" });
    } catch (e) {
      return res.status(503).json({ status: "unhealthy", error: e.message });
    }
  };
}
module.exports = { makeHealthz };

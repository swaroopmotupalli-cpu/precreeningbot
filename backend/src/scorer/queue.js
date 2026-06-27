// backend/src/scorer/queue.js
async function consumeOnce(redis, handle, { queue, dlq, maxAttempts }) {
  const popped = await redis.brpop(queue, 1); // [queue, value] | null
  if (!popped) return null;
  const sessionId = popped[1];
  try {
    await handle(sessionId);
  } catch (e) {
    const n = await redis.incr(`tara:score:attempts:${sessionId}`);
    if (n >= maxAttempts) {
      await redis.lpush(dlq, sessionId);
      await redis.del(`tara:score:attempts:${sessionId}`);
    } else {
      await redis.lpush(queue, sessionId);
    }
  }
  return sessionId;
}

async function runConsumer(redis, handle, opts) {
  while (!opts.stop()) {
    await consumeOnce(redis, handle, opts);
  }
}

module.exports = { consumeOnce, runConsumer };

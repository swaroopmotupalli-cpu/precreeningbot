-- shared/limiter/release.lua
-- KEYS: 1 count:global 2 count:gemini_tpm 3 count:stt_streams 4 count:tts_streams
--       5 reservations(zset)
-- ARGV: 1 room 2 keyprefix
-- returns: 1 if released, 0 if reservation was already gone
if not redis.call('ZSCORE', KEYS[5], ARGV[1]) then return 0 end
redis.call('DECR', KEYS[1]); redis.call('DECR', KEYS[2])
redis.call('DECR', KEYS[3]); redis.call('DECR', KEYS[4])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('DEL', ARGV[2] .. ':res:' .. ARGV[1])
return 1

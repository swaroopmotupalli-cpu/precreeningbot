-- shared/limiter/reap.lua
-- KEYS: 1 count:global 2 count:gemini_tpm 3 count:stt_streams 4 count:tts_streams
--       5 reservations(zset) 6 metric:reclaim:no_show 7 metric:reclaim:crash
--       8 metric:reclaim:false
-- ARGV: 1 now_ms 2 keyprefix
-- returns: flat list [room1, reason1, room2, reason2, ...]
local now = tonumber(ARGV[1])
local prefix = ARGV[2]
local expired = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', '(' .. now)
local out = {}
for _, room in ipairs(expired) do
  local h = prefix .. ':res:' .. room
  redis.call('DECR', KEYS[1]); redis.call('DECR', KEYS[2])
  redis.call('DECR', KEYS[3]); redis.call('DECR', KEYS[4])
  local pj = redis.call('HGET', h, 'participant_joined')
  local hb = redis.call('HGET', h, 'heartbeat_ever')
  local reason
  if pj == '1' then reason = 'false'; redis.call('INCR', KEYS[8])
  elseif hb == '1' then reason = 'crash'; redis.call('INCR', KEYS[7])
  else reason = 'no_show'; redis.call('INCR', KEYS[6]) end
  redis.call('ZREM', KEYS[5], room)
  redis.call('DEL', h)
  out[#out + 1] = room
  out[#out + 1] = reason
end
return out

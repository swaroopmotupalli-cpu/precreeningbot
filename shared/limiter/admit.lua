-- shared/limiter/admit.lua
-- KEYS: 1 count:global 2 count:gemini_tpm 3 count:stt_streams 4 count:tts_streams
--       5 reservations(zset) 6 metric:reclaim:no_show 7 metric:reclaim:crash
--       8 metric:reclaim:false
-- ARGV: 1 room 2 now_ms 3 ttl_ms 4 cap_global 5 cap_gemini 6 cap_stt 7 cap_tts
--       8 keyprefix
-- returns: {"ADMITTED", "new"|"refreshed", g, gm, st, tt}  or  {"REJECTED", bucket}
local now = tonumber(ARGV[2])
local prefix = ARGV[8]

-- (1) REAP expired reservations first (frees capacity before the cap check)
local expired = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', '(' .. now)
for _, room in ipairs(expired) do
  local h = prefix .. ':res:' .. room
  redis.call('DECR', KEYS[1]); redis.call('DECR', KEYS[2])
  redis.call('DECR', KEYS[3]); redis.call('DECR', KEYS[4])
  local pj = redis.call('HGET', h, 'participant_joined')
  local hb = redis.call('HGET', h, 'heartbeat_ever')
  if pj == '1' then redis.call('INCR', KEYS[8])        -- false reclaim
  elseif hb == '1' then redis.call('INCR', KEYS[7])    -- crash
  else redis.call('INCR', KEYS[6]) end                 -- no-show
  redis.call('ZREM', KEYS[5], room)
  redis.call('DEL', h)
end

-- (2) Reconnect idempotency: live reservation exists -> refresh, no re-count
if redis.call('ZSCORE', KEYS[5], ARGV[1]) then
  redis.call('ZADD', KEYS[5], now + tonumber(ARGV[3]), ARGV[1])
  return {'ADMITTED', 'refreshed',
    redis.call('GET', KEYS[1]) or '0', redis.call('GET', KEYS[2]) or '0',
    redis.call('GET', KEYS[3]) or '0', redis.call('GET', KEYS[4]) or '0'}
end

-- (3) Capacity check (global first, then per-resource), all-or-nothing
local g  = tonumber(redis.call('GET', KEYS[1]) or '0')
local gm = tonumber(redis.call('GET', KEYS[2]) or '0')
local st = tonumber(redis.call('GET', KEYS[3]) or '0')
local tt = tonumber(redis.call('GET', KEYS[4]) or '0')
if g  >= tonumber(ARGV[4]) then redis.call('INCR', prefix..':metric:reject:global');     return {'REJECTED','global'} end
if gm >= tonumber(ARGV[5]) then redis.call('INCR', prefix..':metric:reject:gemini_tpm'); return {'REJECTED','gemini_tpm'} end
if st >= tonumber(ARGV[6]) then redis.call('INCR', prefix..':metric:reject:stt_streams');return {'REJECTED','stt_streams'} end
if tt >= tonumber(ARGV[7]) then redis.call('INCR', prefix..':metric:reject:tts_streams');return {'REJECTED','tts_streams'} end

-- (4) Admit: take ALL buckets atomically, create reservation with Tier-1 lease
redis.call('INCR', KEYS[1]); redis.call('INCR', KEYS[2])
redis.call('INCR', KEYS[3]); redis.call('INCR', KEYS[4])
redis.call('ZADD', KEYS[5], now + tonumber(ARGV[3]), ARGV[1])
redis.call('HSET', prefix..':res:'..ARGV[1],
  'heartbeat_ever','0','participant_joined','0','created_ms',ARGV[2])
return {'ADMITTED','new', tostring(g+1), tostring(gm+1), tostring(st+1), tostring(tt+1)}

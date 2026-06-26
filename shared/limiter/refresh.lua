-- shared/limiter/refresh.lua
-- KEYS: 1 reservations(zset)
-- ARGV: 1 room 2 now_ms 3 hb_ttl_ms 4 mark_heartbeat(0/1) 5 mark_participant(0/1)
--       6 keyprefix
-- returns: 1 if refreshed, 0 if reservation missing
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[1])
local h = ARGV[6] .. ':res:' .. ARGV[1]
if ARGV[4] == '1' then redis.call('HSET', h, 'heartbeat_ever', '1') end
if ARGV[5] == '1' then redis.call('HSET', h, 'participant_joined', '1') end
return 1

-- KEYS[1] session payload, KEYS[2] event identity set, KEYS[3] lease
-- ARGV event id,payload,ttl,token,generation
if ARGV[4] ~= '' then
  if redis.call('PTTL', KEYS[3]) <= 0 then return {'stale'} end
  if redis.call('HGET', KEYS[3], 'token') ~= ARGV[4] then return {'stale'} end
  if redis.call('HGET', KEYS[3], 'generation') ~= ARGV[5] then return {'stale'} end
end
if redis.call('EXISTS', KEYS[1]) == 0 then return {'missing'} end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then return {'duplicate'} end
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return {'appended'}

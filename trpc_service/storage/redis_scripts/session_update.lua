-- KEYS[1] session, KEYS[2] lease; ARGV payload,ttl,token,generation
if ARGV[3] ~= '' then
  if redis.call('PTTL', KEYS[2]) <= 0 then return {'stale'} end
  if redis.call('HGET', KEYS[2], 'token') ~= ARGV[3] then return {'stale'} end
  if redis.call('HGET', KEYS[2], 'generation') ~= ARGV[4] then return {'stale'} end
end
if redis.call('EXISTS', KEYS[1]) == 0 then return {'missing'} end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return {'updated'}

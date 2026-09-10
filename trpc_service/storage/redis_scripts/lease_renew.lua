-- KEYS[1] lease; ARGV token,generation,ttl_ms
if redis.call('PTTL', KEYS[1]) <= 0 then return {'lost'} end
if redis.call('HGET', KEYS[1], 'token') ~= ARGV[1] then return {'lost'} end
if redis.call('HGET', KEYS[1], 'generation') ~= ARGV[2] then return {'lost'} end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return {'renewed', tostring(redis.call('PTTL', KEYS[1]))}

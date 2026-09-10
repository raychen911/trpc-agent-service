-- KEYS[1] lease; ARGV token,generation
if redis.call('HGET', KEYS[1], 'token') ~= ARGV[1] then return {'stale'} end
if redis.call('HGET', KEYS[1], 'generation') ~= ARGV[2] then return {'stale'} end
redis.call('DEL', KEYS[1])
return {'released'}

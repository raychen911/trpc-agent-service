-- KEYS[1] lease hash, KEYS[2] generation counter; ARGV token,node,ttl_ms
if redis.call('EXISTS', KEYS[1]) == 1 and redis.call('PTTL', KEYS[1]) > 0 then
  return {'busy', tostring(redis.call('PTTL', KEYS[1]))}
end
local generation = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'token', ARGV[1], 'generation', generation, 'node', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return {'acquired', tostring(generation)}

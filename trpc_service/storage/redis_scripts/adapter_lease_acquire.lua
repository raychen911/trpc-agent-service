local ttl = redis.call('PTTL', KEYS[1])
if ttl > 0 then
  return {'busy', tostring(ttl)}
end
local generation = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1],
  'token', ARGV[1],
  'node', ARGV[2],
  'generation', generation,
  'phase', 'connecting')
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return {'acquired', tostring(generation)}

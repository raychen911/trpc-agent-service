local token = redis.call('HGET', KEYS[1], 'token')
local node = redis.call('HGET', KEYS[1], 'node')
local generation = redis.call('HGET', KEYS[1], 'generation')
if token ~= ARGV[1] or node ~= ARGV[2] or generation ~= ARGV[3] then
  return {'stale'}
end
redis.call('DEL', KEYS[1])
return {'released'}

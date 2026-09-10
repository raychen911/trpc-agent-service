local token = redis.call('HGET', KEYS[1], 'token')
local node = redis.call('HGET', KEYS[1], 'node')
local generation = redis.call('HGET', KEYS[1], 'generation')
if token ~= ARGV[1] or node ~= ARGV[2] or generation ~= ARGV[3] then
  return {'lost'}
end
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {'renewed'}

-- KEYS[1] session payload, ARGV[1] ttl seconds
local value = redis.call('GET', KEYS[1])
if value then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return value

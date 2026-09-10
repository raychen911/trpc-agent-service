-- KEYS record,owner lease; ARGV expected_token,expected_state,replacement,release
local raw = redis.call('GET', KEYS[1])
if not raw then return {'missing'} end
local record = cjson.decode(raw)
if record['owner_token'] ~= ARGV[1] or record['state'] ~= ARGV[2] then
  return {'stale'}
end
if redis.call('GET', KEYS[2]) ~= ARGV[1] or redis.call('PTTL', KEYS[2]) <= 0 then
  return {'stale'}
end
redis.call('SET', KEYS[1], ARGV[3])
if ARGV[4] == '1' then redis.call('DEL', KEYS[2]) end
return {'updated'}

-- KEYS record,owner lease; ARGV initial_json,fingerprint,token,trace,now,lease_ms
local raw = redis.call('GET', KEYS[1])
if not raw then
  redis.call('SET', KEYS[1], ARGV[1])
  redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[6])
  return {'acquired', ARGV[1]}
end
local record = cjson.decode(raw)
if record['content_fingerprint'] ~= ARGV[2] then return {'conflict', raw} end
if record['state'] == 'failed_pre_start' or (record['state'] == 'pending' and redis.call('PTTL', KEYS[2]) <= 0) then
  record['state'] = 'pending'
  record['attempt'] = record['attempt'] + 1
  record['generation'] = record['generation'] + 1
  record['execution_phase'] = 1
  record['owner_token'] = ARGV[3]
  record['owner_trace_id'] = ARGV[4]
  record['execution_trace_id'] = cjson.null
  record['result'] = cjson.null
  record['updated_at'] = ARGV[5]
  local reclaimed = cjson.encode(record)
  redis.call('SET', KEYS[1], reclaimed)
  redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[6])
  return {'acquired', reclaimed}
end
if record['state'] == 'pending' or record['state'] == 'running' then
  if record['state'] == 'running' and redis.call('PTTL', KEYS[2]) <= 0 then
    return {'outcome_unknown', raw}
  end
  return {'processing', raw}
end
return {'completed', raw}

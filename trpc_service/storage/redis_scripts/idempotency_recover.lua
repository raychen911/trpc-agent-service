-- KEYS[1] record; ARGV generation,execution_trace,replacement_json
local raw = redis.call('GET', KEYS[1])
if not raw then return {'missing'} end
local record = cjson.decode(raw)
if tostring(record['generation']) ~= ARGV[1] then return {'stale_generation'} end
if record['execution_trace_id'] ~= ARGV[2] then return {'trace_conflict'} end
if record['state'] ~= 'running' then
  if record['state'] == 'succeeded' or record['state'] == 'failed_post_start' or record['state'] == 'outcome_unknown' then
    return {'already_terminal'}
  end
  return {'phase_conflict'}
end
redis.call('SET', KEYS[1], ARGV[3])
return {'reconciled'}

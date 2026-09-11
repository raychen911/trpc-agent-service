#!/usr/bin/env bash
# Exercise the deployed API, dispatcher, worker, PostgreSQL, and Redis together.
# This intentionally creates one uniquely named mock tenant and never tears down
# the operator's stack or deletes any deployment data.
set -euo pipefail

if [[ "${COMPOSE_INTEGRATION:-}" != "1" ]]; then
  echo "set COMPOSE_INTEGRATION=1 to permit the deployment integration probe" >&2
  exit 2
fi

api_url="${TRPC_COMPOSE_API_URL:-http://127.0.0.1:8000}"
admin_key="${TRPC_COMPOSE_ADMIN_API_KEY:-}"
suffix="$(date -u +%Y%m%d%H%M%S)-$$"
tenant_id="compose-${suffix}"
webhook_key="compose-${suffix}-webhook-key"
request_id="compose-${suffix}"

if [[ -z "$admin_key" ]]; then
  echo "TRPC_COMPOSE_ADMIN_API_KEY is required; the API fails closed by design" >&2
  exit 2
fi

admin_headers=(-H "content-type: application/json" -H "x-admin-key: $admin_key")

wait_for_api() {
  local attempt
  for attempt in $(seq 1 90); do
    if curl --fail --silent --show-error "$api_url/health/ready" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "API did not become ready at $api_url" >&2
  return 1
}

post_admin() {
  local path="$1"
  local body="$2"
  curl --fail --silent --show-error "${admin_headers[@]}" \
    --data "$body" "$api_url$path"
}

docker compose up --build --detach
wait_for_api

post_admin "/admin/v1/tenants" \
  "{\"tenant_id\":\"$tenant_id\",\"display_name\":\"Compose integration $suffix\"}" >/dev/null
post_admin "/admin/v1/tenants/$tenant_id/agents" \
  '{"agent_id":"agent","name":"Compose integration agent"}' >/dev/null
post_admin "/admin/v1/tenants/$tenant_id/agents/agent/releases" \
  '{"version":1,"model_config":{"mode":"mock"},"tool_policy":{"allow":["ticket.lookup"]}}' >/dev/null
post_admin "/admin/v1/tenants/$tenant_id/agents/agent/releases/1/activate" '{}' >/dev/null
post_admin "/admin/v1/tenants/$tenant_id/channels" \
  "{\"binding_id\":\"mock\",\"agent_id\":\"agent\",\"provider\":\"mock\",\"external_account_id\":\"$tenant_id-account\",\"webhook_key\":\"$webhook_key\",\"capabilities\":{\"callback_secret\":\"compose-secret\"}}" >/dev/null

callback="$(curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  -H 'x-mock-secret: compose-secret' \
  -H "x-request-id: $request_id" \
  --data '{"message_id":"message-1","user_id":"sandbox-user","text":"ticket 42"}' \
  "$api_url/callbacks/mock/$webhook_key")"
grep -q '"duplicate":false' <<<"$callback" || {
  echo "callback was not accepted: $callback" >&2
  exit 1
}

for attempt in $(seq 1 90); do
  operation="$(curl --fail --silent --show-error \
    "${admin_headers[@]}" \
    "$api_url/v1/operations/$request_id?tenant_id=$tenant_id")"
  if grep -q '"status":"delivered"' <<<"$operation"; then
    printf 'compose integration succeeded for tenant=%s request_id=%s\n' "$tenant_id" "$request_id"
    exit 0
  fi
  sleep 1
done

echo "message was not delivered before timeout; request_id=$request_id tenant_id=$tenant_id" >&2
docker compose logs --tail=120 api worker dispatcher >&2 || true
exit 1

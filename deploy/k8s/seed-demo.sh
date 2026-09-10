#!/usr/bin/env sh
set -eu

: "${TRPC_ADMIN_API_KEY:?set TRPC_ADMIN_API_KEY first}"
: "${TRPC_TENANT_API_KEY:?set TRPC_TENANT_API_KEY first}"

base_url=${TRPC_GATEWAY_URL:-http://127.0.0.1:18000}

curl -fsS -X POST "$base_url/admin/tenants" \
    -H "X-Admin-API-Key: $TRPC_ADMIN_API_KEY" \
    -H 'Content-Type: application/json' \
    -d '{"tenant_id":"demo","name":"Demo","audit_policy":{"http_api_key_ref":"env://TRPC_DEMO_TENANT_API_KEY"}}'

curl -fsS -X POST "$base_url/admin/tenants/demo/apps" \
    -H "X-Admin-API-Key: $TRPC_ADMIN_API_KEY" \
    -H 'Content-Type: application/json' \
    -d '{"app_id":"assistant","name":"Assistant","system_prompt":"You are a helpful assistant.","tool_policy":{"allow":["calculator"]}}'

curl -fsS -X POST "$base_url/v1/chat" \
    -H 'X-Tenant-ID: demo' \
    -H "X-Tenant-API-Key: $TRPC_TENANT_API_KEY" \
    -H 'Content-Type: application/json' \
    -d '{"app_id":"assistant","user_id":"demo-user","session_id":"demo-session","message":"请计算 (27+15)*3"}'

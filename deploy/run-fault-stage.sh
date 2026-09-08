#!/bin/sh
set -eu

TOXIPROXY_API="${TOXIPROXY_API:-http://127.0.0.1:8474}"
FAULT_SECONDS="${FAULT_SECONDS:-10}"

toggle_proxy() {
  proxy_name="$1"
  enabled="$2"
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d "{\"enabled\":${enabled}}" "${TOXIPROXY_API}/proxies/${proxy_name}"
}

for dependency in redis mysql; do
  printf 'disabling %s for %ss\n' "${dependency}" "${FAULT_SECONDS}"
  toggle_proxy "${dependency}" false >/dev/null
  sleep "${FAULT_SECONDS}"
  toggle_proxy "${dependency}" true >/dev/null
  printf '%s restored\n' "${dependency}"
done

printf '%s\n' 'fault stage complete; validate readyz, DLQ, receipt and outbox metrics'

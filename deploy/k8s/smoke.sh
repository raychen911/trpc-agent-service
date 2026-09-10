#!/usr/bin/env sh
set -eu

log_file=${TMPDIR:-/tmp}/trpc-agent-port-forward.log
local_port=${TRPC_K8S_LOCAL_PORT:-18000}
kubectl -n trpc-agent port-forward service/agent-gateway "$local_port":80 >"$log_file" 2>&1 &
forward_pid=$!
trap 'kill "$forward_pid" 2>/dev/null || true' EXIT INT TERM

attempt=0
while [ "$attempt" -lt 30 ]; do
    if curl -fsS "http://127.0.0.1:$local_port/health" >/dev/null \
        && curl -fsS "http://127.0.0.1:$local_port/ready" >/dev/null; then
        printf '%s\n' "Kubernetes gateway health and readiness passed"
        exit 0
    fi
    attempt=$((attempt + 1))
    sleep 1
done

printf '%s\n' "Kubernetes smoke test failed; see $log_file" >&2
exit 1

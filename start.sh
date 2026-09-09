#!/usr/bin/env bash
set -euo pipefail

python_bin="${PYTHON_BIN:-}"
if [[ -z "$python_bin" ]]; then
  if command -v python >/dev/null 2>&1; then python_bin=python; else python_bin=python3; fi
fi

mkdir -p data
if [[ -f data/trpc-service.pid ]] && kill -0 "$(cat data/trpc-service.pid)" 2>/dev/null; then
  echo "service is already running"
  exit 1
fi
nohup "$python_bin" -m trpc_service._cli serve --config "${TRPC_SERVICE_CONFIG:-examples/config/tenants.yaml}" \
  --host "${TRPC_SERVICE_HOST:-0.0.0.0}" --port "${TRPC_SERVICE_PORT:-8080}" \
  >data/trpc-service.log 2>&1 &
echo $! >data/trpc-service.pid
echo "service started with pid $(cat data/trpc-service.pid)"

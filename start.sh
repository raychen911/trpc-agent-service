#!/usr/bin/env bash
set -euo pipefail

if [[ ! -x .venv/bin/python ]]; then
  ./build.sh
fi
mkdir -p data
if [[ -f data/api.pid ]] && kill -0 "$(< data/api.pid)" 2>/dev/null; then
  echo "API already running (pid $(< data/api.pid))"
  exit 0
fi
.venv/bin/python -m trpc_service._cli api >data/api.log 2>&1 &
echo $! > data/api.pid
echo "API started on http://127.0.0.1:8000 (pid $(< data/api.pid))"

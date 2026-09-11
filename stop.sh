#!/usr/bin/env bash
set -euo pipefail

if [[ -f data/api.pid ]]; then
  pid="$(< data/api.pid)"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    echo "Stopped API (pid $pid)"
  fi
  rm -f data/api.pid
else
  echo "No API pid file found"
fi

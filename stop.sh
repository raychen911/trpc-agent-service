#!/usr/bin/env bash
set -euo pipefail

pid_file=data/trpc-service.pid
if [[ ! -f "$pid_file" ]]; then
  echo "service is not running"
  exit 0
fi
pid=$(cat "$pid_file")
if kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
fi
rm -f "$pid_file"
echo "service stopped"

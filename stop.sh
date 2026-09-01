#!/usr/bin/env bash
set -euo pipefail
if [ -f .run/web-ui.pid ]; then
  pid="$(cat .run/web-ui.pid)"
  kill "${pid}" >/dev/null 2>&1 || true
  rm -f .run/web-ui.pid
  echo "Web UI stopped."
elif command -v pkill >/dev/null 2>&1; then
  pkill -f "uvicorn trpc_service.web.app" || true
  echo "Web UI stopped."
else
  echo "Web UI is not running."
fi

#!/usr/bin/env bash
set -euo pipefail
if command -v pkill >/dev/null 2>&1; then
  pkill -f 'trpc_service._cli serve' || true
else
  echo "stop the uvicorn process started by start.sh"
fi

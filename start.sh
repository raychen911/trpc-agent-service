#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="${PYTHONPATH:-.}"
if command -v uv >/dev/null 2>&1; then
  PYTHON_CMD=(uv run python)
elif [ -x ".venv/bin/python" ]; then
  PYTHON_CMD=(.venv/bin/python)
else
  PYTHON_CMD=(python)
fi
"${PYTHON_CMD[@]}" -c 'import sys; sys.exit("Python 3.12+ is required") if sys.version_info < (3, 12) else None'
export CPA_USE_CODEX_CLI="${CPA_USE_CODEX_CLI:-0}"
export CPA_MODEL="${CPA_MODEL:-}"
export TRPC_AGENT_RUNTIME_MODE="${TRPC_AGENT_RUNTIME_MODE:-local}"
export WORKER_QUEUE_URL="${WORKER_QUEUE_URL:-}"
export WORKER_REMOTE="${WORKER_REMOTE:-0}"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-18001}"
mkdir -p data .run
"${PYTHON_CMD[@]}" -m uvicorn trpc_service.web.app:app --host "${HOST}" --port "${PORT}" \
  > data/web-ui.out.log 2> data/web-ui.err.log &
echo "$!" > .run/web-ui.pid

for _ in $(seq 1 30); do
  if "${PYTHON_CMD[@]}" - <<PY >/dev/null 2>&1
from urllib.request import urlopen
with urlopen("http://${HOST}:${PORT}/health", timeout=2) as response:
    raise SystemExit(0 if response.status == 200 else 1)
PY
  then
    echo "Web UI: http://${HOST}:${PORT}/ui"
    echo "PID: $(cat .run/web-ui.pid)"
    echo "Logs: data/web-ui.err.log"
    exit 0
  fi
  sleep 1
done

echo "Web UI failed to start. See data/web-ui.err.log" >&2
exit 1

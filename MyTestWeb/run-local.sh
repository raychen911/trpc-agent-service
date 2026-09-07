#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
SDK_VENV="$PROJECT_DIR/../.venv"

if [[ -f "$PROJECT_DIR/.env.local" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_DIR/.env.local"
  set +a
fi

# Safe defaults for the disposable local validation stack.  A developer can
# override them in .env.local; production deployments must inject secrets
# through the environment/secret manager instead.
export NEXT_PUBLIC_AGENT_API_URL="${NEXT_PUBLIC_AGENT_API_URL:-http://127.0.0.1:8765}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:16379/0}"
export MYSQL_URL="${MYSQL_URL:-mysql+aiomysql://agent_test:agent_test_only@127.0.0.1:13306/agent_test}"
export TENANT_CONFIG_ENCRYPTION_KEY="${TENANT_CONFIG_ENCRYPTION_KEY:-mytestweb-local-only-key}"

if [[ -x "$SDK_VENV/bin/python" ]]; then
  PYTHON_BIN="$SDK_VENV/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  echo "未找到项目虚拟环境，使用系统 python3；生产环境请创建 .venv。" >&2
  PYTHON_BIN="$(command -v python3)"
else
  echo "未找到可用 Python 运行时。" >&2
  exit 1
fi

cleanup() {
  kill "$BACKEND_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

"$PYTHON_BIN" "$PROJECT_DIR/server.py" &
BACKEND_PID=$!
npm run dev -- --host 127.0.0.1

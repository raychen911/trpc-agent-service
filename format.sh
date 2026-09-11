#!/usr/bin/env bash
set -euo pipefail

if [[ -x .venv/bin/ruff ]]; then
  ruff_bin=.venv/bin/ruff
elif command -v ruff >/dev/null 2>&1; then
  ruff_bin=$(command -v ruff)
else
  ./build.sh
  ruff_bin=.venv/bin/ruff
fi
"$ruff_bin" check --fix trpc_service tests alembic
"$ruff_bin" format trpc_service tests alembic

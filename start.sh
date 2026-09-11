#!/usr/bin/env bash
set -euo pipefail

if command -v uv >/dev/null 2>&1; then
  exec uv run python -m trpc_service._cli serve
elif [[ -x .venv/bin/python ]]; then
  exec .venv/bin/python -m trpc_service._cli serve
elif [[ -x .venv/Scripts/python.exe ]]; then
  exec .venv/Scripts/python.exe -m trpc_service._cli serve
else
  exec python3 -m trpc_service._cli serve
fi

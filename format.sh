#!/usr/bin/env bash
# 格式化项目代码（yapf 风格）
set -euo pipefail
cd "$(dirname "$0")"
PYTHON=.venv/Scripts/python.exe
if [ ! -f "$PYTHON" ]; then
  PYTHON=python
fi
"$PYTHON" -m yapf --in-place --recursive trpc_service tests 2>/dev/null || \
"$PYTHON" -m pip install yapf && "$PYTHON" -m yapf --in-place --recursive trpc_service tests
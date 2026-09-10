#!/usr/bin/env bash
# 代码风格检查（flake8）
set -euo pipefail
cd "$(dirname "$0")"
PYTHON=.venv/Scripts/python.exe
if [ ! -f "$PYTHON" ]; then
  PYTHON=python
fi
"$PYTHON" -m flake8 trpc_service tests --max-line-length=120 --extend-ignore=E501,W503

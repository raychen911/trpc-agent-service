#!/usr/bin/env bash
# 运行单元测试覆盖率（PYTHONUTF8=1：Windows GBK 环境下 SDK docker 客户端需要 UTF-8）
set -euo pipefail
cd "$(dirname "$0")"
PYTHON=.venv/Scripts/python.exe
if [ ! -f "$PYTHON" ]; then
  PYTHON=python
fi
export PYTHONUTF8=1
"$PYTHON" -m pytest --cov=trpc_service --cov-report=term-missing "$@"
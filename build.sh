#!/usr/bin/env bash
# 初始化构建：创建虚拟环境并安装依赖
set -euo pipefail
cd "$(dirname "$0")"
PYTHON=.venv/Scripts/python.exe
if [ ! -f "$PYTHON" ]; then
  python -m venv .venv
fi
"$PYTHON" -m pip install --upgrade pip
"$PYTHON" -m pip install -r requirements.txt
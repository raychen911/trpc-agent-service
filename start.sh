#!/usr/bin/env bash
# 启动服务并记录 PID（nohup + 日志重定向：父 shell 退出不影响后台进程，崩溃可查 server.log）
# PYTHONUTF8=1：Windows 中文环境默认 GBK，SDK 的 docker 客户端解析 UTF-8 输出会炸，统一 UTF-8 模式
set -euo pipefail
cd "$(dirname "$0")"
PYTHON=.venv/Scripts/python.exe
if [ ! -f "$PYTHON" ]; then
  PYTHON=python
fi
export PYTHONUTF8=1
nohup "$PYTHON" -m trpc_service.web.app > server.log 2>&1 &
disown || true
echo $! > .server.pid
echo "服务已启动，PID: $(cat .server.pid)，日志: server.log，访问 http://127.0.0.1:8000"

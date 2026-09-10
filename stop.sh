#!/usr/bin/env bash
# 停止服务（按 PID 文件）
set -euo pipefail
cd "$(dirname "$0")"
if [ -f .server.pid ]; then
  PID=$(cat .server.pid)
  kill "$PID" 2>/dev/null || taskkill //PID "$PID" //T //F 2>/dev/null || true
  rm -f .server.pid
  echo "服务已停止（PID: $PID）"
else
  echo "未找到 .server.pid，可能服务未通过 start.sh 启动"
fi
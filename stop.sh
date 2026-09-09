#!/usr/bin/env sh
# ===================================================================
# stop.sh - 停止 Teneuris 服务
# ===================================================================
# 说明: 读取 data/logs/*.pid 并优雅停止 Gateway / Admin 进程。
# ===================================================================

cd "$(dirname "$0")"

LOG_DIR="data/logs"

for svc in gateway admin; do
    pidfile="$LOG_DIR/$svc.pid"
    if [ ! -f "$pidfile" ]; then
        continue
    fi
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null && echo "[stop] 已停止 $svc（pid=$pid）"
    else
        echo "[stop] $svc 未运行（pid=$pid 不存在）"
    fi
    rm -f "$pidfile"
done

echo "[stop] 完成"

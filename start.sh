#!/usr/bin/env sh
# ===================================================================
# start.sh - 启动 Teneuris 服务（Agent Gateway + Admin API）
# ===================================================================
# 说明: 对齐 Dockerfile —— redis-server 已内置，走 Redis 后端时自动拉起。
#   默认生产启动（redis 存储 + framework 真实 LLM，需 DEEPSEEK_API_KEY）；
#   本地无 Redis / 无 key 开发可用 STORAGE=inmemory RUNNER=mock 显式降级。
# 用法: ./start.sh [--storage redis|inmemory] [--runner framework|mock]
#   或 STORAGE=redis RUNNER=framework ./start.sh
# 配置: 端口/存储等见 config/teneuris.yaml，可用 CONFIG 环境变量覆盖路径。
# ===================================================================

cd "$(dirname "$0")"

CONFIG="${CONFIG:-config/teneuris.yaml}"
STORAGE="${STORAGE:-redis}"
RUNNER="${RUNNER:-framework}"
LOG_DIR="data/logs"

# 解析参数（覆盖环境变量）
while [ $# -gt 0 ]; do
    case "$1" in
        --storage=*) STORAGE="${1#*=}"; shift ;;
        --storage) STORAGE="${2:-redis}"; shift 2 ;;
        --runner=*) RUNNER="${1#*=}"; shift ;;
        --runner) RUNNER="${2:-framework}"; shift 2 ;;
        *) shift ;;
    esac
done

mkdir -p "$LOG_DIR"

# 走 Redis 后端时，确保 redis-server 已运行（镜像已内置）
if [ "$STORAGE" = "redis" ] && ! redis-cli ping >/dev/null 2>&1; then
    echo "[start] 启动 redis-server..."
    redis-server --daemonize yes --dir data
fi

echo "[start] 启动 Agent Gateway（storage=$STORAGE runner=$RUNNER）..."
nohup python -m trpc_service._cli gateway --config "$CONFIG" --storage "$STORAGE" --runner "$RUNNER" \
    > "$LOG_DIR/gateway.log" 2>&1 &
echo $! > "$LOG_DIR/gateway.pid"

echo "[start] 启动 Admin API..."
nohup python -m trpc_service._cli admin --config "$CONFIG" \
    > "$LOG_DIR/admin.log" 2>&1 &
echo $! > "$LOG_DIR/admin.pid"

echo "[start] 完成"
echo "  Gateway : http://127.0.0.1:8000  (pid $(cat "$LOG_DIR/gateway.pid"))"
echo "  Admin   : http://127.0.0.1:8002  (pid $(cat "$LOG_DIR/admin.pid"))"
echo "  日志目录 : $LOG_DIR"

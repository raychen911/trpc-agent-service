#!/usr/bin/env sh
# ===================================================================
# build.sh - 构建项目（安装平台层依赖）
# ===================================================================
# 说明: 依赖以 uv.lock 精确锁定（含框架 trpc-agent-py 的实测基线版本），
#   uv sync --frozen 安装且锁与 pyproject 不同步时直接失败（幂等，可重复执行）。
# 规范: uv 优先（镜像已内置），无 uv 时回退 python -m pip + requirements.txt。
# ===================================================================

set -e
cd "$(dirname "$0")"

echo "[build] 按 uv.lock 安装依赖..."
if command -v uv >/dev/null 2>&1; then
    if [ -n "$VIRTUAL_ENV" ]; then
        uv sync --frozen --no-install-project --active
    else
        uv sync --frozen --no-install-project
    fi
else
    python -m pip install -r requirements.txt
fi
echo "[build] 完成"

#!/usr/bin/env sh
# ===================================================================
# format.sh - 格式化项目代码风格（yapf）
# ===================================================================
# 说明: 使用 yapf 统一代码风格，忽略项见 .yapfignore。
#   yapf 镜像未预装，按需安装。
# ===================================================================

cd "$(dirname "$0")"

if ! command -v yapf >/dev/null 2>&1; then
    echo "[format] 未安装 yapf，正在安装..."
    if command -v uv >/dev/null 2>&1; then
        uv pip install yapf
    else
        python -m pip install yapf
    fi
fi

yapf -i -r trpc_service tests
echo "[format] 完成"

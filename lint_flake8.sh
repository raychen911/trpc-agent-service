#!/usr/bin/env sh
# ===================================================================
# lint_flake8.sh - flake8 静态检查
# ===================================================================
# 说明: 检查代码风格与常见错误（未使用导入/未定义变量等）。
#   flake8 镜像未预装，按需安装。
# ===================================================================

cd "$(dirname "$0")"

if ! command -v flake8 >/dev/null 2>&1; then
    echo "[lint] 未安装 flake8，正在安装..."
    if command -v uv >/dev/null 2>&1; then
        uv pip install flake8
    else
        python -m pip install flake8
    fi
fi

if flake8 trpc_service tests; then
    echo "[lint] ✅ flake8 0 条"
else
    echo "[lint] ❌ flake8 检查未通过（明细见上），先修复再提交"
    exit 1
fi

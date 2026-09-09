#!/usr/bin/env sh
# ===================================================================
# coverage.sh - 运行单元测试并输出覆盖率
# ===================================================================
# 说明: 依赖 pytest-cov（镜像未预装，按需安装）。支持透传 pytest 参数。
# ===================================================================

cd "$(dirname "$0")"

if ! python -c "import pytest_cov" 2>/dev/null; then
    echo "[coverage] 未安装 pytest-cov，正在安装..."
    if command -v uv >/dev/null 2>&1; then
        uv pip install pytest-cov
    else
        python -m pip install pytest-cov
    fi
fi

python -m pytest --cov=trpc_service --cov-report=term-missing "$@"

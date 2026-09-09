#!/usr/bin/env sh
# ===================================================================
# clean.sh - 清理项目中间产物
# ===================================================================
# 说明: 清理 Python 缓存与构建产物（对齐 .dockerignore 的排除清单）。
#   不清理 data/ 运行时数据与日志（属运行产物，见 .dockerignore 说明）。
# ===================================================================

cd "$(dirname "$0")"

find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist 2>/dev/null || true

echo "[clean] 完成"

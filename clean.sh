#!/usr/bin/env bash
# 清理构建与缓存中间产物
set -euo pipefail
cd "$(dirname "$0")"
find . -type d -name __pycache__ -prune -exec rm -rf {} +
rm -rf .pytest_cache .coverage htmlcov _coverage
rm -f *.log
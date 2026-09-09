#!/usr/bin/env bash
set -euo pipefail

find trpc_service tests -type d -name __pycache__ -prune -exec rm -rf -- {} +
rm -rf -- build dist htmlcov .pytest_cache .mypy_cache .ruff_cache
rm -f -- .coverage coverage.xml
echo "Removed generated build/test artifacts; persistent data was preserved."

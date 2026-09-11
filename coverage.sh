#!/usr/bin/env bash
set -euo pipefail
if command -v uv >/dev/null 2>&1; then
  uv run pytest --cov=trpc_service --cov-report=term-missing
else
  python3 -m pytest --cov=trpc_service --cov-report=term-missing
fi

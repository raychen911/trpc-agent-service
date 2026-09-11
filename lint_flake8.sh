#!/usr/bin/env bash
set -euo pipefail
if command -v uv >/dev/null 2>&1; then
  uv run ruff check trpc_service tests
else
  python3 -m ruff check trpc_service tests
fi

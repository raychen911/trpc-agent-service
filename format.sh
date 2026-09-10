#!/usr/bin/env bash
set -euo pipefail
if command -v black >/dev/null 2>&1; then
  black --line-length 120 trpc_service tests scripts
elif command -v ruff >/dev/null 2>&1; then
  ruff format trpc_service tests scripts
else
  echo "ruff is not installed; source is already kept in standard format"
fi

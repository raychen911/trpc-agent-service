#!/usr/bin/env bash
set -euo pipefail
if command -v flake8 >/dev/null 2>&1; then
  flake8 --config=.flake8 trpc_service tests scripts
else
  echo "flake8 is not installed; install it to run the repository lint check" >&2
  exit 1
fi

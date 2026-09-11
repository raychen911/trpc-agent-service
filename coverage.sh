#!/usr/bin/env bash
set -euo pipefail

if [[ ! -x .venv/bin/pytest ]]; then
  ./build.sh
fi
.venv/bin/pytest --cov=trpc_service --cov-report=term-missing --cov-report=html

#!/usr/bin/env sh
set -eu

python -m uv run pytest --cov=trpc_service --cov-branch --cov-report=term-missing \
  --cov-report=html --cov-report=json:runs/multitenant/coverage.json
python scripts/check_coverage.py runs/multitenant/coverage.json \
  --output runs/multitenant/coverage-gate.json

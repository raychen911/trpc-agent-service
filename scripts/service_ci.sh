#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${SERVICE_PYTHON:-}" ]]; then
  PYTHON_BIN="$SERVICE_PYTHON"
elif [[ -x .venv/bin/python ]]; then
  PYTHON_BIN=.venv/bin/python
else
  PYTHON_BIN=python3
fi

"$PYTHON_BIN" -m flake8 trpc_service tests/service \
  --select=E9,F63,F7,F82 --show-source --statistics
"$PYTHON_BIN" -m pytest tests/service \
  --cov=trpc_service \
  --cov-report=term-missing \
  --cov-report=xml:coverage.xml \
  --cov-fail-under=95
"$PYTHON_BIN" -m diff_cover.diff_cover_tool coverage.xml --fail-under=85
docker compose -f deploy/docker-compose.minimal.yml config --quiet
docker compose -f deploy/docker-compose.test.yml config --quiet

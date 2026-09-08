#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PROJECT_PYTHON:-${PROJECT_DIR}/.venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

cd "${PROJECT_DIR}"
"${PYTHON_BIN}" -m pytest tests/service \
  --cov=trpc_service \
  --cov-report=term-missing \
  --cov-report=xml:coverage.xml \
  --cov-fail-under=95
"${PYTHON_BIN}" -m diff_cover.diff_cover_tool coverage.xml --fail-under=85

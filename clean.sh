#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

rm -rf "${PROJECT_DIR}/build" "${PROJECT_DIR}/dist" \
  "${PROJECT_DIR}/htmlcov" "${PROJECT_DIR}/.pytest_cache"
rm -f "${PROJECT_DIR}/.coverage" "${PROJECT_DIR}"/coverage-*.xml \
  "${PROJECT_DIR}/coverage.xml"
find "${PROJECT_DIR}/trpc_service" \
  "${PROJECT_DIR}/tests/service" -type d -name __pycache__ -prune \
  -exec rm -rf {} +

#!/usr/bin/env bash
set -euo pipefail

rm -rf build dist .pytest_cache .coverage htmlcov
find trpc_service tests examples -type d -name __pycache__ -prune -exec rm -rf {} +

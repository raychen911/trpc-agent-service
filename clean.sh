#!/usr/bin/env bash
set -euo pipefail
find . -type d -name __pycache__ -prune -exec rm -rf {} +
rm -rf .pytest_cache .coverage htmlcov

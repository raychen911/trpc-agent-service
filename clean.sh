#!/usr/bin/env bash
set -euo pipefail

rm -rf .pytest_cache htmlcov build dist
rm -f .coverage
find . -type d -name '__pycache__' -prune -exec rm -rf {} +
find . -type d -name '*.egg-info' -prune -exec rm -rf {} +
echo "Removed generated Python test/build artifacts; data and virtualenv were preserved."

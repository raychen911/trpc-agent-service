#!/usr/bin/env sh
set -eu

find trpc_service tests migrations -type d -name __pycache__ -prune -exec rm -rf '{}' +
find trpc_service tests migrations -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete
rm -rf .coverage .mypy_cache .pytest_cache .ruff_cache build dist htmlcov
find . -maxdepth 1 -type d -name '*.egg-info' -exec rm -rf '{}' +

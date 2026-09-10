#!/usr/bin/env bash
set -euo pipefail
uv run coverage run -m pytest tests/ -v
uv run coverage report -m
uv run coverage html
echo "Coverage report generated in htmlcov/index.html"

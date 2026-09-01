#!/usr/bin/env bash
set -euo pipefail
find . -type d -name "__pycache__" -prune -exec rm -rf {} +
find . -type f -name "*.pyc" -delete

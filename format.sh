#!/usr/bin/env bash
set -euo pipefail

python -m ruff check --fix .
python -m ruff format .

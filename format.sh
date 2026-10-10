#!/usr/bin/env sh
set -eu

python -m uv run ruff format .
python -m uv run ruff check --fix .

#!/usr/bin/env sh
set -eu

python -m uv lock --check
python -m uv run ruff format --check .
python -m uv run ruff check .
python -m uv run mypy trpc_service

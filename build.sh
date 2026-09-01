#!/usr/bin/env bash
set -euo pipefail

uv sync --frozen --all-extras
uv run python -m compileall -q trpc_service
uv build

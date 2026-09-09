#!/usr/bin/env bash
set -euo pipefail

uv run ruff format --check trpc_service tests migrations
uv run ruff check trpc_service tests migrations
uv run mypy trpc_service

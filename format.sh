#!/usr/bin/env bash
set -euo pipefail

uv run ruff format trpc_service tests migrations
uv run ruff check --fix trpc_service tests migrations

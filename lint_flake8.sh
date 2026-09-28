#!/usr/bin/env bash

# Run Flake8 against application code, tests, and database migrations.

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$PROJECT_ROOT"
export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/trpc-agent-service-uv-cache}"

uv run flake8 trpc_service tests

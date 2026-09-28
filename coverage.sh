#!/usr/bin/env bash

# Run the test suite with the coverage threshold configured in pyproject.toml.

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$PROJECT_ROOT"
export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/trpc-agent-service-uv-cache}"

uv run pytest --cov=trpc_service --cov-report=term-missing --cov-report=xml "$@"
# Re-evaluate the persisted coverage data as an explicit shell gate. This
# protects CI even if a future pytest/pytest-cov combination only reports the
# threshold failure without propagating a non-zero process status.
uv run coverage report --fail-under=90 >/dev/null

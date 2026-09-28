#!/usr/bin/env bash

# Remove generated build/test artifacts without touching data volumes or secrets.

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$PROJECT_ROOT"

# Guard the recursive cleanup against execution from an unexpected checkout.
if [[ ! -f "$PROJECT_ROOT/pyproject.toml" || ! -d "$PROJECT_ROOT/trpc_service" ]]; then
    echo "error: project root validation failed" >&2
    exit 1
fi

rm -rf -- \
    "$PROJECT_ROOT/build" \
    "$PROJECT_ROOT/dist" \
    "$PROJECT_ROOT/htmlcov" \
    "$PROJECT_ROOT/.cache" \
    "$PROJECT_ROOT/.mypy_cache" \
    "$PROJECT_ROOT/.pytest_cache" \
    "$PROJECT_ROOT/trpc_agent_service.egg-info"

rm -f -- \
    "$PROJECT_ROOT/.coverage" \
    "$PROJECT_ROOT/coverage.xml" \
    "$PROJECT_ROOT/cov.tmp"

# `dev` is locally ignored and may not exist in an upstream checkout, but when
# present its helper clients must not retain stale bytecode from removed tools.
for source_tree in "$PROJECT_ROOT/trpc_service" "$PROJECT_ROOT/tests" "$PROJECT_ROOT/dev"; do
    if [[ ! -d "$source_tree" ]]; then
        continue
    fi

    find "$source_tree" -type d -name '__pycache__' -prune -exec rm -rf -- {} +
    find "$source_tree" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete
done

# Runtime logs are reproducible diagnostics. PID files are deliberately kept so
# an accidental clean cannot hide a process that still needs stop.sh.
if [[ -d "$PROJECT_ROOT/.run" ]]; then
    find "$PROJECT_ROOT/.run" -type f -name '*.log' -delete
fi

echo "Project build and test artifacts removed."

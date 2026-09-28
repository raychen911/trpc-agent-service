#!/usr/bin/env bash

# Reproduce the locked local environment and build distributable Python artifacts.

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$PROJECT_ROOT"
export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/trpc-agent-service-uv-cache}"

if ! command -v uv >/dev/null 2>&1; then
    echo "error: uv is required to build this project" >&2
    exit 127
fi

if [[ ! -f "$PROJECT_ROOT/uv.lock" ]]; then
    echo "error: uv.lock is missing" >&2
    exit 1
fi

if [[ $# -ne 0 ]]; then
    echo "usage: $0" >&2
    exit 2
fi

uv sync --locked
uv build --clear

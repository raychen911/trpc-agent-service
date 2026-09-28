#!/usr/bin/env bash

# Format Python sources or report the exact diff in check-only mode.

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$PROJECT_ROOT"
export UV_CACHE_DIR="${UV_CACHE_DIR:-${TMPDIR:-/tmp}/trpc-agent-service-uv-cache}"

if [[ "${1:-}" == "--check" ]]; then
    # YAPF 0.43 returns 1 when a diff exists and older releases may return 0.
    # Capture both forms so check mode always prints the actionable diff.
    readonly DIFF_FILE="$(mktemp)"
    trap 'rm -f -- "$DIFF_FILE"' EXIT
    set +e
    uv run yapf --diff --recursive trpc_service tests >"$DIFF_FILE"
    yapf_status="$?"
    set -e
    if ((yapf_status > 1)); then
        exit "$yapf_status"
    fi
    if [[ -s "$DIFF_FILE" ]]; then
        cat "$DIFF_FILE"
        exit 1
    fi
else
    uv run yapf --in-place --recursive trpc_service tests
fi

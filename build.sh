#!/usr/bin/env sh
set -eu

case $0 in
    */*) script_dir=${0%/*} ;;
    *) script_dir=. ;;
esac
project_dir=$(CDPATH= cd -- "$script_dir" && pwd)
cd "$project_dir"

UV_CACHE_DIR=${UV_CACHE_DIR:-"$project_dir/.uv-cache"}
UV_LINK_MODE=${UV_LINK_MODE:-copy}
PYTHONPYCACHEPREFIX=${PYTHONPYCACHEPREFIX:-"$project_dir/.build-pycache"}
PACKAGE_OUTPUT_DIR=${PACKAGE_OUTPUT_DIR:-"$project_dir/.package-dist"}
export UV_CACHE_DIR UV_LINK_MODE PYTHONPYCACHEPREFIX PACKAGE_OUTPUT_DIR

uv sync --extra dev --locked
uv run --no-sync python -m compileall -q trpc_service tests
uv build --out-dir "$PACKAGE_OUTPUT_DIR"

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
export UV_CACHE_DIR UV_LINK_MODE

uv run --frozen ruff format trpc_service tests
uv run --frozen ruff check --fix trpc_service tests

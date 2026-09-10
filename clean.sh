#!/usr/bin/env sh
set -eu

case $0 in
    */*) script_dir=${0%/*} ;;
    *) script_dir=. ;;
esac
project_dir=$(CDPATH= cd -- "$script_dir" && pwd)
cd "$project_dir"

rm -rf -- .pytest_cache .ruff_cache .build-pycache .package-dist htmlcov build dist
find trpc_service tests -type d -name __pycache__ -prune -exec rm -rf -- {} +

#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

find "${project_dir}/trpc_service" "${project_dir}/tests" -type d -name __pycache__ -prune -exec rm -rf {} +
rm -rf \
    "${project_dir}/.pytest_cache" \
    "${project_dir}/.coverage" \
    "${project_dir}/htmlcov" \
    "${project_dir}/trpc_agent_service.egg-info"

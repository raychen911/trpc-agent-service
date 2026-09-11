#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
service_python="${TRPC_SERVICE_PYTHON:-${project_dir}/.venv/bin/python}"
if [[ ! -x "${service_python}" ]]; then
    service_python="$(command -v python3)"
fi

cd "${project_dir}"
"${service_python}" -m yapf --in-place --recursive trpc_service tests

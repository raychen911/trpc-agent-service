#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bootstrap_python="${TRPC_SERVICE_BOOTSTRAP_PYTHON:-python3}"
venv_dir="${TRPC_SERVICE_VENV_DIR:-${project_dir}/.venv}"
local_sdk="${TRPC_AGENT_SOURCE:-${project_dir}/../trpc-agent-python}"

"${bootstrap_python}" -m venv "${venv_dir}"
service_python="${venv_dir}/bin/python"

if [[ -f "${local_sdk}/pyproject.toml" ]]; then
    "${service_python}" -m pip install --editable "${local_sdk}"
fi

"${service_python}" -m pip install --editable "${project_dir}[dev]"
"${service_python}" -c "import trpc_agent_sdk; print('trpc_agent_sdk import ok')"

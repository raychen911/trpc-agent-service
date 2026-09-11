#!/usr/bin/env bash
set -euo pipefail

python_bin="${PYTHON_BIN:-python3}"
"$python_bin" -m venv .venv
.venv/bin/python -m pip install --upgrade pip==24.3.1
.venv/bin/python -m pip install --editable ".[dev]"
.venv/bin/python -m compileall -q trpc_service

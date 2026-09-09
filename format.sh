#!/usr/bin/env bash
set -euo pipefail

python_bin="${PYTHON_BIN:-}"
if [[ -z "$python_bin" ]]; then
  if command -v python >/dev/null 2>&1; then python_bin=python; else python_bin=python3; fi
fi

"$python_bin" -m yapf -ir trpc_service tests examples

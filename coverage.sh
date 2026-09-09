#!/usr/bin/env bash
set -euo pipefail

python_bin="${PYTHON_BIN:-}"
if [[ -z "$python_bin" ]]; then
  if command -v python >/dev/null 2>&1; then python_bin=python; else python_bin=python3; fi
fi

pytest_temp=".tmp/pytest-$PPID-$$"
mkdir -p "$pytest_temp"
"$python_bin" -m pytest -m "not integration and not live" -p no:cacheprovider --basetemp="$pytest_temp" \
  --cov=trpc_service --cov-report=term-missing --cov-report=html --cov-fail-under=70

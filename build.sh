#!/usr/bin/env bash
set -euo pipefail
python -c 'import sys; sys.exit("Python 3.12+ is required") if sys.version_info < (3, 12) else None'
python -m compileall -q trpc_service tests
echo "build ok"

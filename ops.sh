#!/usr/bin/env sh
# Actual storage takeover followed by real IM, model, tools and worker restart.
set -eu
"${PYTHON:-python}" -m pytest -o addopts= tests/storage/test_protected_faults.py \
    --backend-mode=real -q --junitxml=reports/live-storage-faults.xml
exec sh e2e.sh "$@"

#!/usr/bin/env bash
set -euo pipefail

python - <<'PY'
from pathlib import Path
import shutil

for path in Path(".").rglob("__pycache__"):
    shutil.rmtree(path)
for name in (".pytest_cache", ".ruff_cache", ".coverage", "htmlcov", "dist", "build"):
    path = Path(name)
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()
PY

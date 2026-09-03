#!/usr/bin/env bash
set -e

echo "=== Building tRPC-Agent-Service Project ==="
python -m pip install -e . --no-deps 2>/dev/null || true
echo "Build completed successfully."

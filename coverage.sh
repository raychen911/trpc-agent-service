#!/usr/bin/env bash
set -euo pipefail

uv run pytest -m "not postgres and not release_gate" \
  --cov=trpc_service \
  --cov-report=term-missing \
  --cov-report=xml

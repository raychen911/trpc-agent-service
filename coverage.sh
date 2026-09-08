#!/usr/bin/env bash
set -euo pipefail

python -m pytest --cov=trpc_service --cov-report=term-missing

#!/usr/bin/env sh
set -eu
pytest --cov=trpc_service --cov-report=term-missing

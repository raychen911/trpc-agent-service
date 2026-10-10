#!/usr/bin/env sh
set -eu

python -m uv sync --extra dev --locked
python -m uv build
docker build --tag "${TRPC_SERVICE_IMAGE:-trpc-agent-service:dev}" .

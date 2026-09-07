#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/deploy/docker-compose.minimal.yml"

cd "${PROJECT_DIR}"
# Keep named volumes by default so tenant/session data remains recoverable.
docker compose -f "${COMPOSE_FILE}" down --remove-orphans

#!/usr/bin/env sh
set -eu
docker compose -f docker-compose.live.yml build
docker compose -f docker-compose.live.yml up -d --wait postgres redis qdrant minio
docker compose -f docker-compose.live.yml run --rm migrate
docker compose -f docker-compose.live.yml run --rm init-resources
docker compose -f docker-compose.live.yml up -d --wait

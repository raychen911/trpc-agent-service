#!/usr/bin/env sh
set -eu

cleanup() {
    docker compose --profile test down --remove-orphans
}
trap cleanup EXIT INT TERM

docker compose up --build --wait gateway worker-1 worker-2
docker compose --profile test run --rm integration

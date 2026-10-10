#!/usr/bin/env sh
set -eu

# Deliberately preserves named volumes. Use `docker compose down --volumes`
# only when local data deletion is explicitly intended.
docker compose down

#!/usr/bin/env sh
# Real IM/model acceptance requires configured backend URLs and IM credentials.
set -eu
exec "${PYTHON:-python}" -m trpc_service._cli live-acceptance --test-timeout 600 "$@"

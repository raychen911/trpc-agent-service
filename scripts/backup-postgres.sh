#!/usr/bin/env bash
# Create a consistent PostgreSQL custom-format backup and sidecar checksum.
# This script never deletes a prior backup.
set -euo pipefail

: "${TRPC_BACKUP_DATABASE_URL:?set the PostgreSQL URL for the database to back up}"
backup_dir="${TRPC_BACKUP_DIR:-./backups}"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"

umask 077
mkdir -p "$backup_dir"
backup_file="$backup_dir/trpc-agent-$stamp.dump"

pg_dump --format=custom --no-owner --no-privileges \
  --file "$backup_file" "$TRPC_BACKUP_DATABASE_URL"
sha256sum "$backup_file" >"$backup_file.sha256"
printf 'backup=%s\nchecksum=%s\n' "$backup_file" "$backup_file.sha256"

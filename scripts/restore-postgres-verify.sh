#!/usr/bin/env bash
# Restore into an explicitly provisioned, empty target database and verify it.
# It never drops or recreates a database; operators choose the isolated target.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 PATH_TO_BACKUP.dump" >&2
  exit 2
fi
: "${TRPC_RESTORE_TARGET_URL:?set an empty, isolated PostgreSQL target URL}"

backup_file="$1"
checksum_file="$backup_file.sha256"
[[ -f "$backup_file" ]] || { echo "backup does not exist: $backup_file" >&2; exit 2; }
[[ -f "$checksum_file" ]] || { echo "checksum does not exist: $checksum_file" >&2; exit 2; }

(cd "$(dirname "$backup_file")" && sha256sum --check "$(basename "$checksum_file")")
existing_tables="$(psql "$TRPC_RESTORE_TARGET_URL" -Atqc "
  SELECT count(*) FROM pg_catalog.pg_tables
  WHERE schemaname = 'public'
")"
[[ "$existing_tables" == "0" ]] || {
  echo "restore target must be empty; refusing to overwrite $existing_tables public tables" >&2
  exit 2
}

pg_restore --no-owner --no-privileges --dbname "$TRPC_RESTORE_TARGET_URL" "$backup_file"
runtime_schema_restored="$(psql "$TRPC_RESTORE_TARGET_URL" -v ON_ERROR_STOP=1 -Atqc "
  SELECT to_regclass('public.tenant') IS NOT NULL
     AND to_regclass('public.tenant_runtime_state') IS NOT NULL
     AND to_regclass('public.outbox') IS NOT NULL;
")"
[[ "$runtime_schema_restored" == "t" ]] || {
  echo "restore did not contain the required runtime schema" >&2
  exit 1
}
echo "restore verification succeeded"

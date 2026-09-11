#!/usr/bin/env bash
# Apply the workload-login setup to an existing Compose PostgreSQL volume.
# It creates/updates the named roles and grants and transfers ownership of
# bootstrap-owned schema objects to platform_schema_owner; it never deletes or
# rewrites tenant data.  The postgres service must have been started with
# .db-bootstrap.env available to it.
set -euo pipefail

if [[ ! -f .db-bootstrap.env ]]; then
  echo "missing .db-bootstrap.env; copy .db-bootstrap.env.example and set secrets first" >&2
  exit 2
fi

docker compose exec -T postgres sh /docker-entrypoint-initdb.d/00-create-workload-roles.sh

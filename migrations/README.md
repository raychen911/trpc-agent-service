# Database migrations

Run `tenant-agent db-init` (which applies `alembic upgrade head`) as a one-shot
release job before starting production nodes. Application nodes set
`TAP_AUTO_CREATE_SCHEMA=false`; they never mutate schema during startup.
Use the migration-owner DSN only for this job. Runtime services use a separate
`NOSUPERUSER NOBYPASSRLS` role and reject privileged PostgreSQL identities.

Run `tenant-agent db-init --database-url-env <ENV_NAME>` for every distinct tenant
platform SQL resource database. Native tRPC SQL Session storage is intentionally a
different database because its `sessions` table is incompatible with the platform
schema; provision it with
`tenant-agent native-session-init --database-url-env <ENV_NAME>` and reference it
with `native_dsn_ref`.

Future changes use expand/contract migrations: add nullable/new structures first,
deploy dual-read/write-compatible code, backfill and verify, then remove old
structures in a later release after rollback expires.

The optional PostgreSQL RLS example is defense in depth and is not enabled by the
portable baseline. Enable it only with a non-owner application role and a tested
transaction hook that sets `app.tenant_id`; otherwise it will correctly deny every
tenant query.
Alembic head `d4e5f607a1b2` explicitly removes the unsafe automatic reservation
policy from the earlier local revision so upgrades converge to this baseline.

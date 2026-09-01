# PostgreSQL RLS production hardening

PostgreSQL row-level security (RLS) is an optional production control. It is
not required for the local demo and is disabled by default. The service keeps
its existing application-level tenant checks when RLS is enabled:

1. Every storage API still requires and validates `tenant_id`.
2. Redis keys, object paths, vector collections, and SQL predicates remain
   tenant scoped.
3. PostgreSQL adds a database-enforced policy using a transaction-local
   `app.tenant_id` setting.

The two layers are intentionally independent. A missing or incorrect
application filter must not become a cross-tenant SQL read, and an RLS
configuration mistake must not remove the explicit tenant boundary from the
service code.

## Roles and DSNs

Use three separate database identities in production:

| Variable | Role | Use |
| --- | --- | --- |
| `POSTGRES_DSN` | `trpc_agent_app` | Gateway, workers, and storage adapters |
| `TENANT_DB_DSN` | `trpc_agent_admin` | Tenant configuration and control-plane reads/writes |
| `POSTGRES_SCHEMA_DSN` | schema owner or controlled `BYPASSRLS` role | Schema and RLS migrations only |

The application role and admin role must be login roles without
`SUPERUSER`, `BYPASSRLS`, `CREATEDB`, `CREATEROLE`, or replication privileges.
The migration identity is intentionally privileged and must not be supplied to
runtime Pods.

`trpc_agent_app` receives DML privileges only for tenant data tables. The
control-plane role also receives access to `tenant_config` and `tenant_active`.
The migration command installs both roles' policies and enables
`FORCE ROW LEVEL SECURITY` on all tables listed in
`trpc_service.storage.postgres_rls.RLS_TABLES`.

## Configuration

Local `.env` defaults remain:

```text
POSTGRES_RLS_ENABLED=0
POSTGRES_AUTO_CREATE_SCHEMA=1
```

Production should use:

```text
POSTGRES_RLS_ENABLED=1
POSTGRES_AUTO_CREATE_SCHEMA=0
POSTGRES_DSN=postgresql://trpc_agent_app:...@postgres.example/trpc_agent
TENANT_DB_DSN=postgresql://trpc_agent_admin:...@postgres.example/trpc_agent
POSTGRES_SCHEMA_DSN=postgresql://schema_owner:...@postgres.example/trpc_agent
POSTGRES_RLS_APP_ROLE=trpc_agent_app
POSTGRES_RLS_ADMIN_ROLE=trpc_agent_admin
POSTGRES_RLS_APP_PASSWORD=...
POSTGRES_RLS_ADMIN_PASSWORD=...
```

Role passwords are needed by the migration only when the roles do not already
exist. Store them in a migration-only Secret. Do not put
`POSTGRES_SCHEMA_DSN` or the role bootstrap passwords in the runtime Secret.

## Migration

Run schema creation first, then install RLS policies with the schema
connection:

```bash
python -m trpc_service.migrate schema \
  --backend postgres \
  --sql-dsn "$POSTGRES_SCHEMA_DSN"

python -m trpc_service.migrate rls \
  --sql-dsn "$POSTGRES_SCHEMA_DSN" \
  --app-role "$POSTGRES_RLS_APP_ROLE" \
  --admin-role "$POSTGRES_RLS_ADMIN_ROLE"
```

After the migration succeeds, restart or roll out runtime processes with
`POSTGRES_RLS_ENABLED=1` and `POSTGRES_AUTO_CREATE_SCHEMA=0`. The runtime
processes set `app.tenant_id` with `SET LOCAL` inside each PostgreSQL
transaction, so a pooled or reused connection cannot retain the previous
tenant context.

The Compose file runs these steps in `db-schema-migration` before the
PostgreSQL authentication preflight. It refuses to run with RLS enabled when
`POSTGRES_SCHEMA_DSN` is missing. The Kubernetes production template uses a
separate migration ExternalSecret and Job; deploy the migration Job and wait
for completion before treating the runtime rollout as ready.

## Verification

Use a disposable PostgreSQL database for RLS verification. The application
role must see no rows without a tenant context, must see only its current
tenant with a context, and must be unable to insert or update another
tenant's row. The admin role should see all tenants.

The repository includes focused unit coverage for context handling. Set
`POSTGRES_RLS_TEST_DSN` only for an explicitly disposable integration database
and run:

```bash
python -m unittest tests.test_postgres_rls -v
```

Do not enable RLS directly on the running local `trpcsmoke` database while
testing this change. Use a temporary PostgreSQL container or a separate
database instead.

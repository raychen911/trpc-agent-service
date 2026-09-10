# Local shared-state profile

This profile runs two external state services for the third-stage local proof:
Redis stores leases, fencing-critical idempotency state and short-lived sessions;
PostgreSQL stores authoritative tenant configuration, audit records and recovery
markers. The local InMemory profile remains available, but the shared profile
never falls back to it when either dependency is unavailable.

Set non-committed demo passwords in the current shell before starting:

~~~powershell
$env:TRPC_DEMO_REDIS_PASSWORD = "<local-only-password>"
$env:TRPC_DEMO_POSTGRES_PASSWORD = "<local-only-password>"
docker compose -f deploy/local-shared/compose.yaml up -d
docker compose -f deploy/local-shared/compose.yaml ps
~~~

Application DSNs are supplied at runtime through TRPC_SHARED_REDIS_URL and
TRPC_SHARED_DATABASE_URL. Do not place their values in source, screenshots,
logs, test evidence or defense material.

Stop the services after the test run:

~~~powershell
docker compose -f deploy/local-shared/compose.yaml down
~~~

Use down -v only for an explicitly disposable local test namespace. Never run
an unscoped reset against a non-test Redis database or PostgreSQL database.

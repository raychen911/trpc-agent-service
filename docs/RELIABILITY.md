# Reliability and Production Controls

This change keeps the existing multi-channel Gateway/Worker workflow and adds
production controls behind explicit configuration switches.

## Source of truth

`DURABLE_INBOX_OUTBOX=1` enables a tenant-scoped Inbox and Outbox on the
structured storage backend. PostgreSQL is the production backend; SQLite and
InMemory implementations keep local demos and tests self-contained.

The Inbox is deduplicated by `(tenant_id, idempotency_key)`. Each item has an
owner and a lease. A crashed owner can be reclaimed after the lease expires.
Successful results are persisted with the Inbox record and an
`agent.response` Outbox event.

The Outbox uses claim leases and `FOR UPDATE SKIP LOCKED` on PostgreSQL.
`python -m trpc_service._cli durable-outbox` publishes claimed events to a
Redis Stream or stdout. Delivery is idempotent and stale claims are
reclaimable. A poison event is moved to `dead` after
`DURABLE_OUTBOX_MAX_ATTEMPTS` failures instead of retrying forever.

## Session fencing

Session execution uses a monotonically increasing fencing token on Redis,
PostgreSQL, and SQLite. A Worker renews and validates its lease before durable
session writes. A superseded Worker receives `SessionLeaseLost` and cannot
continue the write path.

The lease also preserves the existing behavior: the same Session is serialized
while different Sessions can execute in parallel.

## Redis transport

The existing list-based queue remains the default. Set
`WORKER_QUEUE_TRANSPORT=streams` to use Redis Streams consumer groups and
idle-message reclaim. A transient Stream worker failure is re-enqueued without
publishing a terminal Gateway result; only the final retry writes an error
result. Redis is treated as transport only; PostgreSQL Inbox and Outbox remain
the recovery boundary.

Cluster-wide channel rate-limit windows use Unix time in Redis keys. This is
required because process-local monotonic clocks do not share an epoch across
nodes.

## Admin concurrency and identity

Admin GET responses return `ETag: "<config_version>"`. Mutating endpoints
accept `If-Match: "<config_version>"` and return HTTP 412 for stale updates.
Set `REQUIRE_ETAG=1` to return HTTP 428 when the header is missing.

API keys remain supported. Set `OIDC_ISSUER` or `OIDC_JWKS_URL` to enable
optional RS256 OIDC/JWKS Bearer-token authentication. OIDC roles are limited
to `viewer`, `operator`, `platform_admin`, and `superadmin`; tenant scope is
read from the configured tenant claim.

## Production switches

Recommended Kubernetes values:

```text
DURABLE_INBOX_OUTBOX=1
WORKER_QUEUE_TRANSPORT=streams
REQUIRE_ETAG=1
SESSION_LEASE_TTL_SECONDS=120
INBOX_LEASE_SECONDS=180
```

The platform manifest includes a schema migration Job, separate Gateway,
Worker, compensation, outbound, and durable-outbox roles, plus the existing
security context, network policy, probes, and disruption budgets.

## PostgreSQL RLS

PostgreSQL RLS is an opt-in second tenant boundary. The application-level
`tenant_id` checks, Redis prefixes, object paths, vector collection scoping,
and authorization rules remain mandatory. When `POSTGRES_RLS_ENABLED=1`,
PostgreSQL storage methods set `app.tenant_id` with `SET LOCAL` inside a
transaction and the database policies enforce the same tenant value.

Use separate runtime, control-plane, and schema-migration DSNs. The production
Kubernetes template enables RLS and disables application schema creation. The
local demo and development Kubernetes manifest keep RLS disabled.

See [POSTGRES_RLS.md](./POSTGRES_RLS.md) for role requirements, migration
commands, deployment ordering, and disposable-database verification.

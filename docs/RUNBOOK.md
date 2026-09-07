# Operations Runbook

## 1. Local minimal deployment

Copy the example environment file, replace all development credentials, and start:

```bash
cp .env.example .env
uv sync --extra dev
uv run tenant-agent validate-config config/tenants.example.yaml
uv run tenant-agent db-init
uv run tenant-agent serve --role all --host 127.0.0.1 --port 8080
```

Open `http://127.0.0.1:8080`. The default browser tenant is offline and
deterministic. It tests the complete path without a model key.
Tenant-owned environment secrets use an injective prefix. Alphanumeric IDs use
`TENANT_{TENANT_ID}_*` (the demo uses `TENANT_DEMO_WEBHOOK_TOKEN`); IDs containing
`-` or `_` use the Base32 prefix printed by `tenant_environment_prefix`. Tenant file references use canonical relative paths
such as `file://demo/webhook-token`, never `..`, absolute paths, or backslashes.
Arbitrary top-level symlinks, Windows junctions, and any link resolving outside the
tenant directory are rejected. The one top-level exception is the exact Kubernetes AtomicWriter projection chain
`<tenant> -> ..data/<tenant>` and `..data -> ..<timestamp>`; both the visible and
resolved paths must remain inside the configured secret root and tenant snapshot.

Docker alternative:

```bash
docker compose --profile minimal up --build
```

Do not use this profile for multi-node validation: Session, Memory, and Summary are
InMemory by design.

## 2. Pre-production checklist

1. Pin and scan the image and dependency lock. Confirm
   `trpc-agent-py==1.1.19` or the explicitly approved successor.
2. Provision highly available PostgreSQL and Redis/Redis Cluster. Use primary reads
   for Session, receipt, usage, and outbox paths. Use a migration owner only in
   one-shot jobs and a distinct `NOSUPERUSER NOBYPASSRLS` runtime role. Startup
   rejects a privileged PostgreSQL runtime identity.
3. Provision object/vector backends and Vault/KMS. Create per-tenant IAM prefixes or
   collections and least-privilege database roles.
4. Generate three unrelated values for `TAP_SESSION_HMAC_KEY`,
   `TAP_ADMIN_BEARER_TOKEN`, and `TAP_INTERNAL_BEARER_TOKEN`; each should have at
   least 256 bits of entropy. Do not place them in Git or ConfigMaps.
5. Apply schema expansion before application rollout. Set
   `TAP_AUTO_CREATE_SCHEMA=false` on production nodes. Run `db-init` once for the
   control DSN and once with `--database-url-env` for every distinct tenant SQL
   resource DSN. For each SQL Session backend, provision a different native tRPC
   database with `native-session-init`; the platform and native DSNs must never be
   the same. Alembic head `d4e5f607a1b2` removes the unsafe RLS policy introduced
   by an earlier local revision. The application does not set `app.tenant_id`, so
   `postgres_rls.example.sql` is an inactive design reference and must not be
   applied until transaction-scoping middleware and real non-bypass-role tests
   exist.
6. Validate tenant config, resolve every required secret in a preflight identity,
   construct the configured tRPC model/session runtime, test model/tool allow-lists,
   health-check all six resource adapters, and send
   signed callback fixtures. Activation must fail without changing the active
   revision if this bounded preflight fails.
7. Confirm Collector export, prompt/state attribute removal, Prometheus scraping,
   and alert routing before real messages.
8. Load-test callback burst, hot session, many sessions, duplicate updates, provider
   429/timeout, database failover, and Worker termination.

## 3. Compose integration deployment

Create a protected `.env` with production values referenced by Compose, then:

```bash
docker compose --profile production config
docker compose --profile production up --build --scale worker=3
```

Compose is for integrated acceptance. Use managed PostgreSQL, Redis, object/vector
services, and Kubernetes for availability testing. The Channel Adapter binds to
loopback by default. If a TLS reverse proxy is added, expose only
`/v1/channels/*/webhook`; keep `/metrics`, health, and every admin/internal route on
the private network.

On a fresh Compose volume, the Postgres entrypoint creates
`tenant_agent_admin` for migration and a separate `tenant_agent_app` runtime role.
Existing Postgres volumes do not rerun entrypoint initialization: provision/grant
the runtime role explicitly before upgrade. Compose does not provision external
tenant resource/native databases referenced through Vault; run the same one-shot
commands against each of those DSNs before activating the tenant.

## 4. Kubernetes deployment

Apply the prerequisite file first so the namespace exists. Then create
the role-scoped platform Secrets (`tenant-agent-{channel,gateway,worker,outbox,
admin,migration}-secrets`), purpose-scoped provider volumes
(`tenant-{channel,worker,outbox}-provider-secrets`), and `telemetry-secrets`
through External Secrets, Sealed Secrets, Vault CSI, or the cloud secret-manager
controller. Give each Vault identity only its role/tenant paths. Do not apply
`secrets.example.yaml` with placeholders.

Prefer Vault Agent, Vault CSI, or another workload-identity sidecar that renews a
short-lived token into a memory-backed file. Set `VAULT_TOKEN_FILE` to that sink;
the resolver rereads it on every uncached Vault request, so rotation does not
require a pod restart. Keep the secret-cache TTL shorter than the provider's
revocation objective. `VAULT_TOKEN` remains a local/acceptance fallback only.
Kubernetes Secret volumes use AtomicWriter symlinks; the resolver accepts only its
exact tenant-scoped `..data` form and continues to reject arbitrary symlinks or
cross-tenant targets.

Build, scan, and push the application image first. Use an immutable digest, never
the example `tenant-agent-platform:0.1.0` tag:

```bash
export IMAGE='registry.example.com/tenant-agent-platform@sha256:<digest>'
export RELEASE_ID='release-20260829a'
kubectl apply -f deploy/k8s/prerequisites.yaml
# Materialize the three Secrets here and wait for them to become Ready.
uv run python scripts/render_k8s.py --image "${IMAGE}" \
  --release-id "${RELEASE_ID}" --input deploy/k8s/migration.yaml \
  --output .rendered/migration.yaml
kubectl apply -f .rendered/migration.yaml
kubectl -n tenant-agent wait --for=condition=complete \
  "job/schema-migrate-d4e5f607a1b2-${RELEASE_ID}" --timeout=10m
uv run python scripts/render_k8s.py --image "${IMAGE}" \
  --input deploy/k8s/platform.yaml --output .rendered/platform.yaml
kubectl apply -f .rendered/platform.yaml
kubectl apply -f deploy/k8s/telemetry.yaml
kubectl -n tenant-agent rollout status deployment/channel-adapter
kubectl -n tenant-agent rollout status deployment/gateway
kubectl -n tenant-agent rollout status deployment/worker
kubectl -n tenant-agent rollout status deployment/outbox
```

The default-deny NetworkPolicy requires DNS, same-namespace, and approved external
egress. Narrow the example `0.0.0.0/0` egress CIDR to real provider/Vault/database
ranges before production.

## 5. Register IM callbacks

Use a high-entropy opaque `binding_id`. Activate its tenant config before platform
registration.

Telegram webhook URL:

```text
https://agent.example.com/v1/channels/telegram/{binding_id}/webhook
```

Set Telegram's `secret_token` to the value referenced by `webhook_secret`. Restrict
allowed update types and retain pending updates during safe rollout. Validate the
callback with a real signed update and a repeated copy.

Before registering or sending anything, perform the read-only credential probe:

```bash
uv run tenant-agent probe-channel --channel telegram --binding-id tg-acme-prod-01
```

It resolves secrets without printing them and calls Telegram `getMe`; it does not
register a webhook or send a message.

WeCom callback URL:

```text
https://agent.example.com/v1/channels/wecom/{binding_id}/webhook
```

Configure the same callback token, encoding AES key, corp ID, application secret,
and agent ID referenced in tenant configuration. Complete GET verification, then
test direct text, group/application chat where permitted, image/file metadata, and
an intentionally repeated `MsgId`.
Activation validates these locally before accepting the revision: the callback
token is 3-32 alphanumeric characters, the EncodingAESKey is exactly 43
alphanumeric characters decoding to 32 bytes, CorpID starts with `ww`, and AgentID
is a canonical positive decimal integer.

Validate Corp ID/application credentials without a delivery side effect:

```bash
uv run tenant-agent probe-channel --channel wecom --binding-id wc-acme-prod-01
```

The probe requests and discards an access token. Real callback encryption and
delivery still require the enterprise permissions supplied by the mentor/runtime.

### WeCom intelligent bot WebSocket

This mode uses only the Bot ID and Secret and does not expose a webhook. Store them
under the tenant-scoped references in the YAML, then authenticate without sending
user content:

```bash
export TENANT_BOTDEMO_WECOM_BOT_ID='your-bot-id'
export TENANT_BOTDEMO_WECOM_BOT_SECRET='your-bot-secret'
uv run tenant-agent wecom-bot --probe-only
```

To run the offline deterministic chat profile, remove `--probe-only`. To run a
configured tenant model and shared production backends, use
`--config config/tenant.production.example.yaml` plus the environment names from
that file, including `--bot-id-env TENANT_ACME_WECOM_BOT_ID
--bot-secret-env TENANT_ACME_WECOM_BOT_SECRET`. The manager leases one socket per Bot ID across nodes, routes callbacks
through the normal Gateway/Worker path, and sends durable replies through that
binding's isolated outbox lane. A Bot ID must have exactly one active tenant
binding; the refresh loop rejects duplicates. Provider authentication failure is
blocked until the credentials or a new config revision is corrected.

## 6. Tenant configuration rollout and rollback

Validate offline:

```bash
uv run tenant-agent validate-config proposed-tenant.yaml
```

Create a draft with the Admin API, run synthetic tests against the draft in an
isolated tenant, then activate. Roll out by tenant cohorts. Watch revision-specific
error, p95 latency, token/cost, tool denial, and delivery metrics.
The activation endpoint and first-time bootstrap both perform tool registration, secret resolution,
production backend policy, browser-channel, and backend readiness checks; an HTTP
422 is a rejected configuration and 503 is an unavailable dependency. Invalid
provider-specific model names must fail here, not on the first user request.

Rollback is an atomic active-revision switch:

```http
POST /admin/v1/tenants/acme/rollback/17
Authorization: Bearer <admin token>
```

The bearer-token Admin API derives a non-secret credential fingerprint and writes
a fail-closed administrative audit record before activation, rollback, or replay;
caller-supplied actor headers are ignored. A shared token still does not identify
individual humans, and the endpoint has no expected-current-revision CAS. Front it
with individual workload identity, record the change ticket, and serialize operators.

During normal rollout, queued messages continue under the revision recorded at
ingress. Current tenant suspension or binding disable is an authoritative security
override and terminally stops queued execution/delivery. Keep prior secret versions
valid for the normal queue/retry window unless performing emergency revocation.

## 7. Data migration

### Redis Session/Memory/Summary to SQL

The checked-in CLI is a quiesced/offline shadow copier, not a live dual-write
controller. Provision every normalized platform SQL database through Alembic and
every isolated native tRPC SQL database through `native-session-init` before
running it; SQL and local-vector adapters use `create_schema=False`, and missing
SQLite paths are rejected before an engine can create an empty source.

1. Back up both systems, stop tenant writes (or supply a separately implemented and
   tested dual-write/tail-catch-up layer), and record a session/event watermark.
2. Create an empty shadow target namespace. If writes cannot be quiesced, stop: the
   repository does not claim online migration support.
3. Run the tenant-scoped idempotent copier; pass DSNs by environment variable so
   they do not appear in shell history or process arguments:

```bash
export SOURCE_DSN='redis://...'
export TARGET_DSN='postgresql+asyncpg://...'
export TARGET_NATIVE_DSN='postgresql+asyncpg://.../tenant_agent_native'
uv run tenant-agent native-session-init --database-url-env TARGET_NATIVE_DSN
uv run tenant-agent migrate-data \
  --tenant-id acme \
  --source-kind redis --source-dsn-env SOURCE_DSN \
  --target-kind sql --target-dsn-env TARGET_DSN \
  --target-native-dsn-env TARGET_NATIVE_DSN \
  --source-namespace tap:acme-session \
  --resources sessions \
  --include-native-session-history
```

Run Memory and Summary as separate commands when their configured DSNs or
namespaces differ; this is expected in the per-resource data model.

4. Replay native tRPC SessionService history in the same CLI run. Redis native
   history defaults to the normalized Redis endpoint; every SQL side requires an
   explicit, different `--source-native-dsn-env` or
   `--target-native-dsn-env`.
5. Require zero canonical hash mismatches and compare sampled reconstructed state,
   summaries, and retrieval results.
6. Verify the source watermark has not advanced, then activate a config revision
   pointing reads at SQL. Retain the source unchanged through the rollback window.

The CLI rejects an all-empty selected source by default; use
`--allow-empty-source` only for a reviewed intentional no-op. Add
`--include-native-session-history` when migrating SQL/Redis sessions to invoke the
native tRPC replay phase. The command first requires pre-provisioned native SQL
tables/columns so the SDK cannot create or alter source schema during the read.
Native state and ordered event hashes must match after copy. A verified target
prefix resumes after interruption and converges before final state/hash
verification. For cluster-backed Redis, add `--source-redis-cluster` and/or
`--target-redis-cluster` so both the normalized and native services use cluster
clients.

### Local vector to Qdrant

If the vector dimension and embedding model are unchanged, migrate stable chunks
and compare hash/count plus a golden-query recall set. If either changes, provide
an embedding transform, build a new per-tenant Qdrant collection, and compare recall
before switching config. Do not hash-compare old and newly embedded vectors; compare
document/chunk identity, metadata, and retrieval quality instead.

For re-embedding, generate an offline map and golden set. The map format is
`{"chunks":{"document/chunk":{"embedding":[...],"embedding_model":"v2"}}}`;
the golden format is
`{"queries":[{"embedding":[...],"expected_ids":["document/chunk"],"min_matches":1}]}`.
Then run:

```bash
uv run tenant-agent migrate-data \
  --tenant-id acme \
  --source-kind local-vector --source-dsn-env SOURCE_VECTOR_DSN \
  --target-kind qdrant --target-dsn-env TARGET_QDRANT_DSN \
  --resources knowledge \
  --embedding-map embedding-map.json \
  --golden-queries golden-queries.json
```

The command rejects transforms that change tenant/document/chunk/text/metadata,
uses identity hashes instead of vector equality, and exits non-zero on a recall miss.
For an unchanged cosine embedding, verification canonicalizes both source and
Qdrant-returned vectors to unit direction before hashing, avoiding false failures
from server normalization while still detecting changed direction or dimension.

## 8. Incident actions

### Callback error or delivery drop

Check callback 2xx rate, Gateway publish errors, Redis stream pending age, receipt
status, outbox oldest age, and platform-specific rate/error codes. Do not manually
rerun the model for a completed receipt. Requeue only the dead outbox item using its
original ID after correcting credentials or platform state. Inspect `next_segment`
and `delivered_external_ids`: a checkpointed item must resume at the first
unacknowledged segment rather than replaying all earlier chunks.

Use the supported tenant-scoped Admin API after the underlying fault is fixed:

```bash
curl -H "Authorization: Bearer ${TAP_ADMIN_BEARER_TOKEN}" \
  "https://admin.internal/admin/v1/tenants/acme/outbox/dead"
curl -X POST -H "Authorization: Bearer ${TAP_ADMIN_BEARER_TOKEN}" \
  "https://admin.internal/admin/v1/tenants/acme/outbox/<outbox-id>/requeue"
curl -H "Authorization: Bearer ${TAP_ADMIN_BEARER_TOKEN}" \
  "https://admin.internal/admin/v1/tenants/acme/broker/dead"
curl -X POST -H "Authorization: Bearer ${TAP_ADMIN_BEARER_TOKEN}" \
  "https://admin.internal/admin/v1/tenants/acme/broker/dead/<broker-id>/requeue"
```

Each mutation first writes an audit record using a fingerprint of the authenticated
admin credential; caller-supplied actor headers are ignored. Redis dead letters are
partitioned by tenant. Requeue preserves the original inbound identity or outbox
checkpoint, so receipt idempotency prevents a second model side effect.

### Worker crash loop

Stop the bad rollout, keep Gateway accepting into Redis if capacity allows, and
scale the prior Worker image. Pending entries are reclaimed after idle timeout.
Check lease loss and revision-conflict counts; sustained conflicts usually indicate
two coordinators or a lease duration below model/tool p99.
Transient handled failures are not left in the PEL: they move atomically into the
delayed sorted set with incremented attempts, return when their computed delay is
due, and enter the tenant dead-letter stream at `TAP_WORKER_MAX_ATTEMPTS`.
`duplicate_processing` waits use the delayed set without incrementing attempts;
normal long-running idempotency contention must never create a dead letter.

### Summary or Memory projection lag

Inspect `tenant_agent_auxiliary_repair_total` and Outbox rows with kind
`auxiliary-repair`. The repair worker reconstructs data from immutable Session
events and retries automatically; it never invokes the model. Restore only the
affected Session+Summary or Session+Memory backends. Unrelated Artifact/Knowledge
outages cannot block an already-generated IM reply.

### Database unavailable

Before durable Gateway publication, return 503 so IM retries. After publication,
leave the stream entry unacknowledged. Restore the primary, verify transaction and
replica health, then observe pending age decline. Never switch Session correctness
reads to a lagging replica as an emergency shortcut.

### Model degradation

Check provider latency/429/5xx and tenant budgets. Reduce admission, switch an
approved model profile through a new config revision, or return the built-in safe
timeout message. Tool calls already visible to users are not replayed by model retry.
Budget reservation uses `context_window_tokens` for every
`max_llm_calls_per_request`, plus each maximum output, and tRPC enforces matching
LLM/tool-call limits. The bound also multiplies each logical call by
`retry_count + 1`. Size monthly capacity for this intentionally conservative
in-flight reservation, not only average billed tokens. Provider error, exception,
and timeout terminal events carry all usage observed before failure; crash recovery
sums every matching native invocation event rather than only the final event.

### Audit retention or export failure

Check the `audit_maintenance`/`audit_export` safe error metrics, sink reachability,
authorization reference, and oldest expired row. Export failure deliberately keeps
the source rows. Restore the sink and let the worker retry the same ID-derived
idempotency key; do not delete or bulk-replay records manually before reconciliation.

### Suspected secret leak

Disable the affected binding/tenant, revoke and rotate the secret at its provider,
invalidate IM access-token cache by restarting Delivery Workers, drain Runner
bundles when rotating model keys, search sanitized audit by trace ID, and preserve
forensic logs. Do not copy raw exceptions into chat or tickets.

### Backup and ordered restore drill

Define tenant-tier RPO/RTO before launch; a reasonable starting point is RPO <= 5
minutes and RTO <= 60 minutes for the control plane. Use encrypted, access-logged,
cross-failure-domain provider backups rather than copying mounted live volumes.

Capture a recovery manifest containing the active tenant config revisions,
PostgreSQL WAL/PITR timestamp, Redis persistence/checkpoint position, object-store
version markers, Qdrant snapshot IDs, Vault snapshot/version, image digest, and
Alembic head. Restore into an isolated environment in this order:

1. Vault/KMS and role-scoped secret access, without printing resolved values.
2. PostgreSQL to the selected PITR point; run `alembic current` and `alembic check`.
3. Redis/Redis Cluster from a consistent snapshot/AOF and verify consumer groups,
   pending entries, and no unsafe stream trimming.
4. Versioned object storage, then Qdrant collections/snapshots; verify artifact
   SHA-256 values and golden-query recall.
5. Deploy the recorded immutable image digest, validate tenant config checksums,
   run signed duplicate callback fixtures, and confirm Session event/state/summary
   reconstruction before opening ingress.

Run this drill at least quarterly in a disposable environment and after backend or
schema changes. Record measured RPO/RTO, missing objects/events, checksum/recall
results, and remediation. Dead outbox/stream records are archived under an explicit
incident retention policy; they are not deleted by routine completed-row cleanup.

## 9. SLO and alert starting points

- callback accept availability >= 99.95%, p95 < 250 ms excluding external TLS;
- queue oldest age < 30 seconds normally and < 2 minutes during one-node loss;
- completed turn error/degraded rate < 1%;
- p95 Session backend operation < 20 ms Redis / < 50 ms SQL;
- IM final delivery success >= 99.9% after retries;
- dead outbox count = 0 and oldest pending < 60 seconds;
- monthly budget usage alerts at 70%, 85%, 95%, and hard limit;
- no secret scanner finding in image, config repository, logs, or exported spans.

Tune these after baseline traffic. Alert on symptom plus cause: for example queue
age with Worker active count and provider latency, not queue length alone.

## 10. Capacity command

```bash
uv run tenant-agent capacity \
  --peak-rps 100 \
  --p95-latency 4 \
  --concurrency-per-worker 32 \
  --average-input-tokens 1000 \
  --average-output-tokens 500 \
  --headroom 1.5
```

Validate the result by load test and provider quota. Repeat per tenant tier and for
the aggregate callback peak.

For a bounded local socket/load smoke against the deterministic profile:

```bash
uv run pytest tests/test_live_http.py
uv run python scripts/load_smoke.py --requests 1000 --concurrency 32
```

For PostgreSQL-specific row locking and `SKIP LOCKED` evidence, point only at a
disposable database; a skipped test is not a production pass:

```bash
TAP_TEST_POSTGRES_URL='postgresql+asyncpg://...' \
  uv run pytest -m integration tests/test_postgres_integration.py
```

## 11. Handoff boundary

This runbook performs local validation and deployment operations only. Creating a
commit, pushing a branch, opening a pull request, registering a real external
webhook, or submitting the challenge requires explicit owner authorization.

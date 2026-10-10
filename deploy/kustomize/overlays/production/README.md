# Production overlay

This overlay is a self-contained single-cluster default: it renders the tRPC
application roles, Gateway Ingress, pgvector/PostgreSQL, Redis, MinIO,
OpenTelemetry Collector, Prometheus and Prometheus Adapter.  Stateful data uses
retained `ReadWriteOnce` PVCs.  It is a complete functional deployment, not a
claim of cross-zone data-layer HA; use a reviewed operator or managed services
for that requirement.

The overlay intentionally references a pre-created Secret named
`trpc-service-secrets` and a separate `trpc-worker-secrets`; no credential is
stored in this repository. Populate them from an ExternalSecret/Vault/KMS
integration (or create them out-of-band) with
the environment-variable keys consumed by the deployments, including at
least:

- `TRPC_SERVICE_DATABASE_DSN`
- `TRPC_SERVICE_REDIS_URL`
- `TRPC_SERVICE_S3_ACCESS_KEY`
- `TRPC_SERVICE_S3_SECRET_KEY` (the ConfigMap points
  `TRPC_SERVICE_S3_SECRET_KEY_REF` at `env://TRPC_SERVICE_S3_SECRET_KEY`)
- `TRPC_SERVICE_OIDC_ISSUER`
- `TRPC_SERVICE_OIDC_AUDIENCE`

For a private image registry, include one additional Secret of type
`kubernetes.io/dockerconfigjson` in the same external Secret manifest. Keep
its `metadata.namespace` unset so the acceptance gate can bind it to its
disposable namespace, and set the non-sensitive
`TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET` value to that Secret's name. The gate
validates only kind/name/namespace/type metadata before any server-side
dry-run, injects the reference into the migration Job and all Deployments,
and passes the same name to the bounded HPA load Job. Registry credentials are
never copied into the evidence report.

For a direct Kustomize deployment outside the runtime gate, copy
`image-pull-secret-patch.example.yaml` into the environment-specific overlay,
replace the placeholder with the same Secret name, and target both
`Deployment` and `Job` resources from that overlay's `kustomization.yaml`.
The example patch is deliberately not enabled by default, so public-registry
deployments do not contain a hard-coded pull Secret name.

The worker Secret must also provide `TRPC_SERVICE_WORKER_DATABASE_DSN`,
`TRPC_SERVICE_WORKER_DATABASE_DSN_REF`,
`TRPC_SERVICE_WORKER_DATABASE_PASSWORD`, and
`TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF`. Only worker, dispatcher,
projector, connector, and recovery pods mount this Secret. The dedicated
`trpc_worker` role is expected to be a non-superuser with explicit EXECUTE
grants on the cross-tenant SECURITY DEFINER functions; the normal tenant
runtime role must not receive that Secret.

The model provider credential is a separate Secret named
`trpc-model-secrets` with exactly one key, `model_api_key`. The
`model-secret-mount-patch.yaml` patch mounts that key read-only at
`/run/secrets/model_api_key` only in `trpc-worker`. A tenant's immutable model
revision references `file:///run/secrets/model_api_key`; the endpoint host is
also required in `TRPC_SERVICE_MODEL_ENDPOINT_HOSTS`. Supplying either the
Secret or the host allow-list alone does not enable real model calls. Do not
mount this Secret into Gateway, Admin, dispatchers, connectors, migration,
metrics, recovery, projector, or artifact-GC pods. The single-file mount uses
Kubernetes `subPath` so it is not updated in place; after rotating the Secret,
roll out `trpc-worker` and verify the replacement pods before revoking the old
credential.

Model provider, HTTPS base URL, exact model ID, request timeout, reasoning
effort/thinking budget, sampling controls, and optional fallback model are
tenant-revision fields in `deploy/tenant-config.example.json`; they are not
image or ConfigMap constants. `reasoning_effort` selects the OpenAI Responses
API, while `thinking_budget_tokens` selects the SDK built-in planner for
Anthropic or a compatible OpenAI endpoint. They are mutually exclusive and a
positive thinking budget must be smaller than the tenant turn-token budget.
Changing a tenant revision does not require an image rollout, but every base
URL host must already be present in the deployment-wide endpoint allow-list.

Real Feishu and WeCom credentials are supplied through a dedicated Secret named
`trpc-im-secrets`. It contains exactly `feishu_app_secret`,
`feishu_verification_token`, `feishu_encrypt_key`, and `wecom_bot_secret`.
The production `im-secret-mounts-patch.yaml` mounts it read-only at
`/run/secrets/im` only on Gateway, Worker, Channel Dispatcher, and WeCom
Connector. Channel bindings store `file:///run/secrets/im/<key>` references;
they never store the values. Feishu/WeCom account IDs remain non-secret
binding fields. Do not move these values into the ConfigMap or add the IM
Secret to Admin, migration, metrics, outbox, projector, recovery, or artifact
GC pods.

The backlog exporter uses a separate Secret named `trpc-metrics-secrets` with
only `TRPC_SERVICE_METRICS_DATABASE_DSN`.  It must connect as the dedicated
`trpc_metrics` login created before migration `0016`; that login is
`NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS` and can execute
only `public.count_session_ready_backlog()`.  Do not reuse the worker DSN.

The bundled data services require a dedicated Secret named
`trpc-infrastructure-secrets`.  Its exact keys are listed in
`../../base/secrets.example.yaml`: PostgreSQL superuser and four application
role passwords, Redis password, MinIO root credentials, and a separate MinIO
application identity.  Role passwords must match the raw values encoded in the
four DSNs.  The MinIO bootstrap creates a bucket-scoped read/write policy for
the application identity; `trpc-service-secrets` must use that identity for S3,
while root credentials remain mounted only by MinIO and the bootstrap Job.  The
rendered manifests never contain this Secret or any credential value.

PostgreSQL role initialization runs only for an empty PVC.  On an existing
database, rotate each role with a controlled `ALTER ROLE ... PASSWORD ...`
operation before atomically updating its Secret/DSN and rolling dependent
Pods; changing the Secret alone does not change the database role.  The MinIO
bootstrap converges the application password on every rebuild.  Its bucket
policy name is deliberately versioned (`trpc-artifacts-rw-v1`): whenever the
policy document changes, bump that name and reattach it instead of expecting an
existing policy with the same name to be overwritten.

The ConfigMap also sets an explicit tenant-secret policy: file references are
confined to `/run/secrets`, the environment allowlist is empty by default, and
`TRPC_SERVICE_MODEL_ENDPOINT_HOSTS` must be replaced with the reviewed model
provider hosts used by this deployment.  Feishu stale binding-cache fallback
is disabled.  Emergency queue keys are versioned with
`TRPC_SERVICE_EMERGENCY_QUEUE_KEY_VERSION`; populate
`TRPC_SERVICE_EMERGENCY_QUEUE_PREVIOUS_KEY_REFS` only while each referenced
previous key is present in the Secret and being retired.

Replace the example application image name/digest, OIDC values and Ingress
host/TLS settings before applying.  The default S3 and OTLP endpoints are the
bundled `minio` and `otel-collector` Services.  To use managed dependencies,
create a child overlay that applies `managed-services-patch.example.yaml`,
replace its endpoint placeholders, supply managed DSNs in the external
Secrets, and install another provider for `trpc_session_ready_backlog` before
enabling the worker HPA. That child overlay must also add endpoint-specific
egress and metrics scrape/adapter policies; the default only permits managed
private CIDRs on 5432/6379/9000/4317/4318/443 and does not make public services
or arbitrary namespaces reachable. The runtime account is expected to be a non-owner PostgreSQL role;
database migrations run separately with the `migrate` role. The production
runtime gate rejects tag-only images, `example.*` endpoints, and placeholder
digests.

The worker capacity envelope is explicit: the production overlay sets
`TRPC_SERVICE_WORKER_CONCURRENCY=10` and the HPA allows 20 worker replicas,
which is a maximum 200 concurrent-turn envelope. The bundled Prometheus
Adapter exposes `trpc_session_ready_backlog`; CPU-only HPA evidence is
insufficient and keeps the runtime gate `not_run`.  The supplied exporter
derives this value from runnable PostgreSQL `session_mailboxes`;
Redis stream length is not authoritative because its wake-up entries are
reconstructable.

The base NetworkPolicy intentionally does not grant arbitrary external HTTPS
egress. Both production and performance overlays include the shared
`im-external-egress` policy, which selects only `trpc-channel-dispatcher` and
`trpc-wecom-connector`. Production separately selects Gateway/Admin/Worker for
OIDC, model, and reviewed tool HTTPS. Standard Kubernetes NetworkPolicy cannot
express FQDN destinations, so each portable fallback uses `0.0.0.0/0:443` for
only those roles. Where the cluster CNI supports FQDN-aware policies, replace
the two IPv4-wide destinations with their independently reviewed provider
hostnames.

Render and inspect the manifests, then follow the staged migration/bootstrap
procedure in the repository README. Do not apply the whole overlay directly:
plain `kubectl` does not implement Argo hook ordering and cannot replace a
completed Job whose candidate image changed.

```bash
kubectl kustomize deploy/kustomize/overlays/production
```

Argo CD ordering uses normal resources at wave `-2`, rebuildable `Sync` hooks
for schema migration and MinIO bootstrap at wave `-1`, and application
Deployments at wave `0`.  The old migration `PreSync` phase could run before a
first-install PostgreSQL StatefulSet existed; `BeforeHookCreation` also avoids
trying to mutate an already completed Job after an image change.  Migration
has a bounded `pg_isready` init wait for direct `kubectl apply`.

Every concrete checked-in infrastructure/support image is a digest-pinned
`docker.io` reference so ACK nodes can use a transparent Docker Hub accelerator
such as Xuanyuan.  The application entry is intentionally a `docker.io`
repository/digest placeholder and must be replaced with the reviewed candidate
before deployment.  The Prometheus Adapter uses the pre-published Docker Hub
support digest also pinned by the runtime-gate configuration; do not replace it
with an unpinned tag.

## Scheduler version changes

The production ConfigMap is pinned to scheduler `v2` and the matching
`trpc:session-ready:v2` / `trpc-session-ready-v2` transport. A change between
`v1` and `v2` is a protocol cutover, not a normal Deployment rollout. Do not
edit `TRPC_SERVICE_SCHEDULER_VERSION`, the Redis stream, or the consumer group
while old and new pods overlap, and do not use a normal `RollingUpdate` for the
version transition. Pause the Gateway/WeCom ingress, drain the old PostgreSQL
outbox and Redis group, stop the old Worker/Outbox processes, apply the reviewed
version-specific ConfigMap while scheduler-related replicas are zero, then
start recovery/dispatcher/worker and restore ingress in that order.

Only a same-version image/configuration change may use the Deployment
`RollingUpdate` strategy. Keep both versioned Redis streams, groups, emergency
groups, and outbox rows during the observation window; never delete or trim them
to make a drain check pass. The complete SQL/Redis drain criteria and the
v2→v1 rollback guard (all v2 mailboxes must be `IDLE` with
`accepted_sequence = resolved_sequence`) are in
[`docs/scheduler-cutover.md`](../../../../docs/scheduler-cutover.md).

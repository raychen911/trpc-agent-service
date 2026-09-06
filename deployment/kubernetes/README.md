# Kubernetes deployment

## Production manifest

`platform.yaml` is a production-oriented template. It contains no database
password, Admin key, model key, or `latest` image tag. Before applying it:

1. Publish and scan an immutable application image. The checked-in image is a
   placeholder; use the Kustomize overlay to replace it with the image and
   digest produced by CI/CD.
2. Install External Secrets Operator and create the
   `ClusterSecretStore/platform-secrets` referenced by the manifest.
3. Put Redis, PostgreSQL, vector-store, object-store, model, and Admin
   credentials in the external secret manager.
4. Replace example host names such as `agent.example.com`, the object bucket,
   and the external secret paths with environment-specific values.
5. Create the `trpc-agent-tls` TLS Secret through the certificate workflow.

The production ConfigMap sets `PUBLIC_SURFACE_AUTH_REQUIRED=1`. The Ingress only
routes `/webhooks` and the authenticated `/admin` API; `/metrics`, `/ui`, and
`/ui/api/chat` are not public routes. If Prometheus scrapes `/metrics`, use an
internal Service path and inject an Admin API authentication header from a Secret.

```bash
kubectl apply -f deployment/kubernetes/platform.yaml
```

For a real registry, edit `kustomization.yaml` or generate an environment
overlay and apply it with:

```bash
kubectl apply -k deployment/kubernetes
```

The base manifest includes an OTLP HTTP Collector Service at
`http://otel-collector:4318`. Its debug exporter is suitable only for a
development smoke test. Production should replace the Collector exporter and
pin the application image by digest. Replace the example external egress
 CIDR in `platform.yaml` with the approved model, IM, vector, object-storage
 and telemetry address ranges before applying. Redis and PostgreSQL ingress is
 restricted to the platform workloads by label; keep the database services in
 the namespace or adjust the selectors for the organization's service mesh.

Artifacts use S3-compatible object storage and metrics are expected to be
scraped by an external Prometheus. Traces enter the in-cluster Collector and
must be exported to the organization's tracing backend before production. The
production manifest therefore does not mount local `/app/data` or a local
Prometheus database: an `emptyDir` there would lose durable data on a Pod
replacement.

The production topology assumes managed or highly available Redis and
PostgreSQL. Redis provides shared Session, queue, idempotency, and lock state;
PostgreSQL provides tenant configuration, Memory, Summary, and Audit data.

The production template enables PostgreSQL RLS and disables application-side
schema creation. Configure the runtime Secret with:

- `POSTGRES_DSN`: a non-owner `trpc_agent_app` login role.
- `TENANT_DB_DSN`: a separate `trpc_agent_admin` login role for the
  control-plane tenant repository.

Configure the migration-only Secret with:

- `POSTGRES_SCHEMA_DSN`: the schema owner or controlled `BYPASSRLS` DSN.
- `POSTGRES_RLS_APP_PASSWORD` and `POSTGRES_RLS_ADMIN_PASSWORD`: bootstrap
  passwords used only if the two least-privilege roles do not exist.

Apply the manifest, wait for the migration Job, and then monitor the runtime
rollout:

```bash
kubectl apply -k deployment/kubernetes
kubectl -n trpc-agent wait --for=condition=complete \
  job/trpc-agent-schema-migration --timeout=10m
kubectl -n trpc-agent rollout status deployment/gateway
kubectl -n trpc-agent rollout status deployment/worker
```

For later releases, use a release-specific migration Job name or a deployment
controller hook so a completed Job is recreated for the new image and schema.

## Local k3s development

`dev-local.yaml` references a Secret but does not contain credentials. Create
the Secret locally before applying the manifest. Use the same generated
password in both DSNs:

```bash
kubectl create namespace trpc-agent
password="$(openssl rand -hex 24)"
kubectl -n trpc-agent create secret generic trpc-agent-secrets \
  --from-literal=POSTGRES_PASSWORD="$password" \
  --from-literal=POSTGRES_DSN="postgresql://trpc_agent:${password}@postgres:5432/trpc_agent" \
  --from-literal=TENANT_DB_DSN="postgresql://trpc_agent:${password}@postgres:5432/trpc_agent" \
  --from-literal=ADMIN_API_KEY="$(openssl rand -hex 32)"
kubectl apply -f deployment/kubernetes/dev-local.yaml
```

Build and import the fixed local image tag expected by `dev-local.yaml`:

```bash
docker build -t docker.io/library/deployment-gateway:0.1.0 .
```

This manifest is for local verification only. Its local Artifact backend and
single-node Redis are not a production durability design. It explicitly keeps
`POSTGRES_RLS_ENABLED=0` and `POSTGRES_AUTO_CREATE_SCHEMA=1`.

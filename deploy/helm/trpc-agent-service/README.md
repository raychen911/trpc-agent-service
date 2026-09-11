# Helm deployment

Create the referenced Kubernetes Secret through External Secrets, Sealed Secrets,
or your cloud secret manager. It must provide at least
the per-workload URL keys `TRPC_SERVICE_API_DATABASE_URL`,
`TRPC_SERVICE_WORKER_DATABASE_URL`, `TRPC_SERVICE_DISPATCHER_DATABASE_URL`,
`TRPC_SERVICE_MIGRATOR_DATABASE_URL`, and `TRPC_SERVICE_AIBOT_DATABASE_URL`,
plus `TRPC_SERVICE_REDIS_URL` and `TRPC_SERVICE_ADMIN_API_KEY`. Model and
channel credentials are optional for mock mode. Never commit a populated Secret
manifest or reuse the migrator URL in a data-plane workload.

```bash
helm upgrade --install trpc-agent-service . \
  --namespace trpc-agent --create-namespace \
  --set image.repository=registry.example/trpc-agent-service \
  --set image.tag=<immutable-image-digest-or-tag> \
  --set storageProfiles.existingPvc=trpc-agent-storage-rwx
```

The migration Job runs as a Helm post-install/post-upgrade hook. Set
`migration.hook=false` only when migrations are operated by a separate approved
pipeline. HPA requires metrics-server; use KEDA with Redis Stream lag for
queue-aware worker scaling in clusters that provide it.

`storageProfiles.existingPvc` is required in the PostgreSQL runtime. It must be
a durable ReadWriteMany claim mounted by every worker; the worker projects the
active tenant storage profile there and reads it during model execution. Keep
the default mount path unless `storageProfiles.mountPath` is changed together
with the application configuration.

To enable a WeCom Smart Bot long connection, add
`TRPC_LIVE_WECOM_AIBOT_SECRET` to the referenced Secret as JSON containing
`bot_id` and `secret`, create a `wecom_aibot` channel binding, and set
`wecomAibot.enabled=true`. The gateway is intentionally one replica: WeCom
allows only one active WebSocket per Bot ID.

# Real Environment Verification

Date: 2026-09-07

This report records checks performed against the current workstation and the
authorized WeCom Bot credentials. Credential values are intentionally omitted.

## Results

| Target | Result | Evidence |
|---|---|---|
| WeCom intelligent-bot WebSocket | Passed | Connected to `wss://openws.work.weixin.qq.com`, completed `aibot_subscribe`, sent `ping`, and received a successful ACK. The local deterministic Bot service is running on `127.0.0.1:8081` and its readiness endpoint returned HTTP 200. |
| Real callback → Worker → reply | Passed after stream-ID and offline-response fixes | Three real messages reached tenant `botdemo`; all receipts and Bot outbox rows completed with one attempt. The latest identity query returned the configured assistant name without echoing the input, and its initial progress/final response used the same stream identifier. |
| PostgreSQL concurrency and roles | Blocked | No `psql`, `postgres`, or `pg_ctl` executable; `TAP_TEST_POSTGRES_URL` is unset; no listener on port 5432. |
| Redis Cluster | Blocked | No `redis-server`, `redis-cli`, Docker, Podman, or Redis endpoint configured; no listener on port 6379. |
| Docker Compose | Blocked | Docker CLI/daemon is unavailable. Static Compose YAML validation remains green. |
| Kubernetes | Blocked | `kubectl`, `kind`, and `minikube` are unavailable; no kubeconfig is configured. Static manifest validation remains green. |
| Vault | Blocked | No Vault CLI or `VAULT_ADDR`/token is configured. The Windows `VaultSvc` is Credential Manager, not HashiCorp Vault. |
| S3-compatible storage | Blocked | No S3 endpoint or credentials are configured. Existing adapter tests use a mocked client. |
| Qdrant server | Blocked; SDK local mode passed | No server endpoint or credentials are configured. SQL-to-real-Qdrant-Python-local migration, tenant filtering, recall, and on-disk restart tests passed. This is not a remote Qdrant cluster test. |
| Local load, process failure, backup, and restore | Passed | Two real loopback processes shared SQLite; 500 requests at concurrency 32 all completed. A node was killed mid-turn and recovered after real lease expiry. Three databases were backed up/restored and the restored server continued the original session. See `LOCAL_RUNTIME_EVIDENCE_2026-09-07.md`. |
| Production load, chaos, and PITR | Blocked | PostgreSQL/Redis Cluster/cloud storage failover and provider snapshots still require disposable external infrastructure. |

## WeCom command used

The check used an in-memory process environment and discarded it when the process
ended. It did not write the Bot ID or Secret to the repository, `.env`, logs,
traces, or reports. The same check is available as:

```powershell
$env:TENANT_BOTDEMO_WECOM_BOT_ID = '<Bot ID>'
$env:TENANT_BOTDEMO_WECOM_BOT_SECRET = '<Bot Secret>'
uv run tenant-agent wecom-bot --probe-only
```

For an actual callback/reply test, send one text message to the bot from the
WeCom client while the following command is running:

```powershell
uv run tenant-agent wecom-bot
```

That command uses the offline deterministic profile. To test the configured model,
shared Session/Memory, and production adapters, pass the production YAML and
provide its model/Vault/Redis/SQL/object/vector references as documented in
`REVIEW_WECOM_BOT_2026-09-07.md`.

## Reproduction in the mentor environment

Set only the following disposable environment variables before running the
corresponding checks:

```powershell
$env:TAP_TEST_POSTGRES_URL = 'postgresql+asyncpg://...'
uv run pytest -m integration tests/test_postgres_integration.py

docker compose --profile production config
docker compose --profile production up --build --scale worker=3

kubectl apply -f deploy/k8s/prerequisites.yaml
uv run python scripts/render_k8s.py --image 'registry.example/app@sha256:<digest>' `
  --release-id '<release>' --input deploy/k8s/migration.yaml --output .rendered/migration.yaml
kubectl apply -f .rendered/migration.yaml
kubectl apply -f deploy/k8s/platform.yaml

uv run python scripts/load_smoke.py --requests 1000 --concurrency 32
```

Vault, S3, Qdrant, Redis Cluster, signed IM callbacks, chaos, and restore drills
must use the mentor-provided endpoints and credentials. Their absence here is an
environment limitation, not a passed production result.

The WeCom WebSocket command and frame shapes follow the
[WeCom intelligent-bot SDK](https://github.com/WecomTeam/aibot-node-sdk).

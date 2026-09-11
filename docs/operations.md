# Operations and recovery runbook

All runtime processes use `TRPC_SERVICE_RUNTIME_BACKEND=postgres`; PostgreSQL is
the source of truth and Redis Streams is only an at-least-once transport. Start
the complete local stack with `docker compose up --build`. Stateful service
ports bind to localhost so they are not exposed by default.

## Compose integration and failure recovery

After the migration job completes, execute the real-service test suite from an
environment with the project dependencies installed:

```bash
TRPC_TEST_DATABASE_URL=postgresql://trpc:trpc@127.0.0.1:5432/trpc_agent \
TRPC_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
pytest -m integration
```

The test covers atomic Inbox/Outbox acceptance, duplicate callback suppression,
Redis consumer-group delivery, a fenced execution/commit, delivery status, and
the RLS tenant boundary. The focused unit recovery test also simulates the
crash window after a broker publish but before the Outbox publication mark.

For a self-contained deployment probe (including the running API, dispatcher,
worker, PostgreSQL, and Redis), run this from the Compose project directory:

```bash
COMPOSE_INTEGRATION=1 bash scripts/compose-integration.sh
```

The opt-in script builds/starts the stack, creates one uniquely named mock
tenant, then waits for its callback to reach the durable `delivered` state. It
does not stop the stack or delete deployment data. Set `TRPC_COMPOSE_API_URL`
for a non-default API address and `TRPC_COMPOSE_ADMIN_API_KEY` if admin auth is
enabled.

## Backup and restore drill

Run `bash scripts/backup-postgres.sh` with `TRPC_BACKUP_DATABASE_URL` and an
optional `TRPC_BACKUP_DIR`. It writes a PostgreSQL custom dump and SHA-256
checksum without deleting old backups. Copy both files to approved durable,
encrypted storage.

For a recovery drill, provision an *empty isolated* database and set
`TRPC_RESTORE_TARGET_URL`; then run:

```bash
bash scripts/restore-postgres-verify.sh backups/trpc-agent-YYYYMMDDTHHMMSSZ.dump
```

The verifier checks the checksum, refuses a nonempty target, restores, and
checks the required tenant, runtime-state, and Outbox tables. It never drops
or overwrites a database.

## Load and live-channel probes

Use a bounded health probe first:

```bash
python scripts/load-smoke.py --url https://service.example --requests 500 --concurrency 25
```

Live provider tests are deliberately opt-in and only send one sandbox message
per configured provider:

```bash
RUN_LIVE_CHANNEL_TESTS=1 \
TRPC_LIVE_TELEGRAM_SECRET='{"bot_token":"..."}' \
TRPC_LIVE_TELEGRAM_RECIPIENT=12345 \
pytest -m live
```

For the legacy WeCom self-built-app adapter, use `TRPC_LIVE_WECOM_SECRET` (JSON
containing `corp_id` and `corp_secret`), `TRPC_LIVE_WECOM_AGENT_ID`, and
`TRPC_LIVE_WECOM_RECIPIENT`. Keep all values in an operator shell or secret
manager, never in Git.

## WeCom Smart Bot long connection

The Smart Bot (`wecom_aibot`) channel is distinct from the self-built-app
adapter above. It owns one outbound TLS WebSocket per Bot ID and requires no
public callback URL. The `wecom-aibot` gateway is the only process that sends
replies for this provider; ordinary dispatcher replicas deliberately skip them.

Create `/home/ubuntu/trpc-agent-service/.wecom-aibot.env` on the Compose host
with mode `0600` and this single value (do not put the secret in Git or chat):

```bash
TRPC_LIVE_WECOM_AIBOT_SECRET='{"bot_id":"YOUR_BOT_ID","secret":"YOUR_BOT_SECRET"}'
```

Create a `wecom_aibot` channel binding through the admin API. Its
`external_account_id` must equal the Bot ID and `secret_ref` must point to the
environment variable. No `webhook_key` is required for this provider; the API
creates an unexposed random locator value for relational integrity.

```json
{
  "binding_id": "wecom-smart-bot",
  "agent_id": "support",
  "provider": "wecom_aibot",
  "external_account_id": "YOUR_BOT_ID",
  "secret_ref": "env://TRPC_LIVE_WECOM_AIBOT_SECRET"
}
```

Start or refresh the dedicated gateway after the file and binding exist:

```bash
sudo docker compose up -d --build wecom-aibot
sudo docker compose logs -f wecom-aibot
```

The gateway authenticates by sending `aibot_subscribe`, sends a `ping` every
30 seconds, uses bounded exponential reconnection for network loss, and stops
after repeated authentication failures rather than repeatedly submitting a bad
secret. A `disconnected_event` indicates a second connection for the same bot;
the displaced gateway stops, preserving WeCom's one-connection rule. Send a
test message to the Smart Bot in WeCom to perform the live test. The eventual
reply is a final stream response correlated to the original WebSocket request
ID and is retained in the normal delivery ledger.

For Kubernetes, add `TRPC_LIVE_WECOM_AIBOT_SECRET` to the existing Secret and
enable the singleton gateway with `--set wecomAibot.enabled=true`. Keep
`wecomAibot.replicas=1`; it must not be autoscaled.

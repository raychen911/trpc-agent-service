# WeCom Intelligent Bot Review

Date: 2026-09-07

The supplied Bot ID and Secret belong to WeCom's intelligent-bot WebSocket
product. They are not the CorpID, AgentID, callback token, and EncodingAESKey
used by the existing enterprise-application callback adapter. The implementation
now supports both modes as independent channel types.

## Implementation

`ChannelType.WECOM_BOT` uses the provider's authenticated
`wss://openws.work.weixin.qq.com` connection. It sends `aibot_subscribe`, handles
ACK-correlated `aibot_respond_msg` stream frames, sends heartbeats, and reconnects
with bounded backoff. The Bot ID is checked on every callback; the Secret is only
resolved through a tenant secret reference and is never written to configuration,
events, memory, telemetry, audit details, or error messages.

Inbound text, voice, image, file, video, and mixed frames become the existing
`InboundEnvelope`. Direct sessions are derived from tenant, app, binding, channel,
and user; group sessions additionally include the group and thread. The platform's
HMAC receipt key handles duplicate `msgid` delivery. Media URLs and per-message
AES keys are treated as short-lived capabilities and are not persisted until an
authorized media retrieval service is added.

The manager acquires a shared lease keyed by a hash of the Bot ID, so one node owns
each bot connection. Each binding has a dedicated outbox kind and only its socket
owner claims those rows. The initial `Working on it...` stream keeps the provider
callback responsive; the final Agent response updates the same stream. A failed
publication closes the stream with a safe retry message. Replies are bounded to
20,480 UTF-8 bytes and limited to a conservative 2.1-second send interval.

## Configuration

The local example is `config/tenant.wecom-bot.example.yaml`:

```powershell
$env:TENANT_BOTDEMO_WECOM_BOT_ID = '<Bot ID>'
$env:TENANT_BOTDEMO_WECOM_BOT_SECRET = '<Bot Secret>'
uv run tenant-agent wecom-bot --probe-only
uv run tenant-agent wecom-bot
```

The probe authenticates and checks a heartbeat without sending user content. The
second command starts the offline deterministic profile on `127.0.0.1:8081`.
For an existing model and production backends, use the production YAML and its
secret names:

```powershell
$env:TENANT_ACME_WECOM_BOT_ID = '<Bot ID>'
$env:TENANT_ACME_WECOM_BOT_SECRET = '<Bot Secret>'
uv run tenant-agent wecom-bot `
  --config config/tenant.production.example.yaml `
  --bot-id-env TENANT_ACME_WECOM_BOT_ID `
  --bot-secret-env TENANT_ACME_WECOM_BOT_SECRET
```

The production command also requires the model, Vault, Redis, SQL, and object/vector
references in that YAML to be provisioned. A Bot ID must have exactly one active
tenant binding.

## Review result

- Existing enterprise WeCom and Telegram adapters remain intact.
- Bot credentials are format-validated during preflight and can be probed safely.
- Callback routing, group/direct identity derivation, duplicate handling, lease
  ownership, outbox isolation, ACK correlation, rate limits, stream-size limits,
  and shutdown were tested with a local WebSocket server.
- `pytest`: 133 passed, 1 external PostgreSQL integration test skipped because
  `TAP_TEST_POSTGRES_URL` is not configured.
- Branch-aware coverage: 85.13%.
- Ruff format/check, mypy strict (48 source files), Alembic lock, configuration
  validation, YAML parsing (46 documents), AST parsing (83 Python files), and
  locked dependency audit all pass.

The protocol follows the command and frame shapes documented by the
[WeComTeam intelligent-bot SDK](https://github.com/WecomTeam/aibot-node-sdk).
No commit, push, deployment, webhook registration, or submission was performed.

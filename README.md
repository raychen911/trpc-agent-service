# Multi-Tenant tRPC-Agent Platform

A runnable reference implementation for deploying tRPC-Agent-Python across
multiple tenants and stateless worker nodes. It includes tenant-scoped routing,
SQL/Redis/in-memory data adapters, Telegram, encrypted WeCom callbacks, and the
Bot ID/Secret authenticated WeCom intelligent-bot WebSocket,
governance Filters, transactional IM delivery, OpenTelemetry, configuration
rollback, lossless tenant-aware broker backpressure, audited dead-letter recovery,
migration tooling, and both minimal and Kubernetes deployments.

## Quick start

Prerequisites: Python 3.11–3.13 and `uv`.

```bash
cp .env.example .env
uv sync --extra dev
uv run tenant-agent validate-config config/tenants.example.yaml
uv run tenant-agent serve
```

Open <http://localhost:8080>, keep the default demo binding and webhook token,
and send a message. The demo tenant uses a deterministic offline engine so the
complete ingress, routing, persistence, governance, streaming, audit, and reply
path can be tested without a model key.

Run verification:

```bash
uv run pytest
uv run ruff check .
uv run mypy src
uv run python scripts/security_audit.py
```

The suite includes a real loopback-Uvicorn HTTP test. PostgreSQL-specific locking
tests are opt-in and only count as evidence when a disposable database is supplied:

```bash
TAP_TEST_POSTGRES_URL='postgresql+asyncpg://...' uv run pytest -m integration
uv run python scripts/load_smoke.py --requests 1000 --concurrency 32
```

With production credential references available, run safe read-only IM probes
before webhook registration or message delivery:

```bash
uv run tenant-agent probe-channel --channel telegram --binding-id tg-acme-prod-01
uv run tenant-agent probe-channel --channel wecom --binding-id wc-acme-prod-01
```

The production example is at `config/tenant.production.example.yaml`. It uses
Redis Cluster for sessions/memory, SQL for summaries/audit, S3-compatible object
storage, Qdrant, Vault secret references, Telegram, and WeCom.

It now also includes the WeCom intelligent-bot WebSocket channel. The Bot ID and
Secret are read only from secret references. For a local connection test, set
`TENANT_BOTDEMO_WECOM_BOT_ID` and `TENANT_BOTDEMO_WECOM_BOT_SECRET`, then run
`uv run tenant-agent wecom-bot --probe-only`; to run the offline chat profile,
omit `--probe-only`. For the production tenant, pass
`--config config/tenant.production.example.yaml --bot-id-env TENANT_ACME_WECOM_BOT_ID
--bot-secret-env TENANT_ACME_WECOM_BOT_SECRET` and set those two environment
variables. The command never prints either credential.

## Documentation

- `docs/OFFICIAL_TASK.md` — canonical local acceptance specification and mentor-QA interpretation.
- `docs/DESIGN.md` — architecture, consistency, isolation, IM mapping, security,
  recovery, capacity, and deployment decisions.
- `docs/REQUIREMENTS_MATRIX.md` — requirement-to-code/test evidence.
- `docs/REVIEW_REMEDIATION_2026-08-30.md` — nine-finding production-readiness remediation record.
- `docs/REVIEW_REMEDIATION_ROUND2_2026-08-30.md` — second-round closure and reproduced gates.
- `docs/REVIEW_REMEDIATION_ROUND3_2026-08-30.md` — current third-pass closure,
  corrected RLS boundary, and reproduced release gates.
- `docs/REVIEW_WECOM_BOT_2026-09-07.md` — Bot ID/Secret WebSocket integration,
  configuration commands, and current review evidence.
- `docs/REAL_ENVIRONMENT_VERIFICATION_2026-09-07.md` — real endpoint result and
  infrastructure blockers from the current workstation.
- `docs/LOCAL_RUNTIME_EVIDENCE_2026-09-07.md` — two-process SQL, load, kill/recovery,
  backup/restore, Qdrant-local, and wheel-runtime evidence.
- `docs/RUNBOOK.md` — deployment, incident response, migration, rollout, and rollback.
- `docs/DEFENSE.md` — concise presentation and likely defense questions.
- `docs/SUBMISSION.md` — author, institution, branch, and credential handling.

The application does not automatically commit, push, register webhooks, or submit
code to external repositories. Deployment and submission are explicit operator actions.

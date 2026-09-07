# Acceptance and Evidence Matrix

Status legend: **Implemented** means runnable code plus direct or integration test;
**Designed and scaffolded** means a production adapter/deployment is present but
requires the named external service or credentials for a live test.

## Core multi-tenant and multi-node requirements

| Requirement | Status | Implementation evidence | Verification evidence |
|---|---|---|---|
| Tenant includes `tenant_id`, app/model/tool/IM/data/audit policy | Implemented | `models.py`: `TenantConfig` and nested strict models; example YAML files | `test_models_and_ids.py`, config CLI validation |
| Agent Gateway, Worker, Channel Adapter, Storage Adapter, Admin API, Telemetry Collector topology | Implemented | role-specific FastAPI lifecycle, authenticated `/internal/v1/inbound`, storage router, admin routes, Collector configs | API tests; YAML parse validation; `DESIGN.md` topology |
| Multi-node horizontal routing to tenant and session | Implemented | binding lookup, `GatewayRouter`, HMAC IDs, Redis Streams consumer group with unique hostname identity, atomic global/per-tenant broker admission | ID tests, concurrent bootstrap/adapter initialization, Dispatcher concurrency, broker-capacity isolation, SQL two-adapter lease/outbox tests |
| No sticky session; stateless Worker | Implemented | shared tRPC SessionService selection, shared data plane, renewable Redis/SQL lease, optimistic revision | memory/Redis/SQL lease renewal and stale-revision tests |
| Config/data/tool/log/key isolation | Implemented | immutable revisions, runtime/bootstrap preflight, tenant predicates/prefixes, injective tenant-env encoding, fixed Vault hierarchy, AtomicWriter-aware tenant file containment, fixed tool registry and Filter, recursive logging/OTLP redaction | collision/traversal/AtomicWriter/cross-env/Vault/model/security/governance tests, strict gates |

## Data synchronization and multi-backend requirements

| Requirement | Status | Evidence |
|---|---|---|
| Tenant-selectable InMemory, Redis, SQL, vector, object, external Memory | Implemented/scaffolded | `storage/router.py`, `memory.py`, `redis.py`, `sql.py`, `external.py`; production example selects Redis, SQL, S3, Qdrant |
| Unified Session, Memory, Summary, Artifact, Knowledge, Audit abstraction | Implemented | narrow protocols in `storage/base.py`, `TenantDataPlane`, adapter router |
| Concurrent same-session consistency | Implemented with stated ordering limit | Renewable Redis/SQL coordination lease, revision CAS, event sequence constraint; strict callback FIFO across consumers is not claimed |
| Event/state/summary update order | Implemented | atomic `append_event`, Dispatcher happens-before order, co-located InMemory/Redis/SQL Summary checkpoint validation, durable event-derived Summary/Memory repair outbox | future-checkpoint and injected auxiliary-outage repair tests |
| Memory cross-node visibility | Implemented | synchronous Redis pipeline/SQL commit/external contract after final event | immediate visibility test; Redis design |
| Redis-to-SQL and local-to-remote vector migration | Offline implementation; online design | No-DDL pre-provisioned platform/native SQL adapters, explicit isolated native DSN options, empty-source gate, resumable native prefix replay, re-embedding recall and cosine-normalized Qdrant verification; external online phases remain explicit | missing-path/no-create, interrupted CLI replay, empty opt-in, corrupt/prefix/native-hash/Qdrant/re-embedding/recall tests |
| Duplicate IM delivery idempotency | Implemented | HMAC receipt, completed-terminal predicate, pre-budget persisted-output recovery, stable normalized/native IDs, transactional receipt/outbox, non-counting duplicate wait | budget-exhausted recovery, expired-completed receipt, confirmation replay, duplicate-wait/no-dead, native recovery and outbox tests |
| Backend consistency/latency/cost tradeoff | Documented | `DESIGN.md` section 6.5 |
| Minimum tenant/app/session/event/memory/summary/channel/audit schema | Implemented | executable `storage/schema.py`; schema table in `DESIGN.md` |

## IM integration requirements

| Requirement | Status | Evidence |
|---|---|---|
| At least two IM classes | Implemented | Telegram Bot API, encrypted WeCom enterprise application, and WeCom intelligent-bot WebSocket; browser fallback is fourth |
| External message to tRPC input; Agent Event to reply/stream/card | Implemented | channel adapters, authenticated Bot ID/Secret WebSocket frames, safe attachment descriptors, `TrpcAgentEngine`, normalized `AgentEvent`, SSE, Telegram keyboard, WeCom template card/stream |
| Account/tenant binding, URL, token/secret, signature, dedupe, identity | Implemented | versioned `ChannelBindingConfig`, trusted route lookup, Telegram secret header, WeCom callback format/AES/CorpID/AgentID checks, Bot ID/Secret WebSocket subscribe and callback matching, HMAC identity/receipt, safe credential probes |
| Group/direct session rule and cross-scope isolation | Implemented | `IdentityDeriver` direct/group/per-user algorithms | direct/group/cross-tenant ID tests |
| Length/rate/async/media/recall/retry limits | Implemented/documented | 4,000-character Telegram / 2,048-byte enterprise-WeCom splitting, 20,480-byte Bot stream cap, Bot heartbeat/reconnect and rate/error classification, credential-scoped WeCom token cache, attachment model, ordered segment checkpoints; edit/withdraw limitations are explicit and remote recall is not claimed | CJK byte-limit, Bot local-WebSocket auth/callback/reply/limit tests, normalization, invalid-recipient/token-rotation, and partial-delivery resume tests |

## Governance, monitoring, and security requirements

| Requirement | Status | Evidence |
|---|---|---|
| Filter: tool whitelist, redaction, budget, dangerous confirmation, IM ACL | Implemented | atomic context-window × logical-call × provider-attempt token/cost reservation, matching tRPC LLM/tool limits, transactional reconciliation including billed failures, exact confirmation, ACL/redaction Filter | retry-aware bound-size, concurrent SQL/InMemory budget, persisted/native/error recovery, ACL/CJK/filter tests |
| Request/model/tool/delivery/error/token/cost/backend metrics | Implemented | Prometheus instruments in `observability.py`, `/metrics` | API metrics assertion |
| One trace across callback, Runner, Tool, Session/Memory, reply | Implemented | W3C carrier in job; explicit callback/session/runner/summary/memory/outbox/reply spans; inherited tRPC spans | trace-context and exporter-redaction tests; external Collector integration remains an environment smoke test |
| Required audit fields and lifecycle | Implemented | `AuditRecord`, SQL table, isolated turn/tool/delivery writers, tenant retention/export worker | Dispatcher/Filter/delivery audit-failure plus prune/export/preserve-on-failure tests |
| No IM/model/DB secret in log/trace/error | Implemented | canonical tenant-scoped Vault/file refs, tenant-env allow-list, coalesced secret cache, renewable Vault token-file support, hardened non-root/Uvicorn logging, full pinned-tRPC response/tool/query span redaction, Collector defense | traversal/cross-env/nested-config/non-root-log/traceback/token-rotation/actual-attribute exporter tests |

## Recovery and operations requirements

| Requirement | Status | Evidence |
|---|---|---|
| Node/IM/database/model/tool failure degradation | Implemented/documented | lossless backpressure, finite delayed fault retries, non-counting idempotency waits, lease-bound committed recovery, multi-call native recovery, checkpointed delivery, exact-effective-text repair, tenant dead letters, kill switch | delay/attempt/no-false-dead, concurrent recovery checkpoint, billed-error/native recovery, delivery and repair tests |
| Gray release and tenant rollback | Implemented/design | immutable cohorts, bounded secret/tool/backend health plus tRPC model/session construction preflight, same first-bootstrap gate, derived-admin audit, rollback API | invalid LiteLLM, bootstrap callback, unsafe-backend/spoof-resistant activation tests |
| Capacity evaluation | Implemented | Little's Law calculator CLI plus QPS/token/queue formulas | deterministic capacity test |
| Minimal and production deployment | Implemented/scaffolded | Dockerfile, Compose migration/runtime PostgreSQL role split, migration-gated Kubernetes files, startup/readiness/liveness probes, resource limits/PDBs, Collector/Prometheus | automated real-loopback readiness/signed-webhook test; role/manifests policy assertions; Docker/Kubernetes runtime still requires external infrastructure |

## Quality gates

At the time of review:

```text
python -m pytest       -> 133 passed, 1 opt-in PostgreSQL test skipped (pytest 9.1.1)
python -m pytest --cov -> 85.13% branch-aware coverage
uv run ruff check .    -> all checks passed
uv run ruff format     -> 98 files formatted
uv run mypy src        -> no issues in 48 source files (Python 3.11 target)
framework import       -> trpc-agent-py 1.1.19
AST parse              -> 83 Python files
Alembic head           -> d4e5f607a1b2
YAML parsing           -> 46 Compose/Kubernetes/Collector/Prometheus documents parse
security_audit.py      -> no known vulnerabilities in locked dev + production graph
```

The real-loopback HTTP test passed. PostgreSQL integration is present but was
skipped because `TAP_TEST_POSTGRES_URL` was not supplied, so no PostgreSQL pass is
claimed. Docker/Kubernetes and real Redis Cluster startup are also not claimed on
this workstation because Docker and `kubectl` are unavailable; run those gates in
the mentor-provided environment.

# Third Production-Readiness Review Remediation

Date: 2026-08-30

> Historical third-pass closure. The current September runtime, package, and
> repository-alignment evidence is maintained in
> `REAL_ENVIRONMENT_VERIFICATION_2026-09-07.md`,
> `LOCAL_RUNTIME_EVIDENCE_2026-09-07.md`, and
> `REVIEW_WECOM_BOT_2026-09-07.md`.

This is the current remediation record for the twelve third-pass findings. It
supersedes the readiness claims in the first two remediation snapshots. The local
code and evidence gates below are complete; credentialed infrastructure validation
is still an explicit release prerequisite, not a claimed result.

The current tree also includes the separately authenticated WeCom intelligent-bot
WebSocket adapter requested after that review. Its addition is covered by the
local socket test and does not replace the existing encrypted enterprise-WeCom
adapter.

## Finding closure

| Finding | Implemented closure | Regression evidence |
|---|---|---|
| P0 automatic PostgreSQL RLS outage / superuser bypass | Removed automatic RLS from the reservation migration and added corrective Alembic head `d4e5f607a1b2` to drop/disable the unsafe policy for already-upgraded databases. Compose now separates `tenant_agent_admin` migration ownership from a `NOSUPERUSER NOBYPASSRLS` `tenant_agent_app` runtime identity. Production startup and tenant SQL/native preflight reject privileged runtime roles. The standalone RLS SQL is labelled inactive because the runtime does not yet set transaction-local tenant context. | Migration/deployment policy assertions; runtime role hooks; fresh and upgrade-path Alembic tests. A real non-bypass PostgreSQL run remains external. |
| P1 platform/native SQL Session table collision and runtime DDL | `BackendRef.native_dsn_ref` is mandatory for SQL Session storage. Resolved platform/native URLs must differ. `db-init` provisions the normalized platform database; `native-session-init` provisions a separately versioned tRPC 1.1.19 schema. Activation performs read-only table/column/version validation before constructing the service. | Separate SQLite platform/native first-turn test; shared-DSN rejection; native schema validation and CLI provisioning tests. |
| P1 tenant resource databases had no provisioning path | `db-init --database-url-env` applies the platform migration head to any explicit tenant resource DSN without falling back to the control URL. SQL preflight validates only the tables required by that resource while still requiring prior provisioning. | Independent Summary resource database provisioning and resource-specific readiness tests. |
| P1 billed usage lost on provider error/exception/timeout | The tRPC adapter carries accumulated input/output usage on every terminal provider error, generic exception, and timeout event. Reservation reconciliation therefore records observed billable work before releasing unused capacity. | Provider-error, raised-exception, and timeout streams each preserve the prior 7/2 usage sample. |
| P1 native crash recovery undercounted multi-call turns | Recovery scans the complete matching invocation through its final event and sums usage from every LLM/tool-selection/final event instead of reading only the final response. | A recovered two-call invocation reports 15/4 rather than 5/3. |
| P1 Vault cross-hierarchy reference acceptance | Tenant references now match exactly one KV-v1 `<mount>/tenants/<tenant>/...` or KV-v2 `<mount>/data/tenants/<tenant>/...` hierarchy. A later `tenants/<tenant>` subsequence cannot re-scope a path rooted under another tenant; Vault ACLs remain the second barrier. | Cross-hierarchy alpha-under-beta validation rejection. |
| P2 committed recovery outside the session lease / wrong checkpoint | The pre-budget path probes for persisted output, then recovery acquires the normal renewable session lease, re-reads the event, and rebuilds Summary/Memory only through the committed outbound event sequence. | Recovery blocks behind a concurrent turn lease and restores turn A at sequence 2 even when turn B has advanced the Session to sequence 4. |
| P2 native migration could not resume after partial replay | A verified target event prefix may carry prefix state. Migration appends only the missing suffix and requires final state plus canonical event-hash equality after refresh. SQL migration now takes separate source/target native DSN environment options; every SQL side requires them. | Service fail-after-first-append restart test plus an end-to-end CLI test starting with one of two native events already committed; the rerun copies one event and the next rerun copies zero. |
| P2 Qdrant cosine normalization caused false mismatches | Unchanged cosine-vector migrations unit-normalize and quantize both source and Qdrant-returned embeddings before exact content hashing. Re-embedding still uses immutable identity plus golden-query recall. | Qdrant-like server normalization passes; content/direction/dimension differences remain part of verification. |
| P2 Kubernetes AtomicWriter secrets were rejected | File resolution permits only the exact tenant-scoped `<tenant> -> ..data/<tenant>` and `..data -> ..<timestamp>` projection. Both visible and resolved targets must remain beneath the configured mount and tenant snapshot. Arbitrary roots, junctions, traversal, and cross-tenant links still fail. | Simulated AtomicWriter projection succeeds; encoded traversal, sibling symlink, and Windows junction probes fail. |
| P2 WeCom activation accepted malformed credentials | Preflight now validates callback token, strict 43-character AES key/32-byte decode, `ww` CorpID, bounded CorpSecret, and canonical positive decimal AgentID before activation. Callback decryption still verifies both CorpID and AgentID. | Invalid credential format activation test and callback AgentID mismatch test. |
| P2 filesystem Artifact publication was tearable | The filesystem backend now writes one checksummed versioned bundle, fsyncs the temporary file, and publishes via a no-overwrite atomic link. Identical races are idempotent; conflicting races fail. Legacy pairs are read-only and a missing half is corruption. | Concurrent conflicting writers produce one valid winner/one conflict; torn legacy get/iteration fails. |

## Additional integration closures

- The real SQL-native CLI replay test exposed a pinned tRPC-Agent-Python 1.1.19
  async ORM defect: a server-generated timestamp was expired and then accessed
  outside SQLAlchemy's greenlet boundary. `ProvisionedSqlSessionService` performs
  the refresh within the async storage session and uses
  `expire_on_commit=False`. Both runtime and migration use this wrapper.
- `StorageRouter` now retains initialized adapter objects rather than bare numeric
  `id()` values, eliminating garbage-collection ID reuse as a false-ready state.
- The dependency audit exports a fully hashed locked graph and disables pip's
  redundant dependency resolver, removing an observed restricted-network hang
  without weakening the vulnerability query.
- Documentation now treats RLS as disabled, explains separate SQL provisioning,
  and records the exact external-validation boundary.

## Reproduced local gates

```text
pytest                 130 passed, 1 skipped (external PostgreSQL URL absent)
branch coverage        85.06% (configured minimum: 85%)
ruff lint              passed
ruff format --check    90 files already formatted
mypy strict            47 source files passed (Python 3.11 target)
AST parse              78 Python files passed
Alembic head           d4e5f607a1b2
configuration          demo and production examples validated
YAML policy/parsing     46 Compose/Kubernetes/Collector/Prometheus documents
locked dependency audit no known vulnerabilities
```

## Release boundary

This tree now closes the twelve reported defects at code/test/document level. It
must still not be described as fully production-certified until the mentor or CI
environment runs all of the following with disposable, least-privilege resources:

- PostgreSQL migration-owner versus runtime-role tests, including pooled
  connections and the decision either to keep RLS disabled or add a real
  transaction-local tenant-context layer;
- Redis Cluster failover, delayed/Pending Entries List recovery, and hot-session
  load;
- real Vault workload identity/rotation, Kubernetes Secret projection, S3
  conditional publication, and Qdrant snapshot/migration checks;
- signed Telegram and WeCom callbacks plus delivery credentials;
- Docker Compose and Kubernetes rollout/rollback, OpenTelemetry export, chaos,
  load, backup, and restore drills.

No commit, push, deployment, webhook registration, pull request, or external
submission was performed as part of this remediation.

## Primary implementation references

- PostgreSQL row-security behavior, including superuser/`BYPASSRLS` bypass and
  table-owner behavior: <https://www.postgresql.org/docs/17/ddl-rowsecurity.html>
- Docker Official Image bootstrap-user behavior:
  <https://hub.docker.com/_/postgres>
- Kubernetes AtomicWriter projection layout:
  <https://github.com/kubernetes/kubernetes/blob/master/pkg/volume/util/atomic_writer.go>

# Validation Results: 003 Shared-State Multi-Node Flow

This file is append-only implementation evidence. Secret values, complete DSNs,
raw message bodies and vendor exception details must never be copied here.

## Baseline

Date: 2026-09-07

- T001: uv 0.11.21 resolved 100 packages; asyncpg 0.31.0, redis 8.1.0
  and SQLAlchemy 2.0.52 are direct locked dependencies.
- T002: Docker 29.6.2 accepted deploy/local-shared/compose.yaml with
  docker compose config --quiet. Validation used process-local placeholder
  passwords; no secret value was written to this file.
- T003-T005: package imports succeeded and the shared integration fixture
  module collected without import or configuration errors.
- T006 command: uv run pytest -q
- T006 result: 101 passed in 5.22s.
- Environment note: an older local test server on port 8000 was stopped because
  it held the generated CLI executable open during uv sync.

## Foundational

Date: 2026-09-07

- RED: the first shared settings/state/async-port run failed during collection
  because RuntimeProfile, ExecutionPhase and ConfigurationUnavailable did not
  exist. This confirmed that the tests exercised new behavior.
- GREEN after the async port migration: 113 passed. The existing HTTP,
  Gateway, official Runner and unified reply behavior remained compatible.
- Infrastructure RED: shared schema/codec/loader tests initially failed because
  the PostgreSQL model module did not exist.
- Focused foundation result: 16 passed in 0.62s.
- Full local regression result: 117 passed in 4.61s.
- Real backend checkpoint: Docker Engine 29.6.2 started the fixed Redis 7.4.11
  and PostgreSQL 17.11 images; both health checks passed. The schema initializer
  succeeded twice against the same database, proving idempotent initialization,
  and the shared composition root reported readiness=true.
- Security note: backend credentials were generated only in the validation
  process and are not recorded in source, logs, or this evidence document.

## US1

Date: 2026-09-07

- RED command: `uv run pytest -q tests/contract/test_shared_configuration_repository.py tests/contract/test_shared_session_repository.py tests/integration/shared/test_cross_node_session.py tests/e2e/test_two_worker_processes.py`.
- RED result: four collection errors identified the intentionally missing
  PostgreSQL configuration repository, Redis SDK Session adapter and shared app
  factory.
- GREEN result against isolated Redis 7.4.11/PostgreSQL 17.11 containers:
  4 passed in 12.41s.
- Acceptance evidence: two independent CLI Worker processes used different
  node IDs and ports; node A stored the first turn, node B recalled it, node A
  exited, and node B recalled the same context again. The integration path also
  alternated 20 turns for two tenants with identical external user/conversation
  IDs and observed no cross-tenant context leak.
- SDK boundary: the shared implementation subclasses the official
  BaseSessionService and is consumed by the unchanged official Runner path.

## US2

Date: 2026-09-07

- RED result: two collection errors identified the missing shared Redis
  idempotency repository; the HTTP test was already collectible.
- First GREEN attempt: repository/concurrency tests passed; HTTP test correctly
  stopped at the existing loopback-only sender guard because the test used a
  virtual non-loopback hostname. The fixture was corrected to 127.0.0.1.
- Final GREEN result against isolated shared backends: 3 passed in 4.24s.
- Concurrency evidence: 100 sequential duplicate claims remained processing,
  each of 50 two-node claim races produced exactly one owner and one processing
  observer, and an identical external ID in another tenant acquired independently.
- HTTP evidence: the two-node race yielded one execution and one
  processing/duplicate response; the cached response suppressed delivery and a
  changed content fingerprint returned HTTP 409 without Redis, PostgreSQL or DSN
  details.

## US3

Date: 2026-09-07

- RED result: four collection errors identified the missing Redis lease module;
  later audit/metrics RED tests identified the missing persistent audit and
  shared metric implementations.
- The first concurrency run passed lease, fencing and takeover but one of 50
  short holders starved at the 2-second wait boundary. Reducing the bounded
  polling interval from 20ms to 2ms retained Redis PTTL authority and removed
  the avoidable client-side unfairness.
- Core GREEN result: 6 passed in 3.92s against Redis/PostgreSQL.
- Evidence: current unexpired token+generation renewed; expired generation did
  not revive; takeover incremented generation; 50 same-session contenders had
  peak concurrency 1; two different sessions overlapped; stale Session and
  business Audit writes were rejected while an immutable platform diagnostic
  could be queried.
- Local-profile regression after correcting one composition-root wiring error:
  118 passed, 12 shared-backend tests skipped in 4.38s.

## US4

Date: 2026-09-07

- RED result: four collection errors exposed missing fail-closed cache,
  terminal-only reconciler and stable HTTP error mapping. A separate RED test
  then proved the SQL atomic finalization boundary was absent.
- Non-Docker GREEN result: 18 passed in 3.84s, including configuration-cache
  authority, safe backend error conversion, terminal-only reconciliation and
  the complete local message-flow regression.
- Takeover result: 20/20 owners interrupted before EXECUTION_STARTED were
  reclaimed only after their Redis owner lease expired; every takeover advanced
  generation from 1 to 2.
- Cross-stage real-backend result: all 13 shared_backend tests for US1-US4 passed
  in 15.41s (130 non-shared tests deselected).
- Partial-commit semantics: Gateway now commits final Audit and a
  TERMINAL_PENDING RecoveryMarker in one PostgreSQL transaction, performs Redis
  terminal CAS second, and marks the recovery record reconciled only after CAS.
  The RecoveryReconciler constructor has no Agent/Runner dependency and only
  copies an already persisted result to a terminal repository.
- Failure semantics: configuration_unavailable, backend_unavailable,
  processing, conflict, outcome_unknown and audit_incomplete map to fixed safe
  status/code/message tuples without vendor connection details.

## US5

Date: 2026-09-07

- RED result: collection failed because the shared vendor-neutral idempotency
  contract helper did not exist; the audit trace-field test also defined the
  required first_claim_trace_id, owner_trace_id, execution_trace_id and
  generation fields.
- Local GREEN result: 3 passed and the one Redis contract was correctly skipped
  without a shared DSN.
- Real-backend GREEN result: 5 passed in 0.68s.
- Evidence: a PostgreSQL audit written by node A was read through a node B
  repository by tenant+trace and tenant+session, while the other tenant saw no
  row. InMemory and Redis repositories passed the same contract helper with no
  key/table assertions.
- Security evidence: SecretStr representations hide both DSNs; mapped errors
  discard supplied vendor details; shared metric dimensions hash tenant and
  session identities and keep a bounded label set.

## Full Regression

Date: 2026-09-07

- Full command: start isolated Redis 7.4.11/PostgreSQL 17.11 containers with
  process-local credentials, run `trpc-agent-shared-init`, then
  `uv run pytest -q`.
- Full result: 149 passed in 21.33s.
- Stress command: run
  `test_two_hundred_random_routes_preserve_context_and_report_p95` against an
  isolated Redis service.
- Stress result: p95 45.57ms, errors=0, executions=200; 1 passed in 8.96s.
- SDK boundary command: `uv run pytest -q tests/sdk_validation`.
- SDK boundary result: 18 passed in 3.91s.
- `git diff --check` found no whitespace errors; Git emitted only informational
  LF-to-CRLF working-copy warnings on Windows.
- Secret/DSN scan found only the documented PowerShell DSN templates that refer
  to environment variables; no literal runtime password, complete credential or
  raw test message was found in implementation/evidence. Production-readiness
  matches were scope-denial statements, not readiness claims.
- Traceability review covers US1-US5, FR-001-FR-030 (inclusive ranges included),
  D-003-001-D-003-005 and SC-001-SC-012. It also found that several fault and
  trace tests were narrower than their task wording; those tasks were reopened
  rather than treating green tests as sufficient evidence.
- Scope remains local verification only; no production HA/exactly-once claim.
- Interim checklist audit after evidence-quality review: tasks_done=60,
  tasks_pending=20. No `.specify/extensions.yml` exists, so no extension
  post-hook is required. T080 remains open until the reopened items are closed.

## US4 Evidence Closure

Date: 2026-09-07

- The reopened kill-point matrix now distinguishes durable PRE_START from
  durable EXECUTION_STARTED. Twenty pre-start lease expirations advanced the
  generation and were safely taken over; twenty post-start expirations returned
  outcome_unknown, kept generation 1 and never replayed the Agent.
- A separate 20-cycle process-reconstruction test alternated both interruption
  stages and proved a newly constructed Worker repository consults Redis
  evidence rather than process memory.
- The outage matrix covers Redis claim/lock/session and PostgreSQL
  configuration/audit stages. Every failure stays fail-closed, has a stable
  domain error and has no InMemory fallback.
- Configuration tests use authoritative PostgreSQL reads for unknown bindings,
  binding version changes and disabled tenants; a prior positive cache entry
  cannot authorize when SQL is unavailable.
- The real partial-commit test commits final Audit+RecoveryMarker to PostgreSQL,
  forces Redis terminal CAS rejection, and then reconciles the persisted result
  into Redis. AgentSpy remains at zero calls and the marker becomes reconciled.
- FaultController self-test proves the named injection point was reached and
  that one-shot faults are deterministic. The recovery lifecycle test proves a
  transient audit outage does not terminate the background recovery loop.

## US5 Evidence Closure

Date: 2026-09-07

- PostgreSQL audit contracts now cover immutable append plus tenant, agent,
  session and trace queries. Business and diagnostic records retain node_id,
  three trace roles, message generation and stale/current generation linkage.
- RecoveryMarker lookup is tenant+execution-trace scoped and returns the exact
  immutable ExecutionResult previously committed by the finalization
  transaction.
- A real two-node traceability scenario covers success, duplicate, conflict,
  pre-start takeover, stale write rejection and outage diagnostics. Evidence is
  readable from another node while remaining tenant scoped.
- Shared metrics are wired into the Gateway and Redis session-lease path. They
  carry node/backend/outcome, anonymous tenant/session, lease wait, trace roles
  and generation without message text, DSNs, secret values or backend details.

## Final Re-Run Before Convergence

Date: 2026-09-07

- Isolated backend versions: Redis 7.4.11 and PostgreSQL 17.11. Both containers
  used random run-unique names, process-local credentials and automatic cleanup.
- Schema initialization completed successfully with the updated audit agent
  dimension and RecoveryMarker idempotency key locator.
- Focused recovery/trace/audit rerun: 6 passed in 0.99s.
- Full command: initialize isolated shared backends, then `uv run pytest -q`.
- Full result: 163 passed in 30.17s; no skip, failure or warning was reported.
- The run includes SDK validation, two real Worker processes, shared adapter
  contracts, concurrency, restart/takeover, partial recovery, traceability,
  security and 200-route stress coverage.
- This section supersedes the earlier interim tasks_done/tasks_pending count;
  T051-T072 reopened items are now closed. T080 remains the sole convergence
  gate at the time of this append.

## Final Traceability and Convergence Report

Date: 2026-09-07

T080 first found that three tests were semantically correct but below the exact
quantities required by SC-002, SC-003 and SC-005. The tests were strengthened
before closure: 50 claim races now each prove one Agent-authorized branch, one
event result and one deliverable result; 20 distinct-session groups prove
parallel execution; and 20 committed Session+idempotency+Audit samples are read
after all process-local Worker objects are destroyed and reconstructed.

### Functional Requirements

| Requirement | Result | Primary implementation/test evidence |
|---|---|---|
| FR-001 | PASS | unchanged HTTP/HMAC/reply contract; duplicate HTTP and full 002 regression |
| FR-002 | PASS | two independent CLI processes in `test_two_worker_processes.py` |
| FR-003 | PASS | alternating/random node routing without sticky session |
| FR-004 | PASS | reconstructed Worker objects retain all business state through shared adapters |
| FR-005 | PASS | tenant/agent-scoped Redis codec, SQL queries and anonymous metrics |
| FR-006 | PASS | 20-turn cross-node Session continuation and cross-tenant isolation |
| FR-007 | PASS | 20 Session+idempotency+Audit samples survive all Worker reconstruction |
| FR-008 | PASS | tenant+Binding+message key and content-fingerprint conflict contract |
| FR-009 | PASS | 50 two-node atomic claim races, exactly one Agent-authorized branch each |
| FR-010 | PASS | first/owner/execution trace, generation and execution phase assertions |
| FR-011 | PASS | processing/completed/conflict plus delivery suppress HTTP assertions |
| FR-012 | PASS | 50 same-session contenders serialize; 20 distinct-session groups overlap |
| FR-013 | PASS | stale Session, terminal and business Audit writes rejected; diagnostic appended |
| FR-014 | PASS | Redis PTTL/token/generation acquire-renew-expire-takeover contract |
| FR-015 | PASS | durable EXECUTION_STARTED before Agent and pre-start-only takeover tests |
| FR-016 | PASS | 20 post-start expirations return outcome_unknown with zero replay |
| FR-017 | PASS | claim/lock/session outage matrix has no InMemory fallback |
| FR-018 | PASS | SQL unavailable/unknown version/positive-cache tests fail closed |
| FR-019 | PASS | PostgreSQL Tenant/Agent/Binding ownership, status, version and secret_ref contract |
| FR-020 | PASS | immutable business/diagnostic Audit and tenant/agent/session/trace queries |
| FR-021 | PASS | Gateway final Audit+RecoveryMarker transaction precedes Redis terminal CAS |
| FR-022 | PASS | terminal_pending to reconciled/conflict_review; recovery never invokes Agent |
| FR-023 | PASS | ordered unique Redis Session events with fenced append contract |
| FR-024 | PASS | HTTP-to-state-to-Audit trace roles and node/generation correlation |
| FR-025 | PASS | shared implementations conform to existing async Repository/Adapter ports |
| FR-026 | PASS | InMemory/shared vendor-neutral substitutability suite passes |
| FR-027 | PASS | Gateway trace metrics and lease wait/node/backend anonymous dimensions |
| FR-028 | PASS | runtime SecretStr/secret_ref only; credential representations redacted |
| FR-029 | PASS | stable redacted backend/config/processing/conflict/outcome/audit errors |
| FR-030 | PASS | deterministic offline model; no real IM/model/network dependency |

### Success Criteria

| Criterion | Result | Measured evidence |
|---|---|---|
| SC-001 | PASS | 20 alternating turns, 100% correct recall, zero isolation errors |
| SC-002 | PASS | 100 terminal duplicates and 50 races; one authorized execution/result/delivery per race |
| SC-003 | PASS | 50 same-session groups peak at 1; 20 distinct-session groups overlap; stale successes 0 |
| SC-004 | PASS | 20 pre-start safe takeovers and 20 post-start no-replay outcomes |
| SC-005 | PASS | 20/20 Session, idempotency and Audit samples recovered after full Worker reconstruction |
| SC-006 | PASS | claim/lock/session/config/audit/terminal partial-failure matrix returns safe results |
| SC-007 | PASS | all success/duplicate/conflict/takeover/stale/outage/unknown samples query by tenant/session/trace |
| SC-008 | PASS | InMemory/shared common contracts and complete compatibility regression pass |
| SC-009 | PASS | 200 random routes, 0 ownership/context errors; recorded p95 45.57 ms |
| SC-010 | PASS | quickstart contains deterministic dual-node, failure, audit and cleanup walkthrough |
| SC-011 | PASS | zero runtime credential/secret/sensitive-body leaks; four URL candidates are templates/test-only values |
| SC-012 | PASS | all tests offline; seven production-readiness matches are explicit scope-denial statements |

### Clarification Decisions

| Decision | Result | Evidence |
|---|---|---|
| D-003-001 | PASS | Redis-authoritative expiry and monotonic generation lease tests |
| D-003-002 | PASS | all stale business writes rejected and separate diagnostic Audit queryable |
| D-003-003 | PASS | only durable pre-start evidence permits takeover; post-start is outcome_unknown |
| D-003-004 | PASS | real SQL-first partial commit reconciles existing result with AgentSpy=0 |
| D-003-005 | PASS | PostgreSQL remains authorization authority; positive cache cannot grant access |

### Final Gates

- Quantitative focused rerun after gap repair: 6 passed in 6.53s.
- Final isolated Redis/PostgreSQL full result: 164 passed in 30.57s.
- SDK boundary rerun: 18 passed in 3.73s.
- Requirements checklist: 16/16 checked.
- `git diff --check`: no whitespace errors; Windows emitted informational
  LF-to-CRLF conversion notices only.
- Every FR-001-FR-030, SC-001-SC-012 and D-003-001-D-003-005 has an
  implementation boundary plus executable evidence above. No accepted
  requirement, decision or task-evidence gap remains.
- Final task state after T080 closure: 80/80 complete, 0 pending.
- `.specify/extensions.yml` is absent; no mandatory post-implementation hook is
  registered.

## Local Upgrade Compatibility Addendum

Date: 2026-09-07

- A final local-run review found that adding the Audit `agent_id` column by
  rewriting migration 001 would not upgrade an already initialized demo volume.
- Migration 001 was restored to its original boundary and a versioned,
  idempotent `002_audit_agent_scope.sql` migration was added.
- A real PostgreSQL test removes the v2 marker and column to represent the v1
  state, runs shared initialization, then verifies that data reset is not
  required, the column exists and schema version 2 is ready.
- Final isolated Redis/PostgreSQL full result after this compatibility repair:
  165 passed in 31.67s, with zero skips and zero failures.

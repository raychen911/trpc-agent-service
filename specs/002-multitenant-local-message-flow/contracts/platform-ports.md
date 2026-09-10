# Contract: Platform Ports

**Feature**: `002-multitenant-local-message-flow`
**Purpose**: Keep Gateway business semantics independent of InMemory and tRPC-Agent internals

## General Rules

- Port values are immutable domain objects.
- Every tenant-owned operation after context resolution receives tenant scope explicitly.
- Tenant context resolution requires a VerifiedBindingScope emitted only by successful HMAC verification.
- Implementations may be async; errors use typed platform exceptions, not vendor exceptions.
- InMemory implementations and future shared backends must pass the same contract tests.
- Secret values never appear in return objects.

Scope values are capabilities, not caller-provided tenant strings:

```text
TenantScope(tenant_id)  # created only from VerifiedTenantContext
VerifiedBindingScope(binding_id, channel)  # created only by successful HMAC verification
PreAuthScope            # internal capability; cannot access tenant-owned data
```

## BindingAuthRegistry

```text
get_auth_material(binding_id) -> BindingAuthMaterial | NotFound
```

`BindingAuthMaterial` contains `binding_id`, `secret_ref`, signature version and status only.
It must not expose Tenant or Agent configuration before authentication.

## SecretResolver

```text
resolve(secret_ref) -> SecretBytes | SecretUnavailable
```

The returned secret is short-lived, non-serializable and must not be included in equality,
representation, logs or errors.

## TenantDirectory

```text
resolve_active_context(verified_binding_scope, external_user_id, trace_id)
  -> VerifiedTenantContext
  | AccessDenied
```

The implementation verifies active Binding → Tenant → Agent ownership. It rejects a scope whose
binding/channel does not match and never accepts a tenant_id supplied by the message as authorization.

## IdempotencyRepository

```text
claim(key, fingerprint, trace_id)
  -> Acquired(owner_token, attempt)
  | Processing(original_trace_id)
  | Completed(result)
  | Conflict

mark_running(key, owner_token, execution_trace_id) -> Running | ConditionalWriteFailed
mark_pre_start_failed(key, owner_token, safe_error) -> FailedPreStart
complete(key, owner_token, execution_result) -> Completed | ConditionalWriteFailed
mark_post_start_failed(key, owner_token, execution_result) -> Completed
mark_outcome_unknown(key, owner_token, execution_result) -> Completed
get(key) -> IdempotencyRecord | NotFound
reset() -> None
```

Required semantics:

- `claim` is atomic for one key.
- Same key with different fingerprint always returns Conflict.
- Only FAILED_PRE_START with the same fingerprint can transition through claim to PENDING again.
- Owner token and expected state guard every mutation.
- RUNNING, SUCCEEDED, FAILED_POST_START and OUTCOME_UNKNOWN never automatically re-enter PENDING.
- A record keeps first_claim_trace_id, current owner_trace_id and conditional execution_trace_id.
  Safe reclaim changes owner_trace_id but never first_claim_trace_id; mark_running copies the
  current delivery trace to execution_trace_id.

## SessionLockManager

```text
acquire(platform_session_id) -> AsyncLease
```

- At most one live lease exists per platform_session_id.
- Different IDs do not share a global lock.
- Lease release is idempotent and guaranteed by async context management.
- Cancellation or Worker failure releases the lease.
- The key is already tenant-scoped; raw external conversation IDs are forbidden.

## SessionBackendFactory

```text
get_backend(tenant_id, agent_id) -> SDKSessionService
close() -> None
```

The Worker receives a supported official SDK Session Service. Gateway never accesses the SDK
session object directly. This phase may return shared in-process SDK services; future adapters
must preserve tenant/session isolation and lifecycle behavior.

## AgentExecutor

```text
prepare(context, session_identity, text)
  -> PreparedAgentRun
  | AgentPreparationFailed(safe_code, execution_started=false)

PreparedAgentRun.execute(timeout_seconds=30)
  -> AgentExecution(final_text, event_count, final_response_count)
  | AgentExecutionFailed(safe_code, execution_started=true)
  | AgentOutcomeUnknown(safe_code, execution_started=true)

close() -> None
```

Required behavior:

- Uses the fixed official tRPC-Agent public interfaces.
- `prepare` creates the supported Runner/Session/new-message inputs but does not request an Event.
- Gateway marks idempotency RUNNING after prepare succeeds. The execution-start boundary is
  immediately before the first request for a Runner Event.
- Timeout or cancellation after RUNNING maps to AgentOutcomeUnknown; before RUNNING it maps to
  AgentPreparationFailed. Async lease release remains mandatory.
- A successful result has exactly one selected non-empty final response.
- Intermediate Event text cannot be returned as final text.
- Vendor exceptions are mapped to stable safe errors.
- No platform Repository or HMAC responsibility is implemented inside the Worker.

## AuditRepository

```text
append(scope, record) -> AuditRecord | AuditUnavailable
update_final(scope, audit_id, trace_id, decision, fields) -> AuditRecord | AuditUnavailable
list_by_trace(scope, trace_id) -> list[AuditRecord]
list_by_session(tenant_scope, platform_session_id) -> list[AuditRecord]
list_by_tenant(tenant_scope) -> list[AuditRecord]
reset() -> None
```

Required behavior:

- Tenant-owned append/update/query accepts only matching TenantScope; scope/record mismatch fails
  before storage access.
- Pre-auth rejection uses PreAuthScope with null tenant_id. PreAuthScope cannot list or mutate
  tenant-owned records.
- Query by trace/session/tenant never returns records outside the supplied scope.
- Raw text, signature and secret fields are rejected or removed before storage.
- Append/update failures are observable and never silently converted to success.

## MetricsRecorder

```text
record(scope, trace_id, stage, outcome, duration_ms, values) -> None | MetricsUnavailable
snapshot(scope) -> MetricSnapshot
reset() -> None
```

- Tenant activity requires matching TenantScope; unauthenticated rejection uses PreAuthScope.
- Counters and latency aggregates are atomic within one process.
- Snapshot includes request/error counts, stage/Agent/state-backend latency, local delivery count,
  token count and tenant cost.
- Model/tool/real-IM values are `0` or `not_applicable` in this phase.
- Raw text, external identity, signature and secret values are invalid metric labels or values.
- Recorder failure is surfaced as a safe structured `metrics_incomplete` operational event. It never
  mutates an already committed idempotency terminal state or changes an authorization response.

## GatewayService

```text
handle_verified_message(authenticated_request) -> OutboundReply
```

Orchestration order:

1. Resolve verified TenantContext.
2. Derive SessionIdentity.
3. Compute idempotency key and content fingerprint.
4. Atomically claim.
5. Return duplicate/processing/conflict without Worker execution where applicable.
6. Acquire per-session lease.
7. Write audit start with TenantScope; on failure mark pre-start failure.
8. Prepare AgentExecutor; preparation failure remains pre-start and retryable.
9. Mark RUNNING with execution_trace_id, then execute with a 30-second timeout.
10. While still RUNNING, write final audit. Failure maps to terminal
    FAILED_POST_START/audit_incomplete.
11. After final audit succeeds, conditionally persist the business terminal state. Persistence
    uncertainty maps to OUTCOME_UNKNOWN. No audit update occurs after terminal commit.
12. Record scoped metrics, release lease and return uniform reply. A recorder failure emits a safe
    `metrics_incomplete` operational event but does not rewrite the committed business result.

Gateway must expose dependencies through construction; no module-level mutable repository is
permitted outside the application composition root.

## Typed Error Categories

`InvalidRequest`, `Unauthorized`, `AccessDenied`, `IdempotencyConflict`,
`Processing`, `AgentPreparationFailed`, `AgentExecutionFailed`, `OutcomeUnknown`, `AuditUnavailable`,
`AuditIncomplete`, `ConditionalWriteFailed`.

Each category maps once at the HTTP boundary. Internal exception strings are not public messages.

## Contract Test Matrix

| Port | Required Tests |
|---|---|
| BindingAuthRegistry | found, unknown, disabled, no tenant data before auth |
| SecretResolver | present, missing, empty, representation does not leak value |
| TenantDirectory | active, disabled tenant, disabled agent, ownership mismatch, cross-tenant denial |
| IdempotencyRepository | atomic claim, processing, completed, conflict, pre-start reclaim, terminal no-retry |
| SessionLockManager | same key serial, different key parallel, cancellation release |
| SessionBackendFactory | distinct tenant/agent scopes, close lifecycle |
| AgentExecutor | prepare/start boundary, timeout/cancellation, final Event selection, no final Event, SDK exception mapping, offline guard |
| AuditRepository | scoped append/update/query by tenant/session/trace, pre-auth separation, redaction, injected failures |
| MetricsRecorder | scoped counters/timings, tenant isolation, not-applicable fields, redaction, injected failures |

Future Redis/SQL adapters must run these tests unchanged except for backend setup fixtures.

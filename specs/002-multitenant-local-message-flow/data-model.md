# Data Model: 多租户本地消息闭环

**Feature**: `002-multitenant-local-message-flow`
**Date**: 2026-09-05
**Storage Boundary**: 本阶段仅 InMemory；字段、键和状态语义必须可迁移到共享后端

## Modeling Rules

1. HMAC 成功后由认证边界产生不可由请求构造的 `VerifiedBindingScope(binding_id,
   channel)`，TenantDirectory 仅凭该能力解析 TenantContext。上下文建立后的租户业务
   Repository/Metrics 操作必须显式携带 `TenantScope(tenant_id)`；预认证拒绝只能使用
   不具备租户数据访问能力的 `PreAuthScope`。
2. 外部输入标识不能单独成为平台主键；必须与租户及绑定作用域组合。
3. Secret 只以 `secret_ref` 存在于模型中，秘密值不允许序列化。
4. 时间统一使用带 UTC 时区的时间点；HTTP 签名时间使用 Unix 秒。
5. 平台生成的 `trace_id` 使用标准 UUID 字符串。
6. 审计使用消息摘要和伪名身份，不保存完整消息正文。

## Entity Relationship

```text
Tenant 1 ── * AgentApplication
Tenant 1 ── * ChannelBinding * ── 1 AgentApplication
ChannelBinding 1 ── * InboundMessage

VerifiedTenantContext
  ├── 1 Tenant
  ├── 1 AgentApplication
  └── 1 ChannelBinding

Tenant + ChannelBinding + External User + External Conversation
  └── 1 SessionIdentity ── * InboundMessage

Tenant + ChannelBinding + External Message
  └── 1 IdempotencyRecord ── 0..1 ExecutionResult

InboundMessage 1 ── 1..* AuditRecord
ExecutionResult 1 ── 0..1 OutboundReply
TenantScope 1 ── 1 MetricSnapshot
```

## Tenant

| Field | Type | Required | Rules |
|---|---|---:|---|
| tenant_id | string | yes | 1–64 characters; `[a-z0-9][a-z0-9_-]*`; globally unique |
| display_name | string | yes | 1–120 Unicode characters; display only |
| status | enum | yes | `active` or `disabled` |
| created_at | datetime | yes | UTC |
| config_version | integer | yes | Positive and monotonic within tenant |

**Invariants**:

- Disabled tenants cannot create TenantContext or start Agent execution.
- Repository lookup always receives tenant_id explicitly; no unscoped list/get operation is
  available to Gateway.

## AgentApplication

| Field | Type | Required | Rules |
|---|---|---:|---|
| tenant_id | string | yes | Owner; part of compound identity |
| agent_id | string | yes | 1–64 safe characters; unique within tenant |
| agent_name | string | yes | 1–120 Unicode characters |
| status | enum | yes | `active` or `disabled` |
| model_profile | string | yes | This phase uses `deterministic-offline` only |
| instruction | string | yes | Non-secret Agent instruction |
| config_version | integer | yes | Positive |

**Identity**: `(tenant_id, agent_id)`

**Invariants**:

- ChannelBinding can only reference an AgentApplication with the same tenant_id.
- This phase rejects any model profile other than the deterministic offline profile.

## ChannelBinding

| Field | Type | Required | Rules |
|---|---|---:|---|
| binding_id | string | yes | 1–96 safe characters; globally unique opaque public identifier |
| tenant_id | string | yes | Owner |
| agent_id | string | yes | Target Agent within owner tenant |
| channel | enum | yes | `local_http` in this phase |
| status | enum | yes | `active` or `disabled` |
| secret_ref | string | yes | Environment variable name; never secret value |
| signature_version | string | yes | `v1` |
| created_at | datetime | yes | UTC |

**Invariants**:

- `secret_ref` matches `[A-Z][A-Z0-9_]{0,127}`.
- Missing referenced secret is treated as unauthorized and never included in an error.
- Binding lookup before authentication returns only authentication metadata; Tenant and Agent
  business configuration is accessed only after signature verification.

## InboundMessage

| Field | Type | Required | Rules |
|---|---|---:|---|
| channel | enum | yes | `local_http` |
| binding_id | string | yes | Must match authenticated binding |
| external_message_id | string | yes | 1–128 characters; opaque |
| external_user_id | string | yes | 1–128 characters; opaque |
| conversation_type | enum | yes | `direct` or `group` |
| external_conversation_id | string | yes | 1–128 characters; opaque |
| text | string | yes | Trimmed length 1–4000 Unicode characters |
| received_at | datetime | yes | Server-created UTC time |
| trace_id | UUID string | yes | Current delivery trace |

**Canonical Content Fingerprint**:

```text
SHA-256(length-prefixed UTF-8 encoding of:
  channel,
  external_user_id,
  conversation_type,
  external_conversation_id,
  text)
```

The fingerprint deliberately excludes trace_id and received_at so the same business message
has the same fingerprint when redelivered.

## VerifiedTenantContext

| Field | Type | Required | Rules |
|---|---|---:|---|
| tenant_id | string | yes | Derived from verified binding; never trusted from body |
| agent_id | string | yes | Derived from verified binding |
| agent_name | string | yes | Derived from AgentApplication |
| binding_id | string | yes | Verified binding |
| channel | enum | yes | Must match binding |
| external_user_id | string | yes | From validated message |
| trace_id | UUID string | yes | Current delivery |
| config_version | integer | yes | Effective tenant/agent config version |

**Lifecycle**: Created only after HMAC succeeds and Tenant, AgentApplication and
ChannelBinding are all active. Immutable for the request.

## SessionIdentity

| Field | Type | Required | Rules |
|---|---|---:|---|
| tenant_id | string | yes | Explicit isolation scope |
| agent_id | string | yes | Agent application scope |
| binding_id | string | yes | Channel binding scope |
| platform_session_id | string | yes | `sess_` + 64 lowercase hex characters |
| sdk_app_name | string | yes | Stable tenant-and-agent-scoped value |
| sdk_user_id | string | yes | Pseudonymous tenant-scoped external user digest |
| external_user_digest | string | yes | SHA-256, never raw user ID in audit |
| conversation_type | enum | yes | `direct` or `group` |

**Derivation**:

- `platform_session_id` hashes tenant_id, agent_id, binding_id, channel,
  external_user_id, conversation_type and external_conversation_id using an
  unambiguous length-prefixed encoding.
- `sdk_app_name`, `sdk_user_id` and SDK `session_id` are derived from this
  identity and never omit tenant scope.

**Invariants**:

- Equal normalized inputs produce equal platform_session_id.
- Any change to tenant_id produces a different platform_session_id.
- Any change to agent_id produces a different platform_session_id; rebinding to another Agent
  never continues the old SDK Session.
- Raw external user and conversation identifiers are not embedded in the generated ID.

## IdempotencyRecord

**Identity**: `(tenant_id, binding_id, external_message_id)`

| Field | Type | Required | Rules |
|---|---|---:|---|
| tenant_id | string | yes | Isolation scope |
| binding_id | string | yes | Isolation and channel scope |
| external_message_id | string | yes | External delivery identity |
| content_fingerprint | string | yes | 64 lowercase hex characters |
| state | enum | yes | See state machine |
| attempt | integer | yes | Starts at 1; increments only after FAILED_PRE_START reclaim |
| owner_token | string | conditional | Random opaque token while PENDING/RUNNING |
| first_claim_trace_id | UUID string | yes | First delivery that ever created the record; evidence only |
| owner_trace_id | UUID string | conditional | Current PENDING/RUNNING attempt; replaced on safe reclaim |
| execution_trace_id | UUID string | conditional | Set when RUNNING begins; source trace for terminal cached results |
| result | ExecutionResult | conditional | Required for terminal states |
| created_at | datetime | yes | UTC |
| updated_at | datetime | yes | UTC |

### State Machine

```text
ABSENT
  └── atomic claim ──> PENDING

PENDING
  ├── audit pre-write + Worker prepare succeed ──> RUNNING
  └── audit/prepare pre-start failure ──> FAILED_PRE_START

FAILED_PRE_START
  └── same fingerprint atomic reclaim ──> PENDING (attempt + 1)

RUNNING
  ├── final audit succeeds + terminal CAS ──> SUCCEEDED or FAILED_POST_START
  ├── final audit failure ──> FAILED_POST_START / audit_incomplete
  └── timeout/cancellation/persistence outcome uncertain ──> OUTCOME_UNKNOWN

SUCCEEDED / FAILED_POST_START / OUTCOME_UNKNOWN
  └── terminal; no automatic transition
```

**Atomic Claim Results**:

- `acquired`: caller owns PENDING with owner_token.
- `processing`: existing PENDING or RUNNING; no execution; response source trace is the current
  owner_trace_id.
- `completed`: existing terminal state with same fingerprint; return stored result.
- `conflict`: same key but different fingerprint; never overwrite.

All conditional transitions require the current owner_token and expected current state.
Reclaim preserves first_claim_trace_id, replaces owner_trace_id and owner_token, and increments
attempt. mark_running copies the current owner_trace_id to execution_trace_id. A terminal state is
never changed merely because a later audit call failed; therefore final audit occurs while RUNNING.

## ExecutionResult

| Field | Type | Required | Rules |
|---|---|---:|---|
| status | enum | yes | `succeeded`, `failed_post_start`, `outcome_unknown` |
| response_text | string | conditional | 1–4000 chars only for succeeded |
| error_code | string | conditional | Stable safe code for non-success |
| error_message | string | conditional | Sanitized, no stack or secret |
| original_trace_id | UUID string | yes | Trace that executed Agent |
| platform_session_id | string | yes | Tenant-scoped session |
| started_at | datetime | yes | UTC |
| finished_at | datetime | yes | UTC |
| agent_event_count | integer | yes | Non-negative |
| final_response_count | integer | yes | Exactly 1 for succeeded |
| delivery_action | enum | yes | `deliver` for first success, `none` for first failure; every cached terminal response maps to `suppress` |

## OutboundReply

| Field | Type | Required | Rules |
|---|---|---:|---|
| status | enum | yes | Contract result category |
| trace_id | UUID string | yes | Current delivery trace |
| original_trace_id | UUID string | conditional | owner_trace_id for processing; execution_trace_id for cached terminal result |
| tenant_id | string | conditional | Present only after verified context |
| platform_session_id | string | conditional | Present after session resolution |
| external_message_id | string | conditional | Echo only after validation |
| text | string | conditional | Present for succeeded/duplicate |
| delivery_action | enum | yes | `deliver`, `suppress`, or `none` |
| error | ErrorDetail | conditional | Stable safe error |

For a cached FAILED_POST_START or OUTCOME_UNKNOWN result, `data` and `error` are both present:
`data.delivery_action=suppress`, `error.execution_started=true`, `error.retryable=false`, and
`original_trace_id=execution_trace_id`.

## AuditRecord

| Field | Type | Required | Rules |
|---|---|---:|---|
| audit_id | UUID string | yes | Unique per audit entry |
| trace_id | UUID string | yes | Current delivery |
| original_trace_id | UUID string | no | Links duplicate to first execution |
| tenant_id | string | no | Null until verified; never trust claimed value |
| channel | string | yes | Safe normalized channel |
| binding_id_digest | string | yes | Digest, not raw unknown binding |
| user_id | string | no | Pseudonymous `sha256:<digest>` after validation |
| session_id | string | no | Tenant-scoped platform session |
| agent_name | string | no | After validated binding |
| tool_name | string | no | Null in this phase |
| decision | enum | yes | See lifecycle |
| latency_ms | integer | yes | Non-negative |
| error_type | string | no | Stable safe code |
| cost | decimal | yes | `0` in offline phase |
| external_message_digest | string | no | Digest only |
| created_at | datetime | yes | UTC |

### Audit Decisions

`received`, `unauthorized`, `access_denied`, `invalid_request`,
`authorized`, `duplicate`, `processing`, `idempotency_conflict`,
`execution_started`, `succeeded`, `agent_failed`, `outcome_unknown`,
`audit_incomplete`.

**Invariants**:

- No field stores request signature, secret, raw message text, raw external user ID or stack.
- A successful execution has an authorized/execution-started/final decision path that can be
  queried by trace_id.
- Each duplicate delivery has its own trace audit and links to original_trace_id.

## Security Scope Capabilities

`VerifiedBindingScope(binding_id, channel)` is emitted only after successful HMAC verification.
It can resolve exactly one binding/channel into VerifiedTenantContext but cannot query arbitrary
tenant state or be constructed from request JSON.

`TenantScope(tenant_id)` authorizes only one tenant's append, update and query operations.
`PreAuthScope` is a separate internal capability for unauthenticated rejection records; it cannot
read or mutate tenant-owned records. Scope is supplied as an operation argument and is never
inferred from an AuditRecord supplied by the caller.

Authenticated audit queries support:

- `(TenantScope, trace_id)`
- `(TenantScope, platform_session_id)`
- `TenantScope` for the tenant-local record set

## MetricSnapshot

| Field | Type | Required | Rules |
|---|---|---:|---|
| scope | TenantScope or PreAuthScope | yes | Tenant data never appears in PreAuthScope |
| request_count | integer | yes | Non-negative |
| error_count | integer | yes | Non-negative and not greater than request_count |
| stage_latency_ms | map[string, number] | yes | Non-negative aggregates |
| agent_latency_ms | number | yes | Non-negative; zero when Agent did not run |
| state_backend_latency_ms | number | yes | Non-negative |
| channel_delivery_count | integer | yes | Local response deliveries; no production IM claim |
| token_count | integer | yes | `0` for deterministic offline Agent |
| tenant_cost | decimal | yes | `0` for deterministic offline Agent |
| model_metric_status | enum | yes | `not_applicable` in this phase |
| tool_metric_status | enum | yes | `not_applicable` in this phase |
| im_metric_status | enum | yes | `not_applicable` in this phase |

Metrics contain no raw text, signature, secret, external user ID or external conversation ID.
Recorder failure produces a redacted `metrics_incomplete` operational event. Because metrics are not
the correctness ledger, that failure does not mutate an already committed idempotency terminal state
or change a pre-auth authorization response.

## Repository Keys and Port Semantics

| Repository | Key | Required Atomic Behavior |
|---|---|---|
| TenantDirectory | VerifiedBindingScope | Resolve immutable context; reject binding/channel mismatch |
| BindingAuthRegistry | binding_id | Minimal auth metadata lookup |
| AgentDirectory | (tenant_id, agent_id) | Owner-scoped get |
| IdempotencyRepository | (tenant_id, binding_id, external_message_id) | claim, compare fingerprint, owner-token transition |
| AuditRepository | (TenantScope or PreAuthScope, trace_id, audit_id) | scoped append/update/query by tenant, session and trace |
| SessionLockManager | platform_session_id | Exclusive same-session lease; independent keys parallel |
| SessionBackendFactory | tenant_id + agent_id | Provides official SDK session service without exposing storage internals |
| MetricsRecorder | TenantScope or PreAuthScope | Atomic counters/timing aggregates and scoped snapshot |

## Deletion and Retention

This phase has process-lifetime retention only:

- All records disappear on service restart.
- No background cleanup or production retention promise is made.
- Tests must call explicit reset/close methods between cases.
- Third-stage shared adapters must define durable retention and migration without changing the
  identities and state transitions above.

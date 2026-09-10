# Data Model: 共享状态多节点消息闭环

**Feature**: 003-shared-state-multinode-flow
**Date**: 2026-09-07
**Sources**: spec.md, clarification-decisions.md, research.md

## 1. Ownership and Relationship Overview

~~~text
Tenant 1 ── * AgentApplication
Tenant 1 ── * ChannelBinding * ── 1 AgentApplication
Tenant 1 ── * SharedSession 1 ── * SessionEvent
Tenant 1 ── * MessageExecution

MessageExecution * ── 1 SharedSession
MessageExecution 1 ── 0..1 ProcessingLease
SharedSession     1 ── 0..1 SessionLease

MessageExecution 1 ── * PersistentAuditRecord
MessageExecution 1 ── 0..1 RecoveryMarker
WorkerNode        1 ── * lease ownership / audit evidence
~~~

所有 Tenant-owned identity、Redis key 和 SQL 查询都显式包含 tenant scope。请求正文
中的 tenant_id 永远不是授权来源。

## 2. Common Value Objects

### NodeIdentity

| Field | Type | Required | Rules |
|---|---|---:|---|
| node_id | string | yes | 1–64 safe characters；启动参数注入；两个 Worker 必须不同 |
| process_instance_id | UUID | yes | 每次进程启动重新生成 |
| started_at | UTC datetime | yes | 仅诊断，不参与租约裁决 |

NodeIdentity 不授予 tenant 权限，也不是业务状态。node_id 可出现在 lease、Audit 和
Metrics 中，process_instance_id 用于区分同名节点重启。

### MessageFence

| Field | Type | Required | Rules |
|---|---|---:|---|
| key | IdempotencyKey | yes | tenant/binding/external_message identity |
| generation | integer | yes | > 0，单调增长 |
| owner_token | opaque string | yes | 不记录到日志/响应 |
| owner_node_id | string | yes | 必须匹配当前 lease |
| owner_trace_id | UUID | yes | 当前 owner delivery |

### SessionFence

| Field | Type | Required | Rules |
|---|---|---:|---|
| tenant_id | string | yes | 已验证租户 |
| agent_id | string | yes | 已验证 Agent |
| platform_session_id | string | yes | sess_ + 64 lowercase hex |
| generation | integer | yes | > 0，单调增长 |
| owner_token | opaque string | yes | 不记录到日志/响应 |
| owner_node_id | string | yes | 当前持有节点 |
| message_key_digest | string | yes | 关联当前消息，不含原始 ID |

Fence 是平台内部能力对象，不允许从 HTTP 字段反序列化创建。

## 3. Redis Shared-State Entities

### MessageExecution

**Identity**: (tenant_id, binding_id, external_message_id)，Redis key 使用各部分规范化摘要。
**Retention**: 本阶段终态不自动过期；测试通过 namespace 显式清理。

| Field | Type | Required | Rules |
|---|---|---:|---|
| schema_version | integer | yes | 当前为 1；未知更高版本 fail closed |
| tenant_id_digest | string | yes | tenant-scoped key component |
| binding_id_digest | string | yes | binding-scoped key component |
| external_message_id_digest | string | yes | 不保存原始外部 ID |
| content_fingerprint | 64 hex | yes | 创建后不可变 |
| state | enum | yes | 见 Message State Machine |
| attempt | integer | yes | 从 1 开始；仅安全接管时增加 |
| owner_generation | integer | yes | 从 1 开始，安全接管时单调增加 |
| owner_token_digest | 64 hex | conditional | CLAIMED 到 FINALIZING 必须存在 |
| owner_node_id | string | conditional | 当前 owner |
| first_claim_trace_id | UUID | yes | 永不替换 |
| owner_trace_id | UUID | conditional | 新 generation 替换 |
| execution_trace_id | UUID | conditional | EXECUTION_STARTED 后不可变 |
| platform_session_id | string | yes | tenant-scoped Session |
| session_generation | integer | conditional | EXECUTION_STARTED 后必须存在 |
| lease_ttl_ms | integer | conditional | 仅诊断快照；权威有效性读取 PTTL |
| result | ExecutionResult | terminal | 终态必须存在 |
| created_at | UTC datetime | yes | 证据，不参与租约裁决 |
| updated_at | UTC datetime | yes | 证据，不参与租约裁决 |
| pre_start_error | safe code | no | 不含 vendor 文本 |

#### Message State Machine

~~~text
ABSENT
  └─ atomic claim ─> CLAIMED

CLAIMED
  ├─ preparation/audit succeeds ─> PREPARED
  ├─ explicit safe abort ─> FAILED_PRE_START
  └─ lease expires before start ─> CLAIMED(new generation)

PREPARED
  ├─ atomic MessageFence + SessionFence check ─> EXECUTION_STARTED
  ├─ explicit safe abort ─> FAILED_PRE_START
  └─ lease expires before start ─> CLAIMED(new generation)

EXECUTION_STARTED
  ├─ Agent returns and fences remain valid ─> FINALIZING
  └─ owner/lease/result uncertain ─> OUTCOME_UNKNOWN

FINALIZING
  ├─ final Audit + Recovery Marker + Redis terminal CAS ─> SUCCEEDED
  ├─ Agent safe failure result committed ─> FAILED_POST_START
  └─ any commit uncertainty ─> OUTCOME_UNKNOWN

FAILED_PRE_START
  └─ same fingerprint claim ─> CLAIMED(new generation)

SUCCEEDED / FAILED_POST_START / OUTCOME_UNKNOWN
  └─ immutable; no automatic execution transition
~~~

**Invariants**:

- 同一 identity 和 fingerprint 同时最多一个有效 MessageFence。
- 相同 identity、不同 fingerprint 始终 Conflict。
- EXECUTION_STARTED 由当前 MessageFence 与 SessionFence 一次原子检查后写入。
- EXECUTION_STARTED 之后 never re-enter CLAIMED。
- execution_trace_id 是实际跨越执行开始边界的 delivery trace，不用 first claim 代替。
- terminal CAS 必须匹配 message generation、session generation、execution trace 和
  expected state。

### ProcessingLease

**Identity**: MessageExecution key 的 lease 子 key。

| Field | Type | Required | Rules |
|---|---|---:|---|
| owner_generation | integer | yes | 匹配 MessageExecution |
| owner_token_digest | string | yes | 匹配 MessageFence |
| owner_node_id | string | yes | 当前节点 |
| process_instance_id | UUID | yes | 节点本次启动 |
| phase | enum | yes | claimed, prepared, execution_started, finalizing |
| redis_pttl_ms | integer | derived | Redis 返回；唯一有效期权威 |

renew 仅在 key 存在、PTTL > 0、token/generation/node 全匹配时成功。到期后旧代不可续期。

### SessionLease

**Identity**: (tenant_id, agent_id, platform_session_id)。
**Generation counter**: 独立 key，不因 lease TTL 到期而删除或回退。

| Field | Type | Required | Rules |
|---|---|---:|---|
| generation | integer | yes | 每次成功 acquire 单调增加 |
| owner_token_digest | string | yes | 匹配 SessionFence |
| owner_node_id | string | yes | 当前节点 |
| process_instance_id | UUID | yes | 当前进程实例 |
| message_key_digest | string | yes | 当前处理消息 |
| execution_started | boolean | yes | false -> true，不可回退 |
| state | enum | yes | active, quarantined |
| redis_pttl_ms | integer | derived | Redis PTTL |

#### Session Lease State Machine

~~~text
FREE
  └─ acquire ─> ACTIVE(generation + 1, execution_started=false)

ACTIVE
  ├─ valid owner renew ─> ACTIVE(same generation)
  ├─ valid owner marks Agent start ─> ACTIVE(execution_started=true)
  ├─ valid owner releases ─> FREE
  ├─ expires before start ─> FREE, then new generation may acquire
  └─ expires after start ─> QUARANTINED

QUARANTINED
  ├─ recovery proves committed terminal and no active old writer ─> FREE
  └─ otherwise remains blocked; no automatic new message execution

LOST generation
  └─ any business write ─> FENCE_REJECTED
~~~

### SharedSession

**Identity**: (tenant_id, agent_id, platform_session_id)。
**SDK Mapping**: app_name + user_id + session_id 映射到固定 tenant/agent scope。

| Field | Type | Required | Rules |
|---|---|---:|---|
| schema_version | integer | yes | 当前 1 |
| tenant_id_digest | string | yes | key scope |
| agent_id_digest | string | yes | key scope |
| platform_session_id | string | yes | tenant/agent scoped |
| sdk_app_name | string | yes | 从 tenant/agent 派生 |
| sdk_user_id_digest | string | yes | 不存原始 external user |
| state_json | object | yes | 仅最小确定性会话状态 |
| version | integer | yes | 每次 fenced mutation +1 |
| last_event_sequence | integer | yes | 从 0 开始，单调增加 |
| conversation_count | integer | yes | >= 0 |
| updated_at | UTC datetime | yes | 诊断字段 |
| expires_after_seconds | integer | yes | 默认 86400 |

所有 mutation 必须携带 SessionFence。get_session 也要求已验证 tenant/agent identity，
Runner 不得通过原始外部 ID 直接读取。

### SessionEvent

**Identity**: (platform_session_id, sequence)；event_id 在同一 Session 唯一。

| Field | Type | Required | Rules |
|---|---|---:|---|
| tenant_id_digest | string | yes | 与 Session 相同 |
| agent_id_digest | string | yes | 与 Session 相同 |
| platform_session_id | string | yes | 与 Session 相同 |
| sequence | integer | yes | last_event_sequence + 1 |
| event_id | string | yes | SDK Event ID；重复追加返回已存在结果 |
| invocation_id | string | no | SDK 字段 |
| author | string | yes | 安全规范值 |
| event_json | object | yes | 官方 Event 的可恢复字段；不得含秘密 |
| message_key_digest | string | yes | 来源消息 |
| execution_trace_id | UUID | yes | 实际执行 trace |
| session_generation | integer | yes | 写入时 fence |
| created_at | UTC datetime | yes | 诊断字段 |

append_event 原子校验 SessionFence、event_id 去重并增加 sequence；旧 generation 的追加
必须返回 StaleFence，不能只在 terminal 阶段才检查。

## 4. PostgreSQL Persistent Entities

### schema_migrations

| Field | Type | Constraints |
|---|---|---|
| version | integer | primary key, > 0 |
| name | varchar(120) | not null |
| checksum | char(64) | not null |
| applied_at | timestamptz | not null |

Worker 支持版本集合必须包含数据库当前版本；数据库存在未知更高版本时 readiness 失败。

### tenants

| Field | Type | Constraints |
|---|---|---|
| tenant_id | varchar(64) | primary key |
| display_name | varchar(120) | not null |
| status | varchar(16) | active/disabled |
| config_version | bigint | > 0 |
| created_at | timestamptz | not null |
| updated_at | timestamptz | not null |

### agent_applications

| Field | Type | Constraints |
|---|---|---|
| tenant_id | varchar(64) | PK part, FK tenants |
| agent_id | varchar(64) | PK part |
| agent_name | varchar(120) | not null |
| status | varchar(16) | active/disabled |
| model_profile | varchar(64) | deterministic-offline in this phase |
| instruction | text | 1–4000 chars, non-secret |
| config_version | bigint | > 0 |
| updated_at | timestamptz | not null |

### channel_bindings

| Field | Type | Constraints |
|---|---|---|
| binding_id | varchar(96) | primary key |
| tenant_id | varchar(64) | FK owner |
| agent_id | varchar(64) | with tenant_id FK agent_applications |
| channel | varchar(32) | local_http |
| status | varchar(16) | active/disabled |
| secret_ref | varchar(128) | reference only; never secret value |
| signature_version | varchar(16) | v1 |
| config_version | bigint | > 0 |
| created_at | timestamptz | not null |
| updated_at | timestamptz | not null |

**Ownership constraints**:

- (tenant_id, agent_id) 必须引用同一租户 Agent。
- binding_id + channel 在认证前只返回最小 BindingAuthMaterial。
- resolve_active_context 必须在单个一致性读取中确认 Tenant/Agent/Binding 均 active 且
  config_version 可理解。

### persistent_audit_records

| Field | Type | Constraints |
|---|---|---|
| audit_id | UUID | primary key |
| audit_kind | varchar(16) | business/diagnostic |
| decision | varchar(40) | stable enum |
| trace_id | UUID | current delivery |
| first_claim_trace_id | UUID | nullable |
| owner_trace_id | UUID | nullable |
| execution_trace_id | UUID | nullable |
| tenant_id | varchar(64) | nullable only for preauth |
| node_id | varchar(64) | not null |
| process_instance_id | UUID | not null |
| binding_id_digest | char(71) | sha256: prefix |
| external_message_digest | char(71) | nullable |
| platform_session_id | varchar(69) | nullable |
| message_generation | bigint | nullable |
| session_generation | bigint | nullable |
| rejected_generation | bigint | diagnostic only |
| current_generation | bigint | diagnostic only |
| error_type | varchar(64) | nullable, safe code |
| result_digest | char(64) | nullable |
| latency_ms | numeric | >= 0 |
| cost | numeric | 0 in this phase |
| recovery_status | varchar(32) | nullable |
| created_at | timestamptz | not null |

**Indexes**:

- (tenant_id, trace_id, created_at)
- (tenant_id, platform_session_id, created_at)
- (tenant_id, external_message_digest, created_at)
- Partial index for recovery_status not null

**Audit invariants**:

- TenantScope queries always include tenant_id predicate.
- PreAuthScope records have tenant_id/session/user null and cannot query tenant data.
- business terminal uniqueness:
  (tenant_id, external_message_digest, message_generation, decision, audit_kind).
- diagnostic stale-write rows include rejected_generation/current_generation and cannot be used as
  final business results.
- 原始正文、外部 user/conversation、签名、secret_ref value、连接地址、SQL、stack 禁止存储。

### recovery_markers

**Identity**: (tenant_id, binding_id_digest, external_message_digest, execution_trace_id)。

| Field | Type | Constraints |
|---|---|---|
| recovery_id | UUID | primary key |
| tenant_id | varchar(64) | not null |
| binding_id_digest | char(71) | not null |
| external_message_digest | char(71) | not null |
| platform_session_id | varchar(69) | not null |
| message_generation | bigint | > 0 |
| session_generation | bigint | > 0 |
| execution_trace_id | UUID | not null |
| state | varchar(32) | terminal_pending/reconciled/conflict_review |
| result_status | varchar(32) | succeeded/failed_post_start/outcome_unknown |
| result_payload | jsonb | safe ExecutionResult subset |
| result_digest | char(64) | canonical payload digest |
| replay_allowed | boolean | always false |
| failure_stage | varchar(64) | nullable safe code |
| created_at | timestamptz | not null |
| reconciled_at | timestamptz | nullable |

final Audit 与 terminal_pending marker 在同一 SQL transaction 中创建。result_payload
不得包含原始输入、秘密或 vendor exception。reconcile 必须同时匹配 tenant、message
digest、execution trace、message/session generation 和 result digest。

#### Recovery State Machine

~~~text
ABSENT
  └─ final SQL transaction ─> TERMINAL_PENDING

TERMINAL_PENDING
  ├─ Redis already has identical terminal ─> RECONCILED
  ├─ Redis terminal CAS succeeds ─> RECONCILED
  ├─ Redis target differs ─> CONFLICT_REVIEW
  └─ Redis unavailable ─> TERMINAL_PENDING

RECONCILED / CONFLICT_REVIEW
  └─ immutable except diagnostic timestamps/notes
~~~

RecoveryMarker 永远不允许转换到 CLAIMED、PREPARED 或 EXECUTION_STARTED。

## 5. Configuration Cache Entry

CacheEntry 只存在于 Worker 进程并可随时丢弃：

| Field | Type | Rules |
|---|---|---|
| binding_id | string | public lookup identity |
| config_version | integer | last observed |
| loaded_at | UTC datetime | diagnostic only |
| disposition | enum | verified_snapshot/deny |
| data | minimal auth/context data | secret value forbidden |

**Authority rule**: 只有当前请求已成功验证 PostgreSQL schema/config version 时，
verified_snapshot 才能加速同一权威读取链路；数据库不可用时它不能返回 active context。
deny entry 可以继续拒绝，但不能推导其他 binding 可用。

## 6. ExecutionResult

沿用 002 字段并增加可恢复规范：

| Field | Type | Required | Rules |
|---|---|---:|---|
| status | enum | yes | succeeded/failed_post_start/outcome_unknown |
| response_text | string | succeeded | 1–4000 chars；Recovery payload 可保存 |
| error_code | safe string | failure | stable code |
| error_message | safe string | failure | fixed/redacted |
| original_trace_id | UUID | yes | execution_trace_id |
| platform_session_id | string | yes | tenant scoped |
| started_at | UTC datetime | yes | evidence |
| finished_at | UTC datetime | yes | evidence |
| agent_event_count | integer | yes | >= 0 |
| final_response_count | integer | yes | succeeded exactly 1 |
| delivery_action | enum | yes | first success deliver；cached suppress |
| message_generation | integer | yes | recovery CAS condition |
| session_generation | integer | yes | recovery CAS condition |

canonical JSON + SHA-256 形成 result_digest，Redis 与 PostgreSQL 必须一致。

## 7. Error and Validation Rules

- 所有 datetime 为 UTC-aware；但 lease 正确性只依据 Redis PTTL/token/generation。
- 所有 generation > 0 且只增不减。
- external IDs 只在 HTTP/domain 临时对象中出现；持久 key/audit 使用 scoped digest。
- owner_token 只在短期内存和 Redis 内使用；SQL Audit 不存 token。
- unknown schema version、unknown enum、缺失 fence 或 scope mismatch 均 fail closed。
- Redis/SQL vendor exception 不得进入领域对象或统一回复。
- OUTCOME_UNKNOWN、FAILED_POST_START 与 terminal_pending 均 replay_allowed=false。
- 指标不是正确性 ledger，指标失败不能改变上述状态。

## 8. Retention and Cleanup

- Redis Session/Event：默认 24 小时滑动 TTL；终态 Idempotency 与 generation counter
  本阶段不自动过期。
- PostgreSQL Config/Audit/Recovery：本阶段持续保留，测试数据库按 run_id/schema 清理。
- Docker named volumes 在普通 Worker restart 时保留；显式 demo reset 才删除测试数据。
- 实现不得提供模糊的“清空所有租户”业务 API；测试清理需要 feature namespace 和
  明确的测试环境保护。

## 9. Decision Traceability

| Decision | Model Enforcement |
|---|---|
| D-003-001 | ProcessingLease/SessionLease 的 PTTL + generation + token 原子续期 |
| D-003-002 | MessageFence/SessionFence 条件写与 diagnostic audit 分型 |
| D-003-003 | MessageExecution.EXECUTION_STARTED 是唯一接管边界 |
| D-003-004 | RecoveryMarker terminal_pending/reconciled/conflict_review |
| D-003-005 | Persistent Configuration 权威；CacheEntry 无独立授权能力 |

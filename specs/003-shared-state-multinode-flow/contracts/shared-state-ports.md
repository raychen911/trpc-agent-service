# Contract: Shared State and Persistent Ports

**Feature**: 003-shared-state-multinode-flow
**Purpose**: 在不暴露 Redis/PostgreSQL 表示的前提下，使 InMemory 与共享实现保持相同业务语义

## 1. General Rules

- 所有可能访问外部状态的方法均为 async。
- Tenant-owned 操作必须接收 TenantScope、VerifiedBindingScope、MessageFence 或
  SessionFence，不能仅接收调用方提供的 tenant_id。
- 返回值是不可变领域对象；vendor response、connection、cursor、pipeline 不得越过端口。
- vendor exception 在 Adapter 内映射为稳定平台错误。
- owner_token/secret 不得进入 repr、日志、Audit、Metrics 或 HTTP。
- InMemory 和 shared fixtures 必须运行同一核心 contract tests。

## 2. BindingAuthRegistry

~~~text
async get_auth_material(binding_id, channel)
  -> BindingAuthMaterial
  | NotFound
  | ConfigurationUnavailable
~~~

- 认证前只返回 binding_id、secret_ref、signature_version、status。
- PostgreSQL 不可验证时不能从正向缓存返回 active material。
- 未知 binding 与已知但未授权信息继续遵守 HTTP 非披露规则。

## 3. TenantDirectory

~~~text
async resolve_active_context(
  verified_binding_scope,
  external_user_id,
  trace_id
)
  -> VerifiedTenantContext
  | AccessDenied
  | ConfigurationUnavailable
~~~

单个一致性读取必须确认 Binding -> Tenant -> Agent 所有权、active 状态和可支持的
config/schema version。缓存不能在权威读取失败时授予 context。

## 4. IdempotencyRepository

~~~text
async claim(key, fingerprint, trace_id, node_identity, lease_ms)
  -> Acquired(MessageFence, attempt)
  | Processing(owner_trace_id)
  | Completed(ExecutionResult, execution_trace_id)
  | Conflict
  | OutcomeUnknown(execution_trace_id?)

async mark_prepared(message_fence) -> MessageExecution

async mark_execution_started(
  message_fence,
  session_fence,
  execution_trace_id
) -> MessageExecution

async renew(message_fence, lease_ms) -> MessageFence

async mark_pre_start_failed(message_fence, safe_error) -> MessageExecution

async begin_finalization(
  message_fence,
  session_fence,
  execution_result
) -> MessageExecution

async complete(
  message_fence,
  session_fence,
  execution_result
) -> MessageExecution

async mark_outcome_unknown(
  message_fence,
  session_fence?,
  execution_result
) -> MessageExecution

async get(key, tenant_scope) -> MessageExecution | NotFound

async reconcile_terminal(
  tenant_scope,
  recovery_result
) -> ReconcileResult
~~~

Required semantics:

- claim 的 fingerprint 比较、state 判断、generation 增加和 lease 创建是一个原子操作。
- 只有 FAILED_PRE_START 或 lease 已确认过期且状态早于 EXECUTION_STARTED 才能换代。
- renew 只接受未过期当前 generation/token/node；过期 generation 永不复活。
- mark_execution_started 同时检查 MessageFence 与 SessionFence；返回未确认时不得调用 Agent。
- EXECUTION_STARTED、FINALIZING 和所有 terminal state 不得自动重新 claim。
- complete 同时匹配 message/session generation、execution_trace_id、result_digest 和 expected state。
- reconcile_terminal 只写已持久化结果；不得创建执行前状态或调用 Agent。
- Redis 超时后必须 read-back；仍无法确定时抛出 OutcomeUnknown 或 StateBackendUnavailable，
  不得猜测成功。

## 5. SessionLeaseManager

~~~text
async acquire(
  tenant_scope,
  agent_id,
  platform_session_id,
  message_key_digest,
  node_identity,
  lease_ms,
  wait_ms
) -> SessionLeaseHandle | SessionBusy | SessionQuarantined

SessionLeaseHandle.fence -> SessionFence
async SessionLeaseHandle.renew(lease_ms) -> SessionFence
async SessionLeaseHandle.mark_execution_started(message_fence) -> SessionFence
async SessionLeaseHandle.release(reason) -> None
~~~

- 同一 platform_session_id 同时最多一个有效 SessionFence。
- 不同 Session 使用独立 key，不得共享全局锁。
- acquire 成功时 generation 单调增加。
- renew 只在 PTTL > 0 且 generation/token/node 匹配时成功。
- execution_started=false 的过期 lease 可换代；true 或未知状态进入 quarantined。
- release 只有当前 generation/token 能生效；旧 owner release 不得删除新 lease。
- 取消/异常必须尝试 release，但 release 失败不能伪装状态已清理。

## 6. SharedSessionRepository

~~~text
async get_session(session_identity, session_fence) -> SharedSession | None

async create_session(
  session_identity,
  initial_state,
  session_fence
) -> SharedSession

async append_event(
  session_identity,
  event,
  message_fence,
  session_fence
) -> SessionEvent

async update_session(
  session_identity,
  expected_version,
  state_delta,
  message_fence,
  session_fence
) -> SharedSession
~~~

- create/update/append 的 fence 验证与写入必须原子。
- append_event 以 event_id 幂等；首次追加才增加 sequence。
- 旧 generation 返回 StaleFence，且业务状态完全不变。
- 读取/写入 identity 必须匹配 tenant_id、agent_id、sdk app/user 和 platform_session_id。
- Session/Event 编码只使用官方公开 Session/Event 字段和平台 schema version。

## 7. Fenced Session Service Adapter

FencedRedisSessionService 实现官方 BaseSessionService 的公开方法：

~~~text
create_session(...)
get_session(...)
list_sessions(...)
delete_session(...)
append_event(session, event)
update_session(session)
close()
~~~

- Runner 调用期间从受控 execution context 获取 MessageFence/SessionFence。
- 没有有效执行上下文的 mutation 必须拒绝。
- Adapter 将官方 Session/Event 映射到 SharedSessionRepository，不让 Gateway 接触 SDK 对象。
- list/delete 不暴露为本阶段 HTTP 能力；测试只验证 tenant/agent scope 和生命周期。

## 8. ConfigurationRepository

~~~text
async verify_schema() -> SupportedSchemaVersion
async get_auth_material(binding_id, channel) -> BindingAuthMaterial
async resolve_active_context(verified_binding_scope, external_user_id, trace_id)
  -> VerifiedTenantContext
~~~

- PostgreSQL 是授权权威；unknown schema/config version 失败关闭。
- 任何 cache hit 都不能绕过本次权威可验证条件。
- SQL transaction/session 对象不得返回上层。

## 9. AuditRepository

~~~text
async append(scope, record, fence_proof?) -> PersistentAuditRecord
async append_diagnostic(scope, record) -> PersistentAuditRecord
async append_final_with_recovery(
  tenant_scope,
  business_audit,
  recovery_marker,
  message_fence,
  session_fence
) -> FinalizationRecord

async list_by_trace(tenant_scope, trace_id) -> list[PersistentAuditRecord]
async list_by_session(tenant_scope, platform_session_id) -> list[PersistentAuditRecord]
async list_by_tenant(tenant_scope) -> list[PersistentAuditRecord]
async list_preauth(preauth_scope) -> list[PersistentAuditRecord]
~~~

- business Audit 必须携带当前 generation 的 fence proof。
- final Audit 与 Recovery Marker 在同一 PostgreSQL transaction 提交。
- stale fence 不得形成 business Audit；平台可追加独立 diagnostic late_write_rejected。
- diagnostic Audit 不具备改变终态、回复或 Recovery state 的操作。
- 所有 query 必须先应用 scope predicate，再读取记录。

## 10. RecoveryRepository

~~~text
async find_blocking(tenant_scope, idempotency_key_digest)
  -> RecoveryMarker | None

async get_pending(tenant_scope, limit)
  -> list[RecoveryMarker]

async mark_reconciled(
  tenant_scope,
  recovery_id,
  expected_result_digest
) -> RecoveryMarker

async mark_conflict_review(
  tenant_scope,
  recovery_id,
  safe_reason
) -> RecoveryMarker
~~~

- TERMINAL_PENDING 与 CONFLICT_REVIEW 均阻止 Agent 重执行。
- mark_reconciled 不修改 result payload。
- Reconciler 必须先调用 IdempotencyRepository.reconcile_terminal，再更新 marker。
- SQL/Redis 任一结果不确定时 marker 保持原状态，不能删除。

## 11. MetricsRecorder

延续 002 contract，新增 node_id、backend、lease_operation、generation_outcome 等安全
维度。不得使用原始 tenant/user/message/session 外部 ID 作为 label。Metrics 失败不改变
授权、fencing、terminal 或 Recovery。

## 12. SharedPlatformAdapters Lifecycle

~~~text
async create(settings, node_identity) -> SharedPlatformAdapters
async readiness() -> ready | not_ready
async close() -> None
~~~

- create 建立连接并验证 schema，不自动迁移、不 seed、不清空数据。
- Redis 或 PostgreSQL 不可验证时 shared runtime readiness=false。
- close 幂等关闭连接池、heartbeat task 与 Runner；不删除共享数据。
- 不得自动回退 InMemory。

## 13. Contract Test Matrix

| Port | Shared Business Assertions |
|---|---|
| BindingAuthRegistry/TenantDirectory | active/disabled/unknown/ownership/version/outage/no-cache-authorization |
| IdempotencyRepository | atomic claim、conflict、processing、terminal、pre-start takeover、post-start no-replay、three traces |
| SessionLeaseManager | same-session serial、different parallel、renew/expiry race、old release rejection、quarantine |
| SharedSessionRepository | tenant/agent scope、event order/idempotency、fenced mutation、cross-node read |
| SDK Session Adapter | official Runner compatibility、multi-round cross-node、missing fence rejection、close lifecycle |
| AuditRepository | scope isolation、business/diagnostic distinction、stale fence、final+recovery transaction |
| RecoveryRepository | pending blocks execution、conditional reconcile、conflict review、outage |
| SharedPlatformAdapters | readiness、close、no fallback、redacted errors |

Vendor-specific setup tests may inspect Redis/SQL details；core contract tests may only assert domain
objects, stable exceptions and observable business states.

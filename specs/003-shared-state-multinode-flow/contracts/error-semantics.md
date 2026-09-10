# Contract: Shared Runtime Error Semantics

**Feature**: 003-shared-state-multinode-flow
**Rule**: 错误必须由 Adapter 映射为稳定类别，HTTP 层不得暴露 vendor 详情。

## Public Matrix

| HTTP | status | error.code | retryable | execution_started | delivery_action | Meaning |
|---:|---|---|---:|---:|---|---|
| 200 | succeeded | null | false | true | deliver | 首次成功 |
| 200 | duplicate | null | false | true | suppress | 返回原成功结果，不重复投递 |
| 202 | processing | null | true | current | none | 同一消息已有 owner |
| 400 | invalid_request | invalid_request | false | false | none | 请求契约错误 |
| 401 | unauthorized | unauthorized | false | false | none | HMAC/Binding 认证失败 |
| 403 | access_denied | access_denied | false | false | none | 已认证但租户/Agent/Binding 禁用或错配 |
| 409 | conflict | idempotency_conflict | false | false | none | 同幂等键不同指纹 |
| 503 | failed | configuration_unavailable | true | false | none | SQL 权威配置不可验证 |
| 503 | failed | state_backend_unavailable | true | false | none | Redis 在执行开始前不可用 |
| 503 | failed | session_busy | true | false | none | 同 Session 处理权在限定时间内不可取得 |
| 503 | failed | lease_lost | true | false | none | 执行开始前失去有效 fence |
| 503 | failed | audit_unavailable | true | false | none | 执行开始前审计不可用 |
| 503 | failed | audit_incomplete | false | true | none/suppress | 执行后最终审计失败 |
| 503 | failed | outcome_unknown | false | true | none/suppress | 执行或终态结果不确定 |
| 502 | failed | agent_failed | false | true | none/suppress | Agent 已开始后的稳定失败 |

## Phase-Sensitive Rules

### Before EXECUTION_STARTED

- configuration/state/audit/lease failure可以返回 retryable=true。
- 重试同一 external_message_id 仍必须重新经过权威配置、claim 和 Session lease。
- 只有共享状态明确证明旧 owner 未开始且 lease 已失效时才能增加 generation。

### At or After EXECUTION_STARTED

- timeout、cancel、lost lease、stale fence、状态写入不确定均为 retryable=false。
- 同一 external_message_id 的后续投递不得调用 Agent。
- 若终态已知则返回 cached terminal；若未知则返回 outcome_unknown。
- Session 可能进入 quarantined，后续不同消息也不得绕过恢复状态。

## Internal Typed Errors

| Error | Raised By | Public Mapping |
|---|---|---|
| ConfigurationUnavailable | PostgreSQL config adapter | configuration_unavailable |
| StateBackendUnavailable | Redis adapter | state_backend_unavailable before start；outcome_unknown after start |
| SessionBusy | Session lease manager | session_busy |
| SessionQuarantined | Session lease manager | outcome_unknown |
| LeaseLost | renew/release/start transition | lease_lost before start；outcome_unknown after start |
| StaleFence | any conditional mutation | lease_lost/outcome_unknown by phase |
| ConditionalWriteFailed | terminal/state CAS | read-back then conflict or outcome_unknown |
| RecoveryConflict | reconciler | outcome_unknown + conflict_review marker |
| AuditUnavailable | SQL audit adapter | audit_unavailable before start；audit_incomplete after start |

## Unknown Write Result

当 Redis/SQL client timeout 无法证明写入是否发生：

1. 使用同一 tenant scope、key、generation、trace 和 result digest read-back。
2. 读到完全相同状态：按幂等成功继续。
3. 读到不同 generation/结果：停止并形成 conflict/diagnostic evidence。
4. read-back 仍不可用：按当前阶段返回 state_backend_unavailable、audit_incomplete 或
   outcome_unknown。
5. 任何分支都不得通过重新调用 Agent 来“确认”。

## Non-Disclosure

公开错误 message 使用固定安全文本。以下内容不得出现：

- Redis/PostgreSQL host、port、database、URL 或 SQL text。
- secret value、secret_ref、owner token、HMAC、完整正文。
- driver exception、stack、Lua source/sha、表名或内部 key。
- 未认证请求猜测的 tenant/binding 存在性。

## Diagnostic Audit Failure

迟到写诊断审计失败不会：

- 放行旧 generation；
- 修改业务终态；
- 将 outcome_unknown 改为 succeeded；
- 触发 Agent 或重复回复。

该失败只形成脱敏 operational event/metric，后续可人工调查。

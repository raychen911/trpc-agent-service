# 第六阶段数据模型

**功能编号**：`006-tenant-governance-tool-policy`
**原则**：可信租户作用域、不可变决策事实、状态转换幂等、原始敏感值不落库。

## 1. 所有权与存储分工

| 数据 | 权威存储 | 原因 |
|------|----------|------|
| 策略版本与当前有效指针 | PostgreSQL | 需要事务切换、历史审计和跨节点立即读取 |
| 主体授权 | PostgreSQL | 需要持久状态、有效期和跨节点一致授权 |
| 预算账户、预占、实际结算 | PostgreSQL | 需要全维度原子准入和唯一结算 |
| 待确认事实 | Redis | 短期 TTL、一次性 claim、跨节点低延迟消费 |
| 治理审计与恢复事实 | PostgreSQL | 不可变、可查询、重启后保留 |
| 请求内 GovernanceContext | 内存 | 仅单次调用，Worker 不持久保存业务状态 |

所有 Repository 均提供 InMemory 实现并与共享后端执行同一套契约测试。

## 2. 核心值对象

### ChannelPrincipal

| Field | Type | Rule |
|-------|------|------|
| tenant_id | str | 仅来自可信 Channel Binding |
| channel | enum | `feishu` / `wecom` |
| binding_id | str | 已启用 binding 的稳定标识 |
| provider_subject | str | SDK 验证后的稳定用户 ID，不使用显示名 |
| subject_digest | str | tenant-scoped HMAC/不可逆摘要，用于审计和确认 |

唯一身份键为 `(tenant_id, channel, binding_id, provider_subject)`；相同字符串跨渠道不合并。

### ToolDescriptor

| Field | Type | Rule |
|-------|------|------|
| tool_name | str | 平台内稳定且唯一 |
| side_effect_class | enum | `none` / `reversible` / `external` |
| risk_level | enum | `low` / `medium` / `high` |
| confirmation_required | bool | 高风险默认 true |
| usage_dimensions | set | request/tool_call/token/cost 的子集 |
| max_usage | map | 与预算严格预占及 Runner 限制一致 |

### GovernanceContext

请求内不可变对象，包含 tenant、Agent、binding、principal digest、Session、message/execution ID、三类 trace、准入 policy version、reservation_id 和 fencing generation。禁止包含 Secret、原始敏感参数或客户端声明的权限。

### PolicyDecision

`ALLOW | DENY | CONFIRMATION_REQUIRED`，附 policy_version、稳定 reason_code、tool_name（可空）、耗时和 trace。用户回复不携带内部规则正文。

### RedactionFinding

仅保存 `category`、`rule_id`、`action`、长度、位置摘要与不可逆摘要；不得保存命中原文。

## 3. PostgreSQL 实体

### governance_policy_versions

| Field | Constraints |
|-------|-------------|
| policy_id | UUID PK |
| tenant_id | NOT NULL, indexed |
| scope_type / scope_id | tenant、agent 或 binding 范围 |
| version | positive integer；同 scope 唯一 |
| status | `DRAFT/ACTIVE/SUPERSEDED/DISABLED` |
| policy_document | JSONB；经过 schema 校验，不含 Secret |
| created_at / created_by_digest | immutable metadata |
| activated_at / disabled_at | lifecycle timestamps |

规则：版本内容创建后不可修改；激活事务同时锁定 active 指针、切换旧版本状态并写审计。

### governance_policy_active

| Field | Constraints |
|-------|-------------|
| tenant_id / scope_type / scope_id | composite PK |
| policy_id / version | FK to policy version |
| activation_generation | monotonic integer |
| updated_at | commit timestamp |

该行的事务提交是 DEC-001 的立即生效点。缓存中的旧 generation 不得授权。

### principal_grants

| Field | Constraints |
|-------|-------------|
| grant_id | UUID PK |
| tenant_id / channel / binding_id | NOT NULL, indexed |
| provider_subject_digest | NOT NULL |
| agent_name | nullable，空表示 tenant 层 |
| status | `ACTIVE/DISABLED` |
| permissions | JSONB allow set；与更窄层级取交集 |
| valid_from / expires_at | UTC；过期默认拒绝 |
| created_at / revoked_at | audit timestamps |

读取时必须同时匹配 tenant、channel、binding 和 subject；不允许跨 tenant fallback。

### budget_accounts

| Field | Constraints |
|-------|-------------|
| tenant_id / policy_id / period_key / dimension | composite PK |
| hard_limit | non-negative |
| reserved_amount | non-negative |
| settled_amount | non-negative |
| version | optimistic/fencing version |
| period_start / period_end | UTC half-open interval |

不变量：`reserved_amount + settled_amount <= hard_limit`；所有维度的准入在同一事务完成。

### budget_reservations

| Field | Constraints |
|-------|-------------|
| reservation_id | UUID PK |
| tenant_id / execution_id / dimension | UNIQUE |
| policy_id / policy_version | NOT NULL |
| maximum_amount | non-negative |
| actual_amount | nullable；必须 `<= maximum_amount` |
| state | `RESERVED/SETTLED/RELEASED/REVIEW_REQUIRED` |
| execution_started | bool |
| owner_generation | fencing token |
| trace_id / created_at / updated_at | recovery evidence |

状态转换：

- `RESERVED → SETTLED`：一次性计入 actual 并释放差额。
- `RESERVED → RELEASED`：仅允许尚未开始实际执行。
- `RESERVED → REVIEW_REQUIRED`：已开始但结果/用量无法证明。
- 需要确认的危险执行在 PendingConfirmation TTL 内保持 `RESERVED`；确认成功复用同一 reservation，确认过期/取消或执行前校验失败释放 reservation，不创建第二条占用。
- 终态不可反向；重复相同转换返回已有结果，不重复扣减。

### GovernanceAuditEvent（扩展现有 Audit Log）

新增可空字段：`principal_digest`、`policy_id/version`、`governance_decision`、`reason_code`、`confirmation_digest`、`reservation_id`、`reserved_usage`、`actual_usage`、`tool_class`。保持现有 tenant/channel/session/Agent/tool/error/cost/trace 字段与不可变语义。

### RecoveryMarker（扩展现有恢复事实）

新增 `governance_stage`、`reservation_id`、`confirmation_id_digest`、`execution_started`、`actual_usage_digest` 和 `review_reason`，供健康节点只补齐允许的后继状态。

## 4. Redis 实体

### PendingConfirmation

键包含版本化命名空间及 tenant：

```text
gov:v1:confirm:{tenant_id}:{confirmation_id_digest}
```

字段：channel、binding_id、principal_digest、agent_name、session_id、tool_name、operation_digest、arguments_digest、policy_id/version、code_hash、expires_at、state、claim_owner、owner_generation、execution_id、trace_id。

状态：`PENDING → CLAIMED → EXECUTING → COMPLETED`，并允许 `PENDING → EXPIRED/CANCELLED`、`CLAIMED → PENDING`（仅执行前租约过期）、`EXECUTING → OUTCOME_UNKNOWN`。所有 claim/状态推进由 Lua 或等价原子操作校验旧状态、身份、TTL 和 fencing。

不保存完整工具参数、可恢复 Secret、明文确认码或 IM response URL。

## 5. 统一确认输入与回复扩展

### ConfirmationIntent

```text
source: text | button
confirmation_reference: opaque id/code
trusted tenant/channel/binding/principal/session
provider_event_id
trace context
```

Adapter 只负责从 SDK 事件构造该对象，不进行权限判断。

### UnifiedReply.confirmation（可选）

包含安全 prompt、一次性显示码、过期时间和 opaque button token；现有纯文本消费者忽略该字段仍可工作。button token 不携带 tenant 权限或完整操作参数。

## 6. 跨实体不变量

1. 任何主键、查询和缓存键都不能省略可信 tenant scope。
2. 一个 execution_id 在每个预算维度最多一条 reservation，且只结算一次。
3. 一个 confirmation 只产生一个 execution_id；文本与按钮不能创建不同执行。
4. 工具开始前使用的 policy version 必须等于当时 active 指针；否则 `policy_stale`。
5. 旧 fencing generation 不能完成确认、预算、Session 或恢复写入。
6. 出站检查失败不撤销已产生用量，但不得发送原始不安全输出。
7. 回复发送失败只推进交付恢复，不回到 Agent/工具/预算预占阶段。
8. 审计摘要可关联执行，但不能恢复主体原值、确认码或敏感内容。
9. PendingConfirmation 与 reservation 共用有限 TTL；未执行确认过期必须释放唯一 reservation，确认成功不得重新预占。

## 7. 保留与清理

- PendingConfirmation 由 TTL 到期后清理；已完成的最小去重 tombstone 保留到渠道重放窗口结束。
- 策略版本、授权历史、预算结算和审计按项目现有持久化策略保留，不执行级联物理删除。
- `REVIEW_REQUIRED` 不自动过期释放，必须由恢复器基于确定证据或人工决定收敛。

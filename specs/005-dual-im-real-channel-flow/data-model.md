# 第五阶段数据模型

**功能**：005-dual-im-real-channel-flow
**依据**：[spec.md](./spec.md)、[clarification-decisions.md](./clarification-decisions.md)

## 设计规则

- 外部 tenant_id 永不作为授权输入；tenant_id 只能由可信 Channel Binding 推导。
- SDK 对象、原始事件和完整 WebSocket frame 不进入领域模型或持久化层。
- 外部用户、消息和会话标识在 Redis key、Audit 与指标中使用 tenant-scoped digest。
- 业务执行状态和渠道交付状态分离，交付恢复不得创建新的 Agent 执行。
- 所有可变业务写入均携带当前 generation/fencing proof。

## 1. ChannelIdentity

由已认证 SDK 上下文构造，用于查询唯一 Channel Binding。

| Field | Type | Rules |
|---|---|---|
| channel_type | enum | FEISHU 或 WECOM |
| provider_tenant_key | string | 飞书 tenant_key 或企业微信 corp_id；1..128 |
| provider_app_or_bot_id | string | 飞书 app_id/bot_id 或企业微信 bot_id；1..128 |
| identity_digest | sha256 | 对上述字段做长度前缀哈希 |

唯一性：

~~~text
(channel_type, provider_tenant_key, provider_app_or_bot_id)
~~~

任一字段缺失、来源未认证或精确匹配失败均不得签发 VerifiedBindingScope。

## 2. RuntimeBotIdentity

Adapter 认证成功后获得的当前机器人身份，仅保存在当前连接的非业务运行态中。

| Field | Type | Rules |
|---|---|---|
| channel_type | enum | 必须与 Adapter 类型一致 |
| sender_type | string | SDK 认证的机器人类型 |
| sender_id | string | SDK 认证的机器人唯一标识 |
| channel_identity_digest | sha256 | 必须与当前 Adapter ownership scope 一致 |
| authenticated_at | UTC datetime | 必须带 UTC 时区 |

该对象不保存 Secret、access token、ticket 或连接 URL。

## 3. AuthenticatedSender

从事件的结构化认证字段提取。

| Field | Type | Rules |
|---|---|---|
| sender_type | string | 必填，否则身份不确定 |
| sender_id | string | 必填，否则身份不确定 |
| is_bot | bool/unknown | 由 SDK 字段推导，不能从昵称推导 |

判断：

~~~text
sender == RuntimeBotIdentity     -> self_message_ignored
sender complete and not self     -> continue
sender incomplete/untrusted      -> sender_identity_unverified
~~~

## 4. ChannelBinding

扩展第三阶段已有实体。

| Field | Type | Rules |
|---|---|---|
| binding_id | string | 平台内部稳定 ID |
| tenant_id | string | 外键 Tenant |
| agent_id | string | 外键 AgentApplication |
| channel_type | enum | LOCAL_HTTP/FEISHU/WECOM |
| provider_tenant_key | string/null | 真实 IM 必填 |
| provider_app_or_bot_id | string/null | 真实 IM 必填 |
| channel_identity_digest | sha256/null | 真实 IM 必填 |
| status | active/disabled | 非 active 默认拒绝 |
| secret_ref | string | 环境变量/Secret Provider 引用，不是 Secret |
| config_version | positive int | 权威版本 |
| created_at/updated_at | UTC datetime | 审计时间 |

约束：

- 唯一 (channel_type, provider_tenant_key, provider_app_or_bot_id)。
- Binding、Tenant、Agent 必须同属 tenant_id 且全部 active。
- LOCAL_HTTP 的既有字段和查询保持兼容。

## 5. UnifiedInboundMessage

扩展既有 InboundMessage，保持 SDK 无关。

| Field | Type | Rules |
|---|---|---|
| channel | enum | LOCAL_HTTP/FEISHU/WECOM |
| binding_id | string | 由成功 Binding 查询产生 |
| channel_identity_digest | sha256 | 可信身份摘要 |
| external_message_id | string | 飞书 message_id；企微 msgid |
| external_user_id | string | 认证发送者 ID |
| conversation_type | direct/group | 必填 |
| external_conversation_id | string | 单聊或群聊稳定 ID |
| group_sender_id | string/null | group 必填，direct 为空 |
| message_type | text | 本阶段只允许 text |
| text | string | 去除结构化 @ 后 1..4000 |
| received_at | UTC datetime | Adapter 接收时间 |
| trace_id | UUID | 无上游可信值则生成 |

不保存 ProviderReplyContext、tenant_id 或完整原始 payload。

## 6. ProviderReplyContext

仅在 Channel/Delivery 边界使用的最小、可序列化回复路由。

| Field | Type | Rules |
|---|---|---|
| channel | enum | FEISHU/WECOM |
| conversation_type | direct/group | 与入站一致 |
| reply_target_id | string | 飞书 chat_id；企微 userid/chatid |
| protocol_request_id | string/null | 企微 req_id，可用于原回调回复但不得作为幂等 ID |
| provider_message_id | string | 与 external_message_id 相同 |
| context_digest | sha256 | 防篡改/关联摘要 |

禁止保存 SDK frame、Secret、token、ticket 和原始正文。

## 7. SessionIdentity

在第三阶段 SessionIdentity 上扩展渠道语义。

单聊摘要输入：

~~~text
tenant_id
agent_id
channel_type
binding_id
conversation_type=direct
external_conversation_id
~~~

群聊摘要输入：

~~~text
tenant_id
agent_id
channel_type
binding_id
conversation_type=group
group_conversation_id
sender_id
~~~

输出仍为 sess_<sha256>。同一群聊不同 sender 必须得到不同 Session；同一 sender 的
连续消息得到相同 Session。回复目标仍为原群聊。

## 8. IdempotencyKey

摘要输入：

~~~text
tenant_id
channel_type
binding_id
external_message_id
~~~

企业微信 req_id 不参与业务幂等。相同 message_id/msgid 在不同 tenant、channel 或
binding 下不得碰撞。

## 9. UnifiedReply

沿用 OutboundReply，成功结果至少包含：

| Field | Type | Rules |
|---|---|---|
| status | succeeded/duplicate/... | 既有语义 |
| trace_id | UUID | 当前投递 |
| original_trace_id | UUID/null | 重复时指向原执行 |
| tenant_id | string | 来自可信上下文 |
| platform_session_id | sess hash | tenant scoped |
| external_message_id | string | 原业务消息 ID |
| text | string/null | 成功时存在 |
| delivery_action | deliver/suppress/none | 重复结果默认 suppress |

DeliveryService 只接收已持久化成功结果或其安全引用。

## 10. DeliveryRecord

一条已完成 Agent 结果的渠道交付意图。

| Field | Type | Rules |
|---|---|---|
| delivery_id | UUID | 主键 |
| tenant_id | string | tenant scope |
| binding_id | string | 原 Binding |
| channel | enum | 原渠道 |
| idempotency_key_digest | sha256 | 关联执行 |
| execution_trace_id | UUID | 关联唯一执行 |
| reply_context | ProviderReplyContext | 最小路由信息 |
| result_digest | sha256 | 已持久化回复摘要 |
| status | enum | 状态机见下 |
| adapter_generation | int | 创建/发送时的所有权代 |
| next_attempt_at | UTC datetime/null | 仅 retry_wait |
| created_at/updated_at | UTC datetime | 必填 |

唯一约束：

~~~text
(tenant_id, idempotency_key_digest, channel, binding_id)
~~~

状态机：

~~~text
PENDING
  -> SENDING
  -> DELIVERED
  -> RETRY_WAIT -> SENDING
  -> DELIVERY_FAILED
  -> DELIVERY_UNKNOWN

DELIVERED / DELIVERY_FAILED / DELIVERY_UNKNOWN 为自动流程终态。
DELIVERY_UNKNOWN 不得自动回到 SENDING。
~~~

## 11. DeliveryAttempt

| Field | Type | Rules |
|---|---|---|
| attempt_id | UUID | 主键 |
| delivery_id | UUID | 外键 DeliveryRecord |
| attempt_no | int | 1..4；首次 + 最多 3 次重试 |
| trace_id | UUID | 当前交付 trace |
| adapter_node_id | string | 非敏感节点 ID |
| adapter_generation | int | 必须为当前 generation |
| started_at/finished_at | UTC datetime | 必填 |
| outcome | succeeded/transient/permanent/unknown/fence_rejected | 枚举 |
| safe_error_code | string/null | 固定安全错误 |
| retry_delay_seconds | 1/2/4/null | 仅 transient |

同一 delivery_id 的 attempt_no 唯一。错误详情必须脱敏。

## 12. AdapterOwnershipLease

Redis 运行态对象，按 ChannelIdentity digest 分区。

| Field | Type | Rules |
|---|---|---|
| channel_identity_digest | sha256 | scope |
| owner_node_id | string | 当前 Adapter 节点 |
| generation | positive int | 取得新租约时单调增加 |
| fencing_token | secret random | 不可记录或持久化到 Audit |
| phase | standby/connecting/ready/draining | 运行阶段 |
| expires_at | Redis TTL | Redis 服务端裁决 |

状态机：

~~~text
FREE -> OWNED(CONNECTING) -> OWNED(READY)
OWNED -- valid renew --> OWNED same generation
OWNED -- release --> FREE
OWNED -- expiry --> FREE -> OWNED new generation
LOST/old generation -> NOT_READY + CONNECTION_CLOSED
old generation write/send -> FENCE_REJECTED
~~~

## 13. AuditRecord 扩展

在既有字段上增加：

| Field | Type | Notes |
|---|---|---|
| adapter_node_id | string/null | 长连接处理节点 |
| adapter_generation | int/null | 处理时 ownership generation |
| channel_identity_digest | sha256/null | 不含原始身份 |
| provider_message_digest | sha256/null | 不含 message_id/msgid 明文 |
| delivery_id | UUID/null | 交付关联 |
| delivery_attempt_no | int/null | 交付尝试 |
| delivery_status | enum/null | 最终或中间状态 |

新增决策值至少包括 received、self_message_ignored、
sender_identity_unverified、binding_rejected、not_addressed、
unsupported_message、delivery_retrying、delivered、delivery_failed、
delivery_unknown、adapter_lease_acquired、adapter_lease_lost 和
stale_adapter_rejected。

## 14. PostgreSQL 关系

~~~text
Tenant 1 ── * AgentApplication
Tenant 1 ── * ChannelBinding
AgentApplication 1 ── * ChannelBinding
ChannelBinding 1 ── * DeliveryRecord
DeliveryRecord 1 ── * DeliveryAttempt
Tenant 1 ── * AuditRecord
ExecutionResult 1 ── 0..1 DeliveryRecord per original channel
~~~

Migration 003_dual_im.sql 只做向后兼容的新增列、表、索引和约束；不删除第三阶段
字段。schema version 不匹配时 shared runtime fail closed。

## 15. Redis Key 规则

使用版本化、长度前缀摘要：

~~~text
trpc:v1:adapter-lease:<channel-identity-digest>
trpc:v1:adapter-generation:<channel-identity-digest>
trpc:v1:idempotency:<tenant-scope-digest>:<message-digest>
trpc:v1:session:<tenant-scope-digest>:<session-digest>
~~~

key 中不得出现原始 tenant、用户、会话、消息、Secret 或平台连接参数。

## 16. 验证不变量

1. 一个 Channel Identity 同时最多一个 ready owner。
2. tenant_id 只能由 active Binding 产生。
3. 同一业务消息 Agent 执行次数不超过 1。
4. 同一群聊不同 sender 的 Session 不相等。
5. Delivery 重试不改变 execution_trace_id，不增加 Agent 调用。
6. delivery_unknown 自动发送次数不再增加。
7. 旧 Adapter generation 的发送和写入全部失败。
8. 所有 Audit/日志/指标均不包含 Secret 或原始连接票据。

# Contract: 统一 IM 消息与 Gateway 接入

**Feature**：005-dual-im-real-channel-flow
**Rule**：所有真实 IM 在进入 Gateway 前必须转换为同一契约

## 1. Unified Inbound

~~~text
UnifiedInboundMessage {
  channel
  binding_id
  channel_identity_digest
  external_message_id
  external_user_id
  conversation_type
  external_conversation_id
  group_sender_id?
  message_type = text
  text
  received_at
  trace_id
}
~~~

验证：

- string 字段必须有界、非空；text 去除平台结构化 @ 后为 1..4000。
- received_at 必须 UTC-aware。
- group 必须有 group_sender_id；direct 不得伪造 group_sender_id。
- binding_id 由 Binding Repository 查询结果产生，不从 payload 读取。
- 不允许 tenant_id、Secret、token、完整原始 payload 或 SDK object。

## 2. Trusted Binding Resolution

~~~text
async resolve_by_channel_identity(identity, trace_id)
  -> VerifiedBindingScope + VerifiedTenantContext
  | BindingRejected
  | ConfigurationUnavailable
~~~

- PostgreSQL 是权威来源。
- 同一次一致性读取验证 Identity -> Binding -> Tenant -> Agent 所有权和 active 状态。
- 任一复合身份字段缺失/不匹配返回同一非披露 binding_rejected。
- 配置不可验证时 fail closed，不使用过期正向缓存授权。

## 3. Idempotency

~~~text
IdempotencyKey(
  tenant_id,
  channel,
  binding_id,
  external_message_id
)
~~~

- 飞书 external_message_id = message_id。
- 企业微信 external_message_id = msgid。
- 企微 req_id、飞书连接 ticket、trace_id 都不得替代业务 ID。
- 重复成功结果的 delivery_action 为 suppress，除非存在由 Delivery Repository
  明确授权的原结果交付恢复；恢复也不重新执行 Agent。

## 4. Session Identity

Direct:

~~~text
tenant + agent + channel + binding + direct + conversation
~~~

Group:

~~~text
tenant + agent + channel + binding + group + group_conversation + sender
~~~

- 输入使用长度前缀哈希，输出 sess_<sha256>。
- 同群不同 sender 的 Session 必须不同。
- 同一 sender 在同一群的连续消息必须相同。
- 相同外部 ID 跨 tenant/channel/binding 必须不同。

## 5. Gateway Contract

Adapter 调用：

~~~text
async GatewayService.handle_verified_message(
  verified_binding_scope,
  unified_inbound_message
) -> UnifiedReply
~~~

Gateway 继续负责：

- 权威租户上下文。
- Redis 幂等和 Session lease。
- Worker/Runner 执行。
- execution trace、finalization、recovery 和业务 Audit。

Adapter 不得复制上述逻辑。Gateway 不得调用供应商 SDK。

## 6. Unified Reply

~~~text
UnifiedReply {
  status
  trace_id
  original_trace_id?
  tenant_id?
  platform_session_id?
  external_message_id?
  text?
  delivery_action
  error?
}
~~~

- succeeded + deliver 才能新建交付意图。
- duplicate + suppress 不直接发送第二次回复。
- failed/processing/conflict 不得被 Adapter 当作正常 Agent 文本发送。
- 渠道发送状态不反向改写 Agent ExecutionResult。

## 7. Trace Contract

| Identifier | Meaning |
|---|---|
| trace_id | 本次 SDK 投递 |
| first_claim_trace_id | 首次建立幂等记录 |
| owner_trace_id | 当前消息 generation owner |
| execution_trace_id | 唯一 Agent 执行 |
| delivery_trace_id | 一次交付尝试 |

Delivery/Audit 必须能通过 execution_trace_id 回到原业务执行；重复和恢复不能生成新的
execution_trace_id。

## 8. Compatibility

- LOCAL_HTTP 保持现有请求、HMAC、状态和 OutboundReply 结构。
- 新 Channel 枚举不能改变既有序列化值。
- 既有 Repository 端口只做向后兼容扩展。
- 001、002、003 测试必须全部通过。

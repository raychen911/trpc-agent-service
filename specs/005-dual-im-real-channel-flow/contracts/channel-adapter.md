# Contract: 双 IM Channel Adapter

**Feature**：005-dual-im-real-channel-flow
**Purpose**：隔离飞书/企业微信 SDK，统一连接生命周期、可信身份、入站转换和出站发送

## 1. General Rules

- 飞书和企业微信必须是两个独立实现，但运行同一套行为契约测试。
- SDK 消息、连接、frame、异常和响应对象不得越过 channels 包。
- Adapter 不接收或信任外部 tenant_id。
- 所有公开领域对象均为不可变、extra=forbid 的 Pydantic 模型。
- Adapter 只有持有当前 Channel Identity ownership fence 时才可 ready、接收和发送。

## 2. ProviderClientPort

供应商客户端最小端口：

~~~text
async authenticate(secret: SecretBytes) -> RuntimeBotIdentity
async connect(on_event, on_disconnect, on_error) -> None
async close() -> None
async send_text(reply_context, text) -> ProviderSendAck
connection_state() -> disconnected | connecting | authenticated | ready
~~~

约束：

- authenticate/connect 不得把 Secret 写入 repr、异常、日志或状态对象。
- close 幂等；失租、认证失败和进程关闭均必须调用。
- send_text 返回平台无关 ACK；供应商异常在此边界映射。
- 测试替身可注入连接、认证、断线、ACK 成功/失败/超时。
- 明确网络断线按 1、2、4、8、16、30 秒封顶并附加最多 20% jitter 重连；
  稳定 60 秒后重置退避，失租/停止时可立即取消。
- 明确凭证无效进入 not_ready，不自动循环认证；仅在配置版本或 Secret 引用变化、
  或人工重启后再次认证。

## 3. ChannelAdapterPort

~~~text
async start(channel_identity, node_identity) -> AdapterReadiness
async stop(reason) -> None
async handle_provider_event(provider_event) -> AdapterEventResult
async deliver(delivery_intent, adapter_fence) -> DeliveryResult
readiness() -> ready | standby | not_ready
~~~

start 流程：

1. 验证配置只包含 secret_ref。
2. 竞争 AdapterOwnershipLease。
3. 只有 winner 解析 Secret 并认证 SDK。
4. 认证身份必须与配置 Channel Identity 一致。
5. 成功后进入 ready；standby 不建立活动连接。

stop/失租流程：

1. 立即从 ready 退出。
2. 禁止接受新事件和发送。
3. 关闭 SDK 连接。
4. 只有当前 owner 可释放租约；旧 owner release 不影响新代。

## 4. Inbound Filter Order

顺序固定：

~~~text
ownership fence
-> event/schema supported
-> authenticated sender complete
-> self-message comparison
-> direct/group classification
-> group structured mention check
-> normalized text validation
-> trusted Channel Identity extraction
-> Binding resolution
-> UnifiedInboundMessage
-> Gateway
~~~

前置过滤失败不得创建业务 Session 或调用 Agent。

| Condition | Result | Audit | Business reply |
|---|---|---|---|
| stale/no ownership | stale_adapter_rejected | diagnostic | no |
| unsupported event | unsupported_message | optional operational | no |
| sender is runtime bot | self_message_ignored | observable counter; no business session | no |
| sender incomplete/untrusted | sender_identity_unverified | required security audit | no |
| group without explicit @bot | not_addressed | counter | no |
| empty after mention removal | invalid_or_empty_text | stable audit | no normal reply |
| binding unknown/disabled/mismatch | binding_rejected | required security audit | no |

## 5. Feishu Mapping

| Platform value | Platform contract |
|---|---|
| authenticated tenant_key | ChannelIdentity.provider_tenant_key |
| authenticated app_id or bot_id | ChannelIdentity.provider_app_or_bot_id |
| message_id | external_message_id |
| sender open_id/user_id | external_user_id |
| chat_id | external_conversation_id and reply_target_id |
| chat type | ConversationType |
| structured mentions | group @ decision and bot mention removal |
| content_text/text content | normalized text |

Feishu send 使用 SDK send 能力向原 chat_id 发送文本。SDK 返回 ACK 前不标记 delivered。

## 6. WeCom Mapping

| Platform value | Platform contract |
|---|---|
| authenticated corp_id | ChannelIdentity.provider_tenant_key |
| configured/authenticated bot_id | ChannelIdentity.provider_app_or_bot_id |
| body msgid | external_message_id |
| authenticated sender userid | external_user_id |
| single userid or group chatid | external_conversation_id and reply_target_id |
| headers.req_id | ProviderReplyContext.protocol_request_id only |
| structured chat/sender fields | ConversationType and group @ decision |
| body.text.content | normalized text |

企业微信可在有效回调上下文使用 reply/reply_stream，恢复或跨连接发送必须使用可验证
的 reply_target_id 和 SDK 主动发送能力。req_id 永远不能替代 msgid 或 Session ID。

## 7. Provider Error Mapping

~~~text
ProviderTransientError
ProviderPermanentError
ProviderOutcomeUnknown
ProviderAuthenticationError
ProviderConnectionLost
ProviderProtocolError
~~~

- 限流、明确可重试服务不可用、连接前发送失败可映射 transient。
- 参数非法、权限拒绝、会话不存在等明确终态映射 permanent。
- 请求已发出但 ACK 超时/连接在 ACK 前断开映射 outcome_unknown。
- 未知 vendor exception 默认 outcome_unknown，不能乐观映射 transient。
- 公开消息固定且脱敏，不包含响应体、URL、token、ticket 或 frame。

## 8. Contract Test Matrix

两种 Adapter 必须共同通过：

1. 文本单聊转换与原会话回复。
2. 群聊明确 @ 转换，未 @ 忽略。
3. 自身身份过滤和身份不确定拒绝。
4. Channel Identity 完整、缺失、错配。
5. message_id/msgid 稳定，req_id 不参与企微幂等。
6. SDK 对象不泄漏到 Gateway mock。
7. 成功、transient、permanent、unknown 发送映射。
8. 失租后连接关闭、readiness=false、send 被拒绝。
9. 日志和异常中 Secret/URL 查询参数为 0。
10. close/start 生命周期幂等。

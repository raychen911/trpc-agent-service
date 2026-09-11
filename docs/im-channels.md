# 企业微信与飞书接入方案

## 1. 统一接入架构

平台使用 Channel Adapter 屏蔽 IM 协议差异。Adapter 负责回调认证、事件解析、身份提取和平台 API 调用，Gateway 负责租户绑定、Session 路由、限流及消息受理。Worker 仅处理统一的 `InboundMessage`，不依赖外部 XML 或事件 JSON 格式。

输入信封保存 `tenant_id`、`channel`、`account_id`、外部消息与用户 ID、聊天范围、文本、Session ID 和 trace ID。Runner 适配层将文本和已校验媒体转换为 `Content/Part`，完成后提取最终回答，由 Channel Dispatcher 生成分段回复并记录投递结果。

本文以仓库当前实现为范围。通道 API 的可用性仍受应用权限和目标环境配置影响，文中的分段阈值是本项目的设置，不应当作平台全部消息类型的通用限制。

## 2. 企业微信接入

### 2.1 企业应用 HTTP 回调

企业应用使用租户的 `wecom` binding 保存账号及凭据引用。回调地址为 `/webhook/{tenant_id}/wecom`，GET 用于 URL 校验，POST 用于接收消息。应用模式需要区分接收凭据与发送凭据：

| 配置 | 用途 |
|---|---|
| `account_id` | 标识平台保存的企业微信绑定 |
| `verify_token` | 回调 Token，生产配置使用 SecretRef |
| `encrypt_key_ref` | EncodingAESKey 引用，用于解密回调 |
| `corp_id` | 校验解密内容的接收方，并用于获取应用 access token |
| `agent_id`、`secret_ref` | 应用身份与 Secret 引用，用于发送回复 |

Adapter 对 Token、timestamp、nonce 以及存在时的 Encrypt 字段排序后计算 SHA1，并使用常量时间比较验签。加密回调通过 EncodingAESKey 解密，还会检查接收方标识。URL 校验完成后返回解密的 echostr，业务消息随后归一化为平台输入。

接入时先通过租户配置管理写入 binding，再在企业应用端配置对应 HTTPS 回调地址。验证通过后，应分别测试普通文本、加密回调、重复消息和无效签名。开发环境的示例 Token 不能直接用于生产。

应用回复通过 `corp_id` 和应用 Secret 获取 access token，再调用应用消息发送接口。当前发送代码使用 `touser` 指向外部用户，不是通用群聊发送接口。媒体接入支持根据 MediaId 下载临时资源，再交给平台 Artifact 流程处理。

### 2.2 智能机器人长连接

智能机器人使用 Bot ID 和 Secret 建立 WebSocket，不经过上述 HTTP 回调。它与企业应用的 Corp ID、Agent ID 和应用 Secret 是不同配置，不能相互替换。

本地配置示例：

```dotenv
TRPC_SERVICE_ROLE=all
TRPC_SERVICE_WECOM_BOT_ID=your-bot-id
TRPC_SERVICE_WECOM_BOT_SECRET_REF=env://WECOM_BOT_SECRET
WECOM_BOT_SECRET=replace-with-your-secret
TRPC_SERVICE_WECOM_BOT_TENANT_ID=acme
TRPC_SERVICE_IM_DRY_RUN=false
```

运行 `bash start.sh` 后，通过 `/health/ready` 的 `wecom_connections` 检查连接状态。生产凭据应由 Secret 管理系统注入，而不是保存在版本库中。

当前连接收到消息后保存原始回复上下文，发送阶段通过同一进程中的 SDK 客户端回复。因此实现要求 `role=all`，不能将接收与回复直接拆到不同 Worker。Bot 入口当前主要归一化文本，没有完整复用应用模式的媒体下载路径。

Bot 回复调用 `reply_stream`，但传入的是最终文本和完成标志。它使用了平台流式接口，并未实现模型逐 token 推送。进程重启也会丢失本地连接回复上下文，不能仅依赖数据库重放保证 Bot 回复恢复。

## 3. 飞书接入

### 3.1 HTTP Webhook

飞书 binding 使用 App ID 标识应用，使用 App Secret 引用获取发送凭据。Webhook 配置额外包含 Verification Token 和可选 Encrypt Key，接收地址为 `/webhook/{tenant_id}/feishu`。

Adapter 支持 URL challenge、Verification Token 校验和加密事件解析。配置 Encrypt Key 时，普通事件还需校验请求签名与时间戳。随后检查 App ID，过滤机器人自身消息，将 `im.message.receive_v1` 中的用户、消息和聊天标识转换为平台字段。

### 3.2 SDK 长连接

飞书长连接使用 App ID 和 App Secret，不需要公网回调地址。SDK 收到事件后将其放入本地有界缓冲区，主事件循环再解析并受理消息。回复仍通过 Open API 发送，不依赖接收连接中的原始帧。

这种处理减少了 SDK 回调等待时间，但确认与持久化之间存在进程内窗口。如果事件已确认而尚未写入 Inbox，进程退出可能导致丢失。不能将长连接模式描述为与持久化 HTTP 回调完全相同的确认保证。

### 3.3 回复与资源处理

飞书 Adapter 获取并缓存 `tenant_access_token`，调用原消息回复接口。凭据失效时会刷新并重试一次，限流和服务端错误采用有限退避。除文本外，Adapter 提供图片、文件、卡片、撤回及主动发送接口。具体配置、权限和验证步骤见 [飞书接入指南](feishu-setup.md)。

## 4. 企业微信与飞书的差异

| 比较项 | 企业微信 | 飞书 |
|---|---|---|
| 接入身份 | 企业应用使用 Corp ID、Agent ID 和应用 Secret。Bot 模式使用独立 Bot ID/Secret | 应用使用 App ID/App Secret，支持 Webhook 或 SDK 长连接接收 |
| HTTP 回调认证 | Token、timestamp、nonce 和密文参与 SHA1 验签，EncodingAESKey 用于解密与接收方校验 | Verification Token 校验，配置 Encrypt Key 时校验 SHA256 请求签名并处理加密事件 |
| 地址验证 | 校验并解密 echostr | 校验 URL verification 事件并返回 challenge |
| 入站格式 | 企业应用主要解析 XML，Bot 模式接收 WebSocket 帧 | Webhook 和 SDK 事件归一为事件 JSON |
| 用户和消息字段 | 应用使用 FromUserName、MsgId，群聊字段存在时使用 ChatId | 使用 sender_id、message_id、chat_id 和 chat_type |
| 应用回复 | 获取 access token 后调用应用消息接口，当前目标为用户 | 获取 tenant_access_token 后回复原消息，也可主动发送 |
| 长连接回复依赖 | 当前 Bot 回复依赖接收进程的连接上下文，要求 role=all | 回复走 Open API，不要求保留接收帧，但入口有本地缓冲窗口 |
| 本项目分段 | WeComAdapter 按 UTF-8 字节设置 2048 阈值 | FeishuAdapter 按字符设置 20000 阈值 |
| 媒体与卡片 | 应用模式支持临时媒体下载和带 media_id 的发送，Bot 媒体链路尚不完整 | 提供图片/文件资源接口、卡片、撤回和主动发送 |
| 失败处理 | 应用发送对限流及暂时错误有限重试，Bot 错误交由投递层处理 | 对 token 失效、限流、服务端错误和永久错误分别处理 |

## 5. 身份映射与 Session 隔离

平台使用已验证 binding 关联租户和 IM 账号，外部用户不能通过自报 tenant_id 越权。企业微信应用的 FromUserName 和飞书的 sender_id 被保存为外部用户标识，当前没有跨通道统一账号绑定服务。

`make_session_id` 使用租户、通道和聊天范围生成 HMAC。单聊范围为 `user:chat_id`，群聊范围为 `chat_id`，输出为截断后的不透明 ID。对企业应用而言，无 ChatId 时通常回退为发送用户 ID。Bot 帧另有解析路径，单群聊行为应通过实际事件联调确认。

不同租户生成不同 Session，同租户跨群 Session 分离，群内成员共享群上下文。Memory 当前按 `(tenant_id, external_user_id)` 保存，因此同一用户可能跨群复用记忆。多应用也未自动进入独立 Session key。涉及保密域的部署应拆分租户，或同时扩展 Session 与 Memory 作用域并迁移历史数据。

## 6. 异步处理、限制与恢复

生产 HTTP 回调在 Inbox/Outbox 提交后确认，再异步执行 Agent。重复消息由 Inbox 唯一键去重。系统通过 Session 租约串行提交结果，但没有按平台发送时间重排消息，严格顺序需求需要额外调度机制。

回复按通道阈值分段，保留分段序号和投递账本。限流与暂时失败采用有界重试，永久失败进入恢复流程。对于回执不确定的消息，应先查询状态或人工确认，不能盲目重发全部内容。

媒体只有经过租户资源下载、Artifact 保存与完整性检查后才传给 Runner。当前通用回复链路输出最终文本，不自动生成卡片或完整 token 流。飞书已有撤回接口，企业微信尚未提供对应的通用撤回适配。

企业微信实现依据见 [WeComAdapter](../trpc_service/channels/wecom.py) 和 [长连接管理](../trpc_service/channels/wecom_ws.py)，飞书实现依据见 [FeishuAdapter](../trpc_service/channels/feishu.py)。同步与恢复原理见 [一致性方案](consistency.md)。

# Gateway、跨节点路由与 IM 通道

## 请求路径

```text
Telegram / 企业微信
        │ webhook 验签、解密、归一化
        ▼
Agent Gateway ── ChannelBinding(active_version) ── SQL 控制面
        │
        ├── NodeDirectory：InMemory（开发）/ Redis（多节点）
        └── Rendezvous Hash(tenant:app:session)
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
      本地 Worker      HTTP 内部转发到远端 Worker
          │                   │
          └──── session 分布式锁 + 幂等键 ────┘
                              │
                         tRPC Runner
                              │
                  Event → State → Summary → Outbox
```

Gateway 是无状态入口。节点心跳记录包含 `node_id`、内部可达 URL、容量、环境和过期时间。
Rendezvous Hash 使健康节点集合不变时同 session 稳定选中同一节点；节点心跳过期后只重新映射受
影响的 session。这里的稳定选点是性能优化，不是正确性依赖：请求即使被重试到其他节点，仍会
经过 Redis 锁、`tenant_id:channel:external_message_id` 幂等键以及 SQL version CAS。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/gateway/v1/messages` | 受 `x-gateway-token` 保护的规范化消息入口 |
| GET | `/gateway/v1/nodes` | 查看健康节点 |
| GET | `/gateway/v1/routes/resolve` | 查看一个 session 当前选中的节点 |
| POST | `/internal/v1/messages` | 节点间转发；不出现在 OpenAPI，使用 `x-internal-token` |
| POST | `/webhooks/telegram/{account_id}` | Telegram webhook |
| GET/POST | `/webhooks/wecom/{account_id}` | 企业微信 URL 验证和消息回调 |

外部 webhook 不接受 tenant_id。平台使用 `channel_type + account_id` 查询 Agent 当前发布版本中
启用的 ChannelBinding，并核对数据库中的 `webhook_path`，由此得到 tenant/app。租户或应用被
禁用、绑定仍在草稿、版本已回滚或路径不匹配时均不会路由。

## Session 规则

- 私聊：`{channel}:{account_id}:direct:{sender_user_id}`。
- 群聊：`{channel}:{account_id}:group:{conversation_id}`。
- 群聊 Session 的持有者是 `group:{conversation_id}`，原始成员 ID 写入 Event。这使同群不同成员
  共享上下文，同时避免把 Session 错误绑定给第一个发言者。
- tenant_id 和 agent_app_id 是存储键的一部分，因此同一个外部用户跨租户、跨应用完全隔离。

## Telegram

Adapter 使用 `update_id` 作为 external_message_id，读取 `message` 或 `edited_message` 的 text/caption。
若 ChannelBinding 配置 `token_secret_ref`，请求必须携带匹配的
`X-Telegram-Bot-Api-Secret-Token`。返回值使用 Telegram webhook 允许的同步 `sendMessage` JSON。
群组、supergroup、channel 生成群聊 session；topic ID 被保留并用于回复。

推荐 ChannelBinding：

```json
{
  "channel_type": "telegram",
  "account_id": "support-bot",
  "webhook_path": "/webhooks/telegram/support-bot",
  "token_secret_ref": "env://TELEGRAM_WEBHOOK_SECRET",
  "secret_ref": "env://TELEGRAM_BOT_TOKEN",
  "enabled": true,
  "options": {}
}
```

## 企业微信

`token_secret_ref` 指向回调 Token，`secret_ref` 指向 43 字符 EncodingAESKey，`options.receive_id`
是企业自建应用的 CorpID。GET 验证会校验 `msg_signature`、解密 `echostr` 并原样返回；POST 会
验证 `sha1(sort(token,timestamp,nonce,encrypt))`，以 AES-256-CBC 解密 XML，并对被动文本回复
重新加密。XML 解析前拒绝 DTD 和 ENTITY。

推荐 ChannelBinding：

```json
{
  "channel_type": "wecom",
  "account_id": "corp-support",
  "webhook_path": "/webhooks/wecom/corp-support",
  "token_secret_ref": "env://WECOM_CALLBACK_TOKEN",
  "secret_ref": "env://WECOM_ENCODING_AES_KEY",
  "enabled": true,
  "options": {"receive_id": "ww0000000000000000"}
}
```

目前两个 Adapter 只执行文本输入和文本回复。媒体下载、Artifact 入库、主动异步发送、卡片、撤回
和长消息拆分属于下一阶段。

### 企业微信智能机器人 API（URL 回调模式）

智能机器人和自建应用复用 `channel_type=wecom`，由 `options.mode` 区分协议。智能机器人回调为
加密 JSON，ReceiveId 必须为空；平台校验签名、解密 `encrypt`、校验 `aibotid`，并以 `msgid`
执行幂等。文本、语音转写、图文混排、图片、文件和视频会归一化到平台消息；`event` 和 `stream`
控制回调验签后返回空 JSON，不创建 Agent Turn。

推荐 ChannelBinding：

```json
{
  "channel_type": "wecom",
  "account_id": "smart-support",
  "webhook_path": "/webhooks/wecom/smart-support",
  "token_secret_ref": "env://WECOM_AIBOT_CALLBACK_TOKEN",
  "secret_ref": "env://WECOM_AIBOT_ENCODING_AES_KEY",
  "enabled": true,
  "options": {
    "mode": "aibot",
    "aibot_id": "企业微信后台显示的机器人ID",
    "identity_mode": "passthrough"
  }
}
```

Webhook 只完成验签、解密和 Durable Inbox 入队，然后返回 `{}`。Agent 完成后，Outbox 使用签名
消息携带的单次、短期 `response_url` 发送 Markdown 回复；投递器仅允许
`https://qyapi.weixin.qq.com/cgi-bin/aibot/response`，防止 SSRF。`response_url` 不写入长期
Session Event。当前实现针对 URL 回调模式，不包含 BotID + Secret 的 WebSocket 长连接客户端。

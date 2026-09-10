# 第 6 步：企业微信与 Telegram 配置

运行时代码和无凭据协议测试已经完成。推荐使用无需公网地址的 Pull 模式：Telegram 通过
`getUpdates` long polling 收消息，企业微信智能机器人通过 WebSocket 主动连接。原有 HTTPS
Webhook 方案仍然保留为可选部署方式。

## 通用准备

先按 README 配好真实模型、独立 Admin API Key 和 Session HMAC Key，启动服务后创建租户及
Agent App。本地 IM 密钥填写在被 Git 忽略的 `.env` 中，数据库只保存 `env://...` 引用。

创建 Binding 时必须指定现有的 `app_id`。Webhook 收到请求后使用 URL 里的账号标识反查
Binding，由 Binding 决定 `tenant_id` 和 `app_id`，不会信任请求体中的租户信息。

## Telegram

需要准备：

- BotFather 创建的 Bot Token；
- 一个不会变化的 `account_id`，例如 `telegram-main`；

在本地 `.env` 中填写：

```dotenv
TRPC_TELEGRAM_BOT_TOKEN=真实BotToken
```

通过 Admin API 创建 Binding，字段含义如下：

```json
{
  "binding_id": "telegram-main",
  "app_id": "assistant",
  "channel_type": "telegram",
  "connection_mode": "pull",
  "account_id": "telegram-main",
  "token_ref": "env://TRPC_TELEGRAM_BOT_TOKEN",
  "webhook_path": ""
}
```

Pull 模式不能与 Telegram webhook 同时使用。如果此前注册过 webhook，应先通过 Bot API
`deleteWebhook` 删除；当前已验证的机器人没有设置 webhook。启动 `run-channels` 后向 Bot
发送文本即可。

## 企业微信智能机器人 AIBot

需要准备：

- 企业微信智能机器人的 Bot ID；
- 对应的 Bot Secret。

同样在 `.env` 中填写：

```dotenv
TRPC_WECOM_BOT_SECRET=真实BotSecret
```

创建 Binding；`account_id` 填单段 Bot ID，不要重复粘贴：

```json
{
  "binding_id": "wecom-main",
  "app_id": "assistant",
  "channel_type": "wecom",
  "connection_mode": "pull",
  "account_id": "你的单段 AIBot Bot ID",
  "secret_ref": "env://TRPC_WECOM_BOT_SECRET",
  "webhook_path": ""
}
```

`run-channels` 使用 `wecom-aibot-sdk-python` 主动连接
`wss://openws.work.weixin.qq.com`，完成认证、心跳和自动重连，无需配置回调 URL。

## 启动 Pull 模式

数据库中至少要有一个租户、一个 Agent App 和上述 Pull Binding。在已经填写 `.env`
的项目目录中运行；脚本会自动加载同一份配置：

```sh
uv run --frozen python -m trpc_service._cli init-db
sh channels.sh
```

HTTP Admin API 仅在首次创建租户、App 和 Binding 时需要运行；IM Worker 工作时不要求同时启动
Web 服务。两个进程也可以共享同一个小规模 SQLite 数据文件。

## 可选：Webhook 模式

如果以后具备公网 HTTPS，可继续使用 `connection_mode=webhook`。Telegram 额外配置 webhook
secret；企业微信内部应用则需要 Corp ID、应用 Secret、回调 Token 和 EncodingAESKey。两种
企业微信模式是不同产品协议，AIBot Bot ID/Secret 不能填入内部应用回调字段。

## 验收与排错

- `no active pull-mode channel bindings found`：尚未创建 `connection_mode=pull` 的 Binding。
- Telegram `Conflict`：该 Bot 仍注册了 webhook，先调用 `deleteWebhook`。
- `WeCom AIBot authentication timed out`：Bot ID 重复、Secret 不匹配或机器人已被禁用。
- `401 invalid signature`：Webhook 模式签名与 Binding 对应 Secret 不一致。
- `404 binding not found`：URL 中的账号标识没有激活的 Binding，或必要 Secret 引用未配置。
- `503` 或模型运行错误：检查真实模型、Admin 和 Session HMAC 的 Secret 引用。
- `/metrics`：确认能看到 `trpc_service_requests_total`、`trpc_service_agent_seconds` 和
  `trpc_service_channel_sends_total`。
- SQLite `inbound_message.status`：正常发送最终为 `delivered`；发送失败为 `send_failed`。

最终保留不含密钥的证据：两个长连接认证成功、真实用户消息与机器人回复、对应 `trace_id` 和
审计记录。小规模验收可继续使用 SQLite；多副本部署则使用 PostgreSQL、Redis Stream 和独立
Worker，详见 [Kubernetes 多副本部署](kubernetes-deployment.md)。Pull 通道本身保持一个副本，
避免同一账号被多个长轮询连接重复消费。

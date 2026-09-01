# 真实 IM 联调手册

本项目的 Web UI 只用于本地验证 IM 流程，不替代真实 IM Adapter。真实联调建议
先选择 Telegram + 企业微信，二者分别验证一个国际 IM 和一个企业 IM。

## 1. 公网回调地址

平台服务器不能访问本机的 `127.0.0.1`。真实联调必须满足：

- 服务部署在有公网 HTTPS 域名的服务器；或
- 使用内网穿透，将本机端口映射为公网 HTTPS 地址。

Windows 本地启动并允许隧道访问：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\start-web-ui.ps1 --host 0.0.0.0 --port 18001
```

假设公网地址为：

```text
https://im.example.com
```

本项目回调路径固定为：

```text
https://im.example.com/webhooks/{channel}/{account_id}
```

例如：

```text
https://im.example.com/webhooks/telegram/telegram_bot_1
https://im.example.com/webhooks/wecom/wecom_app_1
https://im.example.com/webhooks/wechat_official_account/wechat_official_1
```

微信和企业微信的 GET 回调校验已经实现，POST 消息回调也使用同一 URL。

## 2. 启动前配置密钥

配置只保存 `secret://` 引用，密钥通过环境变量、Vault 或生产密钥系统提供。
本地 PowerShell 示例：

```powershell
function New-RandomHex([int]$Length = 32) {
  $bytes = New-Object byte[] $Length
  $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
  try {
    $rng.GetBytes($bytes)
  } finally {
    $rng.Dispose()
  }
  return ([BitConverter]::ToString($bytes) -replace "-", "").ToLowerInvariant()
}

$env:ADMIN_API_KEY = "<new-local-admin-key>"
$env:SECRET_TENANT_DEMO_TELEGRAM_TOKEN = "Telegram BotFather token"
$env:SECRET_TENANT_DEMO_TELEGRAM_WEBHOOK_SECRET = New-RandomHex
$env:SECRET_TENANT_DEMO_WECOM_TOKEN = "企业微信回调 Token"
$env:SECRET_TENANT_DEMO_WECOM_AES_KEY = "企业微信 EncodingAESKey"
$env:SECRET_TENANT_DEMO_WECOM_CORP_SECRET = "企业微信自建应用 Secret"
```

不要把真实 token、secret 写入 Git、租户 JSON、日志或截图。

## 3. 创建并发布 Telegram 绑定

Telegram 需要先通过 BotFather 创建 Bot，得到 Bot Token。将 Bot Token 放入
`SECRET_TENANT_DEMO_TELEGRAM_TOKEN`。

创建绑定：

```powershell
$headers = @{
  "X-Admin-API-Key" = $env:ADMIN_API_KEY
  "Content-Type" = "application/json"
}
$body = @{
  channel = "telegram"
  account_id = "telegram_bot_1"
  agent_app_id = "app_support"
  token_ref = "secret://tenant_demo/telegram/token"
  secret_ref = "secret://tenant_demo/telegram/webhook-secret"
  config = @{
    sdk_enabled = $true
  }
} | ConvertTo-Json -Depth 8
$created = Invoke-RestMethod `
  -Uri "http://127.0.0.1:18001/admin/v1/tenants/tenant_demo/channels" `
  -Method Post -Headers $headers -Body $body
$created.config_version
```

`add_channel` 会生成新配置版本。必须发布该版本：

```powershell
$version = [int]$created.config_version
Invoke-RestMethod `
  -Uri "http://127.0.0.1:18001/admin/v1/tenants/tenant_demo/publish" `
  -Method Post -Headers $headers `
  -Body (@{version = $version} | ConvertTo-Json)
```

设置 Telegram webhook：

```powershell
$telegramToken = $env:SECRET_TENANT_DEMO_TELEGRAM_TOKEN
$telegramSecret = $env:SECRET_TENANT_DEMO_TELEGRAM_WEBHOOK_SECRET
$webhook = "https://im.example.com/webhooks/telegram/telegram_bot_1"
$telegramBody = @{
  url = $webhook
  secret_token = $telegramSecret
  drop_pending_updates = $true
} | ConvertTo-Json
Invoke-RestMethod `
  -Uri "https://api.telegram.org/bot$telegramToken/setWebhook" `
  -Method Post -ContentType "application/json" -Body $telegramBody
```

然后在 Telegram 中给 Bot 发消息。请求会进入：

```text
Telegram webhook -> TelegramAdapter -> Gateway -> Worker -> Session/Memory
-> python-telegram-bot -> Telegram 回复
```

Telegram webhook 的 Header `X-Telegram-Bot-Api-Secret-Token` 会被 Adapter 校验，
不会写入业务事件。

## 4. 创建并发布企业微信绑定

在企业微信管理后台创建自建应用，准备：

- 企业 ID `CorpID`
- 应用 `AgentId`
- 应用 Secret
- 接收消息配置中的 Token
- 接收消息配置中的 EncodingAESKey

设置密钥环境变量：

```powershell
$env:SECRET_TENANT_DEMO_WECOM_TOKEN = "回调 Token"
$env:SECRET_TENANT_DEMO_WECOM_AES_KEY = "EncodingAESKey"
$env:SECRET_TENANT_DEMO_WECOM_CORP_SECRET = "应用 Secret"
```

创建绑定：

```powershell
$body = @{
  channel = "wecom"
  account_id = "wecom_app_1"
  agent_app_id = "app_support"
  token_ref = "secret://tenant_demo/wecom/token"
  config = @{
    sdk_enabled = $true
    corp_id = "wwxxxxxxxxxxxxxxxx"
    agent_id = 1000001
    aes_key_ref = "secret://tenant_demo/wecom/aes-key"
    corp_secret_ref = "secret://tenant_demo/wecom/corp-secret"
  }
} | ConvertTo-Json -Depth 8
$created = Invoke-RestMethod `
  -Uri "http://127.0.0.1:18001/admin/v1/tenants/tenant_demo/channels" `
  -Method Post -Headers $headers -Body $body
$version = [int]$created.config_version
Invoke-RestMethod `
  -Uri "http://127.0.0.1:18001/admin/v1/tenants/tenant_demo/publish" `
  -Method Post -Headers $headers `
  -Body (@{version = $version} | ConvertTo-Json)
```

在企业微信应用的接收消息配置中填写：

```text
URL:              https://im.example.com/webhooks/wecom/wecom_app_1
Token:            与 SECRET_TENANT_DEMO_WECOM_TOKEN 相同
EncodingAESKey:   与 SECRET_TENANT_DEMO_WECOM_AES_KEY 相同
```

平台保存配置时会调用 GET 握手。服务会校验签名并返回 `echostr`，配置成功后再
在企业微信中向应用发送文本消息。

出站回复优先使用 `wechatpy.enterprise`：

```text
企业微信回调 -> WeComAdapter -> Gateway -> Worker
-> wechatpy.enterprise.WeChatClient.message.send_text -> 企业微信用户
```

如果只接企业微信机器人 webhook，不走自建应用 SDK，机器人 URL 中的 `key=`
必须放入密钥系统，并在绑定中使用引用：

```json
{
  "channel": "wecom",
  "account_id": "wecom_robot_1",
  "agent_app_id": "app_support",
  "config": {
    "sdk_enabled": false,
    "webhook_url_ref": "secret://tenant_demo/wecom/robot-webhook-url"
  }
}
```

不要在 `config.webhook_url` 中保存带 `key=` 的真实 URL；租户配置校验会拒绝
这类明文密钥 URL。

## 5. 微信公众号可选联调

微信公众号使用同一个 webhook 模式。绑定配置需要：

```json
{
  "channel": "wechat_official_account",
  "account_id": "wechat_official_1",
  "agent_app_id": "app_support",
  "token_ref": "secret://tenant_demo/wechat/token",
  "config": {
    "sdk_enabled": true,
    "app_id": "wxxxxxxxxxxxxxxxx",
    "app_secret_ref": "secret://tenant_demo/wechat/app-secret",
    "aes_key_ref": "secret://tenant_demo/wechat/aes-key"
  }
}
```

密钥环境变量对应：

```powershell
$env:SECRET_TENANT_DEMO_WECHAT_TOKEN = "公众号回调 Token"
$env:SECRET_TENANT_DEMO_WECHAT_APP_SECRET = "公众号 AppSecret"
$env:SECRET_TENANT_DEMO_WECHAT_AES_KEY = "公众号 EncodingAESKey"
```

回调地址：

```text
https://im.example.com/webhooks/wechat_official_account/wechat_official_1
```

微信公众号出站文本优先使用 `wechatpy.WeChatClient.message.send_text`。

## 6. 入站媒体和附件联调

外部 IM 发图片、文件或语音时，Adapter 会先把平台字段归一化成 `Attachment`，再由 Gateway 在进入 Worker 前写入租户 Artifact 后端。Worker 只看到 `artifact_id`、`size`、`content_type` 和来源元数据，不依赖平台短时 URL。

- Telegram：支持 `photo`、`document`、`voice`。服务通过 `file_id` 调用 `getFile`，再下载 `file_path` 对应的文件内容。真实联调必须配置 `token_ref` 指向 Bot Token。
- 企业微信：支持 `MsgType=image` 的 `PicUrl` / `MediaId`，以及带 `MediaId` 或 `FileId` 的文件类消息。服务优先使用 `media_id` 走 `/cgi-bin/media/get` 下载。
- 微信公众号：支持 `MsgType=image` 的 `PicUrl` / `MediaId`，其他带 `MediaId` 的消息会作为文件类附件归一化。
- 微信客服：支持图片类 `MediaId` 入站；出站仍按平台能力限制，图片可发，普通文件会明确失败。
- Web UI IM：支持本地上传文件和图片，直接以 `content_base64` 进入同一条 Artifact 持久化链路，用于本地验证附件流程。

租户可配置 `attachment_host_allowlist` 允许普通 HTTPS URL 下载；平台专用的 `file_id` / `media_id` 下载不依赖该 allowlist，但仍受 `MAX_ATTACHMENT_BYTES` 限制。落库后会移除 `content_base64` 和临时 URL，避免把大文件和敏感短链写进 Session event。出站文本按 `max_message_length` 分片、按账号 QPS 限流并进入重试/DLQ。

出站媒体能力按平台区分：

- Telegram：SDK 和 HTTP 两条路径均支持图片和普通文件，分别调用 `send_photo` 和 `send_document`。
- 企业微信应用：通过临时素材上传后发送图片或文件；`wechatpy.enterprise` 上传时传入二进制文件对象。
- 企业微信机器人 webhook：支持图片；不支持原生文件消息，文件会明确失败。
- 微信公众号：通过临时素材上传后发送图片；客服消息接口不提供原生文件消息，文件会明确失败。
- 微信客服：通过临时素材上传后发送图片；客服消息接口不提供原生文件消息，文件会明确失败。

所有已支持的外部媒体发送都复用 `10 MB` 大小限制、账号限流、指数退避重试和死信记录。平台不支持的原生媒体类型会返回失败并进入重试/DLQ 流程，不会伪装成文本成功。

## 7. 严格配置预检

默认 demo 为了便于本地启动，不强制要求所有真实 IM 密钥都存在。准备真实账号联调或生产发布前，建议开启：

```powershell
$env:STRICT_CHANNEL_CONFIG = "1"
$env:VALIDATE_SECRETS = "1"
```

开启后租户配置保存和发布会做更严格校验：

- 所有 `token_ref`、`secret_ref`、`*_ref` 字段必须是 `secret://` 引用。
- `VALIDATE_SECRETS=1` 时会真实解析引用，提前发现环境变量、Vault 或密钥系统缺项。
- Telegram 必须配置 Bot Token 和 webhook secret。
- 企业微信必须配置 `corp_id`、`agent_id`、`corp_secret_ref`、回调 `token_ref`、`aes_key_ref`。
- 微信公众号必须配置 `app_id`、`app_secret_ref`、回调 `token_ref`、`aes_key_ref`。
- 微信客服必须配置 `access_token_ref` 或 `token_ref`，加密回调场景还应配置 `aes_key_ref`。

真实联调不建议承诺代码层面百分百保证。最终通过率还取决于公网 HTTPS、平台后台权限、账号白名单、回调加密模式、平台频率限制和素材接口权限。当前代码已覆盖协议解析、验签、媒体下载、Artifact 落库、幂等、重试、DLQ 和密钥脱敏；剩余风险应通过真实账号 smoke test 消除。

## 8. 联调验收清单

- `/health` 返回 `status=ok`。
- 平台 GET webhook 校验成功。
- 真实用户发一条文本消息，服务日志没有明文 token/secret。
- Gateway 根据 `channel + account_id` 找到正确租户。
- Session ID 按租户、账号和会话隔离。
- 用户身份可在 Channel Binding 的 `config.identity_mapping.external_to_internal`
  中从外部 IM ID 映射到租户内部用户 ID；未配置时使用外部 ID。
- 重复投递同一个外部消息 ID 不会重复执行模型。
- Worker 完成模型调用并写入 Session / Memory。
- Adapter 使用 SDK 发送回复。
- Telegram 发送图片、文件、语音后，Session event 中附件包含 `artifact_id`。
- 企业微信或微信公众号发送图片后，`MediaId` 能下载并写入 Artifact。
- Web UI 上传图片和文件后，也走同一条 Artifact 持久化链路。
- `/metrics` 中能看到请求、模型、投递和错误指标。
- 失败时能在 `data/dead-letter/` 或配置的存储后端看到重试/死信记录。
- 发送撤回/withdraw/recall/delete 类事件时，只生成 `message_revoked` 和审计记录，不触发模型执行或出站回复。

## 9. 撤回与失败重试

Adapter 会把企业微信、微信公众号、微信客服、Telegram 或 Web UI 中的 `Event`、`event`、`ChangeType`、`action`、`event_type`、`MsgType` 统一归一化。`revoke`、`withdraw`、`recall`、`delete`、`message_revoke` 会进入撤回分支，`target_message_id`、`revoke_message_id`、`withdraw_message_id`、`MsgId`、`message_id` 会被识别为被撤回消息。

企业微信和微信公众号通常通过 XML/加密 XML 回调携带 `Event` 或 `ChangeType`，Telegram 更常见的是编辑、删除或业务侧模拟事件，Web UI/JSON 调试可直接传 `normalized_event_type=revoke`。撤回不物理删除历史消息和审计，只追加不可变事件，避免合规链路断裂。IM 平台重复投递撤回事件时仍使用 `tenant_id + channel + account_id + external_message_id` 幂等去重。

## 10. 常见问题

### GET 校验失败

检查公网 HTTPS 是否可访问、URL 是否带正确的 `channel/account_id`、Token 是否
一致，以及企业微信/微信的 AES key 是否完整配置。

### 收到消息但没有回复

依次检查：租户绑定是否已 publish、用户是否在 `allowed_user_ids` 中、模型配置
是否可用、平台应用是否有发消息权限、SDK Secret 是否能通过 `secret://` 解析。

### 本地 Web UI 正常，真实 IM 不通

这通常是公网回调地址、防火墙、HTTPS 证书或平台后台权限问题，不是 Web UI
自测链路问题。

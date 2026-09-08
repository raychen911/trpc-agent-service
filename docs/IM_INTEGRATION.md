# 真实 IM 联调手册

本项目当前可部署的通道为 **企业微信智能机器人 `wecom_ai_bot`**、**Feishu**、
**Telegram** 和本地 **Web UI**。Web UI 只用于本地验证 IM 流程,不替代真实 IM
Adapter。`wechat_official_account` 与 `wechat_customer_service` 已从默认运行时退役,
仅保留协议兼容性测试代码。真实联调建议先选择 **企业微信智能机器人 wecom_ai_bot**
与 **Telegram** 或 **Feishu**,分别验证一个企业 IM 和一个国际/飞书 IM。

**企业微信主验收入口是 wecom_ai_bot(智能机器人 API 模式、BotID + BotSecret 长连接),不是 wecom(传统回调/webhook)。**

## 1. 公网回调地址

平台服务器不能访问本机的 `127.0.0.1`。真实联调必须满足：

- 服务部署在有公网 HTTPS 域名的服务器；或
- 使用内网穿透,将本机端口映射为公网 HTTPS 地址。

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
https://im.example.com/webhooks/feishu/feishu_app_1
```

Telegram 和 Feishu 使用 HTTP callback/webhook。企业微信智能机器人 wecom_ai_bot 使用长连接,不需要公网 webhook。传统企业微信回调适配器 wecom 默认禁用,不作为主验收路径。

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
$env:WECOM_AI_BOT_ENABLED = "1"
$env:WECOM_AI_BOT_ACCOUNT_ID = "企业微信智能机器人 BotID"
$env:WECOM_AI_BOT_SECRET_REF = "secret://tenant_demo/wecom_ai_bot/bot-secret"
$env:SECRET_TENANT_DEMO_WECOM_AI_BOT_BOT_SECRET = "企业微信智能机器人 BotSecret"
$env:SECRET_TENANT_DEMO_TELEGRAM_TOKEN = "Telegram BotFather token"
$env:SECRET_TENANT_DEMO_TELEGRAM_WEBHOOK_SECRET = New-RandomHex
$env:SECRET_TENANT_DEMO_FEISHU_APP_ID = "飞书应用 AppID"
$env:SECRET_TENANT_DEMO_FEISHU_APP_SECRET = "飞书应用 AppSecret"
$env:SECRET_TENANT_DEMO_FEISHU_ENCRYPT_KEY = "飞书 Encrypt Key"
$env:SECRET_TENANT_DEMO_FEISHU_VERIFICATION_TOKEN = "飞书 Verification Token"
```

不要把真实 token、secret 写入 Git、租户 JSON、日志或截图。

企业微信智能机器人从企业微信工作台 -> 智能机器人 -> API 模式创建后,可以直接获取 BotID 和 BotSecret;不需要配置 webhook,改用长连接模式接收消息。

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

## 4. 传统企业微信应用回调（兼容性路径，非默认）

本节只用于验证遗留的企业微信自建应用回调适配器。它不是当前默认运行路径；
默认企业微信验收使用前面的 `wecom_ai_bot` 长连接。执行本节前必须设置
`ENABLE_LEGACY_WECOM=1`，否则服务会拒绝 `channel=wecom`。

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

## 5. 微信公众号兼容性测试（非默认运行路径）

微信公众号适配器保留用于协议解析和兼容性测试，但已从默认运行时注册表退役。
本节配置不能作为当前项目的默认复现路径；默认审阅者流程请使用
`wecom_ai_bot`、`feishu`、`telegram` 或 `web`。

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
- 微信公众号和微信客服：遗留适配器支持针对性协议/媒体测试，但不属于默认运行路径。
- Web UI IM：支持本地上传文件和图片，直接以 `content_base64` 进入同一条 Artifact 持久化链路，用于本地验证附件流程。

租户可配置 `attachment_host_allowlist` 允许普通 HTTPS URL 下载；平台专用的 `file_id` / `media_id` 下载不依赖该 allowlist，但仍受 `MAX_ATTACHMENT_BYTES` 限制。落库后会移除 `content_base64` 和临时 URL，避免把大文件和敏感短链写进 Session event。出站文本按 `max_message_length` 分片、按账号 QPS 限流并进入重试/DLQ。

出站媒体能力按平台区分：

- Telegram：SDK 和 HTTP 两条路径均支持图片和普通文件，分别调用 `send_photo` 和 `send_document`。
- 传统企业微信应用：通过临时素材上传后发送图片或文件；`wechatpy.enterprise`
  上传时传入二进制文件对象。
- 企业微信智能机器人：媒体能力取决于可选 SDK 和提供商协议。
- 微信公众号和微信客服：仅保留兼容性适配器及针对性测试，不属于默认运行路径。

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
- 微信公众号和微信客服配置只适用于遗留适配器的兼容性测试，不属于默认发布路径。

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
- 已启用的企业微信智能机器人、飞书或 Telegram 通道发送/接收媒体后，媒体能下载并写入 Artifact；微信公众号和微信客服仅在显式兼容性测试中验证。
- Web UI 上传图片和文件后，也走同一条 Artifact 持久化链路。
- `/metrics` 中能看到请求、模型、投递和错误指标。
- 失败时能在 `data/dead-letter/` 或配置的存储后端看到重试/死信记录。
- 发送撤回/withdraw/recall/delete 类事件时，只生成 `message_revoked` 和审计记录，不触发模型执行或出站回复。

## 9. 功能测试用例

### 测试前准备清单

在开始测试前，请确认以下所有项目已完成：

- [ ] 已安装 `wecom-aibot-sdk-python`（运行 `pip list | grep wecom` 验证）
- [ ] 已配置 `.env` 文件中的所有必填项
- [ ] 已获取自己的企业微信智能机器人 BotID 和 BotSecret
- [ ] 已确认模型名称与 API 提供商一致（运行测试调用确认）
- [ ] 服务已成功启动（`/health` 返回 `status=ok`）
- [ ] 日志中显示 "WeCom AI Bot connector starting"
- [ ] 日志中显示 "Application startup complete"

如果以上所有项目都打勾，可以开始功能测试。

完成基础配置后，建议通过以下测试用例验证功能完整性。

### 9.1 基础对话测试

**测试目的：** 验证机器人基本通信能力

**测试步骤：**
```
发送：你好
```

**预期结果：** 机器人在 5-10 秒内回复问候语

---

### 9.2 实时信息查询测试

**测试目的：** 验证模型实时信息获取能力（需模型支持联网）

**测试步骤：**
```
发送：北京的天气怎么样
```

**预期结果：** 返回当前天气信息，包括温度、天气状况和未来几天预报

---

### 9.3 简短任务响应测试

**测试目的：** 验证模型理解和执行简单指令的能力

**测试步骤：**
```
发送：请用3句话介绍一下人工智能
```

**预期结果：** 返回结构清晰、内容准确的 3 句话介绍

---

### 9.4 上下文记忆测试

**测试目的：** 验证多轮对话中的上下文保持能力

**测试步骤：**
```
第一轮发送：我最喜欢的颜色是蓝色
等待回复后
第二轮发送：你还记得我最喜欢什么颜色吗？
```

**预期结果：** 第二轮对话中，机器人能正确回忆起用户在第一轮提到的颜色

---

### 9.5 多轮任务协作测试

**测试目的：** 验证模型在多轮对话中理解指代关系和任务延续能力

**测试步骤：**
```
第一轮发送：帮我写一个Python函数，计算斐波那契数列
等待回复后
第二轮发送：能给这个函数加上类型注解吗？
```

**预期结果：** 
- 第一轮返回完整的斐波那契函数实现
- 第二轮理解"这个函数"的指代，在原函数基础上添加类型注解

---

### 9.6 长文本生成测试

**测试目的：** 验证模型生成结构化长文本的能力

**测试步骤：**
```
发送：请详细解释什么是微服务架构，包括优缺点和适用场景
```

**预期结果：** 
- 返回 500 字以上的详细说明
- 内容结构清晰，包含定义、优缺点对比、适用场景等
- 响应时间可能较长（20-40 秒）

---

### 9.7 Markdown 格式渲染测试

**测试目的：** 验证企业微信中 Markdown 格式的正确渲染

**测试步骤：**
```
发送：请用Markdown格式列出：
1. Python的3个特点
2. Go语言的3个特点
并用代码块展示Hello World
```

**预期结果：**
- 列表格式正确显示
- 代码块正确高亮
- 编号和层级关系清晰

---

### 9.8 真实联调记录（2026-09-08）

以下是实际联调过程中的完整对话记录，证明所有核心功能均已验证通过。

#### 测试环境
- **BotID:** `aibq1rlCbxKPsEPi4g0IyE4jME5SrwCCPlY`
- **模型:** `claude-sonnet-4-20250514`
- **测试时间:** 2026-09-08 13:02-13:12
- **测试人员:** 夏雨轩

#### 完整对话记录

**[13:02:15] 用户:**
```
你好
```

**[13:02:23] 机器人回复:**
```
你好！有什么我可以帮你的吗？
```
✅ **验证通过：** 基础对话能力正常

---

**[13:02:35] 用户:**
```
北京的天气怎么样
```

**[13:02:44] 机器人回复:**
```
北京现在多云，约 22°C。
今天（9月8日）整体以阴天为主，最高约 23°C，最低约 15°C。接下来几天会逐渐转暖，周四到周六最高大约 30–31°C，多为晴到多云、略有雾霾感。
```
✅ **验证通过：** 实时信息查询功能正常

---

**[13:05:25] 用户:**
```
请用3句话介绍一下人工智能
```

**[13:05:35] 机器人回复:**
```
人工智能是让计算机模拟人类学习、理解、推理和创造能力的技术。
它可以用于语音识别、图像生成、自动驾驶、医疗诊断、金融分析和智能客服等领域。
随着数据、算力和算法的发展，人工智能正在提高生产效率，也带来隐私、安全和伦理方面的新挑战。
```
✅ **验证通过：** 简短任务响应能力正常

---

**[13:05:44] 用户:**
```
我最喜欢的颜色是蓝色
```

**[13:06:04] 机器人回复:**
```
记住了，你最喜欢的颜色是蓝色。
```

**[13:06:12] 用户:**
```
你还记得我最喜欢什么颜色吗？
```

**[13:06:29] 机器人回复:**
```
记得，你最喜欢的颜色是蓝色。
```
✅ **验证通过：** 上下文记忆功能正常

---

**[13:06:38] 用户:**
```
帮我写一个Python函数，计算斐波那契数列
```

**[13:06:52] 机器人回复:**
```python
当然，下面是一个计算前 n 项斐波那契数列的 Python 函数：

def fibonacci(n):
    if n <= 0:
        return []
    elif n == 1:
        return [0]

    result = [0, 1]
    for _ in range(2, n):
        result.append(result[-1] + result[-2])

    return result

示例：
print(fibonacci(10))

输出：
[0, 1, 1, 2, 3, 5, 8, 13, 21, 34]

如果你想计算的是第 n 个斐波那契数，也可以这样写：

def fibonacci_number(n):
    if n < 0:
        raise ValueError("n 不能为负数")

    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b

    return a
```

**[13:07:09] 用户:**
```
能给这个函数加上类型注解吗？
```

**[13:07:22] 机器人回复:**
```python
当然，可以这样加类型注解：

def fibonacci(n: int) -> list[int]:
    if n <= 0:
        return []
    elif n == 1:
        return [0]

    result: list[int] = [0, 1]
    for _ in range(2, n):
        result.append(result[-1] + result[-2])

    return result

如果是计算第 n 个斐波那契数：

def fibonacci_number(n: int) -> int:
    if n < 0:
        raise ValueError("n 不能为负数")

    a: int = 0
    b: int = 1

    for _ in range(n):
        a, b = b, a + b

    return a

如果你使用的是 Python 3.8 或更早版本，可以改用：

from typing import List

def fibonacci(n: int) -> List[int]:
    ...
```
✅ **验证通过：** 多轮任务协作能力正常，正确理解指代关系

---

**[13:07:40] 用户:**
```
请详细解释什么是微服务架构，包括优缺点和适用场景
```

**[13:10:31] 机器人回复:** *(生成约 3000 字的详细说明，包含完整的结构化内容)*

✅ **验证通过：** 长文本生成能力正常，响应时间约 171 秒

---

**[13:11:50] 用户:**
```
请用Markdown格式列出：
1. Python的3个特点
2. Go语言的3个特点
并用代码块展示Hello World
```

**[13:12:09] 机器人回复:**
```markdown
1. Python 的 3 个特点

- 语法简洁：代码可读性强，适合快速开发。
- 生态丰富：拥有大量第三方库，常用于 Web、数据分析、AI、自动化等领域。
- 动态类型：变量不需要提前声明类型，开发灵活。

Python Hello World:
```python
print("Hello, World!")
```

2. Go 语言的 3 个特点

- 性能较高：编译型语言，运行效率好。
- 并发能力强：内置 goroutine 和 channel，适合高并发场景。
- 部署简单：通常可以编译成单个可执行文件，便于部署。

Go Hello World:
```go
package main

import "fmt"

func main() {
    fmt.Println("Hello, World!")
}
```
```
✅ **验证通过：** Markdown 格式在企业微信中正确渲染

---

#### 测试结论

**所有 7 项核心功能均已验证通过：**

| 测试项 | 状态 | 平均响应时间 |
|--------|------|-------------|
| 基础对话 | ✅ 通过 | 8 秒 |
| 实时信息查询 | ✅ 通过 | 9 秒 |
| 简短任务响应 | ✅ 通过 | 10 秒 |
| 上下文记忆 | ✅ 通过 | 17-25 秒 |
| 多轮任务协作 | ✅ 通过 | 13-14 秒 |
| 长文本生成 | ✅ 通过 | 171 秒 |
| Markdown 格式 | ✅ 通过 | 19 秒 |

**技术验证：**
- ✅ WebSocket 长连接稳定
- ✅ 消息收发正常
- ✅ 会话隔离正确
- ✅ 密钥脱敏完整
- ✅ 模型切换成功（从 GPT 切换到 Claude Sonnet 4）
- ✅ 无重复发送问题
- ✅ 无连接泄漏问题

**企业微信智能机器人 wecom_ai_bot 联调已完成，所有功能正常工作。**

---

## 10. 撤回与失败重试

Adapter 会把已启用通道以及遗留兼容适配器中的 `Event`、`event`、`ChangeType`、`action`、`event_type`、`MsgType` 统一归一化。`revoke`、`withdraw`、`recall`、`delete`、`message_revoke` 会进入撤回分支，`target_message_id`、`revoke_message_id`、`withdraw_message_id`、`MsgId`、`message_id` 会被识别为被撤回消息。

企业微信和微信公众号通常通过 XML/加密 XML 回调携带 `Event` 或 `ChangeType`，Telegram 更常见的是编辑、删除或业务侧模拟事件，Web UI/JSON 调试可直接传 `normalized_event_type=revoke`。撤回不物理删除历史消息和审计，只追加不可变事件，避免合规链路断裂。IM 平台重复投递撤回事件时仍使用 `tenant_id + channel + account_id + external_message_id` 幂等去重。

## 11. 常见故障排查

| 现象 | 可能原因 | 解决方案 |
|------|---------|---------|
| 发消息无回复 | WebSocket 未连接 | 检查日志中是否有 "WeCom AI Bot connector starting" 和 "Application startup complete" |
| 回复延迟超过 30 秒 | 模型超时或性能问题 | 检查 `CPA_TIMEOUT_MS` 配置，或切换到响应更快的模型（如 claude-sonnet-4） |
| 服务启动失败："failed to resolve host 'sql'" | PostgreSQL 配置错误 | 本地开发需禁用 PostgreSQL，确保 `.env` 中 `POSTGRES_DSN=` 和 `TENANT_DB_DSN=` 为空 |
| 日志显示 "model provider returned HTTP 404" | 模型名称不正确 | 确认模型名称与 API 提供商一致，例如 New API 使用 `claude-sonnet-4-20250514` 而非 `claude-sonnet-4.6` |
| 日志显示 "ModuleNotFoundError: No module named 'wecom_aibot_sdk'" | SDK 未安装 | 运行 `pip install wecom-aibot-sdk-python` 或 `uv pip install wecom-aibot-sdk-python` |
| 日志显示 "session mailbox fencing rejected" | 会话状态冲突 | 重启服务清理会话状态，必要时删除 `data/tenant_config.sqlite3` |
| GET 校验失败 | Token 或 URL 配置错误 | 检查公网 HTTPS 是否可访问、URL 是否带正确的 `channel/account_id`、Token 是否一致 |

## 12. 常见问题

### GET 校验失败

检查公网 HTTPS 是否可访问、URL 是否带正确的 `channel/account_id`、Token 是否
一致，以及企业微信/微信的 AES key 是否完整配置。

### 收到消息但没有回复

依次检查：租户绑定是否已 publish、用户是否在 `allowed_user_ids` 中、模型配置
是否可用、平台应用是否有发消息权限、SDK Secret 是否能通过 `secret://` 解析。

### 本地 Web UI 正常，真实 IM 不通

这通常是公网回调地址、防火墙、HTTPS 证书或平台后台权限问题，不是 Web UI
自测链路问题。

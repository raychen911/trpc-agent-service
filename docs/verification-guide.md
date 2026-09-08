# 功能验证手册

下面命令以 Windows PowerShell 和项目根目录为例。

## 1. 安装并运行测试

```powershell
$env:PYTHONUTF8='1'
.\.venv\Scripts\python.exe -m pip install -e ".[dev,storage]"
.\.venv\Scripts\python.exe -m ruff check .\trpc_service .\tests
.\.venv\Scripts\python.exe -m pytest .\tests --cov=trpc_service --cov-report=term-missing
```

测试覆盖 Admin API、SQL 约束、Redis CAS/锁/幂等/限流、Outbox、向量同步、Artifact、
AgentFactory、稳定路由、远端转发认证、Telegram 和企业微信加解密。

## 2. 单节点无模型验证

此步骤不调用 LLM，不需要 API Key。

```powershell
$env:TRPC_SERVICE_NODE_ID='node-local'
$env:TRPC_SERVICE_NODE_BASE_URL='http://127.0.0.1:8000'
$env:TRPC_SERVICE_GATEWAY_INTERNAL_SECRET='local-gateway-secret'
.\.venv\Scripts\python.exe -m trpc_service serve --host 127.0.0.1 --port 8000
```

另开 PowerShell：

```powershell
$headers=@{'x-gateway-token'='local-gateway-secret'}
Invoke-RestMethod http://127.0.0.1:8000/health/ready
Invoke-RestMethod http://127.0.0.1:8000/gateway/v1/nodes -Headers $headers
Invoke-RestMethod 'http://127.0.0.1:8000/gateway/v1/routes/resolve?tenant_id=t1&agent_app_id=a1&session_id=s1' -Headers $headers
```

预期 ready 中 application/database 均为 `ok`；节点列表包含 `node-local`；route 返回同一节点。
Swagger 位于 `http://127.0.0.1:8000/docs`。

## 3. 验证 Admin 配置发布

在 Swagger 中依次操作：

1. `POST /admin/v1/tenants` 创建租户，保存 `id`。
2. `POST /admin/v1/tenants/{tenant_id}/apps` 创建 Agent，保存 app `id` 和 `lock_version`。
3. `PUT .../draft` 提交模型、工具、ChannelBinding 和 BackendConfig。模型密钥只填写
   `env://TRPC_AGENT_API_KEY`，不要提交明文。
4. `POST .../publish`，请求中的 `expected_lock_version` 使用应用当前值。
5. `GET .../draft` 能看到新草稿；应用的 `active_version` 指向刚发布版本。
6. 修改并再次发布后，可调用 rollback 切回历史发布版本；webhook resolver 会立即使用回滚版本。

## 4. 调用真实 Agent

先设置兼容 OpenAI 的模型环境变量，并在已发布模型配置中填写对应 provider/model/base_url：

```powershell
$env:TRPC_AGENT_API_KEY='你的密钥'
```

然后调用规范化入口：

```powershell
$body=@{
  tenant_id='上一步租户ID'; agent_app_id='上一步应用ID'
  channel='http'; account_id='local-api'; external_message_id=('msg-'+[guid]::NewGuid())
  sender_user_id='user-1'; conversation_id='user-1'; conversation_type='direct'
  text='请用一句话介绍你自己'; metadata=@{}
} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8000/gateway/v1/messages `
  -Method Post -ContentType 'application/json' -Headers $headers -Body $body
```

响应包含选中 node、session_id、trace_id、reply_text 和递增的 session_version。使用相同
external_message_id 重试会返回 409，证明幂等生效。

可用下面的命令确认 SQL 固定写入顺序产生的数据：

```powershell
.\.venv\Scripts\python.exe -c "from sqlalchemy import create_engine,text; e=create_engine('sqlite+pysqlite:///./data/trpc_service.db'); c=e.connect(); print({t:c.execute(text('select count(*) from '+t)).scalar() for t in ['sessions','session_events','summaries','outbox_messages','audit_logs']})"
```

## 5. 本机双节点与 Redis 路由

先启动 Redis，例如：

```powershell
docker run --name trpc-redis -p 6379:6379 -d redis:7-alpine
```

两个终端必须使用相同 SQL、Redis 和内部密钥，但使用不同 node_id、端口和 base_url。终端一：

```powershell
$env:TRPC_SERVICE_COORDINATION_BACKEND='redis'
$env:TRPC_SERVICE_REDIS_URL='redis://127.0.0.1:6379/0'
$env:TRPC_SERVICE_DATABASE_URL='sqlite+pysqlite:///./data/trpc_service.db'
$env:TRPC_SERVICE_GATEWAY_INTERNAL_SECRET='cluster-secret'
$env:TRPC_SERVICE_NODE_ID='node-1'
$env:TRPC_SERVICE_NODE_BASE_URL='http://127.0.0.1:8001'
.\.venv\Scripts\python.exe -m trpc_service serve --host 127.0.0.1 --port 8001
```

终端二把 node 改为 `node-2`、端口改为 `8002`。等待约 2 秒，再查询任一节点：

```powershell
$headers=@{'x-gateway-token'='cluster-secret'}
Invoke-RestMethod http://127.0.0.1:8001/gateway/v1/nodes -Headers $headers
1..10 | ForEach-Object {
  Invoke-RestMethod "http://127.0.0.1:8001/gateway/v1/routes/resolve?tenant_id=t&agent_app_id=a&session_id=s$_" -Headers $headers
}
```

预期列表出现两个节点，不同 session 分布到两者；同一个 session 重复查询始终选择相同节点。
停止被选中的节点并等待 `node_ttl_seconds`（默认 15 秒），再次查询会选择剩余节点。

## 6. Telegram webhook

发布 Telegram ChannelBinding，并在启动服务前设置：

```powershell
$env:TELEGRAM_WEBHOOK_SECRET='只包含字母数字下划线或连字符'
$env:TELEGRAM_BOT_TOKEN='BotFather 返回的 token'
```

将服务通过 HTTPS 域名或隧道暴露后注册 webhook：

```powershell
$url='https://你的域名/webhooks/telegram/support-bot'
Invoke-RestMethod "https://api.telegram.org/bot$env:TELEGRAM_BOT_TOKEN/setWebhook" `
  -Method Post -ContentType 'application/json' `
  -Body (@{url=$url; secret_token=$env:TELEGRAM_WEBHOOK_SECRET} | ConvertTo-Json)
```

在 Telegram 给 Bot 发文本，预期收到 Agent 文本回复；数据库中 channel_type 为 telegram，
external_message_id 为 update_id。重复 Update 不会再次调用 Agent。

## 7. 企业微信 webhook

发布企业微信 ChannelBinding，并设置：

```powershell
$env:WECOM_CALLBACK_TOKEN='企业微信后台填写的Token'
$env:WECOM_ENCODING_AES_KEY='企业微信后台43字符EncodingAESKey'
```

在企业微信自建应用“接收消息”中填写公开 HTTPS URL：
`https://你的域名/webhooks/wecom/corp-support`，Token/AES Key 与环境变量一致。保存时平台会完成
GET URL 验证。向应用发送文本后，预期收到加密被动回复。日志中可按 x-request-id/trace_id 查找
该次请求；重复 MsgId 被幂等层拦截。

### 7.1 智能机器人 API URL 回调模式

若使用“智能机器人”而非“自建应用”，在企业微信后台选择“使用 URL 回调”，设置：

```powershell
$env:WECOM_AIBOT_CALLBACK_TOKEN='API配置中的Token'
$env:WECOM_AIBOT_ENCODING_AES_KEY='API配置中的43字符EncodingAESKey'
```

Compose 用户把同名变量写入项目根目录 `.env`。ChannelBinding 使用：

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
    "aibot_id": "真实机器人ID",
    "identity_mode": "passthrough"
  }
}
```

发布配置后，在机器人 API 配置中填写
`https://你的公网域名/webhooks/wecom/smart-support`。保存触发 GET 验证；发送唯一文本后，POST
回调应返回 `{}`，SQL Inbox 最终变为 `completed`，`im.reply.wecom` Outbox 使用一次性
`response_url` 回复，Jaeger 中可按根 Span `HTTP POST /webhooks/wecom/smart-support` 查询。

本实现不需要 CorpID、AgentId 或应用 Secret，也不使用自建应用的消息发送 API。它支持智能机器人
URL 回调模式，不支持 WebSocket 长连接模式。

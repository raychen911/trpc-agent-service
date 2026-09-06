# 本分支实现

本分支已经按方案文档补齐一个可运行的最小平台闭环：

- 租户、Agent App、Channel Binding、配置版本、发布和回滚。
- Gateway + 无状态 Worker,按租户和会话隔离,重复 IM 消息幂等。
- InMemory、SQLite、真实 Redis 和 PostgreSQL 结构化存储；本地持久化向量库和文件对象存储。
- Docker Compose 默认将 Session 放入 Redis、Memory/Summary/Audit 放入 PostgreSQL,保证多 Worker 共享状态和审计持久化；Knowledge 与 Artifact 可按环境切换到远端后端。
- Memory、Summary、Audit 等派生写入失败时进入 `compensation_task` 补偿队列,可由 Web 内置线程、独立 CLI 或部署中的补偿 Worker 重放。
- 企业微信智能机器人(wecom_ai_bot)、飞书(feishu)、Telegram(telegram)和 Web 四类统一 Channel Adapter。企业微信主验收入口是 wecom_ai_bot(BotID + BotSecret 长连接),传统回调适配器 wecom 默认禁用。
- 真实 IM 出站优先使用成熟 SDK：Telegram 使用 `python-telegram-bot`，
  飞书使用 `lark-oapi`；企业微信智能机器人使用可选的
  `wecom-aibot-sdk-python`，传统企业微信和微信生态兼容适配器使用
  `wechatpy`。可选 SDK 未安装或未配置凭据时，不影响本地 Web UI、pytest
  和其他默认通道的复现。
- 工具策略、敏感信息脱敏、审计记录和 tenant-aware trace。
- 可选 FastAPI Admin/Webhook 服务、Docker Compose 和 Kubernetes 部署说明。

## 本地验证

```bash
python -m unittest discover -s tests -v
python -m trpc_service demo
```

运行环境要求 Python 3.12+。当前仓库的实现使用 `StrEnum` 和较新的标准库能力，Python 3.8 虚拟环境不兼容。

安装 `requirements.txt` 后启动 HTTP 服务：

```bash
uvicorn trpc_service.web.app:app --host 127.0.0.1 --port 8000
```

主要接口：

- `GET /health`
- `POST /admin/v1/tenants`
- `GET /admin/v1/tenants/{tenant_id}`
- `PUT /admin/v1/tenants/{tenant_id}/config`
- `POST /admin/v1/tenants/{tenant_id}/publish`
- `POST /admin/v1/tenants/{tenant_id}/rollback`
- `POST /admin/v1/tenants/{tenant_id}/gray-release`
- `POST /admin/v1/tenants/{tenant_id}/channels`
- `GET /admin/v1/tenants/{tenant_id}/health`
- `POST /admin/v1/tenants/{tenant_id}/compensations/replay`
- `GET /metrics`
- `GET /ui`
- `POST /webhooks/{channel}/{account_id}`

架构、时序、数据模型、迁移、容量和风险见 `docs/ARCHITECTURE.md`、
`docs/SEQUENCE.md`、`docs/DATA_MODEL.md`、`docs/MIGRATION.md`、
`docs/CAPACITY.md`、`docs/RISKS.md`，Compose
入口见 `deployment/docker-compose.yml`。真实 IM 联调仍需要各平台的
webhook、token、secret 和账号权限；本地 webhook 使用 JSON 事件模拟。
真实平台联调步骤见 `docs/IM_INTEGRATION.md`。

后台补偿任务可以通过三种方式运行：HTTP 服务默认启用 `COMPENSATION_WORKER=1` 的内置线程；生产推荐在 Compose/Kubernetes 中使用独立 `compensation-worker`；也可以手动执行一次：

```bash
python -m trpc_service._cli compensate --once --limit 100
```

联调 Redis、PostgreSQL、对象存储、远端向量库或外部 Memory 前，可运行不打印密钥的健康检查：

```bash
python scripts/validate_integrations.py
```

## IM SDK 接入

Web UI 是本地自测通道，不替代真实 IM Adapter。真实平台 Adapter 已经分别
实现了统一的验签、消息解析、session 路由和出站发送：

| 通道 | SDK | SDK 发送条件 |
| --- | --- | --- |
| Telegram | `python-telegram-bot>=22.8,<23` | `token_ref` 可解析，且 `sdk_enabled` 不为 `false` |
| 飞书 | `lark-oapi>=1.3.22` | App Secret 和签名配置可解析 |
| 企业微信智能机器人 | 可选 `wecom-aibot-sdk-python` | BotID/BotSecret 可解析，并显式启用连接器 |
| 传统企业微信应用 | `wechatpy>=1.8.18,<1.9` | `corp_id`、`corp_secret_ref`、`agent_id` 都配置；默认禁用 |

示例通道配置只保存密钥引用，不保存明文 token 或 secret：

```json
{
  "channel": "wecom",
  "account_id": "corp_account_1",
  "token_ref": "secret://tenant_demo/wecom/callback-token",
  "secret_ref": "secret://tenant_demo/wecom/aes-key",
  "config": {
    "sdk_enabled": true,
    "corp_id": "wwxxxxxxxx",
    "corp_secret_ref": "secret://tenant_demo/wecom/corp-secret",
    "agent_id": 100001
  }
}
```

上面的 `wecom` 示例是传统企业微信应用回调的兼容性配置，默认不在运行时
注册表中。企业微信智能机器人主验收请使用 `wecom_ai_bot`，配置和长连接
启动方式见 [docs/IM_INTEGRATION.md](IM_INTEGRATION.md)。

企业微信机器人 webhook URL 中的 `key=` 也视为密钥，生产配置应使用
`webhook_url_ref` 指向 `secret://...`；Admin API 的公共配置返回会递归隐藏
`token_ref`、`secret_ref`、`*_ref`、明文 secret 字段和带 token/key/secret 的 URL。
租户配置校验会拒绝新增明文 token、secret、password、api_key 或带密钥参数的
webhook URL。

没有真实平台账号时，可以使用 Web UI 验证完整 Agent 流程；真实 IM 联调时只需
补齐平台后台的 webhook、token、secret、应用 ID 和权限，现有 Adapter 会直接
进入 SDK 发送分支。出站图片/文件也会先按平台要求上传临时媒体，再发送带
`media_id` 的图片或文件消息；Telegram 使用 `send_photo` / `send_document`。

媒体消息行为：

- Web UI 支持选择一个不超过 `10 MB` 的本地文件或图片，并以租户 Artifact
  形式保存后进入 Agent 链路。
- Telegram 支持 SDK 和 HTTP 两条媒体发送路径，分别对应
  `send_photo` / `send_document`。
- 传统企业微信应用支持 `wechatpy.enterprise` 媒体上传后发送图片或文件；
  企业微信智能机器人媒体能力取决于可选 SDK 和提供商协议。
- 微信公众号和微信客服适配器仅保留兼容性代码和针对性测试，不属于默认运行路径。
- 所有外部媒体发送都复用大小限制、账号限流、指数退避重试和死信记录；平台
  不支持的原生媒体类型不会伪装成成功的文本消息。

灰度发布通过 `TenantConfig.gray_release` 和 Admin API 落地，支持按稳定 hash 百分比路由，也支持指定 session 覆盖。迁移工具通过 `trpc-agent-migrate` 提供 `export`、`import`、`verify` 和 `cutover-plan`，结构化快照 CLI 的后端参数是 `memory/redis/sql/postgres`，迁移包覆盖 session、event、state、summary、memory、audit、idempotency、knowledge 和 artifact；远端向量库与 S3 兼容对象存储需按 `docs/MIGRATION.md` 的重建/复制策略在目标环境执行，不可直接把 `qdrant` 或 `s3` 作为 CLI `--backend`。PostgreSQL Schema 由 Alembic `db upgrade` 和 `db check` 独立管理。可观测性通过 `/metrics` 暴露 Prometheus 指标，并可通过 `OTEL_EXPORTER_OTLP_ENDPOINT` 接入 OpenTelemetry Collector。
危险工具二次确认通过租户 `tool_policy.approval_rules`、持久化 `tool_approval_requested` 事件和签名 approval token 串联，未确认前 Worker 只返回 `approval_required`，不会执行真实工具。IM 撤回、withdraw、recall、delete 等事件会归一化为 `message_revoked`，只写 Session/Audit，不触发模型、工具或出站回复。

## tRPC-Agent-Python 能力复用边界

可直接复用的框架能力包括 Agent Runner、Session、Memory、Summary、Knowledge、
Tool/MCP、Filter、Sandbox、模型 Provider、Agent Event、FastAPI/Web 和 Telemetry。
本分支新增的平台层包括 `tenant/` 配置版本与灰度、`gateway/` 无状态路由和
Worker Queue、`channels/` IM Adapter、`storage/` 多后端与迁移、`policy/` 与
`security/` 租户治理、`admin/` RBAC、Webhook 验签和 `deployment/` 部署配置。
框架层负责 Agent 执行语义，平台层负责租户隔离、session 路由、IM 协议、后端
选择、幂等、审计、成本和运维。
默认运行时为 `tRPC-Agent-Python`（`TRPC_AGENT_RUNTIME_MODE=trpc`），通过
`Runner`、`LlmAgent`、`OpenAIModel` 和 SDK Session Service 执行模型链路。
平台层仍负责租户路由、策略 Filter、工具审批、幂等、审计和平台数据后端。
模型声明平台工具时，SDK Agent 的工具入口会回到平台工具执行器，平台负责
权限、审批、幂等、审计和结果回填，不允许工具绕过租户治理。没有模型凭据时
不会静默降级；本地无凭据演示或单测必须显式设置
`TRPC_AGENT_RUNTIME_MODE=local`。`TRPC_AGENT_RUNTIME_FACTORY` 仅在
`TRPC_AGENT_RUNTIME_MODE=external` 时生效，且缺少工厂会直接失败。

## 真实模型调用

服务支持 OpenAI-compatible Responses API。不要把 API key 写入代码、Git
或租户配置；如果 API key 曾在聊天或日志中暴露，建议立即撤销并重新生成。设置新 key：

```powershell
$env:OPENAI_API_KEY = "你的新API_KEY"
$env:CPA_BASE_URL = "https://your-provider.example/v1"
$env:CPA_MODEL = "服务商实际支持的模型名"
python -m trpc_service demo
```

本地启动脚本默认使用显式 `local` 演示运行时，不需要模型凭据即可验证
Web UI、Session、Memory、工具、审计和回复链路：

```powershell
.\start-web-ui.ps1
```

需要验证 tRPC-Agent-Python 和真实模型时，先设置服务商参数，再显式切换运行时。
若本机已经安装原版 `codex-cli`，也可以复用用户目录下的配置，不需要把 key
写入项目：

```powershell
$env:CPA_USE_CODEX_CLI = "1"
$env:CPA_MODEL = "服务商实际支持的模型 ID"
.\start-web-ui.ps1 --runtime-mode trpc --port 18001
```

该模式适合验证真实模型链路；生产部署建议使用独立的
OpenAI-compatible 服务账号和 `ResponsesModelClient`。

停止本地服务：

```powershell
.\stop-web-ui.ps1
```

Linux / macOS 使用仓库内的 shell 脚本，默认使用无凭据的 `local` 演示模式，
端口为 `18001`，启动后会输出验收用 Web UI 地址：

```bash
chmod +x start.sh stop.sh
./start.sh
```

需要验证真实 tRPC-Agent-Python 模型链路时，先设置 `CPA_BASE_URL`、
`CPA_MODEL` 和 `OPENAI_API_KEY`，再执行：

```bash
TRPC_AGENT_RUNTIME_MODE=trpc ./start.sh
```

停止服务：

```bash
./stop.sh
```

启动成功后打开：

```text
http://127.0.0.1:18001/ui
```

请求会发送到 `{CPA_BASE_URL}/responses`，协议固定为 `responses`。未设置
`OPENAI_API_KEY` 时，默认 tRPC 运行时会明确报错；无凭据本地演示请显式设置
`TRPC_AGENT_RUNTIME_MODE=local`，单测已固定使用该显式模式。
Docker Compose 可读取项目根目录的 `.env`；参考 `.env.example`。`CPA_MODEL`
必须填写该代理实际支持的精确模型 ID；如果返回 `model_not_found`，说明认证和
URL 已生效，但模型名需要更换为服务商可路由的模型。

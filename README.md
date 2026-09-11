# 基于 tRPC-Agent-Python 的多租户节点化 Agent 服务

本项目将 tRPC-Agent-Python 的编排能力封装为服务多个企业租户的 Gateway/Worker 平台，提供企业微信与飞书接入、租户级主备模型配置、Session/Memory 管理、工具治理、审计和可观测性。数据后端可以按租户选择。默认 InMemory 模式用于本地演示，生产环境使用 PostgreSQL 与 Redis，并按 Gateway、Worker、Admin、Migration 四类角色部署。

## 1. 上游能力与复用边界

本项目实际复用 `LlmAgent`、`Runner`、`OpenAIModel`、FunctionTool/MCP、Content/Part 和 InMemory/Redis SessionService。租户控制面、Channel Adapter、路由与幂等、Storage Router、治理 Filter、Memory/Knowledge、审计与 OTel 接入由平台实现。详细边界见 [架构设计](docs/architecture.md#11-框架复用与后续扩展)。


## 2. 快速运行

运行环境为 Python 3.10+，主要依赖包括 `fastapi`、`uvicorn`、`cryptography` 和 `trpc-agent-py`。使用项目提供的 `build.sh` 准备完整环境后，可以运行离线演示或启动服务。

```bash
uv run trpc-service demo
bash start.sh
```

服务地址：`GET /health/live`、`GET /health/ready`、`POST /webhook/{tenant_id}/{channel}`、`GET /admin/tenants`。开发租户为 `acme`、`globex`，配置位于 `trpc_service/web/app.py`，生产环境应通过 Admin API/SQL 和 SecretRef 管理，不能使用示例 token。

企业微信应用模式支持普通或加密 XML 回调、URL 校验、应用消息发送和临时素材下载。接入前需配置租户 binding 的 Token、EncodingAESKey 和应用凭据，详细流程见 [企业微信接入](docs/im-channels.md#2-企业微信接入)。

企业微信智能机器人长连接使用 `Bot ID + Secret`，无需公网回调地址。将凭证写入 `.env`：

```dotenv
TRPC_SERVICE_WECOM_BOT_ID=你的BotID
TRPC_SERVICE_WECOM_BOT_SECRET_REF=env://WECOM_BOT_SECRET
WECOM_BOT_SECRET=你的Secret
TRPC_SERVICE_WECOM_BOT_TENANT_ID=acme
TRPC_SERVICE_IM_DRY_RUN=false
```

然后运行 `bash build.sh`、`bash start.sh`。通过 `GET /health/ready` 查看
`wecom_connections` 状态。长连接不使用 `/webhook/{tenant_id}/wecom`，该接口保留给旧的 Corp 应用回调模式。
当前 Bot 长连接要求 `TRPC_SERVICE_ROLE=all`，因为回复依赖接收进程保存的 SDK 连接与消息上下文。该模式不能直接拆分为独立 Gateway 和 Worker。

飞书通道支持 HTTP Webhook 和官方 SDK 长连接。长连接使用 App ID/App Secret，不需要公网回调地址，两种模式均通过 Open API 回复。完整配置见 [飞书接入指南](docs/feishu-setup.md)。

## 3. 设计交付物

以下七项均有对应的设计说明和代码依据。[架构文档](docs/architecture.md) 提供可独立阅读的整体方案，[完整检查清单](docs/requirements-audit.md) 进一步列出具体要求、六项难点、八项交付物及待验证内容。

| # | 原题验收标准 | 对应说明与证据 |
|---|---|---|
| 1 | 多租户、节点部署、同步、多后端、IM、治理监控、恢复 | [架构概览](docs/architecture.md) 按主题分别说明方案，并链接一致性、安全及运维专题 |
| 2 | tenant、agent、binding、session、event、memory、summary、audit 关系 | [数据模型](docs/data-model.md) 统一说明实体与实际存储。[领域模型](trpc_service/tenant/models.py) 和 [DDL](migrations/0001_production.sql) 提供实现依据，App/Binding 存于 revision JSON |
| 3 | 至少两类 IM，含微信或企业微信，解释差异 | [企业微信与飞书对比](docs/im-channels.md#4-企业微信与飞书的差异) 覆盖认证、身份、回复、长连接、媒体及失败处理 |
| 4 | 至少三类后端的存储与同步 | [一致性与迁移](docs/consistency.md) 说明 PostgreSQL、Redis、pgvector、S3 的职责、取舍及恢复方式 |
| 5 | 完整消息时序及 trace/request ID 贯穿 | [企业微信时序与 trace 关联](docs/architecture.md#9-完整消息时序与-trace-关联) 提供完整路径，[回归测试](tests/test_telemetry.py) 验证队列及执行链路的 trace ID |
| 6 | 至少 8 个风险及缓解措施 | [16 项风险清单](docs/risks.md)，措施中的生产操作仍需部署及演练 |
| 7 | 框架复用与新增平台模块边界 | [框架复用与后续扩展](docs/architecture.md#11-框架复用与后续扩展) 区分直接复用、平台自建、替代实现与新增能力 |

本地验证命令为 `bash coverage.sh`、`bash lint_flake8.sh` 和 `uv run python -m compileall -q trpc_service tests`。其中，`coverage.sh` 执行测试并统计覆盖率，`lint_flake8.sh` 执行 Ruff 静态检查。真实 IM/模型凭据、SQL RLS、多节点恢复及容量 SLO 必须按 [生产上线门禁](docs/production-runbook.md) 在目标环境验收。当前迁移协调器不负责自动搬迁，灰度发布由运维控制。

- 架构、拓扑、时序和复用边界：[docs/architecture.md](docs/architecture.md)
- 数据模型：[docs/data-model.md](docs/data-model.md)
- 一致性、同步、幂等和迁移：[docs/consistency.md](docs/consistency.md)
- 企业微信与飞书接入及差异：[docs/im-channels.md](docs/im-channels.md)
- 飞书机器人接入与使用：[docs/feishu-setup.md](docs/feishu-setup.md)
- 治理、监控、OTel 和安全：[docs/security.md](docs/security.md)
- 故障、容量、灰度和部署：[docs/operations.md](docs/operations.md)
- 风险清单（16 项）：[docs/risks.md](docs/risks.md)
- 四周时间计划与完成状态：[docs/implementation-plan.md](docs/implementation-plan.md)
- 完整实现进展与剩余环境验收：[docs/progress-report.md](docs/progress-report.md)
- 生产角色、配置、死信恢复和上线步骤：[docs/production-runbook.md](docs/production-runbook.md)

完整验收口径见 [实施计划](docs/implementation-plan.md)。InMemory 队列和存储仅用于单进程演示。生产启动要求 PostgreSQL/Redis、强 Session HMAC key、真实模型凭据、独立迁移和非 dry-run IM 发送。

## 4. 目录结构

```text
|-- README.md
|-- build.sh / clean.sh / coverage.sh
|-- data
|-- docs
|-- format.sh / lint_flake8.sh
|-- start.sh / stop.sh
`-- trpc_service
    |-- _cli.py
    |-- agent       # Runner 适配与无状态 Worker
    |-- channels    # 企业微信与飞书接入、消息分段及投递
    |-- config      # 环境设置与 SecretRef
    |-- log         # 日志与敏感信息脱敏
    |-- metrics     # trace context 和低基数指标
    |-- queue       # Redis Streams、pending reclaim、DLQ、Outbox
    |-- storage     # PostgreSQL、Redis、pgvector、S3、迁移
    |-- skill       # 预留 Skill 装载入口
    |-- tenant      # 租户模型、revision、Session/Memory/Audit 存储
    |-- tool        # 租户级工具 Filter
    |-- version.py
    |-- web         # FastAPI Gateway/Admin/health
    `-- workspace   # 本地/容器沙箱约定
```

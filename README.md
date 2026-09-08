# 基于 tRPC-Agent-Python 的多租户节点化 Agent 部署平台

> 当前版本：`0.6.0`　|　Python：`>=3.10`　|　服务框架：FastAPI　|　Agent 框架：tRPC-Agent-Python

本项目把单体 Agent 扩展为面向企业场景的多租户平台：多个租户可以创建和版本化发布 Agent App，绑定企业微信等 IM 账号，选择不同数据后端和工具权限，并通过多个无状态 Worker 水平扩展。平台同时提供消息幂等、Session 并发控制、异步 Inbox/Outbox、租户治理、审计、OpenTelemetry 以及故障恢复能力。

![系统架构图](deliverables/system-architecture.png)

## 1. 项目背景与目标

企业在落地 Agent 时，通常需要同时服务多个部门、业务线、IM 入口和数据后端。不同租户之间不仅要隔离 Session 和 Memory，还需要隔离应用配置、模型密钥、工具权限、知识库、审计日志和预算。单进程机器人难以满足节点扩展、故障转移、IM 响应时限、数据同步和合规要求。

本项目基于 tRPC-Agent-Python 的 Agent/Runner、模型、Tool/MCP、Session、Memory、Knowledge、Filter 与 Telemetry 能力，新增平台控制面和可靠运行面，目标包括：

- 支持 Tenant、Agent App、不可变 Revision、发布和回滚；
- 支持企业微信和 Telegram 等 Channel Adapter；
- 支持 Gateway、节点目录、稳定路由和无状态 Worker；
- 支持 PostgreSQL、Redis、Qdrant、MinIO 及本地开发后端；
- 支持 IM 幂等、同 Session 并发控制和跨后端最终一致；
- 支持租户治理、安全审计、Trace、Metrics、灰度和故障恢复。

## 2. 官方要求与验收覆盖

### 2.1 多租户与节点部署

- Tenant 模型包含应用、模型、工具权限、IM 通道、数据后端、审计策略和密钥命名空间；
- Agent Gateway、Agent Worker、Channel Adapter、Storage Adapter、Admin API 和 Telemetry Collector 协作；
- 多节点通过 Node Directory 和 Rendezvous Hash 按 tenant/app/session 路由；
- Worker 不依赖 sticky session，依靠共享 Session/Memory、Redis 锁和 SQL CAS；
- 配置、数据、工具、日志和密钥均执行租户隔离。

### 2.2 数据同步与多后端

- 租户可按数据类型选择 SQL/InMemory Session、Qdrant/InMemory Vector、MinIO/Local Artifact 等实现；
- Storage Adapter 统一 Session、Event、Memory、Summary、Artifact、Knowledge、Audit、Inbox 和 Outbox 接口；
- 同 Session 写入使用 Redis 分布式锁和版本 CAS；
- 一次 Turn 固定按 `Event → State → Summary → Memory → Outbox` 提交；
- SQL 是权威事实源，Qdrant、对象存储和 IM 回复通过 Transactional Outbox 同步；
- 提供 Redis→SQL、SQL Memory→Qdrant、Local→MinIO 的迁移工具。

### 2.3 IM 接入

- 已实现企业微信 AIBot、企业微信自建应用和 Telegram Bot Adapter；
- 支持 Webhook 验签、AES 解密、NormalizedMessage、身份映射和消息去重；
- 单聊与群聊使用不同 Session ID 规则，并包含 channel/account/tenant/app 隔离范围；
- 通过 Durable Inbox 快速确认回调，后台异步执行模型并回复；
- Delivery Adapter 支持长消息拆分、流式编辑、卡片/媒体描述、重试和死信。

### 2.4 治理、监控和安全

- Filter 覆盖 IM ACL、PII 脱敏、租户预算、工具白名单和危险工具二次确认；
- OpenTelemetry 串联 Webhook、Inbox、Gateway、Runner、模型/Tool、Storage 和 Outbox；
- Prometheus 暴露请求、延迟、错误、Token、成本和队列等指标；
- Audit Log 保存 tenant、channel、user、session、agent、tool、decision、latency、error、cost 和 trace id；
- OIDC/RBAC、内部 mTLS、Vault/AWS KMS SecretResolver 和日志字段白名单用于生产加固。

### 2.5 故障恢复与运维

- 节点 TTL、Inbox/Outbox 租约和重新选路处理节点故障；
- Execution Ledger 解决 Runner 与平台 SQL 的双事务恢复；
- Outbox 指数退避、Dead Letter 和可重建向量索引处理外部依赖故障；
- 提供 Docker Compose、Kubernetes stable/canary、Alembic、备份恢复和容量测试工具。

## 3. 系统架构

### 3.1 控制面

`/admin/v1` Admin API 管理 Tenant、Agent App、模型、工具、Channel Binding 和后端配置。App 配置先写入 draft，发布后生成不可变 Revision，`active_version` 指向当前生效版本。更新使用 `lock_version` 乐观锁；运行面只读取已发布版本，因此草稿修改不会影响正在处理的请求，历史 Revision 可以直接回滚。

### 3.2 运行面

一条消息的主链路如下：

```text
IM Callback
  → Channel Adapter（验签 / 解密 / 归一化 / 身份映射）
  → Durable Inbox
  → Agent Gateway（Binding / 节点路由）
  → Governance Filter
  → AgentFactory / tRPC-Agent Runner
  → Model / Tool / MCP
  → Turn Coordinator
  → SQL Transaction + Outbox
  → Memory/Artifact 同步和 IM 异步回复
```

### 3.3 tRPC-Agent-Python 与平台层边界

tRPC-Agent-Python 负责 Agent/Runner 编排、模型调用、Tool/MCP、Session/Memory/Knowledge 概念、Filter 扩展点和 Telemetry。平台层负责 Tenant/Admin API、配置版本、Channel Binding、IM 协议、Durable Inbox、节点路由、租户后端、锁与幂等、Turn Coordinator、Execution Ledger、Transactional Outbox、RLS、审计、安全和部署。

详细架构见 [架构设计文档](deliverables/system-architecture-design.md)。

## 4. 数据后端与一致性

| 后端 | 主要职责 | 一致性定位 |
|---|---|---|
| PostgreSQL | Tenant/App/Binding、Session/Event/Summary、Memory 原文、Audit、Inbox/Outbox、Execution Ledger | 权威事实源，事务强一致 |
| Redis | Session 锁、幂等 claim、限流、短期状态、Node Directory | 低延迟协调层，带 TTL |
| Qdrant | Memory/Knowledge 向量和过滤 metadata | 可重建派生索引，最终一致 |
| MinIO | 图片、文件、知识原件和 Artifact | 大对象存储，SQL 保存 metadata/checksum |
| SQLite/InMemory/Local | 单机开发与单元测试 | 不用于生产多节点状态共享 |

外部消息幂等键统一为：

```text
tenant_id:channel:external_message_id
```

平台采用 Inbox 唯一约束、Redis claim、Event 唯一约束、Execution/Outbox dedupe 四层幂等。它不承诺物理意义的端到端 exactly-once，而通过至少一次接收/投递、SQL 原子事务和幂等副作用，实现业务效果接近恰好一次。

详细设计见：

- [数据模型设计](deliverables/data-model-design.md)
- [数据同步与幂等策略](deliverables/data-sync-idempotency-strategy.md)
- [多后端适配方案](deliverables/multi-backend-adaptation-plan.md)

## 5. Gateway 与多节点

每个实例启动后注册 `node_id`、可直连 `NODE_BASE_URL`、能力和心跳。Redis Node Directory 使用 TTL 清理失联节点。Gateway 根据 `tenant_id + agent_app_id + session_id` 执行 Rendezvous Hash，使相同 Session 在节点集合不变时稳定落点；目标是其他节点时，通过受认证的 `/internal/v1/messages` 转发。

节点亲和只用于降低缓存和锁竞争，不作为正确性条件。Worker 无状态，Session 等数据保存在共享后端；Redis 锁降低同 Session 冲突，SQL `expected_version` CAS 阻止旧状态覆盖新状态。真实双节点集成测试会启动两个 Uvicorn 进程，并连接真实 PostgreSQL 和 Redis，验证注册、稳定路由、RLS 和故障转移。

## 6. IM Channel Adapter

### 6.1 企业微信 AIBot

当前实现支持企业微信智能机器人 API 的 URL 回调模式：

- GET URL 有效性校验；
- `msg_signature`、timestamp、nonce 验签；
- AES-256-CBC 加解密；
- 文本、语音转写、图文混排和媒体元数据归一化；
- `aibotid` 与已发布 Channel Binding 一致性检查；
- `msgid` 进入租户级 Inbox 幂等约束；
- Agent 完成后通过一次性 `response_url` 异步发送 Markdown 回复；
- `response_url` 限制为企业微信官方 HTTPS 主机和固定 API 路径。

项目已经完成真实企业微信账号、真实 DeepSeek API、SQL Inbox/Execution/Outbox、真实异步回复和 Jaeger Trace 验证。

### 6.2 企业微信自建应用

支持 URL 验证、SHA-1 签名、AES 加解密、文本/媒体消息归一化和加密被动回复。

### 6.3 Telegram Bot

支持 Secret Token、Update 归一化、`update_id` 去重、私聊/群聊 Session、`sendMessage`、媒体元数据和 editMessage 流式更新。当前通过自动化测试验证，没有真实 Telegram 公网账号证据。

接口和验证步骤见 [Gateway 与 IM 说明](docs/gateway-im.md) 和 [功能验证手册](docs/verification-guide.md)。

## 7. 治理、安全与可观测性

Worker 在 Runner 前执行 IM ACL、PII 脱敏和租户预算检查；Tool 调用前执行租户白名单和危险工具二次确认。治理决策写入 Audit Log。模型 API Key、IM token 和数据库密码只保存 `env://`、`vault://` 或 KMS 引用，运行时由 SecretResolver 解析。

OpenTelemetry 在异步边界传播 trace context。真实企业微信链路可以在同一 Trace 中看到：

```text
HTTP POST /webhooks/wecom/{account}
  → im.consume
  → gateway.route
  → storage.session.read
  → agent.execute
  → invocation / agent_run / call_llm
  → storage.commit_turn
  → outbox.deliver
```

基础 Compose 启动 OTel Collector，并将指标暴露在宿主机 `8889`。Collector 的 Jaeger OTLP 目标为 `host.docker.internal:14317`，需要按验证手册另行启动 Jaeger；Jaeger UI 通常位于 `http://127.0.0.1:16686`。日志过滤器和 Collector 均移除 Authorization、Cookie、数据库语句、异常正文和 GenAI prompt/completion 等敏感字段。

## 8. 快速开始

### 8.1 本地开发模式

默认配置使用 SQLite、InMemory Coordination/InMemory Vector 和 Local Artifact，不要求 Docker 或模型密钥。

```powershell
cd "<project-root>"

python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
$env:PYTHONUTF8="1"

python -m pip install -e ".[dev,storage]"
python -m trpc_service serve --host 127.0.0.1 --port 8000
```

不激活虚拟环境也可以直接运行：

```powershell
$env:PYTHONUTF8="1"
.\.venv\Scripts\python.exe -m trpc_service serve --host 127.0.0.1 --port 8000
```

启动后访问：

- 服务信息：`http://127.0.0.1:8000/`
- 存活检查：`http://127.0.0.1:8000/health/live`
- 就绪检查：`http://127.0.0.1:8000/health/ready`
- OpenAPI：`http://127.0.0.1:8000/docs`
- Prometheus：`http://127.0.0.1:8000/metrics/`

### 8.2 Docker Compose 联调模式

确保 Docker Desktop 已启动。仅做健康检查时可以不配置模型和企业微信密钥；真实 Agent/企业微信验证需要在当前 PowerShell 会话或 `.env` 中提供相应变量。

```powershell
docker compose up -d --build
docker compose ps
docker compose logs app --tail 100
```

本项目的 `docker-compose.override.yml` 暴露以下端口：

| 服务 | 地址 |
|---|---|
| FastAPI | `http://127.0.0.1:8000` |
| PostgreSQL | `127.0.0.1:15432` |
| Redis | `127.0.0.1:16379` |
| Qdrant | `http://127.0.0.1:16333` |
| MinIO API | `http://127.0.0.1:19000` |
| MinIO Console | `http://127.0.0.1:19001` |
| OTel OTLP gRPC/HTTP | `127.0.0.1:4317/4318` |
| OTel Prometheus Exporter | `http://127.0.0.1:8889/metrics` |

Compose 使用命名卷保存 PostgreSQL、Redis、Qdrant 和 MinIO 数据。`docker compose stop/start` 和普通 `down/up` 不删除数据；`docker compose down -v` 会删除这些卷，执行前请确认。

### 8.3 数据库迁移

Compose 的 `migrate` 服务会在 App 启动前运行：

```powershell
docker compose run --rm migrate
```

本地也可以执行：

```powershell
.\.venv\Scripts\python.exe -m alembic upgrade head
```

当前包含 4 个迁移：平台完整 Schema、IM 用户映射、PostgreSQL RLS、Audit Session 字段扩展。

## 9. 测试与验证

### 9.1 静态检查和单元/集成测试

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest -q
```

真实双节点测试需要显式指定 PostgreSQL 和 Redis：

```powershell
$env:TRPC_INTEGRATION_DATABASE_URL="postgresql+psycopg://trpc:trpc-dev-only@127.0.0.1:15432/trpc"
$env:TRPC_INTEGRATION_REDIS_URL="redis://127.0.0.1:16379/0"

.\.venv\Scripts\python.exe -m pytest .\tests\test_two_node_integration.py -m integration -q -s
```

### 9.2 当前验收证据

`docs/evidence/` 已保存：

- Compose 服务、健康检查、PostgreSQL 表和 Redis 证据，以及 Qdrant/MinIO 服务可访问截图；
- 全量回归 `63 passed, 1 skipped` 的记录；
- 单独执行真实 PostgreSQL+Redis 双节点测试 `1 passed` 的记录；
- 企业微信 AIBot URL 校验成功和真实回复截图；
- Inbox/Execution/Outbox 数据库状态；
- 已脱敏的企业微信完整 Trace JSON。

需要区分验证层次：Telegram、Vault/KMS、mTLS 和 Kubernetes 具备代码、测试或部署清单，但没有全部完成真实第三方/生产环境验证；真实企业微信示例调用了模型但没有触发 Tool，因此该 Trace 不能作为真实 Tool 外部调用证据。

## 10. 生产部署与运维

- `Dockerfile` 与 `docker-compose.yml`：最小联调环境；
- `deploy/kubernetes.yaml`：stable/canary、Pod IP `NODE_BASE_URL`、健康检查和 Telemetry；
- `deploy/otel-collector-config.yaml`：OTLP 接收、敏感字段清理、Prometheus 和 Jaeger 导出；
- `migrations/versions/`：Alembic Schema 与 PostgreSQL RLS；
- `scripts/migrate_data.py`：Redis→SQL、SQL Memory→Qdrant、Local→MinIO 迁移入口；
- `scripts/backup_sqlite.py`、`restore_sqlite.py`：本地数据备份恢复；
- `scripts/capacity_test.py`：容量和并发基线测试。

生产环境应替换 Compose 中所有开发密码和共享密钥，使用非 owner、`NOBYPASSRLS` 的 PostgreSQL 应用账号，并启用 OIDC/RBAC、mTLS、Vault/KMS、高可用后端、备份、告警、HPA、PDB 和 NetworkPolicy。生产环境禁止选择 InMemory Session。

## 11. 项目目录

```text
├── README.md                     # 项目入口、启动与验收说明
├── trpc_service/
│   ├── agent/                    # AgentFactory 与运行配置
│   ├── channels/                 # 企业微信、Telegram、身份与投递
│   ├── gateway/                  # 节点目录、路由、Inbox 和恢复
│   ├── governance/               # ACL、PII、预算和工具治理
│   ├── storage/                  # SQL/Redis/Vector/Artifact/Outbox
│   ├── tenant/                   # 多租户控制面服务与 Schema
│   ├── metrics/                  # OpenTelemetry 与 Prometheus
│   ├── config/                   # Settings 与 SecretResolver
│   └── web/                      # FastAPI、中间件和路由
├── migrations/versions/          # Alembic 迁移与 PostgreSQL RLS
├── deploy/                       # Kubernetes 和 OTel Collector
├── scripts/                      # 迁移、备份恢复与容量测试
├── tests/                        # 单元测试和真实双节点集成测试
├── docs/                         # 代码级详细说明和验证手册
├── docs/evidence/                # 脱敏后的验收证据
├── deliverables/                 # 最终架构、图表和专项方案
├── docker-compose.yml
├── docker-compose.override.yml
└── Dockerfile
```

## 12. 最终交付物

- [架构设计文档](deliverables/system-architecture-design.md)
- [系统架构图](deliverables/system-architecture.png)
- [核心消息时序图](deliverables/wecom-agent-core-sequence-delivery.png)
- [数据模型设计](deliverables/data-model-design.md)
- [数据同步和幂等策略](deliverables/data-sync-idempotency-strategy.md)
- [多后端适配方案](deliverables/multi-backend-adaptation-plan.md)
- [生产风险清单](deliverables/production-risk-register.md)
- [项目实现代码](trpc_service/)

## 13. 详细文档索引

- [Admin API](docs/admin-api.md)
- [核心数据模型（代码级摘要）](docs/data-model.md)
- [Gateway、跨节点路由与 IM](docs/gateway-im.md)
- [平台存储与 AgentFactory](docs/storage-architecture.md)
- [租户后端选择与 IM 绑定](docs/tenant-backend-and-im-binding.md)
- [治理、可观测性与故障恢复](docs/governance-observability-recovery.md)
- [生产安全、迁移、富媒体与运维](docs/production-hardening.md)
- [功能验证手册](docs/verification-guide.md)

## 14. 提交与安全提示

- 不要提交 `.env`、模型 API Key、企业微信 Token、EncodingAESKey 或数据库生产密码；
- 不要公开未经脱敏的 Trace、日志、用户 ID、Session ID、聊天内容或 IM 配置截图；
- `docker-compose.yml` 中的默认账号和密码只用于隔离的本地开发环境，不可用于生产；
- 提交前检查 `git status`、`git diff --cached`，确认 `data/`、`.venv/`、缓存和临时生成目录未入库；
- Docker 命名卷不会随 Git 仓库提交，老师 Clone 后会得到一套新的数据库。

## 15. License

本项目沿用原始仓库的许可证与第三方依赖许可要求。

# tRPC-Agent-Service

基于 tRPC-Agent-Python 理念实现的生产级多租户 Agent 平台，支持水平扩展、多后端存储、IM 通道集成和全面的治理能力。

## 项目概述

本平台帮助企业在多个租户、部门和 IM 通道上部署和管理 Agent 应用，提供统一的配置、审计和可观测性。每个租户可以配置自己的 Agent 应用、模型提供商、工具权限、IM 绑定和存储后端，同时平台确保隔离、可靠性和水平扩展能力。

**核心能力：**
- **多租户隔离**：租户级配置、数据、工具、密钥和审计日志隔离
- **水平扩展**：无状态 Worker 池，共享 Session/Memory/Inbox/Outbox 后端
- **多后端支持**：InMemory、Redis、PostgreSQL、SQLite、向量库、对象存储
- **IM 集成**：企业微信（智能机器人 API）、飞书、Telegram、Web UI
- **治理能力**：工具白名单/黑名单、预算控制、审批流程、密钥脱敏
- **可观测性**：OpenTelemetry 分布式追踪、Prometheus 指标、结构化审计日志
- **可靠性**：幂等处理、补偿机制、基于 lease 的并发控制、优雅降级

## 系统架构

平台由以下组件组成：

- **Agent Gateway**：从 IM 通道路由入站消息到正确的租户和会话，入队工作项，投递出站响应
- **Agent Worker**：执行 SDK Runner、模型调用、Tool/MCP 调用，写入 Session/Memory/Audit 到共享后端
- **Channel Adapter**：处理 IM 特定协议（签名验证、媒体下载、消息格式化），支持企业微信、飞书、Telegram、Web UI
- **Storage Adapter**：为 Session、Memory、Summary、Artifact、Knowledge、Audit、Idempotency 提供统一抽象，支持多后端
- **Admin API**：管理租户、Agent 应用、通道绑定、配置版本、发布/回滚、灰度发布
- **Telemetry**：集成 OpenTelemetry 分布式追踪和 Prometheus 指标

详见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) 查看系统架构图和组件细节。

## 快速开始

### 环境要求

- Python 3.12 或更高版本
- Docker 和 Docker Compose（仅用于 Compose/Kubernetes 复现）
- Redis 和 PostgreSQL（仅在共享后端或 Compose 复现时需要）

### 安装

克隆仓库后使用锁文件创建 Python 3.12 开发环境：

```bash
git clone https://github.com/xyxhhhhh/trpc-agent-service.git
cd trpc-agent-service
uv sync --locked --extra dev
```

Windows PowerShell：

```powershell
uv sync --locked --extra dev --python 3.12
```

没有安装 `uv` 时仍可使用兼容入口：`python -m pip install -r requirements-dev.txt`。
`pyproject.toml` 是依赖声明的唯一来源，`uv.lock` 固定完整依赖图。

### 运行测试

执行完整测试套件。当前仓库共收集 639 个测试，默认结果为 `637 passed、2 skipped`。
默认不会自动启用以下两个 opt-in 集成测试：

- `tests/test_postgres_rls.py::PostgresRLSIntegrationTests::test_app_role_isolation_and_admin_visibility`
  会创建和删除数据库角色、RLS 策略及测试数据，必须明确指定可丢弃的 PostgreSQL 数据库，
  避免误操作开发或生产数据。
- `tests/test_real_model_integration.py::RealModelIntegrationTests::test_responses_api_with_configured_model`
  会向真实模型服务发起网络请求并消耗配额，还会受到服务可用性、延迟和限流影响；默认测试应保持
  无外部凭据、可离线重复执行。

启用相应环境变量后，可以单独运行这两个测试：

```bash
POSTGRES_RLS_TEST_DSN="postgresql://user:password@localhost:5432/disposable_db" \
POSTGRES_RLS_TEST_ALLOW_DESTRUCTIVE=1 \
uv run pytest -q tests/test_postgres_rls.py

RUN_REAL_MODEL_TESTS=1 \
CPA_BASE_URL="https://provider.example/v1" \
CPA_MODEL="provider-model-id" \
OPENAI_API_KEY="<new-key>" \
uv run pytest -q tests/test_real_model_integration.py
```

本次本地 opt-in 验证结果为 PostgreSQL RLS `8 passed`、真实模型 `1 passed`；两项均启用时，
完整套件对应 `639 passed、0 skipped`。API key 只能通过环境变量或 Secret Manager 注入，
不得写入仓库、配置样例、日志或测试输出。

默认质量检查命令仍为：

```bash
uv run python scripts/quality_gate.py
```

默认质量检查使用仓库 `pyproject.toml` 中的实用型 mypy 规则，检查未标注函数体、未使用
的忽略项/冗余类型转换和无效配置。原始实战题不要求 strict mypy；如需额外执行完整
严格检查，可运行 `uv run mypy --config-file mypy-strict.ini trpc_service`。strict
配置作为持续改进工具保留，不作为本次实战功能验收的硬门禁。

默认环境下，项目 release gate 使用 unittest 作为其中一项检查，因此它会报告 191 个测试
（其中 2 个跳过）；启用两个 opt-in 集成测试后，该口径也会相应纳入这两项测试。
pytest 还会收集以函数形式定义的测试。两种命令的
通过结果一致，统计数量不同是测试发现器口径不同造成的。

也可以分别运行测试、编译和代码风格检查：

```bash
python -m compileall -q trpc_service tests
python -m flake8 trpc_service tests
```

安装完成后可以直接使用服务和迁移命令：

```bash
uv run trpc-agent --help
uv run trpc-agent-migrate --help
```

### Reviewer 最短复现路径

不配置模型密钥即可复现核心平台链路：

```bash
uv run python scripts/quality_gate.py
uv run python scripts/release_gate.py
```

然后启动本地 Web UI：

```bash
python scripts/web_ui_launcher.py --runtime-mode local --port 18001
```

浏览器访问 `http://127.0.0.1:18001/ui`。题目要求到代码、测试和部署证据的映射见
[docs/ACCEPTANCE.md](docs/ACCEPTANCE.md)；当前实现边界和外部依赖见
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) 与 [docs/IM_INTEGRATION.md](docs/IM_INTEGRATION.md)。

### 启动 Web UI（本地开发）

Web UI 提供浏览器聊天界面，用于本地测试，无需配置外部 IM 平台：

**Windows:**
```powershell
.\start-web-ui.ps1
```

**Linux/macOS:**
```bash
chmod +x start.sh
./start.sh
```

访问地址：`http://127.0.0.1:18001/ui`

停止服务：
```powershell
.\stop-web-ui.ps1  # Windows
```
```bash
./stop.sh  # Linux/macOS
```

### Docker Compose 部署

用于多节点部署，包含 Redis 和 PostgreSQL：

1. 从示例生成本地 `.env` 文件：

**Windows PowerShell:**
```powershell
Copy-Item .env.example .env
$password = [guid]::NewGuid().ToString("N")
(Get-Content .env) `
  -replace '^POSTGRES_PASSWORD=.*$', "POSTGRES_PASSWORD=$password" `
  -replace '^POSTGRES_DSN=.*$', "POSTGRES_DSN=postgresql://trpc_agent:$password@sql:5432/trpc_agent" `
  -replace '^TENANT_DB_DSN=.*$', "TENANT_DB_DSN=postgresql://trpc_agent:$password@sql:5432/trpc_agent" |
  Set-Content .env -Encoding ascii
```

**Linux/macOS:**
```bash
cp .env.example .env
password="$(openssl rand -hex 24)"
sed -i.bak \
  -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${password}|" \
  -e "s|^POSTGRES_DSN=.*|POSTGRES_DSN=postgresql://trpc_agent:${password}@sql:5432/trpc_agent|" \
  -e "s|^TENANT_DB_DSN=.*|TENANT_DB_DSN=postgresql://trpc_agent:${password}@sql:5432/trpc_agent|" \
  .env
rm -f .env.bak
```

2. 启动平台（2 个 Worker 副本）：

```bash
docker compose --env-file .env -f deployment/docker-compose.yml up --build --scale worker=2
```

Compose 会先运行 Alembic 升级和 Schema 检查，再启动 Gateway 与 Worker。应用容器保持
`POSTGRES_AUTO_CREATE_SCHEMA=0`，不会在请求处理期间执行 DDL。

> `--env-file .env` 是必需的：Compose 默认从 compose 文件所在目录（`deployment/`）读取 `.env`，
> 而上一步生成的 `.env` 在仓库根目录，省略该参数会报 `required variable POSTGRES_DSN is missing a value`。

3. 验证部署：

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/livez
curl http://127.0.0.1:8000/readyz
curl http://127.0.0.1:8000/ui
```

详细部署说明、IM 集成指南和故障排查见 [docs/README.md](docs/README.md)。

## 文档索引

### 核心设计文档

| 文档 | 说明 |
|------|------|
| [docs/README.md](docs/README.md) | **文档入口**：阅读顺序、本地复现、Docker 部署、IM 联调指南 |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | **架构设计**：系统架构图、租户模型、路由机制、隔离策略、部署拓扑 |
| [docs/SEQUENCE.md](docs/SEQUENCE.md) | **核心时序**：企业微信消息 → Agent 执行 → Tool 调用 → IM 回复完整链路 |
| [docs/DATA_MODEL.md](docs/DATA_MODEL.md) | **数据模型**：租户配置、表结构、统一存储抽象（Session、Memory、Summary、Audit） |
| [docs/MIGRATION.md](docs/MIGRATION.md) | **数据同步**：并发控制、幂等处理、补偿机制、Redis/SQL/向量库迁移策略 |
| [docs/IM_INTEGRATION.md](docs/IM_INTEGRATION.md) | **IM 集成**：企业微信智能机器人、飞书、Telegram、Web UI 接入与真实联调 |
| [docs/CAPACITY.md](docs/CAPACITY.md) | **容量规划**：估算方法、监控指标、最小部署与生产部署建议 |
| [docs/RISKS.md](docs/RISKS.md) | **风险清单**：15 项生产风险及缓解措施 |

### 实现与验收

| 文档 | 说明 |
|------|------|
| [docs/ACCEPTANCE.md](docs/ACCEPTANCE.md) | **验收映射表**：题目要求逐项映射到代码、测试用例和证据文件 |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | **实现说明**：实现范围、运行模式、IM SDK 接入、模型配置 |
| [deployment/kubernetes/README.md](deployment/kubernetes/README.md) | **Kubernetes 部署**：生产模板前置条件和本地 dev-local 复现 |

**推荐阅读顺序**：从 [docs/README.md](docs/README.md) 开始，然后按 ARCHITECTURE → SEQUENCE → DATA_MODEL → MIGRATION → IM_INTEGRATION 的顺序阅读核心设计，最后查看 ACCEPTANCE 和 IMPLEMENTATION 了解实现细节。

## 项目结构

```
├── trpc_service/           # 平台源代码
│   ├── tenant/             # 租户配置、版本控制、灰度发布
│   ├── gateway/            # Gateway 路由、Worker 池、运行队列
│   ├── channels/           # IM 适配器（企业微信、飞书、Telegram、Web UI）
│   ├── storage/            # 多后端适配器（Redis、SQL、向量库、对象存储）
│   ├── policy/             # 工具治理、限流、配额执行
│   ├── security/           # 密钥引用、脱敏、RBAC
│   ├── telemetry/          # OpenTelemetry 追踪和 Prometheus 指标
│   ├── web/                # Admin API 和 Web UI
│   └── log/                # 结构化日志与密钥脱敏
├── tests/                  # 单元和集成测试（639 个已收集测试用例）
├── deployment/             # Docker Compose 和 Kubernetes 清单
├── docs/                   # 架构、数据模型、集成指南（见上方索引）
├── scripts/                # 工具脚本（Web UI 启动器、验证、基准测试）
└── data/                   # 发布制品、SBOM、验收证据
```

## IM 通道集成

平台支持以下 IM 通道：

- **wecom_ai_bot**（默认注册，真实连接需显式启用）：企业微信智能机器人 API，使用 BotID/BotSecret 长连接（无需公网 webhook）
- **feishu**：飞书/Lark，支持签名验证和媒体附件
- **telegram**：Telegram Bot API，使用 python-telegram-bot SDK
- **web**：浏览器 Web UI，用于本地测试

传统 **wecom**（基于 webhook 的企业微信回调）默认禁用，仅在设置 `ENABLE_LEGACY_WECOM=1` 时注册。
`wechat_official_account` 和 `wechat_customer_service` 已从默认运行时退役，仅保留兼容性测试代码；当前可部署通道为 `wecom_ai_bot`、`feishu`、`telegram` 和 `web`。

详见 [docs/IM_INTEGRATION.md](docs/IM_INTEGRATION.md) 了解 webhook 配置、签名验证和真实 IM 测试。

**SDK 使用说明**：
- 企业微信智能机器人使用可选的 `wecom-aibot-sdk-python`；传统企业微信和微信生态兼容适配器优先使用 `wechatpy` / `wechatpy-cryptography`
- Telegram 优先使用 `python-telegram-bot`
- 飞书优先使用 `lark-oapi`（飞书官方 SDK），在 SDK 不可用或凭据不完整时降级到 HTTP API

## 测试

测试套件包含：

- **多租户**：租户隔离、配置版本控制、灰度发布
- **路由**：通道绑定、会话路由、并发更新
- **存储**：Redis、SQLite、PostgreSQL 后端，基于 lease 的并发控制
- **可靠性**：幂等处理、补偿机制、Worker 崩溃恢复、Toxiproxy 故障注入
- **IM 通道**：企业微信、飞书、Telegram 消息解析和签名验证
- **Kubernetes**：生产清单静态安全检查

运行测试：
```bash
python -m pytest -q
```

运行覆盖率测试：
```bash
./coverage.sh
```

## 部署

### 最小部署（本地/演示）

- 单个 Gateway + Worker 进程
- InMemory 或 SQLite 存储
- Web UI 用于测试

```bash
python -m trpc_service demo
```

### 生产部署

- Gateway（2+ 副本），使用 Redis 运行队列
- Worker 池（4+ 副本），共享 PostgreSQL 和 Redis
- PostgreSQL 配置 RLS 实现租户隔离
- OpenTelemetry collector 和 Prometheus
- 外部向量库和对象存储

使用 Kubernetes 部署：
```bash
kubectl apply -k deployment/kubernetes
```

详见 [docs/CAPACITY.md](docs/CAPACITY.md) 了解容量规划和扩展建议。

## 配置

租户通过 Admin API 或 YAML 配置。最小租户配置示例：

```python
tenant_config = TenantConfig(
    tenant_id="demo-tenant",
    agent_apps=[
        AgentApp(
            agent_app_id="assistant",
            agent_prompt="你是一个有帮助的助手。",
            model_config=ModelConfig(
                provider="anthropic",
                model="claude-opus-5",
                api_key_ref="secret://demo-tenant/anthropic/api_key"
            )
        )
    ],
    channel_bindings=[
        ChannelBinding(
            channel="web",
            account_id="demo-account",
            agent_app_id="assistant"
        )
    ],
    storage_profile=StorageProfile(
        session_backend="redis",
        memory_backend="sql"
    )
)
```

发布配置：
```python
repo.publish(tenant_config)
```

完整租户 schema 见 [docs/DATA_MODEL.md](docs/DATA_MODEL.md)。

## 安全

- **密钥管理**：所有密钥使用 `secret://` 或 `env://` 引用，绝不明文
- **日志脱敏**：Authorization 头、Token、手机号、邮箱自动脱敏
- **租户隔离**：Redis key 前缀、SQL 复合主键、PostgreSQL RLS、向量库 collection 作用域
- **工具治理**：每租户白名单/黑名单、危险工具审批流程
- **RBAC**：Admin API key 角色用于租户管理
- **公共入口保护**：生产设置 `PUBLIC_SURFACE_AUTH_REQUIRED=1`，`/metrics`、`/ui`
  和 `/ui/api/chat` 需要管理凭据；本地 loopback 演示可保持为 `0`

隔离机制见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)，安全风险见 [docs/RISKS.md](docs/RISKS.md)。

## 可观测性

- **追踪**：OpenTelemetry span 通过 `trace_id` 串联 Gateway → Worker → 模型 → 工具 → 存储 → IM 回复
- **指标**：Prometheus 指标包括请求量、模型延迟、工具耗时、存储 QPS、IM 投递成功率
- **审计日志**：结构化日志包含 `tenant_id`、`session_id`、`tool_name`、`decision`、`latency`、`cost`、`trace_id`

指标端点：
```bash
curl http://127.0.0.1:8000/metrics
```

## 第六阶段安全与灾备验收

安全、供应链和 PostgreSQL 备份恢复演练的执行入口见
`scripts/production_acceptance.py`、`scripts/disaster_recovery_gate.py` 和
`scripts/security_gate.py`。外部数据库、Kubernetes、Toxiproxy 和真实 IM 未配置时，
对应门禁会明确失败或跳过，不会伪装成本地生产验收通过。

## 致谢

本项目基于 [tRPC-Agent-Python](https://github.com/tRPC-Agent/tRPC-Agent-Python) 设计理念，并集成以下 SDK：

- `wechatpy` / `wechatpy-cryptography` 用于企业微信
- `lark-oapi` 用于飞书
- `python-telegram-bot` 用于 Telegram
- `anthropic` / `openai` / `google-generativeai` 用于模型提供商
- `qdrant-client` / `chromadb` 用于向量存储

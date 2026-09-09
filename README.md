# tRPC-Agent-Service

面向生产场景设计并实现的多租户 Agent 平台参考实现。项目基于
[tRPC-Agent-Python](https://github.com/tRPC-Agent/tRPC-Agent-Python)，覆盖节点化部署、
多后端存储、IM 通道接入、租户治理、可观测性和故障恢复。

## 项目概述

平台允许多个租户分别配置 Agent 应用、模型、工具权限、IM 绑定、存储后端、
审计策略和配额。Gateway 与 Worker 采用无状态设计，通过共享 Session、Memory、
Inbox/Outbox 和幂等后端支持水平扩展。

核心能力：

- **多租户隔离**：配置、数据、工具、密钥、日志、审计和成本按租户隔离。
- **节点化部署**：无状态 Gateway/Worker，通过共享队列与存储跨节点路由会话。
- **多后端适配**：支持 InMemory、SQLite、Redis、PostgreSQL、向量库和对象存储。
- **IM 接入**：支持企业微信智能机器人、飞书、Telegram 和本地 Web UI。
- **治理与安全**：工具白名单/黑名单、预算限制、危险工具审批、RBAC 和敏感信息脱敏。
- **可靠性**：幂等、Session lease/fencing、补偿任务、重试、死信和灰度回滚。
- **可观测性**：OpenTelemetry Trace、Prometheus 指标和结构化审计日志。

## 审阅者快速验收

以下路径不需要模型密钥或真实 IM 账号：

```bash
git clone https://github.com/xyxhhhhh/trpc-agent-service.git
cd trpc-agent-service
git checkout feature/xiayuxuan
uv sync --locked --extra dev --python 3.12
uv run python scripts/quality_gate.py
uv run python scripts/release_gate.py
uv run python scripts/web_ui_launcher.py --runtime-mode local --port 18001
```

浏览器访问 `http://127.0.0.1:18001/ui`。Windows 也可以运行
`.\start-web-ui.ps1 --runtime-mode local --port 18001`，停止服务使用
`.\stop-web-ui.ps1`。Linux/macOS 可使用 `./start.sh` 和 `./stop.sh`。

题目要求到代码、测试和文档的逐项映射见[验收映射表](docs/ACCEPTANCE.md)。

### 当前验收状态

| 验收项 | 状态 | 说明 |
| --- | --- | --- |
| 默认质量门禁 | 已本地验证 | `compileall`、pytest、flake8、Ruff 已通过；外部依赖测试按条件跳过。 |
| 发布门禁 | 已本地验证 | unittest、关键恢复测试、安全、可观测性和 Kubernetes 静态门禁已通过。 |
| 本地 Web UI | 已本地验证 | `/health` 和 `/ui/api/chat` smoke test 已通过。 |
| Docker Compose | 配置已验证 | Compose 配置可渲染；完整运行需要启动 Docker daemon。 |
| PostgreSQL RLS | 需要外部服务 | 需要明确指定可丢弃的 PostgreSQL 数据库后执行 opt-in 测试。 |
| 真实模型与 IM | 需要外部凭据 | 需要模型配额、真实平台账号以及公网 HTTPS 或长连接网络。 |
| 生产 Kubernetes | 提供部署模板 | 静态门禁和 Kustomize 渲染通过；真实发布前必须替换镜像、Secret、域名和外部服务。 |

跳过外部依赖测试不等于对应生产能力已经验收。实现边界见
[实现说明](docs/IMPLEMENTATION.md)，真实 IM 前置条件见
[IM 联调手册](docs/IM_INTEGRATION.md)。

## 系统架构

```mermaid
flowchart LR
    IM[企业微信 / 飞书 / Telegram] --> CA[Channel Adapter]
    CA --> GW[Agent Gateway]
    ADMIN[Admin API] --> CFG[Tenant Repository]
    GW --> CFG
    GW --> Q[(Redis Queue)]
    Q --> W[Agent Worker Pool]
    W --> F[Filter / Policy]
    F --> R[tRPC-Agent Runner]
    R --> T[Tool / MCP / Knowledge]
    W --> S[Storage Adapter]
    S --> REDIS[(Redis)]
    S --> SQL[(PostgreSQL / SQLite)]
    S --> VEC[(Vector Store)]
    S --> OBJ[(Object Store)]
    GW --> OUT[IM Outbound]
    CA -. trace_id .-> OTEL[OpenTelemetry]
    GW -. trace_id .-> OTEL
    W -. trace_id .-> OTEL
```

- **Agent Gateway**：解析租户绑定、生成会话与幂等键、执行配额检查并分发任务。
- **Agent Worker**：运行 tRPC-Agent Runner、模型与工具，并写回共享状态。
- **Channel Adapter**：处理平台验签、解密、消息转换、媒体和出站限制。
- **Storage Adapter**：统一 Session、Memory、Summary、Artifact、Knowledge、Audit 和 Idempotency。
- **Admin API**：管理租户配置版本、发布、回滚和灰度。
- **Telemetry**：使用 `trace_id` 串联回调、路由、模型、工具、存储和回复。

完整架构与时序见[架构设计](docs/ARCHITECTURE.md)和[核心时序](docs/SEQUENCE.md)。

## 环境与安装

- Python 3.12 或更高版本。
- 推荐安装 `uv`，依赖由 `pyproject.toml` 声明、`uv.lock` 锁定。
- Docker 和 Docker Compose 仅在 Compose/Kubernetes 复现时需要。
- Redis 和 PostgreSQL 仅在共享后端或 Compose 复现时需要。

```bash
uv sync --locked --extra dev --python 3.12
```

未安装 `uv` 时可以使用兼容入口：

```bash
python -m pip install -r requirements-dev.txt
```

### 企业微信智能机器人（可选）

企业微信长连接模式需要额外安装 SDK：

```bash
pip install wecom-aibot-sdk-python
```

## 测试与质量门禁

推荐直接执行仓库门禁：

```bash
uv run python scripts/quality_gate.py
uv run python scripts/release_gate.py
```

`quality_gate.py` 运行 `compileall`、pytest、flake8 和 Ruff。`release_gate.py`
额外覆盖 unittest、Session/恢复测试、Kubernetes 静态检查、安全和可观测性。
unittest 与 pytest 的发现规则不同，因此统计数量可能不同，以命令实际输出为准。

mypy 是额外检查，不属于默认质量门禁：

```bash
uv run mypy --config-file mypy-strict.ini trpc_service
```

PostgreSQL RLS、真实模型和故障注入测试需要外部服务或凭据，默认保持 opt-in。
具体启用方式见[文档索引与复现指南](docs/README.md#外部依赖测试)。API key 只能
通过环境变量或 Secret Manager 注入，不得写入仓库、配置样例、日志或测试输出。

## Docker Compose 复现

Compose 包含 Gateway、多 Worker、Redis、PostgreSQL、补偿和出站处理。先从示例生成
根目录 `.env`；该文件已被 Git 忽略。无真实模型凭据时，将其中
`TRPC_AGENT_RUNTIME_MODE` 设置为 `local`。

Windows PowerShell：

```powershell
Copy-Item .env.example .env
$password = [guid]::NewGuid().ToString("N")
(Get-Content .env) `
  -replace '^POSTGRES_PASSWORD=.*$', "POSTGRES_PASSWORD=$password" `
  -replace '^POSTGRES_DSN=.*$', "POSTGRES_DSN=postgresql://trpc_agent:$password@sql:5432/trpc_agent" `
  -replace '^TENANT_DB_DSN=.*$', "TENANT_DB_DSN=postgresql://trpc_agent:$password@sql:5432/trpc_agent" |
  Set-Content .env -Encoding ascii
```

Linux/macOS 的等价生成命令见[完整 Docker 复现指南](docs/README.md#docker-复现)。启动：

```bash
docker compose --env-file .env -f deployment/docker-compose.yml up --build --scale worker=2
```

验证：

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/livez
curl http://127.0.0.1:8000/readyz
curl http://127.0.0.1:8000/ui
```

`--env-file .env` 不能省略，因为 `.env` 位于仓库根目录，而 Compose 文件位于
`deployment/`。

## Kubernetes 生产模板

`deployment/kubernetes/platform.yaml` 是生产导向模板，不是可原样启动的本地 Demo。
仓库内的 Kustomize 配置仍包含示例镜像仓库和版本占位符；直接部署可能导致
`ImagePullBackOff`，缺少 External Secrets、TLS 或外部数据服务时也会失败或保持 Pending。

审阅者可以安全执行静态渲染：

```bash
kubectl kustomize deployment/kubernetes
uv run python scripts/kubernetes_runtime_gate.py --static
```

真实部署前必须替换镜像仓库和不可变 tag/digest，并配置 SecretStore、TLS、域名、
Redis、PostgreSQL、向量库、对象存储及遥测后端。准备完成后才执行：

```bash
kubectl apply -k deployment/kubernetes
```

本地 Kubernetes 验证请使用 `deployment/kubernetes/dev-local.yaml`。完整前置条件和
命令见[Kubernetes 部署说明](deployment/kubernetes/README.md)。

## 租户配置示例

以下示例与当前数据模型和仓库接口一致，可以直接运行：

```python
from trpc_service.tenant.models import (
    AgentApp,
    ChannelBinding,
    ModelConfig,
    StorageProfile,
    TenantConfig,
)
from trpc_service.tenant.repository import InMemoryTenantRepository

tenant_config = TenantConfig(
    tenant_id="demo-tenant",
    apps=[
        AgentApp(
            agent_app_id="assistant",
            agent_name="assistant",
            prompt="你是一个有帮助的助手。",
            model_config=ModelConfig(
                provider="openai-compatible",
                model="provider-model-id",
                api_key_ref="secret://demo-tenant/model/api-key",
            ),
        )
    ],
    channel_bindings=[
        ChannelBinding(
            tenant_id="demo-tenant",
            binding_id="web:demo-account",
            channel="web",
            account_id="demo-account",
            agent_app_id="assistant",
        )
    ],
    storage_profile=StorageProfile(
        session_backend="memory",
        memory_backend="memory",
    ),
)

repository = InMemoryTenantRepository()
published = repository.create(tenant_config)
assert repository.get("demo-tenant").config_version == published.config_version
```

配置版本更新后，可使用
`repository.save_and_publish(config, expected_version=current.config_version)` 原子保存并
发布新版本。完整字段和持久化模型见[数据模型设计](docs/DATA_MODEL.md)。

## IM 通道

| 通道 | 状态 | 接入方式 |
| --- | --- | --- |
| `wecom_ai_bot` | 默认注册，真实连接需显式启用 | 企业微信智能机器人 BotID/BotSecret 长连接。 |
| `feishu` | 可部署 | 飞书回调/SDK，支持验签和媒体。 |
| `telegram` | 可部署 | Telegram Bot API/Webhook。 |
| `web` | 本地验证 | 浏览器 Web UI，不替代真实 IM 验收。 |

传统 `wecom` 回调仅在 `ENABLE_LEGACY_WECOM=1` 时注册；
`wechat_official_account` 和 `wechat_customer_service` 仅保留兼容性测试代码。
真实账号配置、媒体和验收清单见[IM 联调手册](docs/IM_INTEGRATION.md)。

## 安全与可观测性

- 密钥只保存 `secret://` 或 `env://` 引用，日志、Trace 和错误统一脱敏。
- Redis Key、SQL 主键、向量集合、对象路径和权限检查均包含租户作用域。
- Admin API 支持 API Key/OIDC、角色和租户授权。
- OpenTelemetry 使用 `trace_id` 串联 IM、Gateway、Worker、模型、工具、存储和回复。
- Prometheus 暴露请求、模型、工具、IM、Token、成本和存储延迟指标。

生产环境必须设置 `PUBLIC_SURFACE_AUTH_REQUIRED=1`；真实灾备、托管数据服务和外部
供应链证据需要在目标环境中单独验收。

## 文档索引

| 文档 | 内容 |
| --- | --- |
| [docs/README.md](docs/README.md) | 文档入口、完整复现命令和外部依赖测试。 |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 架构图、租户隔离、路由、治理和部署拓扑。 |
| [docs/SEQUENCE.md](docs/SEQUENCE.md) | 企业微信消息到 Agent 回复的完整时序。 |
| [docs/DATA_MODEL.md](docs/DATA_MODEL.md) | 租户配置、核心表结构和存储抽象。 |
| [docs/MIGRATION.md](docs/MIGRATION.md) | 并发、幂等、补偿和后端迁移。 |
| [docs/IM_INTEGRATION.md](docs/IM_INTEGRATION.md) | 企业微信、飞书和 Telegram 真实联调。 |
| [docs/CAPACITY.md](docs/CAPACITY.md) | 容量估算和部署建议。 |
| [docs/RISKS.md](docs/RISKS.md) | 15 项生产风险及缓解措施。 |
| [docs/ACCEPTANCE.md](docs/ACCEPTANCE.md) | 题目要求到代码、测试和文档的逐项映射。 |
| [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) | 当前实现范围、运行模式和外部依赖边界。 |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | 探针、指标、告警、审计和故障处理。 |
| [deployment/kubernetes/README.md](deployment/kubernetes/README.md) | Kubernetes 生产模板及本地验证前置条件。 |

## 项目结构

```text
├── trpc_service/           # 平台源代码
│   ├── tenant/             # 租户配置、版本控制、灰度发布
│   ├── gateway/            # 路由、Worker 池和运行队列
│   ├── channels/           # 企业微信、飞书、Telegram、Web UI
│   ├── storage/            # Redis、SQL、向量库和对象存储适配器
│   ├── policy/             # 工具治理、限流和配额
│   ├── security/           # 密钥引用、脱敏和 RBAC
│   ├── telemetry/          # OpenTelemetry 和 Prometheus
│   └── web/                # Admin API 和 Web UI
├── tests/                  # 单元、集成、故障和安全测试
├── deployment/             # Docker Compose 与 Kubernetes 清单
├── docs/                   # 架构、数据模型、集成和验收文档
├── scripts/                # 质量、发布、迁移和运行脚本
└── data/                   # 本地运行数据目录，运行产物不提交 Git
```

## 致谢

本项目复用 tRPC-Agent-Python 的 Runner、模型、Tool/MCP、Filter、Session 接口和
Telemetry 能力，并在其上实现租户、路由、IM、多后端、治理和运维平台层。

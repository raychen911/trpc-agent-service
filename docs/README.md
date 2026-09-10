# 文档索引与复现指南

本文档是 `docs/` 目录的入口，说明项目设计文档的阅读顺序，以及本地、Docker 和真实 IM 联调的复现入口。

## 项目说明

`trpc-agent-service` 是一个基于 tRPC-Agent-Python 思路实现的多租户、节点化 Agent 部署平台。项目包含租户配置与隔离、无状态 Gateway / Worker、IM Channel Adapter、多后端 Storage Adapter、治理审计、可观测性和部署配置。

## 审阅者推荐复现顺序

1. 使用 `uv.lock` 创建 Python 3.12 开发环境。
2. 运行完整测试、编译检查和 `flake8`。
3. 运行本地 Web UI，验证 Gateway、Worker、Session、Memory、Tool、Audit 和回复链路。
4. 使用 Docker Compose 复现 Redis、PostgreSQL、远程 Worker、补偿 Worker 和出站 Worker。
5. 只有具备外部凭据时，才运行真实模型、PostgreSQL RLS 或真实 IM 联调。

题目要求到代码、测试和证据的映射见 [ACCEPTANCE.md](./ACCEPTANCE.md)；复现命令直接使用仓库根目录的 `scripts/` 和 `tests/`，运行结果按需生成到被 Git 忽略的 `data/` 目录。

## 文档索引

建议按下面顺序阅读：

1. [ARCHITECTURE.md](./ARCHITECTURE.md) - 总体架构、租户模型、请求路由、隔离机制、一致性取舍和部署方案。
2. [ACCEPTANCE.md](./ACCEPTANCE.md) - 题目要求到代码、测试和证据文件的逐项映射。
3. [SEQUENCE.md](./SEQUENCE.md) - 企业微信智能机器人消息到 Agent 回复的完整时序链路，展示 `trace_id` 如何贯穿全链路。
4. [DATA_MODEL.md](./DATA_MODEL.md) - 租户配置、核心表结构和 Session、Memory、Summary、Artifact、Knowledge、Audit 的统一数据访问抽象。
5. [MIGRATION.md](./MIGRATION.md) - 多节点并发写、幂等、补偿、Redis 到 SQL 和向量库迁移策略。
6. [IM_INTEGRATION.md](./IM_INTEGRATION.md) - 企业微信智能机器人、飞书、Telegram 和 Web UI 的接入、验签、媒体和真实联调说明。
7. [CAPACITY.md](./CAPACITY.md) - 容量估算、监控指标、最小部署和生产部署建议。
8. [RISKS.md](./RISKS.md) - 生产风险清单及缓解措施。
9. [IMPLEMENTATION.md](./IMPLEMENTATION.md) - 本分支实现边界、运行方式、IM SDK 接入和模型配置说明。
10. [OPERATIONS.md](./OPERATIONS.md) - 探针、指标、告警、审计查询和日常运维。

## 本地复现

### 环境要求

- Python 3.12 或更高版本。
- 推荐使用项目虚拟环境，并安装开发依赖。

Windows、Linux 或 macOS：

```bash
uv sync --locked --extra dev --python 3.12
```

没有安装 `uv` 时可运行 `python -m pip install -r requirements-dev.txt`。依赖以 `pyproject.toml` 为唯一声明来源，并由 `uv.lock` 固定解析结果。

检查环境：

```bash
uv run python -m pip check
uv run python scripts/quality_gate.py
```

### 运行完整测试

```bash
uv run python -m pytest -q
```

跳过项是显式要求真实外部服务或凭据的测试，不代表本地核心测试失败。`scripts/release_gate.py` 内部使用 unittest，pytest 还会收集函数式测试，因此两者的测试数量统计可能不同。

### 启动本地 Web UI

Windows：

```powershell
.\start-web-ui.ps1 --runtime-mode local --port 18001
```

Linux 或 macOS：

```bash
chmod +x start.sh stop.sh
TRPC_AGENT_RUNTIME_MODE=local PORT=18001 ./start.sh
```

启动后访问：

```text
http://127.0.0.1:18001/ui
```

停止服务：

```powershell
.\stop-web-ui.ps1
```

或：

```bash
./stop.sh
```

Web UI 是本地 IM 流程自测入口，可验证消息路由、Session、Memory、工具调用、审计和回复链路；它不替代真实 IM 平台联调。

### 本地 API 验证

本地脚本默认监听 `18001`：

```bash
curl -i http://127.0.0.1:18001/health
curl -i http://127.0.0.1:18001/metrics
curl -i http://127.0.0.1:18001/ui
curl -X POST http://127.0.0.1:18001/ui/api/chat \
  -H 'Content-Type: application/json' \
  -d '{"text":"hello","user_id":"reviewer-user"}'
```

Compose Gateway 默认监听 `8000`。验证 Compose 时将上面地址中的端口替换为 `8000`；本地 Web UI 与 Compose Gateway 是两条不同的启动路径。

## Docker 复现

Docker Compose 部署入口位于：

```text
deployment/docker-compose.yml
```

首次复现请先在仓库根目录生成本地 `.env`。`.env` 已被 Git 忽略，不会提交真实密钥。Windows PowerShell：

```powershell
Copy-Item .env.example .env
$password = [guid]::NewGuid().ToString("N")
(Get-Content .env) `
  -replace '^POSTGRES_PASSWORD=.*$', "POSTGRES_PASSWORD=$password" `
  -replace '^POSTGRES_DSN=.*$', "POSTGRES_DSN=postgresql://trpc_agent:$password@sql:5432/trpc_agent" `
  -replace '^TENANT_DB_DSN=.*$', "TENANT_DB_DSN=postgresql://trpc_agent:$password@sql:5432/trpc_agent" |
  Set-Content .env -Encoding ascii
```

Linux/macOS：

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

无真实模型凭据时，确认 `.env` 中保留：

```text
TRPC_AGENT_RUNTIME_MODE=local
```

然后启动：

```bash
docker compose --env-file .env -f deployment/docker-compose.yml up --build --scale worker=2
```

`--env-file .env` 必须显式指定。Compose 默认从 `deployment/` 目录解析 `.env`，而这里生成的 `.env` 位于仓库根目录。

启动后验证：

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/ui
curl -X POST http://127.0.0.1:8000/ui/api/chat \
  -H 'Content-Type: application/json' \
  -d '{"text":"hello","user_id":"reviewer-user"}'
```

Compose 默认使用 Redis 提供队列、幂等和热 Session，使用 PostgreSQL 持久化租户配置、Memory、Summary、Audit 和补偿任务。Gateway 设置 `WORKER_REMOTE=1` 后通过 Redis 将请求交给无状态 Worker；普通 Compose 不自动应用 `deploy.replicas`，因此复现两 Worker 时必须显式指定 `--scale worker=2`。`DURABLE_INBOX_OUTBOX=0` 仅用于本地演示；多节点、生产或可靠性验收必须设置 `DURABLE_INBOX_OUTBOX=1`。

Compose 启动前会先运行一次 `db-preflight`，使用应用实际的 `POSTGRES_DSN` 登录 PostgreSQL。认证成功后才会启动 Gateway、Worker 和后台 Worker；认证失败会阻止应用进入重启循环，并且不会删除数据卷或打印密码。只有可丢弃的本地数据才允许执行：

```bash
docker compose --env-file .env -f deployment/docker-compose.yml down -v
```

Compose 容器内的 DSN 主机名必须使用 `sql` 和 `redis`，不能使用 `localhost`。无真实模型凭据的本地 Compose 演示可显式设置 `TRPC_AGENT_RUNTIME_MODE=local`；生产默认保持 `trpc` 并配置租户模型凭据。

## 外部依赖测试

以下检查默认不在质量门禁中运行，必须使用专用、可丢弃的外部环境，并且不会使用仓库中的真实密钥：

### PostgreSQL RLS

准备一个仅用于测试的 PostgreSQL 数据库和具有创建测试角色权限的连接，然后设置：

```bash
POSTGRES_RLS_TEST_DSN="postgresql://admin:password@127.0.0.1:5432/trpc_agent_test"
POSTGRES_RLS_TEST_ALLOW_DESTRUCTIVE=1
uv run python -m pytest -q tests/test_postgres_rls.py
```

该测试会创建并清理测试角色、策略和表，禁止指向生产数据库。完整生产验收脚本
`scripts/production_acceptance.py` 也会在设置相同环境变量时包含该检查。

### 真实模型

仅在已配置供应商凭据、额度和网络访问时启用：

```bash
RUN_REAL_MODEL_TESTS=1 uv run python -m pytest -q tests/test_real_model_integration.py
```

模型 API key 只能通过环境变量或 Secret Manager 注入，不能写入命令示例、`.env`、日志或 Git。

### 故障注入与真实 IM

故障注入需要可控的 Toxiproxy/Redis/PostgreSQL 环境；真实企业微信、飞书和 Telegram 还需要账号、Webhook 或长连接网络。相关变量和验收脚本见 `scripts/fault_injection_gate.py`、`scripts/online_im_gate.py`、`scripts/production_acceptance.py` 及 [IM_INTEGRATION.md](./IM_INTEGRATION.md)。

## 真实 IM 联调

真实企业微信、飞书或 Telegram 联调需要公网 HTTPS 地址或长连接网络、平台账号、Webhook、Token、Secret 和应用权限。具体绑定、验签、媒体消息和验收步骤见 [IM_INTEGRATION.md](./IM_INTEGRATION.md)。

联调 Redis、PostgreSQL、对象存储、远端向量库或外部 Memory 前，可以运行不打印密钥的健康检查：

```bash
uv run python scripts/validate_integrations.py
```

Kubernetes 生产模板的 External Secrets、TLS、镜像摘要、外部 Redis/PostgreSQL、向量库和对象存储前置条件，以及本地 `dev-local` 复现方式，见 [deployment/kubernetes/README.md](../deployment/kubernetes/README.md)。生产模板在未准备这些依赖时会被 Kubernetes 拒绝或保持未就绪，这是预期的前置检查结果；不要把生产模板直接当作无依赖的本地 Compose 替代品。

## 验收对照

题目要求与本地实现的逐项对照见 [ACCEPTANCE.md](./ACCEPTANCE.md)。本地实测可按其中命令复现；运行时报告按需写入被 Git 忽略的 `data/` 目录，不作为仓库预置交付物。架构、时序、数据模型、数据同步、IM 接入、治理监控、故障恢复和风险清单均可从上述文档中追溯。

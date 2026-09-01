# 文档索引与复现指南

本文档是 `docs/` 目录的入口，说明项目设计文档的阅读顺序，以及本地、Docker 和真实 IM 联调的复现入口。

## 项目说明

`trpc-agent-service` 是一个基于 tRPC-Agent-Python 思路实现的多租户、节点化 Agent 部署平台。项目包含租户配置与隔离、无状态 Gateway / Worker、IM Channel Adapter、多后端 Storage Adapter、治理审计、可观测性和部署配置。

## 文档索引

建议按下面顺序阅读：

1. [ARCHITECTURE.md](./ARCHITECTURE.md) - 总体架构、租户模型、请求路由、隔离机制、一致性取舍和部署方案。
2. [SEQUENCE.md](./SEQUENCE.md) - 企业微信消息到 Agent 回复的完整时序链路，展示 `trace_id` 如何贯穿全链路。
3. [DATA_MODEL.md](./DATA_MODEL.md) - 租户配置、核心表结构和 Session、Memory、Summary、Artifact、Knowledge、Audit 的统一数据访问抽象。
4. [MIGRATION.md](./MIGRATION.md) - 多节点并发写、幂等、补偿、Redis 到 SQL 和向量库迁移策略。
5. [IM_INTEGRATION.md](./IM_INTEGRATION.md) - 企业微信、微信公众号、微信客服、Telegram 和 Web UI 的 Webhook、验签、媒体和真实联调说明。
6. [CAPACITY.md](./CAPACITY.md) - 容量估算、监控指标、最小部署和生产部署建议。
7. [RISKS.md](./RISKS.md) - 生产风险清单及缓解措施。
8. [IMPLEMENTATION.md](./IMPLEMENTATION.md) - 本分支实现边界、运行方式、IM SDK 接入和模型配置说明。
9. [IMPLEMENTATION_REVIEW.md](./IMPLEMENTATION_REVIEW.md) - 对题目要求、难点、验收标准和实测证据的逐项实现审查。
10. [POSTGRES_RLS.md](./POSTGRES_RLS.md) - PostgreSQL RLS 双层租户隔离、角色、迁移和验证。

## 本地复现

### 环境要求

- Python 3.12 或更高版本。
- 安装项目依赖。Windows 建议明确使用 Python 3.12：

```bash
py -3.12 -m pip install -r requirements.txt
```

Linux 或 macOS：

```bash
python3.12 -m pip install -r requirements.txt
```

### 运行单元测试

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

### 启动本地 Web UI

Windows：

```powershell
.\start-web-ui.ps1
```

Linux 或 macOS：

```bash
chmod +x start.sh stop.sh
./start.sh
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

## Docker 复现

Docker Compose 部署入口位于：

```text
deployment/docker-compose.yml
```

首次复现请先在仓库根目录生成本地 `.env`。`.env` 已被 Git 忽略，
不会提交真实密钥。Windows PowerShell：

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
docker compose -f deployment/docker-compose.yml up --build --scale worker=2
```

启动后验证：

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/ui
curl -X POST http://127.0.0.1:8000/ui/api/chat \
  -H 'Content-Type: application/json' \
  -d '{"text":"hello","user_id":"reviewer-user"}'
```

Windows PowerShell 可将最后三条命令替换为：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-WebRequest http://127.0.0.1:8000/ui
Invoke-RestMethod http://127.0.0.1:8000/ui/api/chat `
  -Method Post -ContentType 'application/json' `
  -Body '{"text":"hello","user_id":"reviewer-user"}'
```

Compose 默认使用 Redis 提供队列、幂等和热 Session，使用 PostgreSQL 持久化租户配置、Memory、Summary、Audit 和补偿任务。Gateway 设置 `WORKER_REMOTE=1` 后通过 Redis 将请求交给无状态 Worker；普通 Compose 不自动应用 `deploy.replicas`，因此复现两 Worker 时必须显式指定 `--scale worker=2`。`DURABLE_INBOX_OUTBOX=0` 仅用于本地演示；多节点、生产或可靠性验收必须在 `.env` 中设置 `DURABLE_INBOX_OUTBOX=1`，启用共享 Inbox/Outbox 和崩溃恢复。详细部署与容量说明见 [CAPACITY.md](./CAPACITY.md)。

Compose 启动前会先运行一次 `db-preflight`，使用应用实际的
`POSTGRES_DSN` 登录 PostgreSQL。认证成功后才会启动 Gateway、Worker 和后台
Worker；认证失败会阻止应用进入重启循环，并且不会删除数据卷或打印密码。
注意 PostgreSQL 官方镜像的 `POSTGRES_PASSWORD` 只在
`postgres_data` 首次初始化时生效，修改 `.env` 不会修改复用旧卷中的数据库
密码。复用旧卷时应使用旧密码，或按运维流程执行受控密码轮换并同步更新
`POSTGRES_DSN`。只有可丢弃的本地数据才允许执行：

```bash
docker compose -f deployment/docker-compose.yml down -v
```

Compose 容器内的 DSN 主机名必须使用 `sql` 和 `redis`，不能使用
`localhost`。无真实模型凭据的本地 Compose 演示可显式设置
`TRPC_AGENT_RUNTIME_MODE=local`；生产默认保持 `trpc` 并配置租户模型凭据。

## 真实 IM 联调

真实企业微信、微信公众号、微信客服或 Telegram 联调需要公网 HTTPS 地址、平台账号、Webhook、Token、Secret 和应用权限。具体绑定、验签、媒体消息和验收步骤见 [IM_INTEGRATION.md](./IM_INTEGRATION.md)。

联调 Redis、PostgreSQL、对象存储、远端向量库或外部 Memory 前，可以运行不打印密钥的健康检查：

```bash
python scripts/validate_integrations.py
```

## 验收对照

题目要求与本地实现的逐项对照、Redis/PostgreSQL 故障注入、Kubernetes 部署验证、fallback 吞吐基准和真实模型小并发压测的实测摘要见 [IMPLEMENTATION_REVIEW.md](./IMPLEMENTATION_REVIEW.md)。架构、时序、数据模型、数据同步、IM 接入、治理监控、故障恢复和风险清单均可从上述文档中追溯。

# tRPC-Agent 多租户节点化 Agent 平台

这是一个基于 tRPC-Agent-Python 的可运行多租户 Agent 平台实现。项目将单体 Agent 扩展为 Gateway、双 Worker、Channel Adapter、Storage Adapter、Admin API 和 OpenTelemetry Collector 协作的节点化系统，支持共享状态、多后端、企业微信与飞书接入、租户治理、审计和故障恢复。

本仓库同时提供最小可运行的 Docker Compose 拓扑、生产推荐的 Kubernetes 清单、完整设计文档、自动化验收入口和对应源码。根 README 是交付导航；设计细节和可执行证据由下列文档与代码共同构成。

## 交付状态

| 交付项 | 交付结果 | 入口 |
|---|---|---|
| 架构设计文档 | 已提供，覆盖拓扑、租户隔离、节点路由、治理、故障与能力边界 | [docs/architecture.md](docs/architecture.md) |
| 系统架构图 | 已提供 Mermaid 图，覆盖 Gateway、Worker、Channel、Filter、Storage、Telemetry、数据库与 IM | [架构图](docs/architecture.md#系统拓扑) |
| 企业微信核心时序图 | 已提供 Mermaid 时序图，覆盖入站、Tool、Session/Memory、审计与回复 | [完整消息链路](docs/architecture.md#完整企业微信消息链路) |
| 数据模型设计 | 已提供核心实体、关系、表结构、版本链和迁移说明 | [docs/data-model.md](docs/data-model.md) |
| 数据同步和幂等策略 | 已提供并发写入、事件顺序、跨节点可见性、重复/乱序和迁移策略 | [docs/data-sync-idempotency.md](docs/data-sync-idempotency.md) |
| 多后端适配方案 | 已提供 Redis、PostgreSQL、MinIO/S3 的实现与向量检索扩展边界 | [docs/backend-strategy.md](docs/backend-strategy.md) |
| 生产风险清单 | 已提供不少于 8 项风险、影响和缓解措施 | [docs/production-risks.md](docs/production-risks.md) |
| GitHub 工程实现 | 当前仓库即完整 Python 实现，包含源码、迁移、部署和验收脚本 | [trpc_service](trpc_service)、[migrations](migrations)、[compose.yaml](compose.yaml)、[deploy/k8s](deploy/k8s) |
| 验收证据矩阵 | 已将每项验收标准映射到实现、测试和命令 | [docs/acceptance-matrix.md](docs/acceptance-matrix.md) |

## 已实现能力

### 多租户与节点部署

- `tenant_id` 贯穿配置、ChannelBinding、Session、Memory、Artifact、Knowledge、审计、用量和 Trace。
- Gateway 负责认证、租户解析、准入和 Rendezvous 路由；Worker 保持无状态，从共享后端恢复 Session/Memory。
- 同一 session 使用 Redis 租约串行写入，不依赖负载均衡层 sticky session。
- 租户配置版本化，支持乐观并发、灰度发布、promote、abort 和通过新版本回滚。
- 工具白名单、用户准入、内容治理、限流、预算和审批均在 Worker 执行前强制生效。

### 数据同步与多后端

- Redis：Session 热状态、Memory、租约、限流、排序水位和短期协调。
- PostgreSQL：租户配置、版本历史、ChannelBinding、消息收据、审计、用量、审批，以及当前 Knowledge 文档与全文检索索引。
- MinIO/S3：Artifact 对象字节；PostgreSQL 保存其元数据、版本状态和对象键。
- 生产扩展可使用向量库保存 Knowledge chunk embedding、向量索引和租户过滤元数据，适合大规模语义检索。
- Session event、state、summary 按固定顺序提交；消息收据和稳定 `message_id` 防止 IM 重投导致模型或工具重复执行。
- 提供离线 `state-backend-migrate`，迁移前冻结租户流量，校验后通过新配置版本切换。

### IM 接入

- 企业微信 AI Bot 长连接：使用 Bot ID/Bot Secret 认证，支持流式快照回复。
- 企业微信标准 HTTP 回调：使用 Callback Token 和 EncodingAESKey，支持 GET URL 验证、POST HMAC 验签、AES-CBC 解密和 `success` 确认响应。
- 飞书长连接：使用 App ID/App Secret 认证，支持事件订阅和流式卡片回复。
- 外部消息不能声明 tenant；Gateway 只根据已认证账号和持久化 ChannelBinding 解析 tenant/app。
- 单聊、群聊、账号和租户共同参与 session 身份投影，避免跨群、跨账号或跨租户串话。
- 重复、乱序、长度限制、媒体拒绝、发送前重试、部分发送不重试和投递审计均有明确策略。

### 治理、监控与安全

- 内容 Filter、工具决策、危险操作审批、租户限流和 token/成本预算采用 fail-closed 行为。
- `request_id` 与 `trace_id` 贯穿 IM、Gateway、Worker、Runner、Tool、Session/Memory 和回复。
- 暴露请求量、延迟、错误、模型/工具调用、IM 投递、token、成本和后端延迟指标。
- 审计记录 tenant、channel、user、session、agent、tool、decision、latency、error、cost 和 trace。
- 密钥只通过 `env:TRPC_*` 引用解析；数据库、日志、Trace、错误响应和验收证据不保存密钥值。

## 部署拓扑

### 最小可运行方案：Docker Compose

Compose 是本项目的正式最小交付拓扑，包含 1 个 Gateway、2 个无状态 Worker、1 个 Admin API、1 个一次性 init，以及 Redis、PostgreSQL、MinIO 和 OpenTelemetry Collector。

Compose 使用独立 project、network 和 named volumes 管理自己的资源，不接管宿主机已有 Redis、PostgreSQL 或 MinIO。Gateway 和 Admin 默认只绑定 `127.0.0.1`；发布端口可通过 `TRPC_GATEWAY_PUBLISH_PORT` 和 `TRPC_ADMIN_PUBLISH_PORT` 覆盖。

### 生产推荐方案：Kubernetes

[deploy/k8s](deploy/k8s) 提供 namespace、ConfigMap、Secret 示例、唯一迁移 Job、Gateway、双副本 Worker、HPA、Admin、Redis、PostgreSQL、MinIO 和 Collector 清单。生产环境中：

- Gateway 单活持有 IM 长连接；Worker 保持无状态并由 HPA 横向扩容。
- init Job 是唯一数据库迁移执行者，业务 Pod 等待迁移完成。
- 使用集群 Secret/KMS 替换示例 Secret，不提交实际凭据。
- 使用 HTTPS Ingress 暴露 Gateway 回调；Admin 只允许管理网络访问。
- 使用托管 Redis、PostgreSQL 和对象存储替换最小单实例后端；需要大规模语义检索时再接入租户隔离的向量服务。

企业微信 HTTPS Ingress 模板见 [deploy/k8s/gateway-ingress.example.yaml](deploy/k8s/gateway-ingress.example.yaml)。模板默认不加入 Kustomization，必须先替换 `REPLACE_WITH_PUBLIC_HOST` 和 `REPLACE_WITH_TLS_SECRET`，且不要暴露 Admin，避免误发布占位配置或管理接口。

## 快速开始

### 1. 前置条件

- Python 3.12
- Docker Engine 与 Docker Compose v2
- 一个 OpenAI 兼容模型账号

```bash
python3.12 -m venv .venv
./.venv/bin/pip install -e '.[dev]'
```

### 2. 配置

复制 [.env.example](.env.example) 到仅存在于本机的 `.env`，或在自己的 shell 安全导出变量。不要提交、打印或在 issue 中粘贴变量值。

Compose 至少需要：

```text
TRPC_MODEL_PROVIDER
TRPC_MODEL_NAME
TRPC_MODEL_BASE_URL
TRPC_MODEL_API_KEY
TRPC_INTERNAL_TOKEN
TRPC_ADMIN_TOKEN
TRPC_COMPOSE_PG_USER
TRPC_COMPOSE_PG_PASSWORD
TRPC_S3_ACCESS_KEY
TRPC_S3_SECRET_KEY
```

```bash
./.venv/bin/python -m trpc_service._cli model-config-check
```

### 3. 启动 Compose

```bash
docker compose up --build --wait
curl -fsS http://127.0.0.1:${TRPC_GATEWAY_PUBLISH_PORT:-8000}/health
curl -fsS http://127.0.0.1:${TRPC_ADMIN_PUBLISH_PORT:-8003}/health
```

```bash
docker compose down -v
```

### 本地开发脚本

`start.sh`/`stop.sh` 仅用于本地进程开发，不是生产守护方式。未配置外部后端时，`start.sh` 只能管理带有本项目精确 ownership marker 的开发容器；发现同名但无 marker 的 Redis、PostgreSQL 或 MinIO 时会安全失败，不会复用或停止其他运行。

```bash
./start.sh
./stop.sh
```

## 企业微信与飞书配置

先通过 Admin API 创建启用的 ChannelBinding。binding 决定 tenant/app/account，所有 secret 字段只能保存 `env:TRPC_*` 引用。实际 secret 放入部署环境；本地长连接模板见 [deploy/im.env.example](deploy/im.env.example)。

### 企业微信 AI Bot 长连接

Bot ID 与 Bot Secret 只用于 AI Bot 长连接认证，不能用于标准 HTTP callback 的 URL 验证或 AES 解密。配置持久化 binding 后，将 Bot Secret 注入 Gateway 环境并重启 Gateway。

### 企业微信标准 HTTP callback

为企业微信 binding 配置：

```text
webhook_token_ref=env:TRPC_WECOM_WEBHOOK_TOKEN
webhook_aes_key_ref=env:TRPC_WECOM_WEBHOOK_AES_KEY
```

对应环境变量分别保存企业微信后台设置的 Callback Token 和 EncodingAESKey。企业微信后台回调地址为：

```text
GET|POST https://PUBLIC_HOST/webhooks/wecom/{external_account_id}
```

Gateway 按企业微信协议执行签名验证和解密。GET 返回解密后的 challenge；POST 文本消息进入同一租户、幂等、Worker 和审计链路，成功后返回 `success`。公网必须使用 HTTPS；Compose 默认 localhost 端口不能直接作为企业微信公网地址。

### 飞书长连接

飞书 binding 使用 App ID 标识外部账号，App Secret 通过 `secret_ref` 注入。事件进入与企业微信相同的租户解析、身份投影、幂等、治理和 Worker 链路，回复使用飞书流式卡片协议。

## 快速验收

按“快速开始”完成 `.env` 配置后，在仓库根目录执行：

```bash
bash scripts/acceptance_final.sh --preflight
bash scripts/acceptance_final.sh
```

`--preflight` 检查部署配置和本地依赖；完整脚本自动启动隔离的 Compose 拓扑，验证真实模型、双 Worker、共享后端、租户隔离、幂等、治理、审计和故障恢复，成功时输出 `PASS` 并自动清理本次创建的资源。

真实 IM 验收只需额外完成以下步骤：

1. 复制 `deploy/im.env.example` 为 `deploy/im.env`，填入企业微信 Bot Secret 和飞书 App Secret。
2. 执行 `docker compose up --build --wait`，打开 Admin API 文档 `http://127.0.0.1:${TRPC_ADMIN_PUBLISH_PORT:-8003}/docs`。
3. 调用 `POST /admin/v1/tenants/{tenant_id}/channel-bindings` 创建 binding，请求携带 `X-TRPC-Admin-Token`。企业微信填写 Bot ID 和 `secret_ref=env:TRPC_WECOM_BOT_SECRET`；飞书填写 App ID 和 `secret_ref=env:TRPC_FEISHU_APP_SECRET`。
4. 日志出现 `WeCom AI Bot authenticated and started`、`Feishu AI Bot connected and ready` 后，分别向两个机器人发送消息并确认收到回复。
5. 复用同一会话连续发送消息，再从不同机器人或不同会话发送消息，确认上下文连续且互不串话。

凭据只保存在已被 Git 忽略的 `.env` 或 `deploy/im.env`；验收完成后执行 `docker compose down -v`。

## 运维与故障恢复

- Worker、模型、工具、Redis、PostgreSQL 和 IM 失败统一映射为固定安全结果，不泄漏正文或凭据。
- 可能已产生外部副作用的工具不会自动重放；孤儿审批需要明确运维处置。
- 数据库迁移由单一 init 执行；应用实例不并发执行 Alembic。
- 配置更新使用 expected version；回滚创建新版本，不覆写历史。
- 容量评估工具：`./.venv/bin/python scripts/capacity_probe.py --help`。
- 详细故障矩阵和恢复步骤见 [docs/production-risks.md](docs/production-risks.md)。

## 验收

### 一键交付验收

```bash
bash scripts/acceptance_final.sh --preflight
bash scripts/acceptance_final.sh
```

`--preflight` 只验证 Compose 结构和本地依赖，不启动拓扑。完整验收使用唯一 Compose project 和自动选择的空闲宿主端口，验证真实模型、双 Worker、共享后端、真实 Gateway Webhook URL 验证、租户隔离、幂等、治理、审批、审计、灰度和故障契约；退出时只清理本次创建的容器、网络和卷。

企业微信和飞书的真实消息验收按“快速验收”中的 IM 步骤执行，并以平台实际收发结果作为验收证据。

```bash
bash scripts/acceptance_webhook.sh
bash scripts/acceptance_r3_operations.sh
kubectl kustomize deploy/k8s >/dev/null
```

完整的“需求 → 实现 → 测试 → 命令”映射见 [docs/acceptance-matrix.md](docs/acceptance-matrix.md)。

## 验收标准对应关系

| 验收标准 | 当前交付证据 |
|---|---|
| 1. 覆盖多租户、节点化部署、数据同步、多后端、IM、治理监控和故障恢复 | [架构设计](docs/architecture.md)、Compose、Kubernetes、治理与故障代码 |
| 2. 模型表达 tenant、agent、channel binding、session、event、memory、summary、audit | [数据模型](docs/data-model.md)、[SQLAlchemy schema](trpc_service/storage/schema.py)、Alembic 迁移 |
| 3. 至少两种 IM，且包含微信或企业微信 | 企业微信 AI Bot/HTTP callback 与飞书 Adapter；差异见 [架构文档](docs/architecture.md#im-账号绑定认证与平台差异) |
| 4. 至少三类后端及同步策略 | Redis、PostgreSQL、MinIO/S3，并说明可选向量后端边界，见 [后端策略](docs/backend-strategy.md) |
| 5. 完整消息链路并贯穿 trace_id/request_id | [企业微信时序图](docs/architecture.md#完整企业微信消息链路) 与 telemetry 实现 |
| 6. 至少 8 个生产风险及缓解措施 | [生产风险清单](docs/production-risks.md) |
| 7. 明确框架复用与平台新增能力 | [责任边界](docs/architecture.md#trpc-agent-python-与平台层责任) |

## 代码结构

```text
trpc_service/
├── agent/          # Runner、Agent 与事件执行
├── admin/          # 租户、binding、审计、审批、灰度管理 API
├── channels/       # 企业微信、飞书、Webhook、身份和投递策略
├── config/         # 租户配置、版本和 secret resolver
├── gateway/        # HTTP/IM 接入、认证和 Worker 路由
├── governance/     # 内容、工具、用户、限流和预算治理
├── storage/        # Redis/SQL/S3/Knowledge/审计仓储
├── telemetry/      # Trace、指标和日志关联
├── worker/         # 无状态执行节点与审批恢复
└── web/            # HTTP 请求/响应模型与静态资源

migrations/         # Alembic 数据库迁移
deploy/k8s/         # 生产推荐 Kubernetes 清单
scripts/            # 验收、容量和运维脚本
tests/              # 单元、契约和真实后端集成测试
compose.yaml        # 最小可运行交付拓扑
```

## 文档索引

- [架构设计、架构图和核心时序图](docs/architecture.md)
- [数据模型](docs/data-model.md)
- [数据同步、一致性和幂等](docs/data-sync-idempotency.md)
- [多后端适配策略](docs/backend-strategy.md)
- [生产风险和缓解措施](docs/production-risks.md)
- [验收证据矩阵](docs/acceptance-matrix.md)
- [完整文档目录](docs/README.md)

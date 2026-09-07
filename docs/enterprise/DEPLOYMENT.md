# tRPC-Agent Enterprise 部署指南

本文给出两套可直接落地的部署方案：

- **最小可运行方案**：使用 Docker Compose，在单机上启动 Gateway、Redis 和 MySQL；可选独立 Worker。
- **生产推荐方案**：使用 Kubernetes，将 Gateway 与 Worker 解耦并分别扩缩容，状态放在高可用 Redis/MySQL/Qdrant/S3 兼容服务中。

相关部署文件位于 [`deploy/`](../../deploy/)；本文中的命令默认在仓库根目录执行。

## 1. 组件与数据流

```text
IM 平台
   │ HTTPS webhook
   ▼
Gateway（验签、解析、幂等、快速 ACK）
   │
   ├─ 进程内模式：直接调用 TenantWorker
   │
   └─ 解耦模式：Redis Streams → Worker
                                │
                                ├─ LLM / Tool
                                ├─ Redis：队列、锁、幂等、预算、Session/Memory（可选）
                                ├─ MySQL：租户配置、审计、Session/Memory（可选）
                                ├─ Qdrant：Knowledge embedding
                                └─ S3/COS/MinIO：Artifact 与知识原文
```

Gateway 暴露以下主要端点：

| 端点 | 用途 |
|---|---|
| `GET /healthz` | Gateway 健康检查 |
| `POST /webhook/{tenant_id}/{channel}` | IM 平台回调入口 |
| `GET /admin/ui` | 管理页面 |
| `/admin/*` | 租户、审计和指标管理 API |

支持的 `channel` 包括 `wecom`、`wechat_kf`、`dingtalk`、`feishu` 和 `qq`。

## 2. 最小可运行方案：Docker Compose

### 2.1 前置条件

- Docker Engine；
- Docker Compose v2；
- 可访问所配置的 LLM API；
- 如需接收真实 IM 回调，需要一个公网 HTTPS 地址或反向代理隧道。

现有 [`docker-compose.minimal.yml`](../../deploy/docker-compose.minimal.yml) 包含：

- `gateway`：FastAPI webhook 和 Admin API；
- `worker`：Redis Streams 消费者；
- `redis`：队列、会话、幂等、分布式锁、预算和 HITL 状态；
- `mysql`：租户配置和审计等持久化数据。
- `artifact-data`：Gateway/Worker 共享的本地 Artifact 卷；向量检索默认使用单 Worker 内存实现。

本地对象卷和内存向量索引只用于最小验证。需要多 Worker 或持久 Knowledge 时，应改用本页
生产方案中的 Qdrant 与 S3/COS/MinIO。

### 2.2 准备环境变量

至少为真实模型调用设置模型密钥，并覆盖示例中的本地密钥：

```bash
export TRPC_AGENT_API_KEY='<model-api-key>'
export TENANT_CONFIG_ENCRYPTION_KEY='<stable-random-secret>'
export ADMIN_API_KEY='<admin-api-key>'
```

按实际接入渠道补充对应环境变量，例如 QQ：

```bash
export QQBOT_APP_ID='<qq-app-id>'
export QQBOT_APP_SECRET='<qq-app-secret>'
```

企业微信、微信客服、钉钉和飞书所需变量可从
[`tenants.yaml`](../../deploy/tenants.yaml) 中的 `${VAR}` 引用确认。不要把真实密钥直接写入或提交到 YAML。

### 2.3 选择运行模式

#### 模式 A：最少进程，Gateway 内执行 Agent

适合本地验证或低流量单机环境。关闭队列且不启动独立 Worker：

```bash
AGENT_QUEUE_ENABLED=0 docker compose \
  -f deploy/docker-compose.minimal.yml \
  up --build gateway
```

Compose 会根据依赖自动启动 Redis 和 MySQL。此模式下 Gateway 收到 webhook 后创建后台任务并在本进程执行 Agent。

限制：Gateway 重启会中断正在执行的后台任务，也不能独立扩展 Worker，不建议用于生产。

#### 模式 B：Gateway 与 Worker 解耦

这是 Compose 文件的默认模式：

```bash
docker compose \
  -f deploy/docker-compose.minimal.yml \
  up --build -d
```

Gateway 将任务写入 Redis Streams，Worker 异步消费；两者可以分别扩容。例如增加两个 Worker：

```bash
docker compose \
  -f deploy/docker-compose.minimal.yml \
  up --build -d --scale worker=3
```

### 2.4 验证

查看服务状态和日志：

```bash
docker compose \
  -f deploy/docker-compose.minimal.yml \
  ps

docker compose \
  -f deploy/docker-compose.minimal.yml \
  logs -f gateway worker
```

检查 Gateway：

```bash
curl --fail http://127.0.0.1:8080/healthz
curl --fail \
  -H "X-Admin-API-Key: ${ADMIN_API_KEY}" \
  http://127.0.0.1:8080/admin/health
```

预期分别返回：

```json
{"status":"ok"}
```

管理页面位于 `http://127.0.0.1:8080/admin/ui`。真实 webhook 必须通过对应平台验签，不能用任意 JSON 代替平台回调完成端到端验证。

### 2.5 停止服务

```bash
docker compose \
  -f deploy/docker-compose.minimal.yml \
  down
```

该命令保留 `redis-data` 和 `mysql-data` 命名卷。只有明确不再需要本地数据时才使用 `down -v`。

## 3. 生产推荐方案：Kubernetes

### 3.1 推荐拓扑

```text
公网 HTTPS LB / Ingress
          │
          ▼
Gateway Deployment（至少 2 副本）
          │ Redis Streams
          ▼
Worker Deployment（至少 3 副本）
          │
          ├─ 托管或高可用 Redis
          ├─ 托管或高可用 MySQL
          ├─ LLM / IM 平台 API
          └─ OpenTelemetry Collector
```

建议职责如下：

| 组件 | 生产职责 | 扩缩容依据 |
|---|---|---|
| Gateway | webhook 验签、去重、入队和快速 ACK | HTTP QPS、CPU、P95 延迟 |
| Worker | Agent、模型和工具执行，向 IM 回发结果 | 队列积压、任务时长、CPU/内存 |
| Redis | Streams、幂等、Session 锁、预算和可选 Session/Memory | 容量、QPS、连接数、复制延迟 |
| MySQL | 租户配置事实源、版本历史、审计和可选 Session/Memory | 写入延迟、连接数、存储量 |
| Qdrant/向量库 | Knowledge embedding、元数据过滤和语义召回 | 查询延迟、索引积压、Recall@K |
| S3/COS/MinIO | 附件、Artifact 和知识原文 | 请求延迟、失败率、容量 |
| OTel Collector | 汇聚 trace | 接收速率、队列和导出失败率 |

Gateway 和 Worker 都是无状态计算节点；Session/Memory 使用共享后端，因此不需要 sticky session。

### 3.2 镜像构建

使用 [`Dockerfile`](../../deploy/Dockerfile) 构建并推送不可变版本镜像：

```bash
docker build \
  -f deploy/Dockerfile \
  -t registry.example.com/trpc-agent-enterprise:<version> .

docker push registry.example.com/trpc-agent-enterprise:<version>
```

将 [`agent.yaml`](../../deploy/kubernetes/agent.yaml) 中两个 Deployment 的镜像替换为该版本。生产环境不要使用 `latest`。

### 3.3 外部依赖

生产环境建议使用：

- 带认证、TLS、持久化和自动故障转移的托管/高可用 Redis；
- 多可用区 MySQL，启用备份、时间点恢复和连接池监控；
- 多副本 Qdrant（或 Milvus/pgvector），collection 按 embedding 版本管理；
- S3/COS/MinIO 对象存储，启用版本、生命周期、服务端加密和最小权限；
- 可用的 OTLP 后端；
- Ingress Controller 或云负载均衡器，用于终止 TLS 并公开 webhook。

[`redis.yaml`](../../deploy/kubernetes/redis.yaml) 只有一个无认证 Redis StatefulSet，适合开发或演示，不属于高可用生产 Redis。仓库没有部署生产 MySQL，`MYSQL_URL` 应指向外部数据库。

### 3.4 Secret 与租户配置

[`agent.yaml`](../../deploy/kubernetes/agent.yaml) 需要以下 Secret：

```bash
kubectl -n trpc-agent create secret generic agent-secrets \
  --from-literal=mysql-url='<mysql-url>' \
  --from-literal=api-key='<model-api-key>' \
  --from-literal=tenant-config-encryption-key='<stable-random-secret>' \
  --from-literal=admin-api-key='<admin-api-key>'
```

渠道凭证放入独立 Secret。下面只展示 QQ 示例，其余变量名称应与
[`configmap.yaml`](../../deploy/kubernetes/configmap.yaml) 中的引用一致：

```bash
kubectl -n trpc-agent create secret generic agent-channel-secrets \
  --from-literal=QQBOT_APP_ID='<qq-app-id>' \
  --from-literal=QQBOT_APP_SECRET='<qq-app-secret>'
```

向量库和对象存储凭据放入 `agent-storage-secrets`；键名会作为环境变量注入：

```bash
kubectl -n trpc-agent create secret generic agent-storage-secrets \
  --from-literal=VECTOR_URL='<qdrant-url>' \
  --from-literal=QDRANT_API_KEY='<qdrant-api-key>' \
  --from-literal=OBJECT_STORE_ENDPOINT='<s3-compatible-endpoint>' \
  --from-literal=OBJECT_STORE_REGION='<region>' \
  --from-literal=OBJECT_STORE_ACCESS_KEY='<access-key>' \
  --from-literal=OBJECT_STORE_SECRET_KEY='<secret-key>'
```

正式环境建议由 External Secrets、Vault 或云 KMS 同步 Secret，而不是把明文命令写进脚本或终端历史。

租户非敏感配置放在 ConfigMap 的 `tenants.yaml` 中。`${VAR}` 由应用加载配置时展开，不是由 Kubernetes ConfigMap 自动展开。`TENANT_CONFIG_ENCRYPTION_KEY` 必须长期稳定；更换前需要制定已有租户密文的轮换/重加密方案。

### 3.5 部署顺序

先创建命名空间和 Secret，再应用配置与工作负载。下面的 Redis 清单只用于开发或部署验证：

```bash
kubectl create namespace trpc-agent

kubectl -n trpc-agent apply \
  -f deploy/kubernetes/configmap.yaml

# 仅开发/演示集群执行下一条命令。
kubectl -n trpc-agent apply \
  -f deploy/kubernetes/redis.yaml

kubectl -n trpc-agent apply \
  -f deploy/kubernetes/agent.yaml
```

正式生产部署应跳过 `redis.yaml`，并在 `agent.yaml` 中将 Gateway 和 Worker 的 `REDIS_URL`
都改为来自 Secret 的外部高可用 Redis 连接串。

如果已有 `jaeger-collector:4317` 等 OTLP 接收端，再按环境调整并应用
[`otel-collector.yaml`](../../deploy/kubernetes/otel-collector.yaml)。最后审阅并应用
[`production-hardening.yaml`](../../deploy/kubernetes/production-hardening.yaml)：

```bash
kubectl -n trpc-agent apply \
  -f deploy/kubernetes/production-hardening.yaml
```

该 hardening 文件是基线而非完整生产策略。启用默认拒绝后，必须按集群网络插件和实际地址补充到以下目标的 egress：

- DNS；
- Redis、MySQL、向量库和对象存储；
- LLM API；
- 各 IM 平台发送 API；
- OTLP Collector。

否则 Gateway/Worker 可能健康但无法调用外部 API。

### 3.6 Ingress 与暴露面

仓库的 Kubernetes 文件仅创建 ClusterIP Service，没有创建 Ingress。生产入口应满足：

- 强制 HTTPS，使用受信任证书；
- 将 `/webhook/*` 暴露给 IM 平台；
- 对 `/admin/*` 额外设置企业 SSO、VPN、IP allowlist 或独立内网 Ingress；
- 保留应用层 `ADMIN_API_KEY`，不要只依赖网络边界；
- 配置请求体大小、超时和限速，但 webhook ACK 超时应小于平台要求。

### 3.7 扩缩容与发布

现有 [`agent.yaml`](../../deploy/kubernetes/agent.yaml) 已包含 Gateway/Worker HPA 和健康探针。生产建议进一步调整：

- Gateway 保持至少两个副本，按 QPS、CPU 和延迟扩容；
- Worker 优先按 Redis Streams backlog/最老任务等待时间扩容，CPU 仅作为辅助指标；
- 当前 Worker 的探针只检查主进程存活，生产应增加 Redis 连通性和消费者就绪状态检查；
- 为 Worker 设置与最长模型/工具调用匹配的 `terminationGracePeriodSeconds`；
- 使用滚动发布、不可变镜像 tag 和 `PodDisruptionBudget`；
- 先灰度少量租户，再扩大流量；配置变更使用 Admin API 的版本历史和 rollback 能力回滚。

## 4. 关键环境变量

| 变量 | 用途 | 最小环境 | 生产要求 |
|---|---|---|---|
| `TENANTS_CONFIG` | 启动时租户 YAML/JSON 路径 | 必填 | 使用只读 ConfigMap 或受控配置文件 |
| `TRPC_AGENT_API_KEY` | 默认模型密钥 | 真实 LLM 必填 | Secret/KMS；也可由租户 `api_key_env` 指向其他变量 |
| `REDIS_URL` | 队列、共享锁、幂等、预算/HITL 和 Redis 存储 | Compose 已配置 | 高可用、认证、TLS |
| `MYSQL_URL` | 租户配置、审计和 MySQL 存储 | Compose 已配置 | 高可用、备份、最小权限账号 |
| `TENANT_CONFIG_ENCRYPTION_KEY` | 加密持久化租户密钥字段 | 使用持久化配置时必填 | 强随机、稳定保存、受控轮换 |
| `ADMIN_API_KEY` | `/admin/*` API 鉴权 | 强烈建议 | 必填，并叠加网络访问控制 |
| `AGENT_QUEUE_ENABLED` | 是否通过 Redis Streams 解耦 | `0` 为进程内，默认 `1` | 推荐 `1` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP HTTP 上报地址 | 可选 | 推荐配置 |
| `VECTOR_URL` / `QDRANT_API_KEY` | Qdrant 地址与凭据 | 内存后端不需要 | Secret/KMS、TLS、最小权限 |
| `OBJECT_STORE_*` | S3 兼容端点、区域与凭据 | 本地卷不需要 | Secret/KMS、加密、版本和生命周期 |

渠道环境变量由租户配置里的 `${VAR}` 决定；同一集群可为不同租户设置不同的 `api_key_env` 和渠道变量。

## 5. 上线验收清单

### 功能

- [ ] `/healthz` 和带密钥的 `/admin/health` 正常；
- [ ] 每个渠道的 challenge、验签、消息解析和文本回发通过；
- [ ] 重复 `message_id` 不会重复执行 Agent；
- [ ] 同一用户在不同租户、群聊之间不会串 Session；
- [ ] 危险工具 HITL、工具白名单和预算超限行为符合预期；
- [ ] Worker 重启后未完成任务可恢复或进入重试/DLQ 流程。

### 安全与可靠性

- [ ] 镜像使用固定版本并完成漏洞扫描；
- [ ] 模型、IM、数据库和管理密钥均来自 Secret/KMS；
- [ ] Redis/MySQL 不对公网开放，连接启用认证和 TLS；
- [ ] `/admin/*` 仅管理网络可达；
- [ ] NetworkPolicy 已验证不会阻断 LLM、IM、存储和 OTel；
- [ ] MySQL 备份恢复和 Redis 故障转移完成演练；
- [ ] Qdrant 索引可重建、对象 checksum 对账与 orphan GC 完成演练；
- [ ] 审计保留期、日志脱敏和租户数据删除策略已配置。

### 可观测性

- [ ] Gateway 请求量、错误率和 P95/P99 延迟可见；
- [ ] Redis Streams 积压、重试和 DLQ 可告警；
- [ ] Worker 执行耗时、模型错误、Token/费用和 IM 投递失败可告警；
- [ ] trace 能从 Gateway 跨 Redis Streams 关联到 Worker；
- [ ] Redis、MySQL 和 OTLP 导出失败有告警。

## 6. 方案选择

| 场景 | 推荐方案 |
|---|---|
| 本地开发、功能演示 | Compose 模式 A：Gateway 进程内执行 |
| 单机联调、验证异步链路 | Compose 模式 B：Gateway + Worker |
| 正式生产 | Kubernetes：多副本 Gateway/Worker + 外部高可用 Redis/MySQL/Qdrant/S3 |

最小方案用于验证功能闭环；生产方案的关键差异不只是容器编排，而是共享状态、高可用依赖、独立扩缩容、密钥管理、网络边界、监控告警和恢复演练。

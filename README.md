# tRPC-Agent-Service

面向企业微信 AI Bot 与飞书应用机器人的多租户 Agent 生产运行时。项目基于
`tRPC-Agent-Python 1.1.19` 的公开 Runner、Agent、Event 与 Tool Safety API，提供可靠 IM
接入、事务型 Session、无状态 Worker、租户隔离、运维治理和可观测性。原始需求归档在
[docs/requirements.md](docs/requirements.md)。

本文既是项目入口，也是正式部署模板。默认 production overlay 在同一个 Kubernetes/ACK namespace 中
部署 Gateway、Admin、Worker、两个 IM 通道、后台任务、Ingress、PostgreSQL/pgvector、Redis、MinIO、
Prometheus、Prometheus Adapter 与 OpenTelemetry Collector。托管数据库、缓存、对象存储或观测平台仍可
替换这些内置组件，但必须使用受审查的 managed-services patch；不能一边删除内置依赖，一边继续声称默认
模板能够独立运行。仓库中的主机联调资产只用于历史兼容，不属于正式部署入口。

## 1. 已实现能力

- 由已认证 `channel_binding_id` 解析租户，服务端 HMAC 生成 Session ID，外部消息不能声明租户。
- PostgreSQL Inbox/Outbox、幂等键、Session lease 与 fencing token；Redis Streams 只作可重建传输。
- 同一 Session 串行提交、不同 Session 并行；一次 turn 的 event/state/outbound 原子可见。
- 企业微信 AI Bot WebSocket 长连接，以及飞书加密 HTTP 回调、URL challenge 和 OpenAPI 异步回复。
- 企业微信 fenced connection epoch/租约证据，以及独立签名 IM 探针。
- OIDC/JWKS、RBAC、ETag 乐观并发、Admin 幂等、审计、DLQ 查询和人工 outbound 重放。
- 工具白名单、预算预留、SDK Tool Safety、一次性确认令牌和非幂等工具歧义状态。
- PostgreSQL/RLS、Redis 单调投影、S3/MinIO staged artifact、pgvector 与外部 Memory 扩展口。
- 隐私优先 OpenTelemetry、Prometheus 指标、Docker Compose 和 Kubernetes/Kustomize。
- `prepare → backfill → shadow-read → dual-write → cutover → verify → cleanup/rollback` 迁移状态机。

项目不包含管理 UI、Telegram、微信公众号或微信客服；InMemory 后端仅用于单进程开发。

## 2. 部署模式

| 模式 | 用途 | 配置入口 | 是否为正式部署 |
| --- | --- | --- | --- |
| 本地 Docker Compose | 开发、功能联调、离线故障测试 | `.env` | 否 |
| Kubernetes/ACK production overlay | 完整应用、Ingress、数据依赖、观测、真实 IM、HPA、迁移与发布验收 | Kustomize + Kubernetes Secret | 是 |

正式部署只采用第二行。主机路径、systemd、面板数据库或临时测试域名都不得复制进正式集群配置。

## 3. 正式配置包的组成

本项目使用 Kustomize，不再引入另一套会产生配置漂移的 `values.yaml`。一个环境的正式输入由两份
非敏感文件和六类 Secret（五份 env Secret + 一份 IM 文件 Secret）组成：

```text
deploy/kustomize/overlays/production/
├── kustomization.yaml                 # namespace、资源、镜像仓库和不可变 digest
├── production-config-patch.yaml       # OIDC、S3、模型主机、调度器等非敏感设置
├── infrastructure-config.yaml         # PostgreSQL bootstrap、Prometheus、OTel 配置
├── infrastructure-services.yaml       # 集群内数据与观测 Service
├── infrastructure-statefulsets.yaml   # PostgreSQL/Redis/MinIO/Prometheus + PVC
├── infrastructure-workloads.yaml      # OTel Collector、MinIO bucket bootstrap
├── infrastructure-network-policy.yaml # 数据与观测最小网络边界
├── prometheus-adapter.yaml             # external metrics API、RBAC、Adapter
├── ingress.yaml                        # Gateway HTTPS 入口
├── deployment-order-patch.yaml         # Argo CD 依赖创建顺序
├── migration-order-patch.yaml          # 首次部署迁移等待与顺序
├── managed-services-patch.example.yaml # 可选：删除内置依赖并改接托管服务
├── replicas-patch.yaml                # 固定副本基线
├── wecom-ha-patch.yaml                # 企业微信连接器跨节点约束
├── im-secret-mounts-patch.yaml        # IM Secret 只挂载给需要的角色
└── namespace.yaml

受控 Secret 系统或仓库外目录：
├── service.env                        # runtime、Redis、S3、OIDC、HMAC
├── worker.env                         # 跨租户 worker 数据库身份
├── migration.env                      # schema owner/迁移身份
├── metrics.env                        # 只读 backlog 指标身份
├── infrastructure.env                 # 内置 PG/Redis/MinIO bootstrap 凭据
└── im/
    ├── feishu_app_secret
    ├── feishu_verification_token
    ├── feishu_encrypt_key
    └── wecom_bot_secret
```

`kustomization.yaml` 与 `production-config-patch.yaml` 可以放在受审查的私有部署仓库；六类 Secret 只能
进入 Vault、KMS、ExternalSecret 或权限为 0600 的仓库外文件，不能提交到本项目。

## 4. 通用准备与安全规则

### 4.1 工具与版本

- Python 3.11–3.13；生产镜像使用 Python 3.12 + Alpine 3.24。
- Git、Docker Engine 24+ 与 Docker Compose v2。
- 本地开发推荐 [uv](https://docs.astral.sh/uv/)。
- Kubernetes 1.29+、`kubectl` 和内置 Kustomize；ACK 使用受支持版本。
- 首次使用裸 `kubectl` 部署时建议安装 `yq` v4，用于安全拆分迁移和工作负载。
- 默认 overlay 自带 PostgreSQL/pgvector、Redis、MinIO、Prometheus、OTel Collector 与 External Metrics
  provider；存储类必须能动态供应 `ReadWriteOnce` PVC。
- 集群仍需提供真实 OIDC issuer/audience、Ingress Controller、DNS 与 TLS Secret；应用调用的模型和 IM
  平台是经出口策略批准的外部服务，不属于集群内组件。

初始化开发环境：

```bash
uv sync --extra dev --locked
uv run trpc-service --help
uv run trpc-service doctor --output runs/multitenant/sdk-upgrade.json
```

PowerShell：

```powershell
uv sync --extra dev --locked
uv run trpc-service --help
uv run trpc-service doctor --output runs\multitenant\sdk-upgrade.json
```

### 4.2 密钥规则

1. 不要提交 `.env`、Kubernetes Secret、kubeconfig、机器人 Secret、发布 nonce、签名私钥或测试账号。
2. 不要把 Secret 放在命令行参数、聊天、截图、日志或报告中；使用文件、stdin 或 Secret 管理系统。
3. 租户 binding 只保存 `file://` 或经过白名单批准的 `env://` 引用，不保存明文。
4. Session HMAC 至少 32 个随机字节；Emergency Queue key 解码后必须恰好 32 字节。
5. PostgreSQL 至少拆分 `trpc_migration`、`trpc_runtime`、`trpc_worker`、`trpc_metrics` 四个账号。
6. Gateway/Admin 不得获得 owner、superuser 或 worker 身份；metrics 不得复用 worker DSN。
7. 生产必须禁用 development token、关闭内容采集、关闭飞书陈旧 binding cache，并配置真实 OIDC。
8. 应用镜像必须写成 `repository@sha256:<64-hex>`；tag 不能作为发布身份。

### 4.3 配置命名与 SecretRef

应用设置统一使用 `TRPC_SERVICE_` 前缀。配置由进程环境读取；`*_REF` 再解析 Secret：

- `env://NAME`：读取进程环境变量 `NAME`。
- `file:///absolute/path`：读取绝对路径文件；租户 Secret 必须在
  `TRPC_SERVICE_TENANT_SECRET_ROOT` 下。
- `.env`：只用于本地 Compose。
- Kubernetes：非敏感项放 ConfigMap，敏感项放下述七类 Secret。

生产禁止 `literal://`。环境专用配置必须覆盖所有 `example.*`、`samples.*`、`REPLACE_*` 值。

## 5. 本地 Docker Compose

### 5.1 填写 `.env`

```bash
cp .env.example .env
```

PowerShell：

```powershell
Copy-Item .env.example .env
```

必须替换：

| 变量 | 说明 |
| --- | --- |
| `POSTGRES_PASSWORD` | Compose 初始化管理员，只用于建库/建角色 |
| `MIGRATION_DATABASE_PASSWORD` | `trpc_migration` 密码 |
| `RUNTIME_DATABASE_PASSWORD` | `trpc_runtime` 密码 |
| `WORKER_DATABASE_PASSWORD` | `trpc_worker` 密码 |
| `METRICS_DATABASE_PASSWORD` | `trpc_metrics` 密码 |
| `REDIS_PASSWORD` | Redis ACL 密码 |
| `MINIO_ROOT_PASSWORD` | 本地 MinIO 密码 |
| `SESSION_HMAC_KEY` | 至少 32 个随机字节 |
| `EMERGENCY_QUEUE_KEY` | 恰好 32 个随机字节 |
| `DEVELOPMENT_TOKEN` | 仅本地使用 |
| `MODEL_API_KEY` | 真实模型提供方密钥；只挂载给 Agent Worker |
| `TRPC_SERVICE_MODEL_ENDPOINT_HOSTS` | JSON 数组，只列批准的模型 API 主机 |
| `TRPC_SERVICE_TENANT_SECRET_ENV_NAMES` | 默认 `[]`，只增加精确批准的名称 |

生成随机值：

```bash
openssl rand -base64 36 | tr -d '\r\n'
openssl rand -hex 32
```

PowerShell：

```powershell
$bytes = New-Object byte[] 36
[Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
[Convert]::ToBase64String($bytes)

$key = New-Object byte[] 32
[Security.Cryptography.RandomNumberGenerator]::Fill($key)
[Convert]::ToHexString($key).ToLowerInvariant()
```

不要随意更换 Emergency Queue key。轮换时先设置新
`TRPC_SERVICE_EMERGENCY_QUEUE_KEY_VERSION`，在
`TRPC_SERVICE_EMERGENCY_QUEUE_PREVIOUS_KEY_REFS` 暂时保留旧版本引用，排空后再删除。

### 5.2 启动、检查和停止

```bash
docker compose --env-file .env config --quiet
docker compose --env-file .env up --build -d
docker compose --env-file .env ps
```

`migrate` 必须先成功。失败时查看：

```bash
docker compose --env-file .env ps --all
docker compose --env-file .env logs --tail=200 migrate gateway worker
```

默认地址：

| 服务 | 地址 |
| --- | --- |
| Gateway | `http://127.0.0.1:8080` |
| Admin | `http://127.0.0.1:8081` |
| PostgreSQL / Redis | `127.0.0.1:5432` / `127.0.0.1:6379` |
| MinIO API / Console | `http://127.0.0.1:9000` / `http://127.0.0.1:9001` |
| Prometheus / Jaeger | `http://127.0.0.1:9090` / `http://127.0.0.1:16686` |

```bash
curl -fsS http://127.0.0.1:8080/health/live
curl -fsS http://127.0.0.1:8080/health/ready
curl -fsS http://127.0.0.1:8081/health/ready
docker compose --env-file .env exec worker python -m trpc_service.probe --role worker
```

停止并保留卷：

```bash
docker compose --env-file .env down
```

`docker compose down --volumes` 会永久删除本地 PostgreSQL、Redis、MinIO 和 Prometheus 数据；不要把它
当普通停止命令。

### 5.3 服务角色

| 角色 | 职责 | 凭据 |
| --- | --- | --- |
| `gateway` | 外部 API、飞书加密回调 | runtime + Feishu Secret |
| `admin` | 租户、binding、DLQ、重放与审计 | runtime + OIDC |
| `worker` | Agent turn、IM 媒体下载 | worker + IM Secret |
| `outbox-dispatcher` | PostgreSQL outbox 投递 | worker |
| `channel-dispatcher` | 飞书 OpenAPI 发送 | worker + Feishu Secret |
| `post-turn-projector` | turn 后投影 | worker |
| `wecom-connector` | 企业微信 WSS 收发 | worker + WeCom Secret |
| `session-recovery` | 过期 lease 与 ready mailbox 恢复 | worker |
| `artifact-gc` | staged artifact 回收 | worker + S3 |

## 6. 构建不可变候选镜像

本地镜像：

```bash
docker build -t trpc-agent-service:dev .
```

生产验收需要初始候选和升级候选两个不同 digest。推荐用仓库脚本统一 build/push、release context 和
candidate lock；Docker credential helper 负责登录：

```powershell
$repository = "docker.io/<owner>/trpc-agent-service"

./runs/multitenant/run-current-candidate-session.ps1 -Repository $repository

.venv\Scripts\python.exe scripts\candidate_lock.py verify
```

输出的 `images.initial.reference` 与 `images.upgrade.reference` 必须是完整
`repository@sha256:...`。publish 后源码发生任何变化都必须创建新候选；不能复用旧 lock。详见
[镜像候选发布](docs/registry-release.md)。

## 7. 填写生产 Kubernetes 配置

### 7.1 `kustomization.yaml`：镜像和资源

复制 `deploy/kustomize/overlays/production` 到私有部署仓库的环境目录，或在受审查分支维护环境 patch。
填写：

```yaml
namespace: trpc-service

images:
  - name: trpc-agent-service
    newName: docker.io/<owner>/trpc-agent-service
    digest: sha256:<CURRENT_INITIAL_DIGEST>
```

保留 base、namespace、`im-external-egress`、全部 `infrastructure-*`、`prometheus-adapter.yaml` 与
`ingress.yaml`；保留 replicas、production config、WeCom HA、IM Secret mounts、deployment order 和
migration order patch。不要在这里写 registry token 或机器人 Secret。

默认渲染的正式拓扑是：

| 层 | 默认资源 | 网络暴露 |
| --- | --- | --- |
| 入口 | `trpc-gateway` Ingress/Service | 仅 Gateway 公网 HTTPS；Admin 保持内网 |
| 权威数据 | pgvector/PostgreSQL StatefulSet + PVC | 仅 namespace 内 5432 |
| 通知/投影 | Redis StatefulSet + PVC | 仅 namespace 内 6379；不是权威 Session |
| 对象存储 | MinIO StatefulSet + PVC + bucket bootstrap Job | 仅 namespace 内 9000；Console 不暴露 |
| 遥测 | OTel Collector、Prometheus StatefulSet + PVC | 仅集群内 |
| 弹性 | backlog exporter、Prometheus Adapter、External Metrics APIService、HPA | 仅 Kubernetes API/集群内 |

这些镜像均在清单中固定为 `registry/repository@sha256:<digest>`。默认模板是单副本数据依赖，适合完整功能
验收和单集群部署；生产高可用需要把数据层替换为多可用区托管服务或有明确复制/备份策略的 operator，不能
把单副本 PVC 当成跨区灾备。

### 7.2 `production-config-patch.yaml`：非敏感设置

逐项填写或确认：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: trpc-service-config
data:
  TRPC_SERVICE_ENVIRONMENT: production
  TRPC_SERVICE_ALLOW_DEVELOPMENT_TOKEN: "false"
  TRPC_SERVICE_CAPTURE_CONTENT: "false"
  TRPC_SERVICE_OIDC_ISSUER: https://<OIDC_HOST>/
  TRPC_SERVICE_OIDC_AUDIENCE: <OIDC_AUDIENCE>
  TRPC_SERVICE_S3_ENDPOINT: http://minio:9000
  TRPC_SERVICE_S3_BUCKET: trpc-artifacts
  TRPC_SERVICE_OTLP_ENDPOINT: http://otel-collector:4317
  TRPC_SERVICE_MODEL_ENDPOINT_HOSTS: '["api.openai.com"]'
  TRPC_SERVICE_TENANT_SECRET_ROOT: /run/secrets
  TRPC_SERVICE_TENANT_SECRET_ENV_NAMES: "[]"
  TRPC_SERVICE_FEISHU_ALLOW_STALE_BINDING_CACHE: "false"
  TRPC_SERVICE_FEISHU_SEND_API_ROOT: https://open.feishu.cn
  TRPC_SERVICE_ONLINE_TESTS_ENABLED: "false"
  TRPC_SERVICE_SCHEDULER_VERSION: v2
  TRPC_SERVICE_REDIS_STREAM: trpc:session-ready:v2
  TRPC_SERVICE_REDIS_CONSUMER_GROUP: trpc-session-ready-v2
  TRPC_SERVICE_WORKER_CONCURRENCY: "10"
```

默认内置依赖时保留 `minio` 与 `otel-collector` 的集群内地址；只有使用
`managed-services-patch.example.yaml` 时才替换为托管 endpoint。无论哪种模式，都不要保留
`samples.auth0.com`、`example.internal` 或 `REPLACE_*`。模型主机列表是出口白名单，不是模型 Secret。
飞书正常运行必须保持官方 API root；只有独立在线验收 witness 才可在显式启用 online tests 时临时覆盖。

### 7.3 运行时 `service.env`

在 Secret 系统或仓库外创建：

```dotenv
TRPC_SERVICE_DATABASE_DSN=postgresql://trpc_runtime:<URL_ENCODED_PASSWORD>@<PG_HOST>:5432/trpc_service?sslmode=require
TRPC_SERVICE_REDIS_URL=redis://:<URL_ENCODED_PASSWORD>@<REDIS_HOST>:6379/0
TRPC_SERVICE_SESSION_HMAC_KEY=<AT_LEAST_32_RANDOM_BYTES>
TRPC_SERVICE_EMERGENCY_QUEUE_KEY=<EXACTLY_32_RANDOM_BYTES>
TRPC_SERVICE_S3_ACCESS_KEY=<S3_ACCESS_KEY>
TRPC_SERVICE_S3_SECRET_KEY=<S3_SECRET_KEY>
TRPC_SERVICE_OIDC_ISSUER=https://<OIDC_HOST>/
TRPC_SERVICE_OIDC_AUDIENCE=<OIDC_AUDIENCE>
```

OIDC 可放 ConfigMap，但由 Secret 覆盖也受支持。不要在 DSN 中留下未 URL-encode 的 `@`、`:`、`/`。

### 7.4 后台 `worker.env`

```dotenv
TRPC_SERVICE_WORKER_DATABASE_DSN_REF=env://TRPC_SERVICE_WORKER_DATABASE_DSN
TRPC_SERVICE_WORKER_DATABASE_DSN=postgresql://trpc_worker:<URL_ENCODED_PASSWORD>@<PG_HOST>:5432/trpc_service?sslmode=require
TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF=env://TRPC_SERVICE_WORKER_DATABASE_PASSWORD
TRPC_SERVICE_WORKER_DATABASE_PASSWORD=<RAW_WORKER_PASSWORD>
```

只有 Worker、dispatcher、projector、connector、recovery 与 artifact GC 可以挂载这份 Secret。

### 7.5 模型 Secret 文件

把真实模型 API key 写入仓库外文件 `/secure/trpc-service/model_api_key`，文件权限设为 0600，且不要添加
尾随空行。Kubernetes 将它作为 `trpc-model-secrets/model_api_key` 只挂载给 `trpc-worker`，容器内固定路径为：

```text
file:///run/secrets/model_api_key
```

这个文件只提供凭据；模型提供方、模型 URL、模型 ID、推理强度、采样参数、预算和系统指令属于租户不可变
配置，必须按 11.3 节通过 Admin API 创建并激活。仅设置 `TRPC_SERVICE_MODEL_ENDPOINT_HOSTS` 不会启用
模型调用。Kubernetes `subPath` 不会热更新 Secret；轮换 `model_api_key` 后必须滚动重启
`trpc-worker`，确认新 Pod Ready 后再撤销旧 key。

### 7.6 迁移 `migration.env`

```dotenv
TRPC_SERVICE_DATABASE_DSN=postgresql://trpc_migration:<URL_ENCODED_PASSWORD>@<PG_HOST>:5432/trpc_service?sslmode=require
```

`trpc_migration` 是 schema owner，只进入 migration Job，不进入任何长期 Pod。

### 7.7 指标 `metrics.env`

```dotenv
TRPC_SERVICE_METRICS_DATABASE_DSN=postgresql://trpc_metrics:<URL_ENCODED_PASSWORD>@<PG_HOST>:5432/trpc_service?sslmode=require
```

`trpc_metrics` 必须是 `NOSUPERUSER NOBYPASSRLS`，只允许执行
`public.count_session_ready_backlog()`。

### 7.8 IM Secret 文件

创建四个无尾随空行的文件，权限 0600：

```text
/secure/trpc-service/im/feishu_app_secret
/secure/trpc-service/im/feishu_verification_token
/secure/trpc-service/im/feishu_encrypt_key
/secure/trpc-service/im/wecom_bot_secret
```

飞书 App ID 和企业微信 Bot ID 不是 Secret，放在 PostgreSQL channel binding 的 `account_id`；四个 Secret
文件挂载到 `/run/secrets/im`。正式 binding 使用：

```text
file:///run/secrets/im/feishu_app_secret
file:///run/secrets/im/feishu_verification_token
file:///run/secrets/im/feishu_encrypt_key
file:///run/secrets/im/wecom_bot_secret
```

### 7.9 内置基础设施 `infrastructure.env`

默认 full-stack overlay 还需要一份只给 PostgreSQL、Redis、MinIO 与 bootstrap Job 使用的 Secret：

```dotenv
postgres_superuser_password=<RANDOM_POSTGRES_SUPERUSER_PASSWORD>
runtime_database_password=<SAME_RAW_PASSWORD_AS_service.env_RUNTIME_DSN>
migration_database_password=<SAME_RAW_PASSWORD_AS_migration.env_DSN>
worker_database_password=<SAME_RAW_PASSWORD_AS_worker.env_DSN>
metrics_database_password=<SAME_RAW_PASSWORD_AS_metrics.env_DSN>
redis_password=<SAME_RAW_PASSWORD_AS_service.env_REDIS_URL>
minio_root_user=<RANDOM_MINIO_ROOT_USER>
minio_root_password=<RANDOM_MINIO_ROOT_PASSWORD>
minio_application_user=<RANDOM_NON_ROOT_MINIO_APPLICATION_USER>
minio_application_password=<RANDOM_NON_ROOT_MINIO_APPLICATION_PASSWORD>
```

这里相同角色的值必须逐字一致；DSN 中仍使用 URL-encoded 版本。内置 MinIO bootstrap 会创建
`trpc-artifacts` bucket、受限 application user 和只覆盖该 bucket 的读写 policy。`service.env` 的
`TRPC_SERVICE_S3_ACCESS_KEY`/`TRPC_SERVICE_S3_SECRET_KEY` 必须分别等于
`minio_application_user`/`minio_application_password`；MinIO root 凭据只挂载给 MinIO 与 bootstrap，
应用 Pod 不得使用或挂载 root 身份。

若使用托管 PostgreSQL/Redis/S3/OTel/Prometheus，创建一个环境 overlay 引用 production，并把
`managed-services-patch.example.yaml` 加入 `patches`。该 patch 会删除内置 StatefulSet、PVC 模板对应
工作负载、Adapter/APIService 与 PostgreSQL 等待器；随后必须在 Secret/ConfigMap 中提供托管地址，并在
启用 Worker HPA 前安装能提供 `trpc_session_ready_backlog` 的 External Metrics provider。子 overlay 还
必须按托管 endpoint 的实际 CIDR/FQDN/端口补充 egress 与 metrics scrape/adapter policy；默认规则只允许
私网 CIDR 的 5432/6379/9000/4317/4318/443，不能假定公网托管服务或任意 namespace 自动可达。

## 8. 创建 Kubernetes Secret

先创建 namespace：

```bash
kubectl apply -f deploy/kustomize/overlays/production/namespace.yaml
```

创建五份 env Secret：

```bash
kubectl -n trpc-service create secret generic trpc-service-secrets \
  --from-env-file=/secure/trpc-service/service.env \
  --dry-run=client -o yaml | kubectl apply --server-side -f -

kubectl -n trpc-service create secret generic trpc-worker-secrets \
  --from-env-file=/secure/trpc-service/worker.env \
  --dry-run=client -o yaml | kubectl apply --server-side -f -

kubectl -n trpc-service create secret generic trpc-migration-secrets \
  --from-env-file=/secure/trpc-service/migration.env \
  --dry-run=client -o yaml | kubectl apply --server-side -f -

kubectl -n trpc-service create secret generic trpc-metrics-secrets \
  --from-env-file=/secure/trpc-service/metrics.env \
  --dry-run=client -o yaml | kubectl apply --server-side -f -

kubectl -n trpc-service create secret generic trpc-infrastructure-secrets \
  --from-env-file=/secure/trpc-service/infrastructure.env \
  --dry-run=client -o yaml | kubectl apply --server-side -f -
```

创建只给 Agent Worker 使用的模型文件 Secret：

```bash
kubectl -n trpc-service create secret generic trpc-model-secrets \
  --from-file=model_api_key=/secure/trpc-service/model_api_key \
  --dry-run=client -o yaml | kubectl apply --server-side -f -
```

创建 IM 文件 Secret：

```bash
kubectl -n trpc-service create secret generic trpc-im-secrets \
  --from-file=feishu_app_secret=/secure/trpc-service/im/feishu_app_secret \
  --from-file=feishu_verification_token=/secure/trpc-service/im/feishu_verification_token \
  --from-file=feishu_encrypt_key=/secure/trpc-service/im/feishu_encrypt_key \
  --from-file=wecom_bot_secret=/secure/trpc-service/im/wecom_bot_secret \
  --dry-run=client -o yaml | kubectl apply --server-side -f -
```

生产更推荐 ExternalSecret/Vault/KMS；无论工具为何，Secret 名称和 key 必须完全一致。

## 9. 镜像拉取与 ACK 轩辕加速

ACK 节点使用透明 Docker Hub 镜像加速（例如轩辕）时，Kubernetes 清单和证据仍保留规范引用：

```text
docker.io/<owner>/trpc-agent-service@sha256:<digest>
```

`deploy/runtime-gate.yaml` 保持：

```yaml
kubernetes:
  pull_registry: docker.io
```

镜像加速在 ACK 节点池/containerd 侧配置，不把清单仓库改成镜像站域名。production overlay 的默认
应用与基础设施镜像均使用 `docker.io/...@sha256`；Prometheus Adapter 使用已同步到 Docker Hub 的固定
support digest。这样节点通过轩辕加速拉取，candidate lock、Pod `imageID` 和证据仍绑定 Docker Hub
原始 digest。创建或扩容节点池后，先在新节点用每个不同 digest 做小 Pod 拉取并核对 `imageID`，避免
只在旧节点有缓存。

私有仓库需创建 `kubernetes.io/dockerconfigjson` Secret，并把
`image-pull-secret-patch.example.yaml` 复制为环境 patch 后加入 `kustomization.yaml`。不要把解码后的凭据
写进报告。

## 10. 渲染、迁移与部署

### 10.1 渲染和静态检查

```bash
mkdir -p /tmp/trpc-render
kubectl kustomize deploy/kustomize/overlays/production \
  >/tmp/trpc-render/production.yaml

! grep -E 'REPLACE_|example\.internal|samples\.auth0\.com' \
  /tmp/trpc-render/production.yaml

kubectl apply --dry-run=server -f /tmp/trpc-render/production.yaml
```

确认渲染结果包含：

- 所有应用镜像均为同一个 `repository@sha256`。
- Ingress、PostgreSQL、Redis、MinIO、Prometheus、OTel Collector、Prometheus Adapter 与 APIService。
- `trpc-im-secrets` 只挂载到 Gateway、Worker、Channel Dispatcher、WeCom Connector。
- `trpc-model-secrets` 只以 `/run/secrets/model_api_key` 挂载到 Agent Worker。
- Gateway/Worker HPA、PDB、NetworkPolicy、backlog exporter 与 PVC 均存在。
- production namespace、真实 OIDC/S3/OTLP 值，无 placeholder。

### 10.2 先迁移后启动

production overlay 已把旧 migration `PreSync` 改成可重建的 `Sync` hook：内置
PostgreSQL/Redis/MinIO 及其 Service 使用 Argo CD sync wave `-2`，schema migration 与 MinIO bootstrap
使用带 `BeforeHookCreation,HookSucceeded` 清理策略的 wave `-1` Sync hook，应用使用默认 wave `0`。
migration 另有最多 300 秒的 `pg_isready` init wait；这样首次部署不会在 PostgreSQL 创建前抢跑，后续候选
改变 Job Pod template 时也不会尝试原地修改已完成的不可变 Job。

使用裸 `kubectl` 首次部署时仍应显式分阶段并等待 StatefulSet：

```bash
yq eval 'select(.kind != "Deployment" and .kind != "HorizontalPodAutoscaler" and .kind != "Job" and .kind != "Ingress")' \
  /tmp/trpc-render/production.yaml >/tmp/trpc-render/prerequisites.yaml
yq eval 'select(.kind == "Job")' \
  /tmp/trpc-render/production.yaml >/tmp/trpc-render/bootstrap.yaml
yq eval 'select(.kind == "Deployment" or .kind == "HorizontalPodAutoscaler" or .kind == "Ingress")' \
  /tmp/trpc-render/production.yaml >/tmp/trpc-render/workloads.yaml

kubectl apply --server-side -f /tmp/trpc-render/prerequisites.yaml
kubectl -n trpc-service rollout status statefulset/postgres --timeout=10m
kubectl -n trpc-service rollout status statefulset/redis --timeout=10m
kubectl -n trpc-service rollout status statefulset/minio --timeout=10m
kubectl -n trpc-service delete job trpc-schema-migration minio-bootstrap --ignore-not-found
kubectl apply --server-side -f /tmp/trpc-render/bootstrap.yaml
kubectl -n trpc-service wait --for=condition=complete \
  job/trpc-schema-migration --timeout=1800s
kubectl -n trpc-service wait --for=condition=complete \
  job/minio-bootstrap --timeout=10m
kubectl -n trpc-service logs job/trpc-schema-migration
kubectl -n trpc-service logs job/minio-bootstrap
kubectl apply --server-side -f /tmp/trpc-render/workloads.yaml
kubectl apply --server-side -f /tmp/trpc-render/production.yaml
```

迁移失败时不要启动新工作负载。保存日志，修复数据库权限、扩展或 schema 问题；不要手工修改
`alembic_version`，也不要用 runtime/worker 账号迁移。

### 10.3 启动验收

```bash
kubectl -n trpc-service get pods -o wide
kubectl -n trpc-service get deploy,job,svc,endpointslice
kubectl -n trpc-service rollout status deploy/trpc-gateway --timeout=10m
kubectl -n trpc-service rollout status deploy/trpc-worker --timeout=10m
kubectl -n trpc-service rollout status deploy/trpc-wecom-connector --timeout=10m
kubectl -n trpc-service get hpa
kubectl get --raw \
  '/apis/external.metrics.k8s.io/v1beta1/namespaces/trpc-service/trpc_session_ready_backlog'
kubectl -n trpc-service top pods
```

必须确认：Pod Ready、restart 为 0 或有解释；Pod `imageID` 等于候选 digest；Gateway EndpointSlice 无
unready/terminating endpoint；HPA `ScalingActive=True`；external backlog metric 可读；WeCom 两副本位于
不同节点。

Pod 长时间 `Pending` 时检查 node label、affinity、taint/toleration、配额和可用区。性能验收常用
`trpc-role=workload`、`trpc-role=load-driver` 等标签；它们是验收调度约束，不应无审查写进普通生产池。

## 11. Ingress、TLS 与统一集群 IM 配置

### 11.1 Ingress

Gateway Service 为 `trpc-gateway:8080`，Admin 为 `trpc-admin:8081`。默认清单已包含
`deploy/kustomize/overlays/production/ingress.yaml`；部署前必须把其中的 `trpc.example.com`、
`ingressClassName` 与 `trpc-gateway-tls` 改成真实值。其等价结构如下：

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: trpc-gateway
  namespace: trpc-service
spec:
  ingressClassName: nginx
  tls:
    - hosts: [agent.example.com]
      secretName: trpc-gateway-tls
  rules:
    - host: agent.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: trpc-gateway
                port:
                  number: 8080
```

Admin 建议使用独立内网域名、VPN 或零信任访问。不要暴露 PostgreSQL、Redis、MinIO Console、debug 或
metrics 管理端口。

飞书回调必须公网 HTTPS 可达，证书链完整；企业微信只需要 connector 出站访问
`wss://openws.work.weixin.qq.com:443`。production overlay 有两条独立的角色级 HTTPS fallback：IM policy
只选择 Channel Dispatcher/WeCom Connector；OIDC/model/tool policy 只选择 Gateway/Admin/Worker。标准
NetworkPolicy 不能表达 FQDN，因此默认目标为 `0.0.0.0/0:443`；支持 FQDN policy 的 CNI 必须分别替换为
审核后的飞书/企业微信、OIDC、模型和工具域名，不能扩展到其他 Pod 或其他端口。

### 11.2 创建租户

所有租户和 channel binding 存 PostgreSQL，通过 Admin API 管理；不要再为每个租户复制一份进程 `.env`。
先取得 platform admin OIDC token：

```bash
export ADMIN_URL=https://admin.internal.example.com
export TOKEN='<OIDC_BEARER_TOKEN>'

curl -i -X POST "$ADMIN_URL/v1/tenants" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Idempotency-Key: create-tenant-example-001' \
  -H 'Content-Type: application/json' \
  --data '{"tenant_id":"tenant-example","display_name":"Example Tenant"}'
```

保存响应 `ETag`。后续写操作每次先 GET 当前 tenant，使用最新 `ETag` 作为 `If-Match`，并使用新的
`Idempotency-Key`。

### 11.3 创建并激活真实模型配置

复制并编辑 [租户配置模板](deploy/tenant-config.example.json)：

模型配置字段都属于当前租户 revision，而不是进程级硬编码：

| 字段 | 填写方式 | 运行时行为 |
| --- | --- | --- |
| `provider` | `openai`、`anthropic` 或 `litellm` | 选择对应 tRPC-Agent 模型适配器 |
| `base_url` | 完整 HTTPS API 根地址，例如 `https://api.openai.com/v1` | Worker 将其直接交给适配器；主机还必须列入 `TRPC_SERVICE_MODEL_ENDPOINT_HOSTS` |
| `model` | 账户实际可用的精确模型 ID；LiteLLM 使用 `provider/model` | 每个租户 revision 可独立选择，不需要重建镜像 |
| `api_key_ref.uri` | 默认 `file:///run/secrets/model_api_key` | Worker 在运行时解析 Secret，数据库不保存明文 |
| `timeout_seconds` | `0 < value <= 600` | 转换为 SDK 的单次模型 HTTP 请求超时，不再只是描述字段 |
| `reasoning_effort` | OpenAI 可填 `none`、`minimal`、`low`、`medium`、`high`、`xhigh`、`max` | 自动使用 OpenAI Responses API，并原样传递 `reasoning.effort`；具体取值仍须由目标模型支持 |
| `thinking_budget_tokens` | Anthropic 或支持 SDK thinking 参数的 OpenAI 兼容端填 `-1`（自动）、`0`（关闭）或至少 `1024` | 通过 SDK 内置 planner 设置 token 思考预算；正数必须小于 `budget.max_tokens_per_turn` |
| `temperature` | `null` 或 `0..2` | 非 `null` 时传给模型；推理模型不支持时保持 `null` |
| `top_p` | `null` 或 `0..1` | 非 `null` 时传给模型；通常不要和 `temperature` 同时调节 |
| `stop_sequences` | 最多 8 个、每个 1–256 字符 | 非空时作为停止序列传入模型 |
| `fallback_model` | 可选的同 provider 备用模型 ID | 主模型在输出任何可用内容前失败时才切换；沿用同一 URL、密钥和生成参数 |

`reasoning_effort` 与 `thinking_budget_tokens` 是两套不同供应商协议，配置模型会拒绝同时设置，避免参数被
静默忽略。模板默认展示 OpenAI Responses API 用法：保留 `reasoning_effort`，让
`thinking_budget_tokens` 为 `null`。Anthropic 示例应反过来设置：

```json
{
  "provider": "anthropic",
  "base_url": "https://api.anthropic.com",
  "model": "REPLACE_WITH_CLAUDE_MODEL_ID",
  "api_key_ref": {"uri": "file:///run/secrets/model_api_key"},
  "timeout_seconds": 60,
  "reasoning_effort": null,
  "thinking_budget_tokens": 2048,
  "temperature": null,
  "top_p": null,
  "stop_sequences": []
}
```

若使用自建 OpenAI 兼容网关，先确认它实现 `/responses` 再填写 `reasoning_effort`；只实现
`/chat/completions` 的网关应将该字段设为 `null`。修改 `base_url` 后还必须同步更新 production
ConfigMap 的 `TRPC_SERVICE_MODEL_ENDPOINT_HOSTS`，否则 Worker 会按 SSRF 防护策略拒绝加载 revision。
`storage` 默认值对应仓库内置的 PostgreSQL/S3/pgvector 正式实现。

创建 revision；服务端会注入 `tenant_id`、`app_id` 和递增的 `version`：

```bash
export ETAG='"1"'  # 替换为 GET tenant 返回的当前值

curl -i -X POST \
  "$ADMIN_URL/v1/tenants/tenant-example/config-revisions" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Idempotency-Key: config-tenant-example-001' \
  -H "If-Match: $ETAG" \
  -H 'Content-Type: application/json' \
  --data @deploy/tenant-config.example.json
```

保存响应中的 `version`，再次 GET tenant 取得新的 `ETag`，然后 100% 激活：

```bash
export CONFIG_VERSION='<VERSION_FROM_CREATE_RESPONSE>'
export ETAG='"2"'  # 替换为最新值

curl -i -X POST \
  "$ADMIN_URL/v1/tenants/tenant-example/config-revisions/$CONFIG_VERSION:activate" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Idempotency-Key: activate-tenant-example-001' \
  -H "If-Match: $ETAG" \
  -H 'Content-Type: application/json' \
  --data '{"app_id":"support","percentage":100}'
```

不要把 `scripts/performance_fixture.py`、migration/fault bootstrap 中的 `provider=offline` 当成正式配置；它们
刻意不调用外部模型。真实验收必须从飞书和企业微信分别发送不可预置的问题，确认回复不等于
`offline deterministic response`，并在 Worker telemetry 中看到对应模型请求成功或明确失败。

### 11.4 飞书 binding

在飞书开放平台创建企业自建应用，启用机器人、加密事件回调、`im.message.receive_v1`、消息发送和媒体
权限。App ID 放 `account_id`，Secret 只在 `trpc-im-secrets`：

```bash
export ETAG='"1"'  # 替换为 GET tenant 返回的当前值

curl -i -X PUT \
  "$ADMIN_URL/v1/tenants/tenant-example/channel-bindings/feishu-primary" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Idempotency-Key: bind-feishu-example-001' \
  -H "If-Match: $ETAG" \
  -H 'Content-Type: application/json' \
  --data '{
    "app_id":"support",
    "channel":"feishu",
    "account_id":"cli_REPLACE",
    "secret_refs":{
      "app_secret":{"uri":"file:///run/secrets/im/feishu_app_secret"},
      "verification_token":{"uri":"file:///run/secrets/im/feishu_verification_token"},
      "encrypt_key":{"uri":"file:///run/secrets/im/feishu_encrypt_key"}
    },
    "capabilities":["text","proactive"],
    "enabled":true
  }'
```

飞书后台回调地址：

```text
https://agent.example.com/v1/channels/feishu/feishu-primary/callback
```

完成 URL challenge、应用版本发布和可用范围审批，再用测试成员发送唯一 marker；必须只产生一条 Inbox
和一条最终回复。

### 11.5 企业微信 binding

企业微信使用“智能机器人 → API 模式 → 长连接”，不是群机器人 webhook，也不是普通自建应用
CorpID/AgentID。Bot ID 放 `account_id`：

```bash
export ETAG='"2"'  # 替换为上一步后的当前值

curl -i -X PUT \
  "$ADMIN_URL/v1/tenants/tenant-example/channel-bindings/wecom-primary" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Idempotency-Key: bind-wecom-example-001' \
  -H "If-Match: $ETAG" \
  -H 'Content-Type: application/json' \
  --data '{
    "app_id":"support",
    "channel":"wecom_ai_bot",
    "account_id":"REPLACE_WITH_BOT_ID",
    "secret_refs":{
      "bot_secret":{"uri":"file:///run/secrets/im/wecom_bot_secret"}
    },
    "capabilities":["text","proactive"],
    "enabled":true
  }'
```

企业微信不需要入站公网 callback。确认两个 connector 副本中只有一个 binding owner，另一个可接管；发送
唯一 marker，必须只产生一条回复。企业微信协议没有断线期间的历史补投或 resume cursor：全部 WSS
同时断开期间未送达服务的消息无法由应用恢复，必须记录 provider delivery gap，不能把恢复后的新消息
冒充补投。

### 11.6 多租户统一存储位置

正式配置分层如下：

| 内容 | 存储位置 | 是否含明文 Secret |
| --- | --- | --- |
| 集群级非敏感参数 | ConfigMap/Kustomize patch | 否 |
| 数据库、Redis、S3、OIDC 私密值 | Kubernetes Secret/外部 Secret 管理器 | 是 |
| 模型 API key | `trpc-model-secrets`，只挂载 Agent Worker | 是 |
| 飞书/企业微信私密值 | `trpc-im-secrets` | 是 |
| tenant、app、binding、account ID、能力、SecretRef | PostgreSQL 控制面表 | 否 |
| release ID、source、image digest | candidate lock 与审计报告 | 否；nonce 原值不公开 |

因此新增租户通常不创建新 Pod 或复制 `.env`：创建 tenant、配置 agent revision、创建 binding，并让
binding 引用已批准的 Secret key。需要每租户独立凭据时，使用独立 Secret key/CSI 挂载路径并更新
SecretRef；不要把所有租户合并成一个 JSON 明文表。

### 11.7 多后端与 storage profile 的真实边界

`TenantConfig.storage.profile_id` 是 immutable config revision 的一部分，Worker 每次按
`(tenant_id, profile_id, 完整 StorageSelection)` 路由；不同租户不需要 sticky session。内置正式镜像只
实现下列组合：

| 数据域 | 内置正式后端 | 说明 |
| --- | --- | --- |
| Session / Memory / Summary / Audit | PostgreSQL | 权威状态、事务和 RLS/fencing |
| Artifact metadata / object | PostgreSQL / S3-compatible | 默认对象端为同集群 MinIO |
| Knowledge | PostgreSQL + pgvector | 内置维度固定 1536 |
| Redis | 仅 SessionReady 通知和可重建投影 | 不能声明为权威 Session/Memory |
| InMemory | 仅单进程开发/测试 | 不能进入内置 production fallback |
| external memory/vector | `RegisteredTenantServiceBundle` 扩展口 | 必须由部署代码预构造并显式注册 |

这意味着宽泛 `StorageSelection` 可以描述迁移源/目标和外部适配器，但未注册的组合不会静默退回
PostgreSQL：Worker 会失败关闭。`TRPC_SERVICE_STORAGE_PROFILE_REGISTRY_FILE` 是可选、严格且无 Secret
的 JSON，只能把 tenant/profile 绑定到当前进程已经配置好的内置 bundle；它不能携带 DSN，也不能把同一
物理 pool 冒充成独立后端：

```json
{
  "schema_version": 1,
  "profiles": [
    {
      "tenant_id": "tenant-example",
      "profile_id": "default",
      "bundle": "default_postgresql_s3_pgvector"
    }
  ]
}
```

文件必须是绝对路径、非符号链接、最大 64 KiB。标准内置部署不设置它，因为 default fallback 已覆盖上述
组合。若高监管租户需要独立 PostgreSQL/S3/external-memory 资源，部署扩展必须用各自 SecretRef 构造不同
`TenantDataServices` bundle，再按 exact tenant/profile 注册；仅修改数据库里的 backend 字符串不算完成。

### 11.8 IM 出站能力矩阵

binding 的 `capabilities` 是配置声明/要求，当前不会单独执行发送授权，也不会扩大适配器已经实现的能力。
发送权限应由租户策略与业务入口在创建 Outbox 前校验。当前两通道都能接收入站媒体；出站
则是严格 text-only：

| 通道 | text/proactive | stream | card | outbound media | recall | 自动长度拆分 |
| --- | --- | --- | --- | --- | --- | --- |
| 飞书 | 支持 | 不支持 | 不支持 | 不支持 | 不支持 | 不支持 |
| 企业微信 AI Bot | 支持（markdown body） | 不支持 | 不支持 | 不支持 | 不支持 | 不支持 |

`recall()` 对两通道都返回非重试的 `FAILED/unsupported_capability`，不会请求供应商。当前 delivery attempt
没有持久化分片游标；长文本若在适配器内部分片，部分成功后整条重试会制造重复消息，因此代码不做隐式
拆分，也不声称未知的供应商长度上限。企业微信 markdown 仍是文本，不作为 card。

更完整的通道权限、媒体、群聊、幂等和故障语义见 [IM 通道文档](docs/im-channels.md)。

## 12. 生产运行参数参考

| 变量 | 建议 | 约束/用途 |
| --- | --- | --- |
| `TRPC_SERVICE_ENVIRONMENT` | `production` | 启用生产不变量 |
| `TRPC_SERVICE_ALLOW_DEVELOPMENT_TOKEN` | `false` | 生产必须关闭 |
| `TRPC_SERVICE_CAPTURE_CONTENT` | `false` | 防止正文进入 telemetry |
| `TRPC_SERVICE_OIDC_ISSUER` | 真实 HTTPS issuer | 生产必填 |
| `TRPC_SERVICE_OIDC_AUDIENCE` | 服务 audience | 生产必填 |
| `TRPC_SERVICE_DATABASE_POOL_MIN_SIZE` | 按角色设置 | 不得大于 max |
| `TRPC_SERVICE_DATABASE_POOL_MAX_SIZE` | 按 PG 预算设置 | 所有 Pod 总和不能超过连接预算 |
| `TRPC_SERVICE_WORKER_CONCURRENCY` | 默认 `10` | HPA 20 副本时上限 200 turn |
| `TRPC_SERVICE_SCHEDULER_VERSION` | `v2` | 与 stream/group 成套 |
| `TRPC_SERVICE_REDIS_STREAM` | `trpc:session-ready:v2` | 不可在普通滚动中切版本 |
| `TRPC_SERVICE_REDIS_CONSUMER_GROUP` | `trpc-session-ready-v2` | 不可在普通滚动中切版本 |
| `TRPC_SERVICE_REDIS_RECLAIM_AFTER_MS` | `60000` | 与 lease/恢复策略一致 |
| `TRPC_SERVICE_TENANT_SECRET_ROOT` | `/run/secrets` | file SecretRef 根目录 |
| `TRPC_SERVICE_TENANT_SECRET_ENV_NAMES` | `[]` | 使用文件 Secret 时保持 fail-closed |
| `TRPC_SERVICE_STORAGE_PROFILE_REGISTRY_FILE` | 默认不设置 | 可选的绝对路径、无 Secret、内置 bundle 精确绑定文件 |
| `TRPC_SERVICE_MODEL_ENDPOINT_HOSTS` | 审批后的 JSON 数组 | 阻止租户任意模型出口 |
| `TRPC_SERVICE_FEISHU_ALLOW_STALE_BINDING_CACHE` | `false` | 生产不得放宽 |
| `TRPC_SERVICE_FEISHU_SEND_API_ROOT` | `https://open.feishu.cn` | 正常运行固定官方地址 |
| `TRPC_SERVICE_ONLINE_TESTS_ENABLED` | `false` | 仅在线门禁显式开启 |
| `TRPC_SERVICE_S3_ENDPOINT` | 私有 S3/OSS | 不包含 Secret |
| `TRPC_SERVICE_S3_BUCKET` | 私有 bucket | 禁止匿名 |
| `TRPC_SERVICE_OTLP_ENDPOINT` | 集群内 Collector | 不直发未知公网 |
| `TRPC_SERVICE_MEDIA_DOWNLOAD_MAX_BYTES` | `20971520` | 媒体上限 |
| `TRPC_SERVICE_MEDIA_DOWNLOAD_TIMEOUT_SECONDS` | `30` | 1–300 秒 |
| `TRPC_SERVICE_RECOVERY_BATCH_SIZE` | `25` | recovery 单批数量 |
| `TRPC_SERVICE_ARTIFACT_STAGING_TTL_SECONDS` | `86400` | staged artifact 清理阈值 |

调度器 `v1↔v2` 不是普通滚动升级。必须停入站、排空旧 PostgreSQL outbox 和 Redis group、缩容相关角色
到 0，再切换 stream/group，并按 recovery → dispatcher → worker → ingress 恢复。完整判据见
[调度器切换手册](docs/scheduler-cutover.md)。禁止用 `DEL`、`XTRIM` 或直接改状态伪造排空。

## 13. 运行态、HPA 与性能验收

复制 `deploy/runtime-gate.example.yaml` 为被 Git 忽略的 `deploy/runtime-gate.yaml`，填写：

- 明确 kubeconfig/context 和当前 candidate lock。
- Secret manifest 与可选 image pull Secret。
- `pull_registry: docker.io`，ACK 节点侧走轩辕透明加速。
- workload、load-driver、data/control 节点标签与 taint。
- digest-pinned PostgreSQL、Redis、MinIO、Prometheus、Adapter、load Job 镜像。
- `trpc_session_ready_backlog` 外部指标与 HPA driver 身份。

正式验收入口不包含个人 kubeconfig 或 context。通过参数或环境变量显式传入：

```powershell
$env:TRPC_ACK_KUBECONFIG = "C:\secure\ack-admin.kubeconfig"
$env:TRPC_ACK_CONTEXT = "your-explicit-ack-context"
./runs/multitenant/run-current-final-acceptance.ps1 -StartStage 1 -EndStage 8
```

性能 Job 必须在专用 `trpc-role=load-driver` 节点，不在独立 IM 探针主机、开发机端口转发或业务 Pod 中执行。性能、
Kubernetes runtime、迁移和故障注入必须串行使用同一候选，不能并发改 namespace、HPA、节点 drain 或
固定报告文件。详见[测试与发布门禁](docs/testing.md)。

完整 `online_im` 还要求每个真实通道证明入站、发送 ACK、幂等、媒体、重连/接管、长故障、真实限流和
ambiguous response。独立签名探针的安装与信任配置见
[在线 IM 探针](deploy/im_probe/README.md)。没有真实证据时必须保持 `production_gate=not_run`。

## 14. 升级、备份与回滚

### 14.1 升级前

1. 备份 PostgreSQL并验证可恢复，记录 `alembic_version`。
2. 备份 Redis AOF/RDB 与对象存储 bucket/versioning 状态。
3. 记录当前 `repository@sha256`、ConfigMap hash、Secret 版本、release ID 与 scheduler 版本。
4. 生成新候选并验证 candidate lock，不复用旧 tag。
5. 先跑 expand-compatible 迁移，再滚动应用；观察期内保留旧字段、旧 key 和旧 transport。

### 14.2 应用回滚

同 scheduler/schema 兼容范围内，把环境 overlay digest 恢复为上一个已验证 digest，并对所有角色使用同一
候选。示例只展示 Gateway：

```bash
kubectl -n trpc-service set image deploy/trpc-gateway \
  gateway=docker.io/<owner>/trpc-agent-service@sha256:<previous-digest>
kubectl -n trpc-service rollout status deploy/trpc-gateway --timeout=10m
```

正式操作应在 Kustomize/GitOps 中回滚，不长期保留命令式漂移。数据库迁移不做盲目 downgrade；按
[迁移与回滚](docs/migration.md)的 verify/rollback 判据执行。

### 14.3 Secret 轮换

- 先把新 Secret 版本写入 Secret 管理器，确认文件 key/路径不变或创建新路径。
- binding 使用新 SecretRef 时携带最新 `If-Match` 与唯一 `Idempotency-Key`。
- 先滚动/重连一小部分实例，验证供应商 ACK，再完成其余实例。
- Emergency Queue key 按版本双读窗口轮换；不能直接覆盖后立即删除旧 key。
- Ed25519 IM probe trust 轮换后旧在线报告失效，必须重跑真实门禁。
- 内置 PostgreSQL 的角色脚本只在空 PVC 初始化时执行。已有 PVC 轮换密码时，先由受控管理会话执行
  `ALTER ROLE ... PASSWORD ...`，再原子更新对应 Secret/DSN 并滚动依赖 Pod；只改 Secret 不会修改数据库
  中的角色密码。
- MinIO application password 会在重跑 bootstrap Job 时收敛；bucket policy 内容变化时必须同步升级
  `trpc-artifacts-rw-v1` 的版本化名称并重新附加，不能在原名称下静默修改后期待 Job 覆盖已有 policy。

## 15. 测试与完成定义

本地离线门禁：

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy trpc_service scripts
uv run pytest --cov=trpc_service --cov-branch
uv run python scripts/mock_production_gate.py
kubectl kustomize deploy/kustomize/overlays/production >/dev/null
```

默认测试不调用外部模型或 IM 平台。真实 IM、性能、故障注入和部署门禁需要显式凭据/基础设施。

“功能完成”至少要求：静态/单元/契约通过；Compose 与 Kubernetes 运行态通过；零成本功能灾备通过；
当前候选在真实飞书和企业微信上证明唯一入站/唯一出站/供应商回执；镜像、源码、release context 与报告
一致。

“生产发布完成”还要求完整 `online_im` 8-case、最终 release manifest，以及按政策显式启用的
破坏性生产灾备。唯一允许的灾备豁免是 `--allow-functional-dr`：它只能授权功能灾备替代未运行的
破坏性 DR，并保留
`production_gate=not_run` 与 `authorized_not_run_gates=[disaster_recovery]`；它不豁免 `online_im`、
任何 `fail` 或其他门禁。

## 16. 常见故障

| 现象 | 首查 |
| --- | --- |
| Compose 服务全部等待 | migration 日志、PG 扩展和数据库密码 |
| `another connector owns this binding` | 是否重复 owner、租约/epoch、主备接管 |
| 同一 IM 消息回复两次 | provider message ID、Inbox 唯一键、旧 connector 是否仍在线 |
| 企业微信停机仍立即回复 | 是否只停一个 owner，另一个 connector 是否接管 |
| 企业微信全断线消息无回复 | 协议无历史补投/resume，记录 delivery gap |
| 飞书 challenge 失败 | TLS、Ingress 路径、binding ID、token/encrypt key、body limit |
| Pod `Pending` | node label、affinity、taint/toleration、配额 |
| HPA `ScalingActive=False` | Metrics Server、exporter、Adapter、metrics DSN 权限 |
| `ImagePullBackOff` | digest、pull Secret、ACK 新节点轩辕/containerd 配置 |
| 报告 identity mismatch | publish 后改了源码、复用了旧 lock/report |
| P95 偶发超门槛 | CPU throttle、DB pool wait、active queries、placement、transport errors、p99/max |

## 17. 文档索引

- [架构与完整消息时序](docs/architecture.md)
- [数据模型](docs/data-model.md)
- [一致性与多后端](docs/consistency.md)
- [IM 接入与真实账号配置](docs/im-channels.md)
- [独立在线 IM 探针](deploy/im_probe/README.md)
- [安全、治理与隐私](docs/security.md)
- [运维、容量和故障恢复](docs/operations.md)
- [数据迁移与回滚](docs/migration.md)
- [调度器版本切换](docs/scheduler-cutover.md)
- [镜像候选发布](docs/registry-release.md)
- [测试和发布门禁](docs/testing.md)
- [生产风险清单](docs/risks.md)

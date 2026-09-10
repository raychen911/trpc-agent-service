# Kubernetes 部署

## 生产清单

`platform.yaml` 是面向生产的模板，不是自包含的本地集群演示。文件不包含数据库
密码、Admin 密钥、模型密钥，也不使用 `latest` 镜像标签。不要直接应用未修改的
模板：占位镜像和外部服务必须由部署环境提供后才能使用。

应用前需要完成以下准备：

1. 发布并扫描不可变的应用镜像。仓库中的镜像只是占位符，使用 Kustomize 覆盖层
   替换为 CI/CD 生成的镜像和摘要。
2. 安装 External Secrets Operator，并创建清单引用的
   `ClusterSecretStore/platform-secrets`。
3. 将 Redis、PostgreSQL、向量库、对象存储、模型和 Admin 凭据放入外部密钥系统。
4. 将 `agent.example.com` 等示例主机名、对象存储桶和外部密钥路径替换为当前环境的值。
5. 通过证书流程创建 `trpc-agent-tls` TLS Secret。

生产 ConfigMap 设置 `PUBLIC_SURFACE_AUTH_REQUIRED=1`。Ingress 只路由 `/webhooks`
和已鉴权的 `/admin` API；`/metrics`、`/ui`、`/ui/api/chat` 不作为公网路由。如果
Prometheus 需要抓取 `/metrics`，应使用内部 Service 路径，并从 Secret 注入 Admin API
认证请求头。

使用真实镜像仓库时，编辑 `kustomization.yaml` 或生成环境覆盖层，替换示例主机和
Secret 后执行：

```bash
kubectl apply -k deployment/kubernetes
```

如果没有先替换 `registry.example.com/platform/trpc-agent-service` 和
`replace-with-release` 就执行 `kubectl apply -k deployment/kubernetes`，清单虽然
可以正常渲染，但应用 Pod 会因镜像不存在而无法启动。若集群没有 External Secrets
Operator、清单引用的 SecretStore、TLS Secret 和外部数据服务，直接应用
`platform.yaml` 也会失败或保持 Pending。这些是部署前置条件，不是应用测试失败。

审阅者进行本地验证时，应从仓库根目录使用 Docker Compose，或按下方
`dev-local.yaml` 小节操作。这两条路径使用本地镜像，不需要生产镜像仓库、External
Secrets 或托管 Redis/PostgreSQL。

基础清单包含地址为 `http://otel-collector:4318` 的 OTLP HTTP Collector Service，
其中的 debug exporter 只适合开发 smoke test。生产环境应替换 Collector exporter，
并使用摘要固定应用镜像。应用前，将 `platform.yaml` 中示例外部出口 CIDR 替换为
经过批准的模型、IM、向量库、对象存储和遥测地址段。Redis 和 PostgreSQL 的入站
访问已按标签限制为平台工作负载；应将数据库 Service 保留在该命名空间，或按组织的
Service Mesh 调整选择器。

Artifact 使用兼容 S3 的对象存储，指标由外部 Prometheus 抓取。Trace 进入集群内
Collector，生产前必须导出到组织的 Trace 后端。因此生产清单不挂载本地 `/app/data`
或本地 Prometheus 数据库，避免 Pod 替换时因 `emptyDir` 丢失持久数据。

生产拓扑假设 Redis 和 PostgreSQL 使用托管服务或高可用部署。Redis 保存共享的
Session、队列、幂等和锁状态；PostgreSQL 保存租户配置、Memory、Summary 和 Audit 数据。

生产模板启用 PostgreSQL RLS，并关闭应用侧建表。运行时 Secret 需要配置：

- `POSTGRES_DSN`：非 owner 的 `trpc_agent_app` 登录角色。
- `TENANT_DB_DSN`：供控制面租户仓库使用的独立 `trpc_agent_admin` 登录角色。

仅供迁移使用的 Secret 需要配置：

- `POSTGRES_SCHEMA_DSN`：Schema owner 或受控的 `BYPASSRLS` DSN。
- `POSTGRES_RLS_APP_PASSWORD` 和 `POSTGRES_RLS_ADMIN_PASSWORD`：仅在两个最小权限
  角色不存在时使用的初始化密码。

应用清单，等待迁移 Job 完成，然后观察运行时滚动发布：

```bash
kubectl apply -k deployment/kubernetes
kubectl -n trpc-agent wait --for=condition=complete \
  job/trpc-agent-schema-migration --timeout=10m
kubectl -n trpc-agent rollout status deployment/gateway
kubectl -n trpc-agent rollout status deployment/worker
```

后续版本应使用带发布版本的迁移 Job 名称，或使用部署控制器 Hook，确保新镜像和新
Schema 会重新创建已完成的 Job。

## 本地 k3s 开发

`dev-local.yaml` 引用了 Secret，但不包含凭据。应用清单前先在本地创建 Secret，
两个 DSN 使用同一个随机密码：

```bash
kubectl create namespace trpc-agent
password="$(openssl rand -hex 24)"
kubectl -n trpc-agent create secret generic trpc-agent-secrets \
  --from-literal=POSTGRES_PASSWORD="$password" \
  --from-literal=POSTGRES_DSN="postgresql://trpc_agent:${password}@postgres:5432/trpc_agent" \
  --from-literal=TENANT_DB_DSN="postgresql://trpc_agent:${password}@postgres:5432/trpc_agent" \
  --from-literal=ADMIN_API_KEY="$(openssl rand -hex 32)"
kubectl apply -f deployment/kubernetes/dev-local.yaml
```

构建并导入 `dev-local.yaml` 预期的固定本地镜像标签：

```bash
docker build -t docker.io/library/deployment-gateway:0.1.0 .
```

该清单仅用于本地验证。本地 Artifact 后端和单节点 Redis 不具备生产持久性设计，
并且明确保持 `POSTGRES_RLS_ENABLED=0` 和 `POSTGRES_AUTO_CREATE_SCHEMA=1`。

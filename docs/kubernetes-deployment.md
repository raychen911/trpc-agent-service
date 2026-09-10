# 简化多副本 Kubernetes 部署

这套清单部署 2 个 Gateway、2 个 Redis Stream Worker、1 个 PostgreSQL 和 1 个 Redis。
长连接 IM Deployment 默认是 0 副本，创建 Pull Binding 后再扩为 1，避免空配置时反复重启。

## 前置条件

- `kubectl` 可连接目标集群；
- 镜像 `trpc-agent-service:local` 已加载到本地集群，或已将清单中的镜像名替换为仓库地址；
- 集群有默认 StorageClass；
- 已准备模型、Admin、Session HMAC 和租户 HTTP API Key。

## 部署

不要直接修改或提交 `secret.example.yaml`。复制为被 Git 忽略的本地文件：

```sh
cp deploy/k8s/secret.example.yaml deploy/k8s/secret.local.yaml
# 编辑 secret.local.yaml，把 replace-me 全部替换掉
kubectl apply -f deploy/k8s/base/namespace.yaml
kubectl apply -f deploy/k8s/secret.local.yaml
kubectl apply -k deploy/k8s/base
kubectl -n trpc-agent rollout status statefulset/postgres --timeout=180s
kubectl -n trpc-agent rollout status deployment/redis --timeout=180s
kubectl -n trpc-agent rollout status deployment/agent-gateway --timeout=180s
kubectl -n trpc-agent rollout status deployment/agent-worker --timeout=180s
```

本地 kind 可运行 `sh deploy/k8s/kind-up.sh`。脚本只使用占位 Secret 验证基础设施和健康探针，
不会调用真实模型或 IM。随后运行：

```sh
sh deploy/k8s/smoke.sh
```

如果本机 `18000` 已被占用：

```sh
TRPC_K8S_LOCAL_PORT=18080 sh deploy/k8s/smoke.sh
```

## 创建演示租户

先端口转发：

```sh
kubectl -n trpc-agent port-forward service/agent-gateway 18000:80
```

另一个终端设置本地变量并执行初始化脚本：

```sh
export TRPC_ADMIN_API_KEY='你在 Secret 中配置的 Admin Key'
export TRPC_TENANT_API_KEY='你在 Secret 中配置的租户 Key'
sh deploy/k8s/seed-demo.sh
```

`seed-demo.sh` 最后会向配置的外部模型发送一次计算测试，因此会产生一次少量模型调用费用；
只想验证集群健康时不要运行它。

生产模式调用 `/v1/chat` 时同时发送 `X-Tenant-ID: demo` 和 `X-Tenant-API-Key`。租户记录只保存
`env://TRPC_DEMO_TENANT_API_KEY` 引用。

## 启动 IM 长连接

先通过 Admin API 创建 Telegram/企业微信 Pull Binding，再执行：

```sh
kubectl -n trpc-agent scale deployment/agent-channels --replicas=1
```

长轮询/长连接通道保持 1 副本；Gateway 和 Worker 可以增加副本：

```sh
kubectl -n trpc-agent scale deployment/agent-gateway --replicas=3
kubectl -n trpc-agent scale deployment/agent-worker --replicas=3
```

这是简化版多副本部署：PostgreSQL 和 Redis 各为单实例，不提供跨可用区高可用；正式环境应
替换为托管或高可用服务。Ingress/TLS、NetworkPolicy、PodDisruptionBudget 和 HPA 仍属于生产
扩展项。

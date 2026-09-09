# K8s / kustomize 生产部署

> 对应 PRD §5「生产推荐部署（Kubernetes）」的可交付清单：
> Deployment 多副本 + HPA（CPU 70%）+ Secret 注入 DSN/密钥 + 健康探针
> `/healthz` `/readyz`。

## 目录结构

```
deploy/kustomize/
├── base/                       # 环境无关基线
│   ├── teneuris.yaml           #   应用配置（prod，密钥经 Secret 注入）
│   ├── redis.yaml              #   共享 Redis（Session/Memory/锁/广播）
│   ├── gateway.yaml            #   Gateway 2 副本 + Service
│   ├── admin.yaml              #   Admin API 1 副本 + Service
│   ├── hpa.yaml                #   HPA（CPU 70%，2–10 副本）+ PDB
│   ├── secrets.example.yaml    #   密钥模板
│   └── kustomization.yaml
└── overlays/production/        # 生产覆盖（镜像 tag / 副本数）

Dockerfile                      # 生产运行镜像（仓库根，多阶段构建）
.ide/Dockerfile                 # 开发环境镜像（CNB 云开发专用，与本部署无关）
```

## 前置条件

1. **构建并推送运行镜像**（根 Dockerfile，多阶段：uv.lock 锁定依赖 + 源码；
   与开发环境镜像 `.ide/Dockerfile` 职责分离，互不混用）：
   ```bash
   docker build -t <registry>/trpc-agent-service:v1.0.0 .
   docker push <registry>/trpc-agent-service:v1.0.0
   ```
2. **密钥**（含 Admin 密钥 / 托管 SQL DSN / 模型 API key）：
   ```bash
   cp deploy/kustomize/base/secrets.example.yaml secrets.yaml   # 填写真实值
   kubectl apply -f secrets.yaml
   ```
3. **托管 MySQL/PG**：生产禁止 sqlite（启动时 fail-closed 校验强制）；
   DSN 经 Secret 的 `TENEURIS_STORAGE_SQL_DSN` 注入。
   示例中的集群内 Redis 可直接使用；生产建议托管 Redis，届时在 Secret
   覆盖 `TENEURIS_STORAGE_REDIS_DSN` 并删除 base/redis.yaml。

> 镜像仓库地址与 tag 在 `overlays/production/kustomization.yaml` 的
> `images` 段调整（`newName` / `newTag`）。

## 部署与验证

```bash
# 预览渲染结果（不落盘、不应用）
kubectl kustomize deploy/kustomize/overlays/production

# 应用
kubectl apply -k deploy/kustomize/overlays/production

# 验证：Gateway 副本就绪（/readyz 真实探测 Redis/SQL 连通）
kubectl get pods -l app=teneuris-gateway
kubectl get hpa teneuris-gateway

# 冒烟：经 Service 访问
kubectl port-forward svc/gateway 8000:8000
curl -s localhost:8000/healthz && curl -s localhost:8000/readyz
```

## 设计要点

- **无状态扩缩**：会话/记忆/幂等全在共享 Redis/SQL（PRD 1.3），Gateway
  副本可任意伸缩，无需 sticky session。
- **探针语义**：liveness=`/healthz`（进程存活）；readiness=`/readyz`
  （依赖连通），Redis 短暂不可用时 Pod 自动摘除流量，恢复后自动回归。
- **配置与密钥分离**：ConfigMap 挂应用配置；DSN/密钥走 Secret 环境变量
  （`TENEURIS_*` 覆盖机制），配置文件中只有引用（PRD 4.5）。
- **fail-closed**：`env=prod` 触发生产安全校验，危险配置拒绝启动。
- **不在最小清单内**（生产演进项，见 PRD §5）：
  自定义指标 HPA（Prometheus Adapter）、NetworkPolicy、Redis 持久化
  PVC / Cluster、外部指标驱动的容量弹性。

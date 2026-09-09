# Kubernetes 部署骨架

`base/` 提供 Gateway、Worker、Dispatcher、Projector、migration Job、Service、PDB、HPA 和入站 NetworkPolicy。它是经过边界设计的生产起点，不包含真实 Secret，也不会替用户猜测托管 PostgreSQL、OTel、Ingress 或 CNI 的地址。

部署前必须完成：

1. 将镜像替换为本次构建且不可变的 digest；
2. 修改 `TRPC_SERVICE_PUBLIC_BASE_URL`、OTLP 地址和模型路由；
3. 在 `trpc-agent` namespace 预先创建 `trpc-agent-database-owner`、`trpc-agent-database-runtime`、`trpc-agent-platform-secrets`、`trpc-agent-channel-secrets`、`trpc-agent-model-secrets`；
4. 根据目标 CNI 添加 PostgreSQL、OTel、模型和 IM 官方域名的 egress allowlist；
5. 先运行 migration Job，确认成功后再滚动业务 Deployment；
6. 以队列 age 和模型并发配额配置 Worker/Projector 的自定义指标 HPA，不用 CPU 冒充业务容量信号。

渲染检查：

```bash
kubectl kustomize deploy/k8s/base >/tmp/trpc-agent-rendered.yaml
kubectl apply --server-side --dry-run=server -f /tmp/trpc-agent-rendered.yaml
```

示例 Secret 只展示键名，值应来自 External Secrets、Vault 或云 KMS，而不是提交到 Git：

| Secret | 必需键 |
|---|---|
| `trpc-agent-database-owner` | `database_url` |
| `trpc-agent-database-runtime` | `database_url` |
| `trpc-agent-platform-secrets` | `secret_key`, `admin_api_key` |
| `trpc-agent-channel-secrets` | `wecom_token`, `wecom_aes_key`, `telegram_bot_token`, `telegram_webhook_secret` |
| `trpc-agent-model-secrets` | `api_key` |

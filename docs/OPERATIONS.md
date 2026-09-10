# 生产运营与可观测性

第五阶段将平台的治理能力收敛为可执行的运营契约。应用指标不记录完整
Prompt、消息正文、Token、Secret、邮箱或手机号；Trace 中的用户和 Session
标识使用稳定哈希，便于关联同一对象但不可逆。审计记录在构造时递归脱敏，
因此 InMemory、Redis、SQLite 和 PostgreSQL 后端遵守同一条边界。

## 探针

- `/livez` 只表示进程仍能接收请求，不访问 Redis、PostgreSQL 或租户存储，
  适合 Kubernetes livenessProbe。
- `/readyz` 复用 `/health` 的依赖检查，检查租户配置仓库、远程队列和每个活跃
  租户的 Session 后端；依赖失败返回 HTTP 503，适合 readinessProbe。
- `/health` 保留详细依赖状态，便于人工诊断。生产监控应同时采集响应码和
  `dependencies` 中的错误类型，不把异常正文返回给调用方。

## 指标与告警

`/metrics` 暴露请求、模型延迟、工具调用、IM 投递、Token、成本、后端延迟和
错误指标。租户、通道和工具是受控维度；不得新增 Prompt、消息、用户输入或
任意 URL 作为 Label。多进程部署设置 `PROMETHEUS_MULTIPROC_DIR`，并将该目录
配置为进程组独占的临时目录，避免不同应用共享指标文件。

Prometheus 规则模板位于 `deployment/prometheus-alerts.yml`，覆盖网关不可用、
请求错误率、IM 投递失败率、Session 后端延迟和工具/配额拒绝。规则中的阈值
只是初始值，应按租户 SLA、历史基线和告警值班策略调整。

## 审计查询

已授权的 `viewer` 和 `operator` 可以查询：

```text
GET /admin/v1/tenants/{tenant_id}/audit?limit=100&decision=allow
```

`limit` 限制在 1 到 500，结果始终由 `tenant_id` 作用域约束，返回字段不包含
凭据，元数据再次经过脱敏。跨租户平台管理员可以按租户分别查询；普通租户
管理员不能读取其他租户记录。

## 发布与故障处理

1. 发布前执行 `uv run python scripts/observability_gate.py`、质量门禁和 Kubernetes
   静态门禁。
2. 先确认 `/livez` 正常，再确认 `/readyz` 为 200；就绪失败时保留 Pod 但从
   Service 流量中摘除，不重启仍存活的进程。
3. 通过 `trace_id` 在审计、指标和 Trace 后端关联单次请求。外部队列消费、
   补偿、Outbox 和 IM 回复必须保留原始 Trace 上下文。
4. 发现错误率或投递失败率上升时，先查看租户审计和死信，再按
   `compensate`、`durable-outbox`、`mailbox-maintenance` 的顺序处理积压。
5. 数据库备份、Redis 高可用、对象存储版本控制和 OTel/Prometheus 持久化由
   部署环境负责；应用不会把这些外部系统的成功状态伪装成本地已验证。

## 安全责任边界

Admin API 继续使用 API Key 或 RS256 OIDC，并按角色与租户授权。生产环境设置
`PUBLIC_SURFACE_AUTH_REQUIRED=1`，使 `/metrics`、`/ui` 和 `/ui/api/chat` 也要求
同一套管理凭据；Prometheus 应通过受限的 Secret 注入认证头。`/health`、`/livez`
和 `/readyz` 应通过集群网络策略限制访问；公网 Ingress 只暴露 `/webhooks` 和
受保护的 `/admin`，不要使用 `/` 前缀把内部或本地调试路径全部暴露。生产必须将
示例 NetworkPolicy 中的宽泛外部 CIDR 替换为批准的模型、IM、向量库、对象存储和
Telemetry 地址段。

## 第六阶段安全与灾备验收

安全与供应链门禁:

```bash
uv run python scripts/security_gate.py
```

该门禁检查运行源码和部署配置中的硬编码凭据、锁定依赖、镜像非 root 用户以及
Kubernetes 安全上下文。灾备演练使用 `scripts/disaster_recovery_gate.py`，需要显式设置
`DISASTER_RECOVERY_SOURCE_DSN`、`DISASTER_RECOVERY_TARGET_DSN` 和
`DISASTER_RECOVERY_ALLOW_DESTRUCTIVE=1`。未配置一次性数据库或备份工具时返回 `not_run`。

统一报告 `uv run python scripts/production_acceptance.py` 会分别记录安全门禁、灾备演练和
其他真实外部依赖的 `pass`、`fail`、`not_run` 状态。

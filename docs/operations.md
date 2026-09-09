# 部署、容量与运维手册

## 1. 运行形态

| 形态 | 用途 | 包含内容 | 不可用于 |
|---|---|---|---|
| SQLite 本地模式 | 开发、API 和合同测试 | Gateway、Admin API、迁移和单进程数据库 | 多节点、RLS 验证、生产持久化 |
| Docker Compose | 完整本地链路联调 | API、Worker、Dispatcher、Projector、migration、PostgreSQL、Redis、OTel Collector | 不能替代真实 IM/模型账号和生产故障演练 |
| Kubernetes 起点 | 水平扩缩和故障域隔离 | 四类独立 Deployment、migration Job、ServiceAccount、PDB、Gateway HPA、入站 NetworkPolicy | 需按目标集群补 Secret、Ingress、托管后端、egress 与业务指标 HPA |

## 2. 本地运行

前置条件：Python 3.12、[uv](https://docs.astral.sh/uv/) 和 Git。项目的 SDK 兼容面锁定在 `trpc-agent-py==1.1.19`，不要在未更新兼容测试时手工升级。

```bash
cp .env.example .env
uv sync --frozen --all-extras
uv run trpc-agent-service migrate
uv run trpc-agent-service doctor
uv run trpc-agent-service demo-seed
uv run trpc-agent-service serve --host 127.0.0.1 --port 8000 --reload
```

Windows PowerShell 中第一条改为：

```powershell
Copy-Item .env.example .env
```

默认 `.env.example` 是开发配置：SQLite、mock 模型标识、local 加密 event store、禁用 OTLP。它可以启动 Gateway 和 `/console` 控制台，但不会自动运行一个可用的 LLM Agent。`demo-seed` 创建的通道全部禁用且默认拒绝身份，只用于展示控制面。`doctor` 只检查数据库连接和 SDK 精确版本，不检查 IM、模型或投影后端。

启动后验证：

```bash
curl --fail http://127.0.0.1:8000/health/live
curl --fail http://127.0.0.1:8000/health/ready
curl --fail http://127.0.0.1:8000/metrics
```

`start.sh` 会先迁移再启动，通过 `ROLES=api,worker,dispatcher,projector` 选择角色，默认写 PID 和日志到 `data/`；它需要 Bash 和 curl。Windows 上建议使用 Git Bash/WSL，或直接执行上面的 `uv run` 命令。

## 3. Compose 联调

```bash
docker compose config --quiet
docker compose up --build
```

Compose 中 migration 容器使用 `agent_owner`，API 使用最小权限 `agent_runtime`。本地密码是公开示例值，不得复用到共享环境。

Collector 服务已在 Compose 中，但 API 只有在 `TRPC_SERVICE_OTEL_ENABLED=true` 时才导出 trace。示例 Collector 使用 debug exporter，它用于本地观测，不是生产 trace 存储。

Compose 文件包含完整四角色，但当前开发机没有真实容器、PostgreSQL 或 Redis 通过记录；部署结论必须以目标环境或 GitHub Actions 证据为准。

## 4. 生产前置条件

### 应用配置

`TRPC_SERVICE_ENV=production` 时进程会强制：

- 唯一且至少 32 字符的 root secret；
- 唯一且至少 32 字符的 Admin API key；
- 非 SQLite 共享 SQL 后端；
- HTTPS public base URL。

这些是启动下限，不是全部上线条件。还需：

- 用 KMS/Vault/Secret Manager 替换环境密钥适配器，并演练轮换；
- 将 Admin 静态 key 升级为 OIDC 或 mTLS 和 RBAC；
- 验证四角色优雅停机期间的 lease 释放、接管和零半提交；
- 在目标 PostgreSQL 上运行 RLS、并发领取、备份恢复和连接池压测；
- 使用真实企微测试机器人和 Telegram 测试 bot 执行验签、重投、限流、超时和文本分段联调；
- 配置反向代理 TLS、WAF、egress allowlist、备份、告警和数据保留任务。

### Kubernetes 拓扑

- Gateway Deployment：只执行验证、解密和 T0；HPA 主要参考 HTTP 并发、CPU 和入站延迟。
- Worker Deployment：按模型并发配额和活动 session 租约扩缩，设置严格优雅停机期。
- Dispatcher Deployment：独立 egress NetworkPolicy，仅允许企微和 Telegram 官方端点。
- Projector Deployment：处理 Summary、Memory、Knowledge 索引和后端迁移，其积压不得阻塞 T2。
- migration Job：使用单独 ServiceAccount 和数据库 owner secret，不与运行 Pod 共享。
- 每类 workload 设 PodDisruptionBudget、topology spread、resource request/limit 和 NetworkPolicy。

`deploy/k8s/base` 已提供这些角色的可审阅起点和 Gateway HPA。它不提交真实 Secret，也没有假设目标集群的数据库/模型/IM 出口地址；生产应先用 `kubectl kustomize` 与 server-side dry-run 验证，再补 topology spread、FQDN egress 和队列自定义指标。

## 5. 监控和告警

### 已定义指标

| 指标 | 主要维度 | 作用 | 当前接线状态 |
|---|---|---|---|
| `agent_platform_inbound_total` | tenant channel outcome | 入站接收与拒绝 | 已在 IM ingress 增加 |
| `agent_platform_agent_duration_seconds` | tenant app outcome | Agent turn 耗时 | 已在 Runner 外围按成功/错误观测 |
| `agent_platform_model_duration_seconds` | tenant provider model outcome | 模型耗时 | 已定义，尚未接线 |
| `agent_platform_tool_duration_seconds` | tenant tool outcome | Tool 耗时 | 已定义，尚未接线 |
| `agent_platform_storage_duration_seconds` | backend operation outcome | 存储延迟 | SQL Memory 查询已接线，其他操作待补 |
| `agent_platform_delivery_total` | tenant channel outcome | IM 投递结果 | 已接到 Dispatcher 的持久化结果分类 |
| `agent_platform_model_tokens_total` | tenant app direction | token 用量 | 已从 SDK usage metadata 记录输入/输出 |
| `agent_platform_cost_micros_total` | tenant app category | 成本微单位 | 已定义，尚未接线 |
| `agent_platform_session_leases` | worker | 活动租约 | 已随 Worker 领取和释放增减 |

不应给指标加 user ID、session ID、request ID 或 trace ID label。这些值应留在日志/追踪/审计中，否则会形成 Prometheus 高基数故障。

### 初始告警集

- readiness 连续 2 分钟失败；
- callback 5xx 比例或签名失败比例异常；
- oldest Inbox age、retry_wait 数量或 dead letter 增长；
- session lease takeover 率和 stale fence 率超基线；
- Outbox `unknown`、dead letter 和 oldest pending age；
- PostgreSQL 连接池、锁等待、事务失败、备制延迟；
- Redis 命中率、eviction、内存和 Lua CAS 冲突；
- 每租户 token/成本速率突增；
- OTel exporter drop 和 Collector 队列积压。

上述模型/Tool 耗时、成本、队列 age、Projector 和数据库指标尚未完整暴露，是生产观测补齐清单。

## 6. 容量评估

不用未压测的“每节点并发数”作为承诺。先在目标模型和数据库上测量以下参数：

| 参数 | 含义 |
|---|---|
| `lambda_in` | 峰值入站消息数每秒 |
| `t_turn_p95` | Agent turn P95 秒数 |
| `e_turn` | 每 turn 持久的完整 event 平均数 |
| `h` | 租约心跳周期 |
| `tok_in` `tok_out` | 平均输入/输出 token |
| `q_t0` `q_claim` `q_event` `q_t2` `q_out` | 各阶段 SQL 语句或事务量 |
| `s_event` | 每个已加密 event 平均字节数 |

初始估算：

```text
稳态并发 turn 数  = lambda_in * t_turn_p95
所需 Worker 数       = ceil(稳态并发 turn 数 / 每 Worker 已压测并发槽) * 安全系数
SQL QPS             = lambda_in * (q_t0 + q_claim + e_turn*q_event + ceil(t_turn_p95/h) + q_t2 + q_out)
日模型 token         = 86400 * 平均 lambda_in * (tok_in + tok_out)
日事件对象存储     = 86400 * 平均 lambda_in * e_turn * s_event
```

安全系数建议从 1.5 开始，再根据峰值形状、模型限额、租户公平性和单节点故障容量调整。最终参数必须由压测和故障注入得出。

### 建议的首轮 SLO 验证项

下列是压测验收目标，不是当前实测成果：

- T0 持久化 ACK P99 在目标 IM 超时预算内，数据库故障时宁可非 2xx 也不假 ACK；
- 单 session 严格有序，不同 session 在目标峰值下可并行；
- 杀死 30% Worker 后无 committed event 重复、无 stale writer 覆盖；
- 所有模糊副作用进入 `unknown` 且可对账；
- 租户成本、队列延迟和错误率可分租户观测，无 user/session 高基数 label。

## 7. 灰度与回滚

### 租户配置

Admin API 支持发布不可变 revision、读取 active spec 和将旧 revision 重新物化为 active。Inbox 会保留接收时 revision，所以切换前已接收的消息不会突然使用新权限。

当前还没有按租户百分比、绑定或用户 cohort 分流的灰度路由。建议先以内部测试 tenant 发布，观测固定窗口，再扩大 tenant allowlist；异常时调用 rollback 而不删除历史版本。Agent revision 进入 session HMAC 命名空间，新版不污染旧会话，回滚后恢复旧命名空间。

### 代码发布

1. 在新镜像上执行全部静态、单测、迁移和 PostgreSQL 合同门禁。
2. migration Job 先执行只向前且兼容旧代码的 schema 变更。
3. 先灰度 Gateway，再 Worker，最后 Dispatcher/Projector；避免同时更换全链路。
4. 监视 T0 5xx、stale fence、retry、unknown 和每租户成本。
5. 回滚代码时不自动 downgrade 数据库；使用 expand-contract 迁移流程。

## 8. 备份、恢复与对账

- PostgreSQL：开启 PITR，分别演练全库恢复、单 tenant 逻辑导出和审计保留。RPO/RTO 需由业务定义。
- Redis：视为可重建投影，可使用 AOF/备份缩短恢复，但恢复正确性以 SQL committed watermark 为准。
- 向量库：保留 document version 和 index watermark，支持从对象原文全量重建。
- 对象存储：开启版本化、加密、防误删和生命周期策略，用 content hash 抽样验证。
- Outbox/Tool Effect `unknown`：建独立对账队列和审批操作，所有人工决策写新审计记录，不直接修改历史行。

## 9. 发布门禁

```bash
uv lock --check
uv sync --frozen --all-extras
uv run ruff format --check trpc_service tests migrations
uv run ruff check trpc_service tests migrations
uv run mypy trpc_service
uv run pytest -m "not postgres and not release_gate" --cov=trpc_service --cov-report=term-missing
uv pip check
docker compose config --quiet
```

PostgreSQL 门禁必须用 migration owner 建表、runtime 角色执行 `tests/postgres`，不能用 superuser 跑测试来证明 RLS。最终通过数和未通过项以 CI 最新运行为准，见 [acceptance.md](acceptance.md)。

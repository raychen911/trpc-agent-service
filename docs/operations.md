# 部署、探针和故障手册

## 1. 启动方式

开发模式合并角色并使用内存状态，便于单进程学习。生产 CLI 使用 PostgreSQL 控制面与 Redis 平台依赖，Session/Memory 选择共享后端，post-turn 同步完成后提交请求结果。

```bat
docker compose --profile all up --build
```

启动前，在 `.env` 中填写模型配置、`TRPC_SERVICE_ADMIN_TOKEN` 和 `TRPC_TENANT_DEMO_TOKEN`。Compose 的默认 PostgreSQL 密码用于隔离的本地环境，生产部署通过 Secret 注入独立密码。

`all` Profile 会启动 Gateway、Worker 和 Delivery。企业微信长连接通过 `wecom` Profile 启动，并读取真实 Binding 和凭据。普通停止使用 `docker compose down`；`down -v` 用于明确的数据重置操作。

迁移服务按编号执行 `migrations/*.sql`。生产升级先备份已有数据，在预发布环境验证增量 SQL，再滚动更新各角色。

## 2. Kubernetes

`deploy/kubernetes` 包含主角色、Admin/WeCom 以及 ConfigMap/Secret 模板。PostgreSQL、Redis、入口 TLS、NetworkPolicy 和 OTel Collector 作为集群基础设施单独部署并通过配置接入。

部署前应准备这些服务并执行数据库迁移，再根据 StorageClass 配置 Artifact 的 RWX 共享卷。只共享 PostgreSQL 元数据、却把文件留在 Pod 本地，会导致其他节点无法读取附件。

Gateway 与 Worker 支持多副本，Worker HPA 示例配置了 CPU request。容量规划同时观察模型等待、队列积压与热点 Session，并据此调整 HPA 指标。部署时用 Secret 管理系统替换模板中的 `REPLACE_ME`。

Docker Compose 可以启动 Gateway、双 Worker、Delivery、Redis 和 PostgreSQL，并已验证角色就绪、共享会话、Worker 接管和优雅停机。Kubernetes 交付物包含 3 个 YAML 文件和 12 个资源，已通过解析检查。部署到具体集群时，还应结合该集群验证资源 Schema、镜像拉取、网络、存储和扩缩容策略。

## 3. 探针、角色、停止

`healthz` 表示事件循环能够响应。`readyz` 检查活动租户和后台任务，并按角色验证 PostgreSQL `SELECT 1`、Redis Queue/Idempotency/Guard 或 Outbox。依赖异常时返回 503，响应中保留归一化错误类型。

普通 API 只由 Gateway 提供，Admin API 只由 Admin 提供。WeCom 连接持有者同时领取自己绑定的 Outbox；备用实例循环争取 lease，失锁关闭连接。

停机时先停止领取新任务，最多等待 15 秒处理在途任务；超时后取消，再依次关闭 SDK Runner、Channel、Redis、PostgreSQL 和 OTel。待 ACK 的任务保留在 PEL，由其他 Worker 接管。Worker 默认在任务空闲 300 秒后执行 reclaim，该值与 `max_run_seconds` 配套设置，使正常长任务拥有完整执行时间。

## 4. 故障排查

| 现象 | 处理方式 | 优先检查 |
|---|---|---|
| Redis 连接异常 | 服务保持共享后端语义并循环退避；租约失败时取消当前执行 | 连接、延迟、token、PEL |
| Worker 崩溃 | reclaim/RequestRepair 重新处理 | request state、完成标记、attempt |
| 仅 Session 中间事件 | fail/dead，人工核查 | ToolExecution 是否 running/unknown |
| Outbox 插入失败 | 请求进入可恢复状态，复用 Session 阶段记录补提交 | PostgreSQL、`Session.state._platform_turns`、Request 状态 |
| Delivery 崩溃 | lease 到期重试 | locked_at、locked_by、下一次时间 |
| Telegram 429 | Retry-After 延后 | Bot 限额、分片记录 |
| 配置回滚 | 后续请求使用指定旧版本 | active pointer、入队 config_version |
| Bot 配置更改 | 滚动重启相关角色 | Adapter 缓存、在途回复对应账号 |
| 微信客服回调 503 | 平台等待通知可靠保存后再确认接收 | PostgreSQL、`customer_service_state`、回调错误码 |
| 微信客服人工接管 | 准入/发送实时查状态，暂停自动回复 | service_state、Inbox状态、Outbox错误 |
| 微信客服 UNKNOWN | 外部发送结果待确认 | 核对微信记录后决定是否重新投递 |

外部 IM 采用“至少一次尝试 + 幂等记录 + 未知状态核查”的投递方式。PostgreSQL 与 Redis 之间通过阶段记录、修复任务和数据对账恢复一致性。

## 5. 容量与验证

容量测试先使用 Fake Model，记录请求数、并发 Session、p50/p95 时延、模型调用次数和失败数。同一 Session 按顺序执行，吞吐能力主要来自不同 Session 之间的并行处理。20 轮本地并发测试验证会话一致性，生产容量再结合真实模型时延测算。

真实集成环境和 Anaconda 命令见 [testing](testing.md)。真实模型通过显式命令验证；真实 Bot 使用测试账号和 `im-live` 单独执行，默认测试使用模拟客户端。

## 6. 升级与数据保留

升级前先备份控制面并依次应用数据库迁移，再滚动更新 Gateway、Worker 和 Delivery。微信客服使用这三个角色，企业微信智能机器人由 `wecom` 角色维护长连接。已有 PostgreSQL 数据卷通过增量迁移更新，升级命令见[测试说明](testing.md)。数据库迁移会创建执行租约与客服状态表，并补齐 Outbox 和幂等记录所需字段。

IM 身份规则变化后，旧会话保留在原命名空间，新消息按新规则建立会话。若 Redis 中只有旧幂等占位、PostgreSQL 中缺少对应 Request，任务会进入待核查状态，由运维确认原执行结果。SDK 数据后端需要创建原生 Lease 表；生产 SQL 后端使用 PostgreSQL，Redis 的原子 Lua 操作将相关键放在同一 slot。

停止或失锁后，取消 Runner 是第一道保护，真正的拒写由数据后端完成。SDK SQL 后台清理被停用，避免无租约写入；SQL 保留清理、Session 阶段记录和客服 JSONB 归档由独立管理作业按租户保留策略执行。

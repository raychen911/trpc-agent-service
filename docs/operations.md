# 部署、探针和故障手册

## 1. 启动方式

开发模式合并角色、内存状态，只用于单进程学习。生产 CLI 使用 PostgreSQL 控制面与 Redis 平台依赖，配置的 Session/Memory 禁止 memory，并要求同步 post-turn。

```bat
docker compose --profile all up --build
```

启动前，在 `.env` 中填写模型配置、`TRPC_SERVICE_ADMIN_TOKEN` 和 `TRPC_TENANT_DEMO_TOKEN`。Compose 的默认 PostgreSQL 密码只适用于隔离的本地环境，不能用于公网部署。

`all` Profile 会启动 Gateway、Worker 和 Delivery。企业微信还需要启用 `wecom` Profile，并提供真实 Binding 和凭据。除非明确要清空数据，不要执行 `down -v`。

迁移服务按顺序执行 migrations/*.sql；生产已存在数据时先备份并验证迁移，不应把示例自动 DDL 当完整迁移版本管理系统。

## 2. Kubernetes

`deploy/kubernetes` 包含主角色、Admin/WeCom 以及 ConfigMap/Secret 模板。模板不会创建 PostgreSQL、Redis、入口 TLS、NetworkPolicy 或 OTel Collector。

部署前应准备这些服务并执行数据库迁移，再根据 StorageClass 配置 Artifact 的 RWX 共享卷。只共享 PostgreSQL 元数据、却把文件留在 Pod 本地，会导致其他节点无法读取附件。

Gateway 与 Worker 有副本，Worker HPA 示例带 CPU request。真实容量主要受模型等待、队列积压与热点 Session 限制，CPU HPA 不代表最佳扩缩容策略。Secret 模板的 REPLACE_ME 必须替换，不提交真实值。

Docker Compose 可以启动 Gateway、双 Worker、Delivery、Redis 和 PostgreSQL，并已验证角色就绪、共享会话、Worker 接管和优雅停机。Kubernetes 交付物包含 3 个 YAML 文件和 12 个资源，已通过解析检查。部署到具体集群时，还应结合该集群验证资源 Schema、镜像拉取、网络、存储和扩缩容策略。

## 3. 探针、角色、停止

`healthz` 表示事件循环能够响应。`readyz` 检查活动租户和后台任务，并按角色验证 PostgreSQL `SELECT 1`、Redis Queue/Idempotency/Guard 或 Outbox。依赖异常时返回 503，但不会暴露原始连接信息。

普通 API 只由 Gateway 提供，Admin API 只由 Admin 提供。WeCom 连接持有者同时领取自己绑定的 Outbox；备用实例循环争取 lease，失锁关闭连接。

停机时先停止领取新任务，最多等待 15 秒处理在途任务；超时后取消，再依次关闭 SDK Runner、Channel、Redis、PostgreSQL 和 OTel。未 ACK 的任务保留在 PEL，由其他 Worker 接管。Worker 默认在任务空闲 300 秒后执行 reclaim，该值应与 `max_run_seconds` 配套设置，避免正常长任务被提前接管。

## 4. 故障排查

| 现象 | 处理方式 | 优先检查 |
|---|---|---|
| Redis 不通 | 无内存降级，循环退避；租约失败取消 | 连接、延迟、token、PEL |
| Worker 崩溃 | reclaim/RequestRepair 重新处理 | request state、完成标记、attempt |
| 仅 Session 中间事件 | fail/dead，人工核查 | ToolExecution 是否 running/unknown |
| Outbox 插入失败 | 请求不标成功，复用 Session 阶段记录补提交 | PostgreSQL、`Session.state._platform_turns`、Request 状态 |
| Delivery 崩溃 | lease 到期重试 | locked_at、locked_by、下一次时间 |
| Telegram 429 | Retry-After 延后 | Bot 限额、分片记录 |
| 配置回滚 | 后续请求使用指定旧版本 | active pointer、入队 config_version |
| Bot 配置更改 | 滚动重启相关角色 | Adapter 缓存、在途回复对应账号 |
| 微信客服回调 503 | 通知尚未可靠保存，不确认接收 | PostgreSQL、`customer_service_state`、回调错误码 |
| 微信客服人工接管 | 准入/发送实时查状态，暂停自动回复 | service_state、Inbox状态、Outbox错误 |
| 微信客服UNKNOWN | 外部发送结果不确定，不自动重试 | 先核对微信记录，再人工决定；无自动重发API |

如果外部 IM 已经发送成功，但确认响应丢失，仍可能出现重复消息，因此不能承诺 exactly-once delivery。PostgreSQL 与 Redis 之间没有跨库事务，需要依靠修复任务和数据对账恢复一致性。

## 5. 容量与验证

容量测试先使用 Fake Model，记录请求数、并发 Session、p50/p95 时延、模型调用次数和失败数。同一 Session 必须串行执行，吞吐能力主要来自不同 Session 之间的并行处理。20 轮离线并发测试用于验证正确性，不代表生产 QPS。

真实集成环境和 Anaconda 命令见 [testing](testing.md)。真实模型已经完成显式调用验证；真实 Bot 仍须使用测试账号单独执行，默认测试不会产生这些外部副作用。

## 6. 升级与数据保留

升级前先备份控制面并依次应用数据库迁移，再滚动更新 Gateway、Worker 和 Delivery。微信客服使用这三个角色，不依赖 `wecom` 角色。已有 PostgreSQL 数据卷不会再次执行初始化脚本，升级命令见[测试说明](testing.md)。数据库迁移会创建执行租约与客服状态表，并补齐 Outbox 和幂等记录所需字段。

IM 身份规则变化后，旧会话不会自动并入新的命名空间。若 Redis 中只有旧幂等占位、PostgreSQL 中却没有对应 Request，任务会进入待核查状态，不能通过清缓存强制重跑。SDK 数据后端还需要允许创建原生 Lease 表。生产 SQL 后端使用 PostgreSQL；Redis 的原子 Lua 操作要求相关键位于同一 slot。

停止或失锁后，取消 Runner 是第一道保护，真正的拒写由数据后端完成。SDK SQL 后台清理被停用，避免无租约写入；SQL 保留清理、Session 阶段记录和客服 JSONB 归档由独立管理作业按租户保留策略执行。

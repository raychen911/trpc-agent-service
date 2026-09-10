# 运行数据与持久化

默认单服务模式将 SQLite 数据库写入 `data/trpc_agent_service.db`，本地 Artifact 默认写入
`data/artifacts/<tenant_id>/`。数据库、Artifact 内容和临时文件都不会提交到 Git；`data/.gitkeep`
只用于保留空目录。

多副本模式使用 PostgreSQL 保存租户配置、消息、Session Event、Memory/Summary、审计和
Execution Outbox，使用 Redis Stream 分发任务。租户仍可通过 `storage_config` 为 tRPC Session
和 Memory 选择 SQL 或 Redis 后端。具体一致性边界见[架构设计](multi-tenant-agent-platform-design.md)，
容器卷和 Kubernetes PVC 配置见[环境配置](environment-setup.md)及
[Kubernetes 部署](kubernetes-deployment.md)。

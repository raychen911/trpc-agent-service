# 多后端适配策略

| 后端 | 真实存放对象 | 一致性/延迟 | 迁移与运维取舍 |
|---|---|---|---|
| Redis | 共享 Session、Event/State/Summary、Memory、执行租约、IM 乱序水位、限流 | Session 内租约串行；低延迟；可用性故障失败关闭 | 适合高频状态；需持久化、容量和故障转移；可离线迁移到 PostgreSQL state backend |
| PostgreSQL | tenant/config history、binding、receipt、审批、审计、usage、Artifact 目录、Knowledge、SQL state | 事务、FK、唯一约束提供强终态一致性；比 Redis 更高延迟 | 是审计与目录事实源；需连接池、备份、迁移和旧连接恢复策略 |
| MinIO/S3 | Artifact 对象字节和不可变版本内容 | 对象写入后由 SQL `available` 发布；读者只取已发布版本 | 适合大文件和共享 Worker；需 bucket 凭据、生命周期和对象成本管理 |
| 向量库 | Knowledge chunk embedding、向量索引和 tenant/document metadata filter | SQL 文档作为事实源，异步建索引时采用最终一致；查询必须强制 tenant filter | 适合大规模语义召回；迁移需快照、重建索引、数量与抽样查询校验，不能存配置或审计事实 |
| InMemory | 仅开发单 Worker 的临时 Session/Memory | 无跨进程可见性，进程重启丢失 | 不计入生产三类后端，生产多节点不得选择 |

统一装配由 tenant 的 `TenantBackendProfile` 决定：state 为 `redis|sql`，artifact 为 `s3`，knowledge/audit 为 `sql`。接口分别复用 SDK Session/Memory/Knowledge/Artifact 抽象，平台负责 resolver、SQL 元数据、S3 服务和审计 repository。向量库作为 Knowledge 的派生索引层，以 SQL 文档和版本为事实源，通过 tenant/document metadata filter 保持隔离。

租户只有在 Agent 配置的 `allowed_tools` 显式加入 `knowledge_search` 后，Worker 才会把该租户绑定的 KnowledgeBase 注入 tRPC-Agent 的检索工具。没有 Knowledge 后端时工具不会创建，`deny` 由 Filter 阻断；因此检索工具不可能成为跨租户共享实例。知识检索是只读能力，当前不支持 `review`；这类配置会在运行时创建前拒绝，避免绕开危险工具审批链。

生产 Compose 和 Kubernetes 都使用 Redis、PostgreSQL、MinIO/S3 三个共享后端。`backend-init` 验证它们可用；`state-backend-migrate` 提供 Redis↔SQL 的受控离线迁移。对象数据不通过 Redis 或数据库 BLOB 复制，目录和对象键共同决定可读版本。

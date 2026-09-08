# 平台存储与 AgentFactory

## 接口边界

平台代码只依赖 `trpc_service.storage.contracts` 中的 Protocol：`SessionStore`、
`ConversationStore`、`CoordinationStore`、`VectorStore`、`KnowledgeStore`、
`SemanticMemoryStore`、`ArtifactStore`、`OutboxStore` 和 `AuditStore`。本地开发与单元测试使用
InMemory 实现；生产可分别切换 SQL、Redis、远端向量库和 MinIO，而不改变消息处理代码。

| 数据 | 默认实现 | 可选实现 | 一致性 |
| --- | --- | --- | --- |
| Session state/version | SQL | InMemory、Redis CAS | 单记录强一致、乐观锁 |
| 同 session 锁、幂等、限流、短状态 | InMemory | Redis | Redis 原子命令/Lua |
| Event、Summary、Memory、Audit、Outbox | SQL | InMemory（测试） | 单事务强一致 |
| Knowledge/Memory 检索 | 哈希向量 InMemory | 实现 `VectorStore` 接远端库 | Outbox 最终一致 |
| Artifact | 本地文件 | MinIO | 写入后校验 SHA-256 |

Redis 相关键都经过组件前缀隔离。外部消息幂等键的规范值固定为
`tenant_id:channel:external_message_id`；session 锁键还包含 tenant、agent app 和 session。

## 一次消息的写入协议

入口必须调用 `TurnCoordinator.commit`，不能分别写各个存储：

1. 获取同 session 分布式锁，避免多 Worker 同时推进相同状态。
2. 以 `tenant_id:channel:external_message_id` 抢占幂等键。
3. 校验 `expected_version`，随后在一个 SQL 事务中依次追加 Event、CAS 更新 State、写
   Summary、写结构化 Memory、写 Outbox。
4. 事务提交后把幂等记录标为 completed；失败则释放 processing claim，允许 IM 重试。
5. 后台 Outbox Worker 领取记录并 upsert Memory/Knowledge 向量。成功标记 processed；失败按
   退避时间重试。processing 记录带租约，Worker 崩溃后可被其他节点重新领取。

因此权威数据（Event/State/Summary/Memory）与“需要同步向量库”这一事实不会出现一边提交、
另一边丢失的双写窗口。向量检索是最终一致的，SQL 中的 Memory 是事实来源。Outbox 的
`dedupe_key` 包含 Memory 业务键与版本，向量 upsert 使用稳定业务键，重复执行安全。

## CAS 与锁的职责

锁降低同 session 冲突率，CAS 才是最终正确性屏障。每次 state 更新必须携带读取到的版本；
版本不匹配抛出 `VersionConflictError`，调用方重新读取并决定是否重放。Redis Session 使用 Lua
将版本校验和写入合并成单个原子操作；SQL 使用带版本条件的 UPDATE。锁有租期和唯一 token，
只有持有者能续租或释放。

## Artifact 与后端选择

本地模式将对象放在 `TRPC_SERVICE_ARTIFACT_ROOT/<tenant_id>/...`，采用临时文件加原子替换，
并拒绝绝对路径与 `..`。MinIO 模式设置 `TRPC_SERVICE_ARTIFACT_BACKEND=minio` 以及 `.env.example`
中的连接参数；对象名始终以 tenant_id 为前缀。两个实现都返回内容摘要，读取方可校验完整性。

## tRPC-Agent-Python 接入

`AgentFactory` 只读取指定 tenant/app 的已发布配置，解析 `env://` 密钥引用，按 provider 创建
官方 `OpenAIModel` 或 `AnthropicModel`、`LlmAgent` 与 `Runner`。ToolRegistry 只暴露租户配置
白名单内的工具。Runner 可按配置使用 tRPC-Agent-Python 自带的 InMemory、Redis 或 SQL
Session/Memory service；Factory 按发布版本缓存实例，发布或回滚后调用 `invalidate` 即可重建。

生产环境推荐 SQL 保存权威事件与审计，Redis 承担协调和短期状态，向量库服务检索，MinIO
保存大对象。开发环境无需外部服务，全部可退回 InMemory、本地 SQLite 和本地文件。

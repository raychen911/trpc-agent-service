# 多租户 Agent 平台多后端适配方案

> 基于当前项目的 `Storage Adapter`、租户版本化配置、PostgreSQL、Redis、Qdrant、MinIO 与 Transactional Outbox 实现整理。

## 1. 设计目标

平台不让 Gateway、Agent Runner、Tool 或 Channel Adapter 直接依赖某个数据库 SDK，而是统一依赖存储契约。每类数据根据访问方式、一致性要求和容量特征选择合适后端：

- Redis 处理高频、短期、需要原子操作的协调数据；
- SQL 保存结构化、可审计、需要事务和强一致性的权威数据；
- 向量库保存可重建的语义检索索引；
- 对象存储保存图片、文件、音视频和其他大对象；
- SQL 与 Transactional Outbox 负责把权威事实可靠同步到向量库、对象存储相关处理流程和 IM 投递端。

核心原则是：**SQL 保存事实，Redis 提供协调，向量库负责检索，对象存储承载大文件**。不同后端之间不做同步双写，而是通过 Outbox 实现可重试的最终一致性。

## 2. 当前项目的统一访问抽象

项目在 `trpc_service/storage/contracts.py` 中定义了面向业务的 Protocol：

| 抽象接口 | 业务职责 | 当前实现 |
|---|---|---|
| `SessionStore` | Session 读取、创建、版本化状态 CAS 更新 | SQL、InMemory；已有 `RedisSessionStore` |
| `ConversationStore` | Event、State、Summary、Memory 与 Outbox 的一次 turn 提交 | `SqlDataPlane`、`InMemoryConversationStore` |
| `CoordinationStore` | 分布式锁、幂等、限流和短期状态 | Redis、InMemory |
| `SemanticMemoryStore` / `KnowledgeStore` | Memory 与 Knowledge 语义检索 | `SemanticStore` + InMemory/Qdrant |
| `ArtifactStore` | 二进制对象写入、读取和删除 | Local、MinIO |
| `OutboxStore` | 异步任务领取、完成、失败和死信 | SQL、InMemory |
| `AuditStore` | 追加式审计记录 | SQL、InMemory |

`StorageRuntime` 在进程启动时装配具体适配器。业务层只接收这些接口，因此切换后端不需要修改 Agent 消息处理流程。

### 当前实现边界

- 租户发布版本中的 `BackendConfig` 可以记录 Session、Memory、Knowledge、Artifact 等后端选择；
- 当前按 `(tenant_id, agent_app_id, active_version)` 真正动态路由的是 Session，支持 `sql` 和 `inmemory`；
- 生产环境禁止使用 InMemory Session；
- Redis 协调、Qdrant 和 MinIO 当前主要由平台环境变量选择，是平台级实例；
- Memory、Knowledge、Artifact 的租户级配置已经能够保存和展示，但要实现每租户独立实例，还需为这些类型补充与 `TenantTurnCoordinator` 类似的运行时 Router。

因此，本方案既描述当前可运行形态，也给出沿现有接口继续扩展租户级选择的方法。

## 3. 各后端分别适合存什么

### 3.1 Redis：协调、短期状态与热点数据

Redis 适合高频读写、带 TTL、需要原子操作但不适合作为长期审计依据的数据。

推荐存储：

- 同一 Session 的分布式锁和锁 token；
- IM 消息幂等记录，键为 `tenant_id:channel:external_message_id`；
- API、模型、工具和 IM 账号的滑动窗口限流计数；
- Node Directory、节点心跳、容量和 TTL；
- 工具二次确认、临时 OAuth state、流式回复进度等短期状态；
- 可丢失或可从 SQL 重建的 Session 热点缓存；
- 对极低延迟租户，可使用 Redis Session CAS，但 Event、Summary 和审计仍建议落 SQL。

不建议存储：租户配置、完整 Event 历史、长期 Summary、审计日志、文件正文及唯一一份 Memory 事实。

键空间必须带租户边界，例如：

```text
trpc:{tenant_id}:lock:{agent_app_id}:{session_id}
trpc:{tenant_id}:idempotency:{channel}:{external_message_id}
trpc:{tenant_id}:rate:{resource}:{window}
trpc:nodes:{node_id}
```

Redis 的锁降低同 Session 冲突概率，最终正确性仍由 SQL/Redis Session 的版本号 CAS 保证。

### 3.2 SQL：平台权威数据和事务边界

PostgreSQL 适合结构化关系、强一致事务、历史查询、审计和故障恢复，是生产环境的事实源。

推荐存储：

- Tenant、Agent App、版本化模型配置、工具权限和治理策略；
- Channel Binding、IM 用户身份映射和 Secret Reference；
- Session 身份、State、`version` 和更新时间；
- Message/Event、顺序号、外部消息 ID 和 trace_id；
- Summary 版本及其覆盖的 Event sequence；
- Memory 的结构化原文、业务键、版本和 metadata；
- Artifact metadata、object key、MIME、大小和 checksum；
- Audit Log、预算使用量、Execution Ledger；
- Durable Inbox、Transactional Outbox 和 Dead Letter。

一次 turn 必须在同一 SQL 事务中按固定顺序执行：

```text
Event
  → Session State CAS
  → Summary
  → Memory 事实表
  → Transactional Outbox
```

只有事务提交后才允许 Outbox Worker 同步向量索引或投递 IM 回复。这样即使 Qdrant、MinIO 或企业微信暂时不可用，SQL 中的消息事实和待处理任务也不会丢失。

多租户隔离采用所有业务表携带 `tenant_id`、复合外键、唯一约束和 PostgreSQL RLS。应用连接必须使用非 owner、`NOBYPASSRLS` 账号，并在每个事务中设置 tenant context。

### 3.3 向量库：Memory 与 Knowledge 的语义检索索引

Qdrant 等向量库适合根据自然语言相似度查找内容，不适合承担事务事实源。

推荐存储：

- 用户长期 Memory 的向量、文本副本和筛选 metadata；
- Agent App 的 Knowledge 文档分片、向量、来源和版本；
- 可选的语义缓存或历史问答索引。

命名空间必须隔离租户：

```text
memory:{tenant_id}:{agent_app_id}:{user_id}
knowledge:{tenant_id}:{agent_app_id}
```

向量文档 ID 使用稳定业务 ID，重复执行采用 upsert。SQL 中的 Memory/Knowledge 元数据是事实源，向量库是派生索引：

1. SQL 事务写入 Memory 和 `memory.upsert` Outbox；
2. 事务提交；
3. Outbox Worker 调用 `VectorOutboxHandlers`；
4. `SemanticStore` 写入 Qdrant；
5. 成功后将 Outbox 标为 processed，失败则退避重试，超过阈值进入死信。

因此，Memory 跨节点可见性分为两层：SQL 事实在事务提交后立即可见，语义检索在 Outbox 同步完成后最终可见。向量库损坏时可使用 `SqlMemoryToVectorMigrator` 从 SQL 全量重建。

### 3.4 对象存储：Artifact、媒体和大文件

MinIO/S3 类对象存储适合非结构化二进制数据和大对象，不应把文件内容直接塞入 Event 或 Session JSON。

推荐存储：

- 企业微信/Telegram 收发的图片、语音、视频和文件；
- Tool 生成的报表、CSV、PDF、PPTX、压缩包；
- 长期 Artifact、模型中间产物和导出文件；
- 可选的知识库原始文件，解析后的文本与向量进入 SQL/向量库。

对象键建议使用不可跨租户的规范路径：

```text
{tenant_id}/{agent_app_id}/{session_id}/{artifact_id}/{filename}
```

SQL 的 Artifact 表只保存 metadata 和 object key；对象正文位于 MinIO。写入后计算 SHA-256 checksum，读取时可校验完整性。Local 实现只用于开发，生产使用 MinIO/S3，并启用版本化、生命周期、服务端加密和预签名 URL。

已有 `LocalToMinioArtifactMigrator` 可完成本地文件到 MinIO 的迁移与 checksum 校验。

## 4. 数据归属总表

| 数据类型 | 主存后端 | 辅助后端 | 一致性要求 | 说明 |
|---|---|---|---|---|
| Tenant / Agent App / 配置版本 | PostgreSQL | Redis 可短期缓存 | 强一致 | 发布和回滚以 SQL active version 为准 |
| Channel Binding / 用户映射 | PostgreSQL | Redis 可缓存解析结果 | 强一致 | Secret 只保存引用 |
| Session State / Version | PostgreSQL | Redis 可作热点缓存或特定 Session 后端 | CAS 强一致 | 生产默认 SQL |
| Session Lock | Redis | InMemory 仅本地开发 | 原子、租约一致 | key 包含 tenant/app/session |
| IM 幂等与限流 | Redis + SQL 唯一约束 | 无 | 原子 claim + 最终防重 | Redis 快速拒绝，SQL 负责最终约束 |
| Event / Message | PostgreSQL | 无 | 事务强一致、顺序写 | 不放向量库 |
| Summary | PostgreSQL | 可缓存到 Redis | 与 Event sequence 一致 | 记录 through_sequence |
| Memory 事实 | PostgreSQL | Qdrant 检索索引 | SQL 强一致、向量最终一致 | Outbox 异步 upsert |
| Knowledge 原文/元数据 | PostgreSQL 或对象存储 | Qdrant 检索索引 | 元数据强一致、向量最终一致 | 大文件正文进入 MinIO |
| Artifact / 媒体 / 文件 | MinIO | PostgreSQL 存 metadata | 对象写入后校验 checksum | 不放 Redis 或 Event JSON |
| Audit / Budget / Execution Ledger | PostgreSQL | 指标系统做聚合 | 追加写、可追溯 | 不允许仅存日志系统 |
| Inbox / Outbox / Dead Letter | PostgreSQL | Redis 可做唤醒信号 | 持久、可重试 | SQL 是恢复依据 |
| Node Directory / 临时确认状态 | Redis | InMemory 用于测试 | TTL、最终收敛 | 过期自动清理 |

## 5. 租户级后端选择

后端配置属于 `tenant → agent_app → config_version`。每个 `backend_kind` 最多选择一个后端，不要求租户把全部后端组合起来；未配置的类型使用平台默认值。

示例：

```json
{
  "expected_lock_version": 6,
  "backends": [
    {
      "backend_kind": "session",
      "backend_type": "sql",
      "options": {}
    },
    {
      "backend_kind": "memory",
      "backend_type": "qdrant",
      "secret_ref": "env://QDRANT_API_KEY",
      "options": {"collection": "tenant-a-memory"}
    },
    {
      "backend_kind": "artifact",
      "backend_type": "minio",
      "secret_ref": "vault://tenant-a/minio",
      "options": {"bucket": "tenant-a-artifacts"}
    }
  ]
}
```

运行时路由步骤：

1. 按 tenant、agent app 和 active version 查询 `BackendConfig`；
2. `TenantBackendResolver` 生成 configured/effective backend；
3. 对应 Router 从注册表取得适配器实例；
4. SecretResolver 解析连接引用，配置正文中不出现密码；
5. 未配置时使用 platform-default；不支持的配置应在发布预检中拒绝，而不是运行时静默降级。

建议把现有 Session Router 扩展为：

```text
TenantStorageRouter
├── session_router: sql / redis / inmemory
├── coordination_router: shared-redis / dedicated-redis
├── vector_router: qdrant / external-memory / inmemory
└── artifact_router: minio / s3 / local
```

同一个租户可以只配置一种业务后端，也可以按数据类别组合。例如小型租户只使用 SQL；检索型租户使用 SQL + Qdrant；包含大量文件的租户再增加 MinIO。平台不应要求租户理解或单独选择 Inbox、Outbox、Audit 等内部事实表，它们始终由平台 SQL 承担。

## 6. 一致性与故障降级

| 场景 | 处理策略 |
|---|---|
| 多节点同时写同一 Session | Redis 分布式锁降低冲突，版本 CAS 负责最终正确性 |
| 企业微信重复投递 | Redis 幂等 claim + Inbox/Event SQL 唯一约束，重复请求返回已有结果 |
| Qdrant 暂时不可用 | SQL turn 正常提交，Outbox 重试；检索暂时返回旧索引或降级为空 |
| MinIO 暂时不可用 | 不把未成功上传的对象标为可用；任务进入重试或死信 |
| Redis 暂时不可用 | 对同 Session 写入 fail-closed，避免绕过锁；只读操作可视业务降级 |
| PostgreSQL 暂时不可用 | Webhook 快速返回可重试错误，不声明消息已可靠接收 |
| Worker 在同步时崩溃 | Outbox processing 租约过期后由其他节点重新领取，upsert 保证重复执行安全 |

需要监控的核心指标包括 Redis 锁等待和 CAS 冲突、SQL 事务与连接池延迟、Outbox backlog/oldest age/dead letter、向量同步延迟、Artifact 上传失败率和各租户存储成本。

## 7. 后端迁移方案

### Redis Session → SQL

使用现有 `RedisToSqlSessionMigrator` 分批扫描租户 Session，按 tenant/app/session 主键 upsert SQL，保留 state、version 和更新时间。迁移采用“暂停写入或短期双写 → 全量回填 → 增量校验 → 修改 active backend → 观察 → 清理旧数据”的顺序。

### InMemory/SQL Memory → Qdrant

SQL 作为事实源，使用 `SqlMemoryToVectorMigrator` 分租户、分批重建索引。通过稳定 document ID 保证重复执行安全；切换读取前核对记录数、抽样召回和 Outbox 水位。

### Local Artifact → MinIO

使用 `LocalToMinioArtifactMigrator` 上传对象并校验 checksum。切换前同时核对 SQL metadata、对象数量和大小，保留本地副本直到回滚窗口结束。

所有迁移都应生成包含 tenant、成功数、失败数、checksum/版本差异和开始结束时间的报告，不直接删除源数据。

## 8. 推荐落地配置

### 本地开发与单元测试

- Session/Conversation：InMemory 或 SQLite；
- Coordination：InMemory；
- Vector：InMemory；
- Artifact：Local。

优势是零外部依赖，但进程重启后允许丢失状态，不作为生产证据。

### 当前 Compose 联调环境

- 权威数据：PostgreSQL；
- Session 锁、幂等、限流和节点目录：Redis；
- Memory/Knowledge 检索：Qdrant；
- Artifact：MinIO；
- 跨后端同步：PostgreSQL Transactional Outbox。

该组合与当前已验证的企业微信、双节点和 Jaeger 链路一致。

### 生产推荐

- PostgreSQL HA + 强制 RLS + PITR；
- Redis Sentinel/Cluster 或托管 Redis，启用副本与 AOF；
- Qdrant 集群或托管向量服务，支持按 tenant namespace 过滤；
- MinIO 集群或云对象存储，启用版本化、加密和生命周期；
- 所有连接凭据通过 SecretResolver 获取；
- Outbox Worker 独立扩缩容并配置 backlog、失败和死信告警。

## 9. 结论

当前项目的接口分层已经能够把业务逻辑与具体后端隔离。生产环境应以 PostgreSQL 为权威事实源，以 Redis 提供低延迟协调，以 Qdrant 提供可重建的语义检索，以 MinIO 承载二进制大对象，并用 Transactional Outbox 消除 SQL 与外部后端之间的同步双写窗口。下一步实现重点不是新增另一套存储接口，而是把现有租户 Session 路由模式推广到 Vector 和 Artifact，并在发布阶段严格校验租户选择是否被当前运行时真正支持。

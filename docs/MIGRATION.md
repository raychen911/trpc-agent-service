# 数据同步、幂等与迁移策略

本文档说明多节点并发、Session 更新顺序、Memory 跨节点可见性、IM 重复投递幂等，以及 Redis/SQL/向量库等后端迁移方案。

## 多节点并发写入同一 Session

同一 session 可能被多个 Gateway 接收消息，也可能被多个 Worker 并发消费。为避免覆盖状态，Session 采用 event sourcing + 乐观锁：

1. Worker 读取 `session_state`，获得 `state_version` 和 `latest_event_seq`。
2. Worker 为本次执行生成连续 event，并尝试追加到 `message_event`。
3. Worker 使用 CAS 条件更新 `session_state`，条件是当前 `state_version` 未变化。
4. 如果 CAS 冲突，Worker 重新读取最新 event/state，在有限次数内重放本次输入并重试。
5. 超过重试次数后，请求进入失败态或队列重试，并写入审计日志。

Redis 后端通过 Lua 脚本或事务保证 event 追加和 state 版本更新的原子检查；PostgreSQL 后端通过事务、唯一键和 `WHERE state_version = ?` 实现乐观锁。

## Session Event、State、Summary 更新顺序

当前代码的更新顺序如下：

1. 写入用户入站事件，记录原始消息摘要、幂等键和 trace_id。
2. 执行 Filter、Knowledge、Tool、Model，产生工具事件和助手事件。
3. 追加工具事件、助手事件和必要的系统事件。
4. CAS 更新 `session_state.latest_event_seq` 和 `state_version`。
5. 写入 Memory；如派生写失败，写入 `compensation_task`。
6. 根据最新 event 序号生成并写入 Summary，校验 `source_event_seq >= current.source_event_seq`，旧摘要不能覆盖新摘要。
7. 写入 Audit Log；如派生写失败，写入 `compensation_task`。
8. 完成幂等状态并保存 response reference。
9. 补偿 Worker 后台重放 `compensation_task`，成功后标记 completed，失败后按 `available_at` 延迟重试。

如果 Summary 或 Memory 写入失败，不应回滚已经成功追加的 message_event。系统应记录审计错误，并通过后台任务补偿生成 Summary 或重试 Memory 写入。这样可以保证对话事件不丢失，同时允许派生数据最终一致。
IM 撤回事件按特殊入站事件处理：Gateway 只追加 `message_revoked` session event 和 `revoked` 审计记录，完成幂等状态，不进入 Worker、模型、工具或出站回复。撤回事件乱序到达时，以目标 `external_message_id` 或 `target_message_id` 做业务关联，不删除历史审计。

## Memory 写入后的跨节点可见性

Memory 必须写入租户配置指定的共享后端。Redis 和 PostgreSQL 写入成功后，其他 Worker 下次读取即可可见。向量库或外部 Memory 服务通常存在索引刷新延迟，因此需要在 metadata 中记录 `embedding_version`、`updated_at` 和 `source_event_seq`，检索结果也必须带这些字段用于排查旧索引问题。

长期 Memory 推荐使用 PostgreSQL 或外部 Memory 服务作为权威源，向量库只作为检索索引。索引更新失败时，可以从权威源重新构建。

## IM 重复投递幂等

IM 平台可能因为超时、网络失败或平台重试机制重复投递同一消息。幂等键规则如下：

```text
idempotency_key = hash(tenant_id + channel + account_id + external_message_id)
```

处理流程：

1. Gateway 收到消息后，原子创建 `processing` 状态。
2. 如果创建成功，说明是首次处理，继续配额检查和队列投递。
3. 如果已存在 `completed`，直接返回保存的 `result_json` 或 `response_ref`。
4. 如果已存在 `processing` 且未超时，返回平台允许的异步确认，避免重复入队。
5. 如果已存在 `failed` 或 processing 超时，按策略允许重试，并增加 retry count。
6. Worker 成功后写入 `completed`，失败后写入 `failed`、错误类型和 trace_id。

幂等记录 TTL 应覆盖 IM 平台最长重试窗口。对企业微信、微信公众号和 Telegram，建议至少保留 24 小时；高价值业务可延长到 7 天。

## Redis 到 SQL 迁移

迁移以租户为单位执行，避免一次性全平台切换。

1. 冻结目标租户配置变更，记录迁移版本和 Redis Key 前缀。
2. 导出 Redis 中的 session events、state、summary、memory、idempotency、quota、audit 和 compensation 记录。
3. 按 `tenant_id`、`session_id`、`seq` 保留原始顺序写入 PostgreSQL。
4. 校验行数、最大 event seq、幂等 checksum、summary `source_event_seq` 和抽样会话状态。
5. 打开双写：新请求同时写 Redis 和 SQL。
6. 打开双读：SQL 为主，Redis 为回退，持续一个回滚窗口。
7. 发布新的租户配置版本，将 `storage_profile.session_backend` 等字段切到 SQL。
8. 监控错误率、CAS 冲突、存储延迟、队列积压和重复投递命中率。
9. 回滚窗口结束后，按租户前缀清理 Redis 旧数据。

`trpc_service.migrate` 命令提供导出、导入、导入后校验和切换计划能力。迁移期间必须保留旧配置版本，回滚时只切换 active version 指针。迁移窗口可设置 `MIGRATION_DUAL_WRITE_BACKEND=sql`，由 `TenantStorageManager` 为租户建立主后端与目标后端的同步写入、主读和目标回退；双写任一侧失败会让本次请求失败并由消息幂等键重试，避免“主成功、目标静默丢失”。迁移完成后删除该环境变量，发布新的租户配置版本切换权威后端。

迁移 CLI 的结构化快照命令 `export`、`import`、`verify` 当前支持 `redis`、`sql` 和 `postgres` 后端；`migration-run` 额外支持 `memory` 作为迁移源或目标。不要把 `qdrant`、`s3`、`oss` 或 `minio` 直接作为 `--backend` 参数，否则 CLI 会按设计返回无效选项错误。远端向量库和对象存储通过租户 `StorageProfile` 配置，由对应 Adapter 执行写入、读取和重建：向量库迁移采用“权威文本导出后重新 embedding/upsert，再抽样 top-k 校验”，对象存储迁移采用“按租户前缀复制对象并校验元数据/校验和”。这两类真实供应商迁移需要目标服务、凭据和供应商侧权限，当前仓库提供适配器和策略，不宣称已在外部 Qdrant/S3/OSS/MinIO 上完成实测。

```bash
uv run python -m trpc_service.migrate export --tenant tenant_demo --backend redis --redis-url redis://localhost:6379/0 --output tenant_demo.json
uv run python -m trpc_service.migrate import --backend sql --sql-dsn sqlite:///data/target.sqlite3 --input tenant_demo.json
uv run python -m trpc_service.migrate verify --backend sql --sql-dsn sqlite:///data/target.sqlite3 --input tenant_demo.json
uv run python -m trpc_service.migrate cutover-plan --input tenant_demo.json
```

导出包版本为 `version=2`，覆盖 session state、message/event、summary、memory、audit log、idempotency、compensation、knowledge chunk 和 artifact 内容。`verify` 会按各类对象数量比较源导出包与目标后端现状，`cutover-plan` 输出冻结、导出、导入、校验、补偿队列 drain、配置切换、灰度发布和回滚窗口保留步骤。

## 本地向量库到远端向量库迁移

Knowledge 迁移同样以租户和 collection 为单位进行：

1. 从本地向量库导出 `chunks.json`，包含 `tenant_id`、`collection_id`、`chunk_id`、文本、metadata、embedding 版本。
2. 使用目标远端向量库的 embedding 版本重新导入或重建索引。
3. 对抽样查询比较 top-k 召回结果、排序变化和引用 metadata。
4. 在租户配置中新增远端 `knowledge_backend`，但先保持旧后端可回滚。
5. 灰度部分租户或部分 Agent App，观察检索延迟、召回率和模型回答质量。
6. 发布新配置版本，全量切换后保留本地索引一个回滚窗口。

## 多后端适配方案

| 后端 | 适合存储 | 不适合存储 | 说明 |
| --- | --- | --- | --- |
| InMemory | 单测、本地 demo、无状态接口模拟 | 生产 Session、租户配置、审计 | 进程退出即丢失，不能跨节点共享。 |
| Redis | Run Queue、幂等、热 Session、配额计数、短期缓存 | 长期审计、大文件、强事务报表 | 延迟低，原子操作方便，但要管理 TTL 和内存。 |
| SQL/PostgreSQL | 租户配置、事件、Memory、Summary、Audit Log | 高频队列、大文件内容 | 强一致、可查询，适合作为权威存储。 |
| 向量库 | Knowledge chunk、embedding、语义检索索引 | 权威业务配置、审计日志 | 检索能力强，但索引刷新通常最终一致。 |
| 对象存储 | 图片、文件、长文本 Artifact、导出包 | 低延迟 CAS 状态 | 成本低容量大，配合 SQL 保存 metadata。 |
| 外部 Memory 服务 | 托管长期记忆、跨应用用户画像 | 对延迟极敏感的热状态 | 降低自建成本，但需关注 SLA、限流和数据合规。 |

## 不同后端的一致性取舍

Redis 适合低延迟和原子计数，但持久性和查询能力弱于 SQL。PostgreSQL 提供事务强一致，适合配置、事件和审计，但写入延迟和连接数需要规划。向量库和对象存储通常是最终一致或供应商定义一致性，适合派生数据和大对象，不应作为 Session state 的唯一权威来源。外部 Memory 服务可以降低本地运维复杂度，但会引入网络延迟、供应商限流和跨境合规风险。

## PostgreSQL RLS 迁移

PostgreSQL Schema 版本由 Alembic 管理，和本文件描述的租户数据后端切换相互独立。
升级、检查、已有数据库接管和降级保护见
仓库中的 Alembic 脚本和 `scripts/database_migration_gate.py` 负责升级、检查、已有数据库接管和降级保护。

RLS 是生产加固选项，不是本地演示的必需项。首先使用独立的 Schema owner
连接执行常规 PostgreSQL Schema 迁移，然后运行 `rls` 迁移，创建最小权限的运行时
角色和控制面角色，并安装租户策略。切换完成后设置
`POSTGRES_AUTO_CREATE_SCHEMA=0`；应用进程必须使用运行时 DSN，绝不能使用 Schema
owner DSN。

RLS 策略是第二层隔离边界。现有的显式 `tenant_id` 条件、Redis 租户前缀、对象存储
路径、向量集合范围和鉴权检查仍然必须保留。角色、策略和验证见
PostgreSQL RLS 的角色、策略和验证由 `tests/test_postgres_rls.py` 覆盖，部署时按数据库迁移脚本执行。

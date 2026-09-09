# Session 与 Memory 数据迁移

## 实现范围

平台支持 Redis 与 PostgreSQL 之间的双向 Session/Memory 迁移，迁移内容包括：

- Session 基本信息、Session/App/User State；
- 当前 Events、`historical_events`、Summary Event；
- Event ID、`request_id`、时间、actions、usage 等 SDK 字段；
- Memory 中由 SDK SQL Memory 后端支持的 Event；
- Redis 使用 `SCAN`、PostgreSQL 使用稳定游标分批读取，并保存 checkpoint、逐资源 hash、dirty 记录和执行租约；
- 支持 Redis → PostgreSQL 和 PostgreSQL → Redis；
- 请求固定 `storage_route_version`，切换时短暂停止该租户的新请求。
- 每次阶段或批次推进都会记录平台 Trace、低基数 Metric 和 PostgreSQL Audit，观测字段采用资源摘要和脱敏信息。

实现适配 `trpc-agent-py 1.1.19` 的存储格式，全部迁移逻辑位于服务仓库。迁移完成后源数据进入保留期，用于观察和回滚。

## 迁移流程

```text
preparing
  检查 SDK 版本、两个后端、PostgreSQL 表结构和租户范围
    ↓
dual_write
  源后端负责读取；每次 Session/Memory 写入同步写到目标后端
    ↓
backfilling
  分批搬历史数据，每批保存游标和资源 hash
    ↓
verifying
  再次扫描当前源后端，补齐回填期间的新数据并重新对账
    ↓
shadow_read
  正式结果仍取源后端，同时比较目标后端数据
    ↓
cutover
  暂停该租户准入、排空在途任务，切为目标后端主读并镜像源后端
    ↓
completed
  观察期结束后使用目标后端；源数据继续保留
```

每次 `advance` 最多处理一个批次。进程中断后，下次从 PostgreSQL 中保存的 checkpoint 继续。两个 Admin 同时推进同一个 Job 时，只有取得 Job lease 的实例可以执行。

## 一致性边界

Redis 与 PostgreSQL 通过阶段记录协调跨库一致性。主库写成功而镜像写失败时，请求进入可恢复状态，失败资源写入 `migration_dirty_key`。再次验证时，服务从当前主库重建完整快照并幂等覆盖目标库，同时复用已经保存的模型结果。

Session 以完整 Event JSON 的规范化 hash 对账。时间统一到 PostgreSQL 可表示的微秒精度；空的 `long_running_tool_ids` 使用同一表示。Memory 的 SQL 表只保存 SDK 支持的投影，因此按 Event ID 集合及检索效果核对，完整 Event 仍以 Session 为真值。

迁移在 `mismatch_count=0` 且 `dirty_count=0` 时切换。观察期内可以按原路由回滚；进入 `target_only` 后，通过新建反向迁移把增量数据同步回原后端。

PostgreSQL → Redis 时，SQL Reader 使用只读 ORM 查询，保持源记录的 TTL 语义。Redis 目标中仅存在于目标端的旧 Session/Memory 会先写入 `migration_target_backup`，再按租户和 App 范围清理。清理操作限定具体 Key 范围，确保其他租户数据保持原样。

## 管理接口

- `POST /api/v1/admin/migrations`：创建迁移；URL 从租户配置读取。
- `GET /api/v1/admin/migrations/{job_id}`：查看阶段、游标、计数和错误。
- `POST /api/v1/admin/migrations/{job_id}/advance`：执行一个阶段或一个批次。
- `GET /api/v1/admin/migrations/{job_id}/items`：查看逐资源对账结果。
- `POST /api/v1/admin/migrations/{job_id}/rollback`：在观察期内真实切回源后端。

## 测试

先启动测试 Redis/PostgreSQL，并应用 `migrations/006_real_storage_migration.sql` 和 `007_reverse_storage_migration.sql`，然后在 Anaconda Prompt 中运行：

```bat
set "TRPC_TEST_REDIS_URL=redis://127.0.0.1:16379/0"
set "TRPC_TEST_POSTGRES_URL=postgresql://trpc_agent:test@127.0.0.1:15432/trpc_agent_test"
python -m pytest tests/test_real_storage_migration.py -p no:cacheprovider -vv
```

预期 `13 passed`。其中 8 项是协议、方向校验、SDK 表初始化与版本检查、观测、批次续跑和并发推进单元测试；5 项连接真实 Redis/PostgreSQL，验证双向迁移、运行期双写和双向观察期回滚。

显式迁移当前测试租户：

```bat
python -m trpc_service._cli demo migration-live --confirm --json
python -m trpc_service._cli demo migration-reverse-live --confirm --json
```

该命令会写入测试 PostgreSQL 的配置和迁移表，因此使用专用测试库。命令会检查两个测试 URL 和 `--confirm`，条件齐全后开始执行。

验收场景包括：

- 回填期间新增 20 个 Session；
- 重复执行批次通过资源 Hash 和 Event ID 复用已有数据；
- Coordinator 重建后从 checkpoint 继续；
- 目标端存在差异时阻止切换；
- 旧目标数据隔离备份；
- 双向观察期回滚。

真实 Redis/PostgreSQL 用例会核对 Session、State、Event、Summary 与 Memory 的数量和 Hash。生产压测使用相同指标观察 batch latency、dirty 和 mismatch。

本页实现范围是 Redis 与 PostgreSQL 之间的 Session/Memory 双向迁移。Knowledge 向量索引采用独立 Provider 和版本化重建策略，与 Session/Memory 快照分别管理。

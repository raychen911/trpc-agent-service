# 数据模型与存储结构

## 1. 文档依据与建模方式

本文以 [实际 SQL 迁移](../migrations/0001_production.sql) 和 [领域模型](../trpc_service/tenant/models.py) 为依据，在同一处说明实体含义、存储位置和关联方式，不再维护一套与实现不同的示意 SQL。

租户配置作为一个整体进行版本管理。Agent App、Channel Binding、工具策略、审计策略和 StorageProfile 存放在 `tenant_revisions.config_json` 中，而不是分别创建独立 SQL 表。这使一次发布能够同时固定应用、权限与通道设置，Worker 也可以按消息记录的版本恢复配置。

Session、Event、Memory、Summary 和 Audit 属于持续增长的运行数据，采用独立表或对应的 Redis 状态实现。下文的表名指 PostgreSQL 结构，选择其他后端时仍需保持相同的租户范围和业务语义。

## 2. 核心实体与关系

| 业务实体 | 实际存储 | 标识及关联关系 |
|---|---|---|
| Tenant | `tenants` | `tenant_id` 标识租户，`active_version` 选择当前配置版本 |
| 配置版本 | `tenant_revisions` | 主键为 `(tenant_id, version)`，`config_json` 保存完整租户配置 |
| Agent App | `config_json.apps` | 以 `app_id` 区分应用，保存模型、工具和知识集合配置 |
| Channel Binding | `config_json.channels` | 以通道名选择绑定，保存 `account_id`、密钥引用和用户权限。当前每个租户每类通道对应一个 binding |
| Session | `sessions` | 主键为 `(tenant_id, session_id)`，`app_id` 对应配置中的应用，另存 `user_id`、状态及版本 |
| Event | `session_events` | 主键为 `(tenant_id, session_id, sequence)`，关联 Session。`event_id` 在租户范围内唯一 |
| Memory | `memories` | 主键为 `(tenant_id, memory_id)`，通过 `user_id` 归属外部用户，不直接归属某个 Session |
| Summary | `summaries` | 主键为 `(tenant_id, session_id)`，`source_version` 表示摘要覆盖的事件版本 |
| Audit Log | `audit_logs` | 主键为 `(tenant_id, audit_id)`，通过 session、agent、tool 和 trace 字段关联操作上下文 |

一个租户拥有多个配置版本，每个版本可以包含多个 Agent App。Session 记录关联应用，事件按 Session 追加。Memory 与 Session 并非父子关系，而是在租户用户范围内共享。Summary 是 Session 事件的派生数据，Audit 则独立记录入口、治理、工具和处理结果。

以上为业务关联，并非全部由数据库外键强制维护。代码通过租户作用域、版本读取、复合键和 RLS 控制访问，生产验收还应检查孤儿记录、配置版本保留和删除一致性。

## 3. 核心字段与更新约束

### 3.1 租户与应用配置

`TenantConfig` 包含 `tenant_id`、`name`、`status`、`version`、`apps`、`channels`、`policy`、`audit` 和 `storage`。应用配置可指定主备模型、API Key SecretRef、服务地址、超时、工具集合、知识集合和 token 单价。通道配置保存账号、验签凭据引用、加密密钥引用及可访问用户。

敏感配置使用 SecretRef，不把实际密钥写入配置 JSON。审计策略提供 `retention_days` 和 `allow_export`，后端选择使用 StorageProfile。消息受理时将 `config_version` 写入输入信封，再保存在 Inbox 的 `payload_json` 中。该版本固定本条消息及重试行为，不代表整个 Session 永久使用同一版本。

### 3.2 Session 与 Event

`sessions` 包含 `state_json`、`version`、`lease_owner`、`lease_expires_at` 和 `fencing_epoch`。租约负责协调写入者，版本比较避免覆盖已有状态，fencing 拒绝失效节点提交。

`session_events` 保存 `sequence`、`event_id`、`event_type`、`payload_json`、`trace_id` 和创建时间。当前平台事件记录用户与助手消息，工具明细另存工具执行账本和审计。不能将平台事件表理解为框架所有内部事件的完整镜像。

### 3.3 Memory 与 Summary

`memories` 保存内容、用户标识、`source_version` 和 `projection_status`。读取范围是 `(tenant_id, user_id)`，因此同一租户的同一外部用户可能跨群复用记忆。群级保密需求必须进一步缩小作用域，具体限制见 [IM 身份隔离](im-channels.md)。

`summaries` 保存内容、`source_version` 和更新时间。更新时拒绝更旧的版本，防止异步任务乱序覆盖新摘要。当前默认摘要生成器提取近期事件文本，不应描述为已接入独立的大模型摘要服务。

### 3.4 审计

`audit_logs` 包含 `tenant_id`、`channel`、`user_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency_ms`、`error_type`、`cost` 和 `trace_id`，并使用 `detail_json` 保存扩展信息，`occurred_at` 记录发生时间。

表结构能够表达原题要求的审计字段，但不同执行路径的填充程度不同。部分工具耗时或费用仍使用默认值，模型成本为估算值。审计查询、导出、保留与脱敏策略见 [安全文档](security.md)。

## 4. 可靠消息与辅助数据

| 表 | 用途与关键约束 |
|---|---|
| `inbound_messages` | 保存输入信封、状态和已有结果。`(tenant_id, channel, account_id, external_message_id)` 唯一约束用于去重 |
| `outbox_events` | 保存待发布通知或投影任务，记录领取状态、重试次数、可执行时间和死信状态 |
| `idempotency_keys` | 保存业务幂等键及结果状态，避免重复提交同一处理结果 |
| `outbound_messages` | 按账号、原消息和分段序号记录投递状态，保存平台回执与错误类型 |
| `tool_executions` | 记录工具执行 key、参数摘要、结果和状态，支持副作用对账 |
| `tool_confirmations` | 记录绑定租户、用户、Session、工具和参数的一次性确认 |
| `tenant_budget_usage` / `budget_reservations` | 记录每日 token 用量和执行前的预算预留 |
| `migration_checkpoints` | 保存迁移阶段、数量、checksum、差异和状态，不负责自动复制数据 |

## 5. Knowledge 与 Artifact

`knowledge_documents` 按 `(tenant_id, collection, item_id)` 保存文档事实及版本。`knowledge_vectors` 保存检索内容和 embedding，当前以 `(tenant_id, item_id)` 为主键。索引器将 collection 编入向量 item ID，检索时同时限制租户和允许的集合。

Artifact 的二进制内容保存到 S3 兼容对象存储。`artifacts` 表保存 `object_key`、`checksum`、`size_bytes`、`content_type` 和状态，下载时校验租户路径与 checksum。对象上传与 SQL 元数据写入并非一个分布式事务，失败补偿和孤儿清理需要单独考虑。

这两类数据的同步与一致性策略见 [多后端方案](consistency.md)。

## 6. 数据生命周期

配置历史需要覆盖消息重试和回滚窗口，不能在仍有任务引用时删除。Session 事件按保留策略归档，摘要和向量可依据事实数据重建。Memory 的保留范围应结合用户数据策略确定，不能简单等同于 Session 生命周期。

租户删除应先冻结入口并处理在途任务，再清理 SQL、Redis、向量和对象数据。审计根据保留策略清理或归档。当前文档描述的是跨后端删除的运维顺序，不代表 Admin 删除接口已自动完成全部物理擦除。

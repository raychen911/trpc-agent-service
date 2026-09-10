# 第七阶段数据模型

**功能编号**：`007-memory-summary-backend-flow`

**权威存储**：除明确标注的 Redis 协调状态和对象/向量 fixture 外，均为 PostgreSQL。

## 通用约定

- 所有业务实体必须包含可信 `tenant_id`；Repository 不提供跨租户列表或省略 tenant 的重载。
- 标识、namespace、session key 与 stream name 使用规范化非空字符串；时间统一为 UTC。
- JSON 内容规范化后保存 `content_digest=sha256(canonical_bytes)`；digest 为 64 位小写十六进制。
- 内容实体不可变；更新创建新 version 或通过 expected_version CAS 替换当前记录。
- `trace_id`、`owner_trace_id`、`execution_trace_id` 贯穿数据变化和 Audit，但不作为高基数指标标签。
- 原文内容不进入普通日志、指标、错误详情和 Audit detail；Audit 只保存 digest、大小、版本和安全元数据。

## 1. SessionStream

表示一个 tenant-scoped Session Event 流及其已确认水位。

| Field | Type | Rules |
|---|---|---|
| tenant_id | string | 主键组成，可信上下文派生 |
| session_key | string | 主键组成，由 tenant/agent/channel/session 身份稳定派生 |
| watermark | integer | `>=0`，只按 1 递增 |
| authority | enum | `POSTGRES`；迁移期可处于受控 legacy/cutover 状态 |
| rollback_eligible | bool | cutover 后首笔 PG 新写同事务关闭 |
| generation | integer | migration/authority CAS fencing token |
| created_at/updated_at | datetime | UTC |

**Primary key**：`(tenant_id, session_key)`。

## 2. SessionEvent

| Field | Type | Rules |
|---|---|---|
| tenant_id, session_key | string | 关联 SessionStream |
| event_id | string | 同一流中的稳定幂等标识 |
| sequence | integer | 必须等于提交前 watermark+1 |
| event_type | string | 平台稳定类型，不保存供应商类名 |
| payload | JSON | 治理后的规范化内容 |
| content_digest | string | payload 的确定性 digest |
| trace_id / owner_trace_id / execution_trace_id | UUID | 全链路追踪 |
| created_at | datetime | UTC，不可变 |

**Unique**：`(tenant_id, session_key, event_id)`、`(tenant_id, session_key, sequence)`。

**Replay rule**：相同 event_id+digest 返回已有 Event；相同 ID 不同 digest 冲突。任何非 `watermark+1` 的新事件都返回 `sequence_gap` 且不落库。

## 3. MemoryRecord

| Field | Type | Rules |
|---|---|---|
| tenant_id | string | 主键组成 |
| namespace | string | 主键组成；如 agent/user/session，但不代替 tenant |
| memory_key | string | 主键组成 |
| content | JSON | 规范化小型结构化内容 |
| content_digest | string | 内容 digest |
| version | integer | 从 1 开始，CAS 单调递增 |
| source_event_watermark | integer? | 若由 Event 派生，不得大于已确认水位 |
| updated_at | datetime | UTC |

**Primary key**：`(tenant_id, namespace, memory_key)`。

**Size boundary**：规范化 UTF-8 字节数超过配置上限返回 `content_too_large`；不自动转 Artifact。

## 4. SummaryRecord

| Field | Type | Rules |
|---|---|---|
| tenant_id, session_key | string | 主键组成 |
| content | JSON | 规范化摘要 |
| content_digest | string | 摘要 digest |
| event_watermark | integer | `0..SessionStream.watermark`，只前进 |
| version | integer | CAS 单调递增 |
| updated_at | datetime | UTC |

**Primary key**：`(tenant_id, session_key)`。

**Conflict rules**：更低水位拒绝；相同水位同 digest 幂等；相同水位异 digest=`summary_conflict`；更高且不超过 Event watermark 才可提交。

## 5. ArtifactMetadata

| Field | Type | Rules |
|---|---|---|
| tenant_id | string | 主键组成 |
| artifact_id | string | 主键组成 |
| storage_ref | string | tenant-scoped immutable ref；只在发布后可读 |
| content_digest | string | 上传内容 digest |
| byte_size | integer | `>=0` |
| media_type | string | 白名单/规范化 |
| status | enum | `PUBLISHED`、`DELETED`；暂存事实由对象端/上传记录表达 |
| version | integer | metadata CAS version |
| created_at/updated_at | datetime | UTC |

**Primary key**：`(tenant_id, artifact_id)`。

**Publication invariant**：ObjectStore 写 temporary key → digest 验证 → PostgreSQL metadata CAS+Audit。CAS 失败时原 `storage_ref` 不变。

## 6. ArtifactUpload

用于对象存储非事务副作用恢复和孤儿清理。

| Field | Type | Rules |
|---|---|---|
| tenant_id, upload_id | string | 主键 |
| artifact_id | string | 目标 Artifact |
| temp_ref_digest | string | 不记录可泄露的完整对象 key |
| expected_digest | string | 上传期望 digest |
| status | enum | `STAGED`, `VERIFIED`, `PUBLISHED`, `ORPHANED`, `DELETED` |
| expected_metadata_version | integer? | 发布 CAS 条件 |
| expires_at | datetime | 到期后且未被引用才可 GC |
| generation | integer | 清理/恢复 fencing |
| timestamps | datetime | UTC |

## 7. KnowledgeDocument

| Field | Type | Rules |
|---|---|---|
| tenant_id, document_id | string | 主键 |
| metadata | JSON | 规范化、不得含 Secret |
| content_digest | string | 文档/分块事实 digest |
| embedding_ref | string? | 供应商无关 opaque reference |
| index_status | enum | `PENDING_INDEX`, `INDEXED`, `FAILED_RETRYABLE`, `DELETED` |
| version | integer | CAS version |
| updated_at | datetime | UTC |

**Search invariant**：只有 `INDEXED` 可返回；VectorStore 必须在相似度计算和候选返回前应用 tenant filter，否则返回 `tenant_filter_unsupported`。

## 8. MigrationState

| Field | Type | Rules |
|---|---|---|
| tenant_id, stream | string | 主键；故障域边界 |
| state | enum | 见迁移状态机 |
| authority | enum | `REDIS_LEGACY` 或 `POSTGRES` |
| source_watermark | integer? | 停写后锁定，不再变化 |
| copied_watermark | integer | 可幂等推进的 checkpoint |
| source_digest / target_digest | string? | 校验结果 |
| rollback_eligible | bool | 首笔 PG 新写前为 true |
| generation | integer | 状态转换 fencing |
| lease_owner_digest | string? | 不记录原节点敏感信息 |
| failure_code | string? | 稳定错误，不含供应商详情 |
| created_at/updated_at | datetime | UTC |

**Primary key**：`(tenant_id, stream)`。

**States**：

```text
PLANNED
PAUSING
SNAPSHOT_LOCKED
COPYING
VERIFYING
CUTOVER_READY
ACTIVE_ROLLBACK_ELIGIBLE
ACTIVE_FORWARD_ONLY
ROLLED_BACK
FORWARD_REPAIR_REQUIRED
```

所有转换使用 `(expected_state, expected_generation)` CAS。一个 tenant/stream 停写不得影响其他 scope。

## 9. DataAuditRecord

沿用现有不可变 Audit 模型并扩展以下安全字段：

| Field | Type | Rules |
|---|---|---|
| tenant_id | string | 必需 |
| audit_id | UUID | 唯一 |
| operation | enum | APPEND_EVENT、READ_CONTENT、PUT_MEMORY、PUT_SUMMARY、PUBLISH_ARTIFACT、INDEX/SEARCH_KNOWLEDGE、MIGRATION_TRANSITION 等 |
| resource_type | string | 低基数 |
| resource_key_digest | string | 不保存原 key |
| content_digest | string? | 不保存原文 |
| from_state / to_state | string? | 状态转换 |
| result | enum | COMMITTED、REPLAYED、REJECTED、FAILED |
| reason_code | string? | 稳定错误 |
| trace fields | UUID | 三类 trace |
| created_at | datetime | UTC，不可变 |

Mutation Audit 与对应 PostgreSQL 写同事务。原文读取必须在返回内容前形成正式 access Audit。

## 10. OperationalEvent

Audit 不可用时允许的本地最小诊断，不属于业务事实，不可用于恢复或合规证明。

允许字段仅为：timestamp、component、operation、stable error_type、retryable、trace digest。禁止 tenant/user/session/message/对象 key 原文和业务内容。

## Relationships

```text
Tenant
 ├─ SessionStream 1 ── * SessionEvent
 │       └─ 0..1 SummaryRecord
 ├─ * MemoryRecord
 ├─ * ArtifactMetadata 1 ── * ArtifactUpload
 ├─ * KnowledgeDocument
 ├─ * MigrationState (per stream)
 └─ * DataAuditRecord
```

## Validation and Invariants

1. 所有查询首先匹配 tenant，再匹配资源键；不存在“先全局查再过滤”。
2. 所有写入携带 expected version 或 fence；旧节点不能无条件覆盖。
3. Event/watermark/Audit 原子；Summary 不超过 Event watermark。
4. Memory/Summary/metadata 的 version 和 digest 在所有节点可见且一致。
5. Artifact 内容只有权威 metadata 发布后可读；GC 不删除任何被引用对象。
6. Knowledge backend 缺少 pre-filter 能力即不执行搜索。
7. 迁移 authority 同一时刻只能有一个；切换后首笔写永久关闭安全回滚。
8. Audit 故障时没有新的业务写、迁移或原文返回。

## Schema Migration

新增 `006_memory_summary.sql`，只做前向、可重复 schema 变更：新增 Session Event/watermark、Memory、Summary、Artifact metadata/upload、Knowledge metadata、MigrationState 表及 tenant-scoped 唯一索引；扩展 Audit 的可空数据操作字段。不得删除前序阶段表或清空现有数据。Schema gate 从 5 升至 6，旧 v5 数据库必须原地升级并通过回归。

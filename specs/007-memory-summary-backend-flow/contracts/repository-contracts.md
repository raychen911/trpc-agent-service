# Repository 与 Adapter 契约

本文件定义第七阶段可由测试直接验证的供应商无关契约。示例签名表达语义，不强制具体 Python 类名；实施时必须使用异步 `Protocol` 并复用同一套 InMemory/PostgreSQL contract suite。

## 通用请求上下文

所有公开方法必须接收 `DataScope`：

```python
DataScope(
    tenant_id: str,
    agent_id: str | None,
    trace_id: UUID,
    owner_trace_id: UUID,
    execution_trace_id: UUID,
    fence_generation: int | None,
)
```

- scope 只能由可信 Channel Binding/Gateway/Governance 创建。
- tenant 缺失或不匹配返回 `tenant_scope_invalid`，不得尝试全局查找。
- mutation 和 raw-content read 必须通过 `AuditedDataAccess`；底层 adapter 不直接暴露给 Gateway/Worker。

## DataUnitOfWork

```python
class DataUnitOfWork(Protocol):
    events: SessionEventRepository
    memories: MemoryRepository
    summaries: SummaryRepository
    audits: AuditRepository
    migrations: MigrationRepository

    async def __aenter__(self) -> DataUnitOfWork: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
```

同一业务事务内的 Repository 必须共享同一 PostgreSQL connection/transaction。任一 Audit append 失败不得 commit。

## SessionEventRepository

```python
async def append(scope, event, *, expected_watermark: int) -> AppendResult
async def list_metadata(scope, session_key, *, after_sequence=0, limit=100) -> list[EventMetadata]
async def read_content(scope, session_key, *, after_sequence=0, limit=100) -> list[SessionEvent]
async def get_watermark(scope, session_key) -> int
```

`AppendResult.outcome` 只允许 `CREATED` 或 `REPLAYED`。

- event_id 相同且 digest 相同：返回原记录，outcome=`REPLAYED`。
- event_id 相同但 digest 不同：`idempotency_conflict`。
- 新 Event sequence 不是当前 watermark+1：`sequence_gap`，不写入、不缓冲。
- CREATED 必须与 watermark 更新及 mutation Audit 同事务。
- response lost 后重试必须得到同一 event/version/watermark。

## MemoryRepository

```python
async def compare_and_set(scope, record, *, expected_version: int | None) -> WriteResult
async def get_metadata(scope, namespace, key) -> MemoryMetadata | None
async def read_content(scope, namespace, key) -> MemoryRecord | None
```

- create 使用 `expected_version=None/0`；update 必须等于当前 version。
- 内容规范化字节数超限返回 `content_too_large`，且 Artifact 调用次数为 0。
- 相同 version/digest 的安全重试返回原结果；同 expected version 的不同内容只有一个提交成功。
- 正式写入与 Audit 同事务；读取原文必须有 access Audit。

## SummaryRepository

```python
async def compare_and_set(scope, summary, *, expected_version: int | None) -> WriteResult
async def get_metadata(scope, session_key) -> SummaryMetadata | None
async def read_content(scope, session_key) -> SummaryRecord | None
```

- proposed watermark 不得超过 Event confirmed watermark。
- 小于当前 watermark：`version_conflict`。
- 等于当前 watermark 且 digest 相同：`REPLAYED`；digest 不同：`summary_conflict`。
- 更高水位使用 version CAS 提交 Summary+Audit；任何失败保留旧 Summary。

## ArtifactRepository 与 ObjectStorePort

```python
class ObjectStorePort(Protocol):
    async def put_temporary(tenant_scope, upload_id, content: bytes) -> TemporaryObject: ...
    async def read(tenant_scope, storage_ref) -> bytes: ...
    async def delete_temporary(tenant_scope, storage_ref) -> DeleteResult: ...

class ArtifactRepository(Protocol):
    async def publish(scope, request, *, expected_version: int | None) -> ArtifactMetadata: ...
    async def get_metadata(scope, artifact_id) -> ArtifactMetadata | None: ...
    async def read_content(scope, artifact_id) -> bytes | None: ...
    async def collect_orphans(scope, *, before, limit) -> GcResult: ...
```

- publish 必须先通过 Audit/权威 PostgreSQL readiness gate；初始 gate 失败时 `put_temporary` 调用次数必须为 0。
- `put_temporary` 的 key 必须 tenant-scoped、不可变且不可由调用方直接指定最终 ref。
- publish 在对象 digest 验证后用 SQL metadata CAS+Audit 发布；失败时旧引用不变。
- read 只能解析当前 PUBLISHED metadata，不接受外部直接传 storage_ref。
- GC 只删除 TTL 到期、未被任一 metadata 引用、fence 有效的 temporary object；重复调用幂等。
- 本阶段 ObjectStorePort 只有 deterministic fake；测试名称/报告必须注明 fixture。

## KnowledgeRepository 与 VectorStorePort

```python
class VectorStorePort(Protocol):
    supports_tenant_prefilter: bool
    async def upsert(tenant_scope, document_id, digest, vector) -> None: ...
    async def search(tenant_scope, query_vector, limit) -> list[VectorHit]: ...

class KnowledgeRepository(Protocol):
    async def stage(scope, document, *, expected_version) -> KnowledgeDocument: ...
    async def mark_indexed(scope, document_id, digest, *, expected_version) -> KnowledgeDocument: ...
    async def search(scope, query, *, limit=10) -> list[KnowledgeHit]: ...
```

- stage/index 必须先通过 Audit/权威 PostgreSQL readiness gate；初始 gate 失败时 VectorStore 调用次数必须为 0。
- `supports_tenant_prefilter=False` 时 search 在调用 VectorStore 前返回 `tenant_filter_unsupported`。
- 底层查询必须收到 tenant filter；不得查全局候选后在 Python 中过滤。
- 只有 `INDEXED` 且 metadata digest 与 hit digest 一致的文档可返回。
- 向量 upsert 按 tenant/document/digest 幂等；本阶段仅 deterministic fake。

## AuditRepository

```python
async def append_mutation(scope, audit_record, *, transaction) -> AuditRecord
async def append_access(scope, audit_record) -> AuditRecord
async def list_metadata(scope, filters) -> list[AuditMetadata]
```

- mutation Audit 与 PG 状态变更共享事务。
- access Audit 成功前不得返回原文。
- Audit detail 只保存 digest、版本、大小、状态和 reason code；不保存原文、Secret 或完整对象 ref。
- Audit unavailable 时写入、迁移、raw read 返回 `audit_unavailable`；metadata 诊断仍要 tenant-scoped。

## AuditedDataAccess

Gateway、Worker、Runner adapter 只调用此 Facade：

```python
async def append_event(...)
async def put_memory(...)
async def put_summary(...)
async def publish_artifact(...)
async def index_knowledge(...)
async def read_*_content(...)
async def get_*_metadata(...)
```

Facade 负责规范化、大小限制、UoW、Audit gate、错误翻译和低基数指标；不得在数据库失败时降级写本地内存或 Redis。

## Contract Suite Matrix

| Behavior | InMemory | PostgreSQL | Object fake | Vector fake |
|---|---:|---:|---:|---:|
| tenant isolation | MUST | MUST | MUST | MUST |
| immutable result / stable digest | MUST | MUST | MUST | MUST |
| async API | MUST | MUST | MUST | MUST |
| event atomicity/watermark | MUST | MUST + fault injection | N/A | N/A |
| version CAS | MUST | MUST | metadata path | metadata path |
| audit fail closed | MUST | MUST | via facade | via facade |
| cross-node visibility | N/A | MUST | deterministic shared fixture only | deterministic shared fixture only |
| tenant pre-filter | N/A | metadata only | N/A | MUST |

共享后端未启动可用 `shared_backend` marker 明确 skip；最终阶段验收不得以 skip 代替 PostgreSQL PASS。

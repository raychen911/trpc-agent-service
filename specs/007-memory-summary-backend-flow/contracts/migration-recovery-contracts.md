# 迁移、恢复与错误契约

## MigrationRepository

```python
async def get(scope, stream) -> MigrationState
async def transition(scope, stream, *, expected_state, expected_generation, target_state, fields) -> MigrationState
async def checkpoint(scope, stream, *, expected_generation, copied_watermark, target_digest) -> MigrationState
async def activate(scope, stream, *, expected_generation, verified_watermark, verified_digest) -> MigrationState
async def mark_first_authoritative_write(scope, stream, *, expected_generation, transaction) -> MigrationState
```

### 状态转换

```text
PLANNED → PAUSING → SNAPSHOT_LOCKED → COPYING → VERIFYING → CUTOVER_READY
CUTOVER_READY → ACTIVE_ROLLBACK_ELIGIBLE → ACTIVE_FORWARD_ONLY
pre-cutover failure → RETRYABLE or ROLLED_BACK
post-first-write fault → FORWARD_REPAIR_REQUIRED
```

- 状态按 `(tenant_id, stream)` 隔离；迁移租约与 generation 防止两个节点同时推进。
- `PAUSING` 之后该 scope 的新写返回 `migration_write_paused`，不触碰 Redis/PG 业务数据。
- `SNAPSHOT_LOCKED` 后 source watermark 不可变化；复制到该水位即可完成一致性验证。
- `activate` 必须验证 source/target count、watermark 和 digest 一致，再 CAS authority 为 PostgreSQL。
- 第一笔 cutover 后 PostgreSQL 新写与 `rollback_eligible=false` 同事务。
- `ACTIVE_FORWARD_ONLY` 不存在回到 `REDIS_LEGACY` 的合法转换。

## Recovery Contract

恢复器只能推进持久化状态允许的下一步，不能通过重新运行 Agent、重新生成 Summary 或无条件覆盖数据来猜测结果。

| Failure point | Observable state | Recovery |
|---|---|---|
| Event transaction before commit | 无 Event/watermark/Audit | 原请求按 stable id 重试 |
| Event commit, response lost | 三者均存在 | 重试返回 REPLAYED |
| Memory/Summary CAS conflict | 旧版本仍有效 | 重新读取后由调用方决定重算 |
| Artifact upload before publish | temporary object，无 metadata 引用 | TTL GC 或同 upload_id 继续 |
| Artifact metadata commit, response lost | PUBLISHED metadata | 按 artifact_id/digest 返回原结果 |
| Knowledge vector upsert failed | PENDING_INDEX | 同 document/digest 幂等补索引 |
| Migration copy interrupted | persisted checkpoint | 新 owner/fence 从 checkpoint 继续 |
| Cutover before first new write failed | rollback eligible | 可 CAS 回 legacy authority |
| First PG write after cutover committed | rollback false | 只能进入 forward repair |
| Audit unavailable | 无可审计授权 | 不开始/回滚/不返回原文 |

## Stable Error Envelope

```json
{
  "error": "sequence_gap",
  "retryable": true,
  "trace_id": "<uuid>",
  "details": null
}
```

允许的稳定 code：`tenant_scope_invalid`、`sequence_gap`、`idempotency_conflict`、`version_conflict`、`summary_conflict`、`content_too_large`、`digest_mismatch`、`tenant_filter_unsupported`、`migration_write_paused`、`migration_conflict`、`forward_repair_required`、`audit_unavailable`、`state_backend_unavailable`、`stale_fence`。

`details` 不得包含 SQL、Redis key、对象 ref、tenant/user/session/message 原文、业务内容或供应商异常文本。内部异常映射到 stable code 后，仅以脱敏 operational event 记录。

## Required Fault Injection Points

1. Event insert 前、insert 后、watermark 后、Audit 前、commit 后响应返回前。
2. Memory/Summary row lock、CAS、Audit、commit。
3. Object put 前后、digest verify、metadata CAS、Audit、GC delete。
4. Vector upsert 前后、metadata mark-indexed、search capability check。
5. Migration pause、source watermark lock、copy checkpoint、verify、authority CAS、首笔 PG write。
6. Audit connect/append/commit 失败。

每个点都必须断言：租户不串、状态不部分提交、旧 fence 不生效、重试不重复副作用、错误不泄露后端细节。

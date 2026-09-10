# Implementation Plan: 完整数据抽象与同步策略

**Git Branch**: `feature/luwenjie` | **Feature**: `007-memory-summary-backend-flow` | **Date**: 2026-09-10 | **Spec**: [spec.md](./spec.md)

**Input**: `/specs/007-memory-summary-backend-flow/spec.md`

**Decision Record**: [clarification-decisions.md](./clarification-decisions.md)

## Summary

在第二、第三、第五和第六阶段既有的多租户消息、共享状态、双 IM 和治理链路之上，建立 Session Event、Memory、Summary、Artifact、Knowledge 与 Audit 的统一异步数据访问边界。PostgreSQL 是 Event、watermark、Memory、Summary、Audit 和迁移状态的唯一持久权威；Redis 继续只承担锁、幂等和缓存协调；对象存储与向量库本阶段只提供供应商无关端口和确定性替身。

五项人工决策确定了本阶段的一致性路线：Event、watermark、Audit 同事务；乱序 Event 不落库；Summary 同水位以 digest 判定幂等或冲突；Redis→PostgreSQL 采用 tenant/stream 短暂停写切换；Memory、Artifact、Knowledge 按数据形态分离；Audit 不可用时业务写入、迁移和原文读取全部 fail closed。

## Implementation Baseline

- 第二阶段已提供可信租户上下文、tenant-scoped Session、消息幂等、Gateway、Worker、Runner、统一回复和 Audit。
- 第三阶段已提供 Redis/PostgreSQL 共享后端、双 Worker、租约、generation、fencing、跨节点恢复与无 sticky session 运行方式。
- 第五阶段已提供飞书/企业微信正式 Channel Adapter、真实身份键、交付状态和 Adapter 接管。
- 第六阶段已提供租户治理、内容策略、主体授权、危险操作确认、预算与治理审计。
- 当前 `storage/data_models.py`、`storage/memory.py`、`storage/sync.py` 及少量测试只是早期骨架：它们尚未实现 digest 冲突、Audit 原子提交、严格 watermark、迁移权威状态和真实 PostgreSQL 数据链路，必须按本计划重构，不作为验收完成证据。
- 继续固定使用 `trpc-agent-py==1.1.19`；平台不复制官方 Runner、Session、Memory 或 Knowledge 内部实现。

## Technical Context

**Language/Version**: Python 3.12（项目约束 `>=3.12,<3.13`）

**Primary Dependencies**: trpc-agent-py 1.1.19、Pydantic 2.13.5、SQLAlchemy 2.0.52、asyncpg 0.31.0、redis 8.1.0、pytest/pytest-asyncio

**Storage**: PostgreSQL 保存权威 Event、watermark、Memory、Summary、Artifact/Knowledge metadata、Audit 和迁移状态；Redis 保存锁、消息幂等和非权威缓存；对象/向量端口使用确定性替身

**Testing**: Unit、共享 Repository contract suite、Redis/PostgreSQL integration、双节点 E2E、迁移故障注入、安全扫描与全量回归

**Target Platform**: Windows/Linux 本地开发；真实共享验收使用现有 `deploy/local-shared/compose.yaml`

**Project Type**: Python 异步服务，保持 Channel Adapter → Gateway → Governance → Worker → 官方 Runner 的单一路径

**Performance Goals**: InMemory 参考路径不含 Agent/外部网络的 p95 < 50ms；跨节点测试以正确性为主，并记录 Event append、Memory CAS、Summary update 和迁移耗时

**Constraints**: 强租户隔离、严格连续 Event、无隐式 last-write-wins、无双权威、无本地降级、Audit 故障分级拒绝、Worker 无业务状态、不得记录原文或 Secret

**Scale/Scope**: 至少 2 个租户、2 个 Worker、相同 key/session 的跨租户测试、并发 Event/Summary/Memory 冲突、按 tenant/stream 的迁移与接管

## Constitution Check

*GATE: Phase 0 前检查，并在 Phase 1 设计完成后复查。*

### Pre-Research Gate

| Principle | Result | Evidence |
|---|---|---|
| I. Framework-First | PASS | 继续使用官方 Runner/Event/Session/Memory/Knowledge 能力；平台只定义多租户 Repository、事务、迁移和适配边界 |
| II. Tenant Isolation | PASS | 每个实体、主键、查询、缓存、对象 key、向量查询和 Audit 都显式携带可信 tenant scope |
| III. Stateless Workers | PASS | 所有可接管状态进入 PostgreSQL/Redis；Worker 仅保留请求内对象 |
| IV. Contract-First | PASS | InMemory 与 PostgreSQL 复用同一契约测试；对象/向量端口以确定性替身证明可替换性 |
| V. Security by Default | PASS | 未知租户、过期 fence、后端不可用、Audit 不可用或不支持 tenant pre-filter 均默认拒绝 |
| VI. Observability | PASS | 三类 trace、digest、水位、状态转换和稳定错误进入 Audit/低基数指标，不记录原始内容 |
| VII. Spec-Driven Evidence | PASS | DEC-001～005 映射到设计、契约、状态机、恢复及可重复测试证据 |

**Gate result**: PASS。无宪法例外、无未解决澄清。

## Framework Reuse and Platform Ownership

### 直接复用官方 tRPC-Agent

- 复用官方 `Runner`、Event、Session、Memory、Knowledge 和上下文对象的公开接口，不复制 Agent 执行循环。
- 通过 Adapter 将平台的 tenant-scoped 领域对象映射到官方公开对象；官方对象不得携带供应商数据库句柄。
- 使用确定性 Runner/Embedding fixture 验证数据流，不接真实模型 API。

### 复用现有平台能力

- Channel Binding 与治理层继续提供不可由外部覆盖的 tenant、Agent、Session 和 principal scope。
- Redis 消息幂等、Session/Adapter 租约、generation、fencing 和恢复协调保持不变。
- PostgreSQL 数据库生命周期、schema version gate、Audit、trace 和 shared composition 继续作为统一基础设施。
- 现有统一错误翻译层负责把 Repository 异常映射为安全响应，不暴露 SQL、Redis、对象 key 或供应商细节。

### 本阶段新增的平台能力

- 版本化、带 digest 的 Event/Memory/Summary/Artifact/Knowledge 领域模型。
- PostgreSQL Data Unit of Work 与 Event、Memory、Summary、Audit、migration Repository。
- ObjectStorePort、VectorStorePort 及显式标注为 fixture 的确定性替身。
- tenant/stream authority、迁移水位、校验摘要、rollback eligibility 与 forward-repair 状态机。
- Artifact 暂存发布/孤儿清理及 Knowledge 安全索引状态。

## Architecture

```text
Feishu / WeCom / HTTP
          │ trusted tenant/session context
          ▼
 Gateway + Governance ───────► Agent Worker ───────► official tRPC-Agent Runner
          │                                             │
          └────────────── Data Access Facade ◄──────────┘
                              │
               ┌──────────────┼────────────────┐
               ▼              ▼                ▼
      PostgreSQL UoW     Redis coordination  Adapter ports
      - Event/watermark  - idempotency       - ObjectStore fake
      - Memory/Summary   - locks/cache       - VectorStore fake
      - metadata/Audit   (never authority)   (not production proof)
      - migration state
```

核心模块只能依赖领域模型和 Protocol。PostgreSQL Adapter 可使用 SQLAlchemy/asyncpg；Redis、对象和向量供应商对象不得越过 `storage` 包。

## DEC-001～DEC-005 Design Mapping

| Decision | Architecture / Repository | Transaction / State | Errors & Recovery | Required tests |
|---|---|---|---|---|
| DEC-001 | PostgreSQL `DataUnitOfWork` 是 Event/watermark/Audit 唯一提交入口；Redis 无写回权威能力 | Event insert + watermark CAS + mutation Audit 同事务提交 | `audit_unavailable`/`state_backend_unavailable` 整体回滚；响应丢失按 event_id/digest读取原结果 | 原子回滚、提交后重放、双节点连续水位、Redis 命中不能绕过 PG |
| DEC-002 | EventRepository 强制 `current+1`；SummaryRepository 强制 watermark/digest 规则 | Event 和 Summary 分别使用唯一约束与行锁/CAS 状态机 | `sequence_gap` 可重试但不缓冲；`idempotency_conflict`、`summary_conflict` 保留旧状态 | gap 前后重试、同 ID 异 digest、同水位异 digest、并发唯一胜者 |
| DEC-003 | MigrationCoordinator 使用 tenant/stream authority Repository 与既有 Redis lease/fence | `PAUSING → SNAPSHOT_LOCKED → COPYING → VERIFYING → CUTOVER → ACTIVE`；首笔新写关闭回滚 | `migration_write_paused`、`migration_conflict`、`forward_repair_required`；旧 generation 不得切换 | 两租户互不阻塞、各提交点中断、切换前回滚、首写后禁止回滚 |
| DEC-004 | Memory 在 PG；Artifact metadata 在 PG+ObjectStorePort；Knowledge metadata 在 PG+VectorStorePort | Memory CAS；Artifact temp→digest→metadata CAS；Knowledge pending→indexed | `content_too_large`、`digest_mismatch`、`version_conflict`、`tenant_filter_unsupported` | 跨租户同 key、旧引用保留、孤儿只清一次、查询前 tenant filter |
| DEC-005 | 真实 PG/Redis；对象/向量仅契约替身；Audit 是敏感操作前置依赖 | 写入/迁移与正式 Audit 在同一可证明边界；原文读取必须先形成访问 Audit | Audit 故障拒绝写入、迁移、原文读取；只允许 metadata/digest/status 诊断并产生本地 operational event | 真实共享 suite、明确 skip、审计断连零业务调用、敏感扫描零命中 |

## Repository and Unit-of-Work Contracts

- 所有方法异步，tenant scope 为必需参数且来自可信上下文；不接受可选 tenant 或全局扫描。
- `SessionEventRepository.append()` 返回 `CREATED` 或 `REPLAYED`；相同 event_id/digest 返回原记录，不新增成功 Audit；不同 digest 冲突。
- `SessionEventRepository.read_content()` 与所有原文读取均要求 Audit gate；`get_metadata()` 可只返回 digest、水位、版本和状态。
- `MemoryRepository.compare_and_set()` 使用 expected_version；内容规范化后计算 digest，超过大小限制在进入写事务前拒绝。
- `SummaryRepository.compare_and_set()` 必须验证目标 watermark 不超过已确认 Event watermark，且处理同水位 digest 规则。
- `ArtifactRepository.publish()` 编排对象暂存和 SQL metadata CAS；ObjectStorePort 只处理 tenant-scoped immutable key，不决定租户授权。
- `KnowledgeRepository.search()` 必须把 tenant filter 作为底层查询条件；VectorStorePort 必须声明 `supports_tenant_prefilter=True` 才能执行。
- `MigrationRepository` 以 expected_state、generation、source_watermark 做 CAS；任何 Worker 本地字段都不是恢复事实。
- `DataUnitOfWork` 只覆盖同一 PostgreSQL 数据库内的原子边界；对象/向量副作用使用可恢复状态，不伪装为分布式事务。

详细签名与不变量见 [contracts/repository-contracts.md](./contracts/repository-contracts.md)。

## Transaction Boundaries

1. **Event append**：锁定 `(tenant_id, session_key)` watermark；先判 event_id/digest，再校验 sequence；插入 Event、更新 watermark、写 mutation Audit，单事务提交。
2. **Memory CAS**：规范化 JSON 和 digest 在事务外计算；事务内按 expected_version 更新/插入并写 Audit。Audit 失败则内容和版本均回滚。
3. **Summary CAS**：同事务锁定 Event watermark 与当前 Summary；验证覆盖范围和 digest；更新 Summary 并写 Audit，拒绝时保留原记录。
4. **Artifact publish**：先通过 Audit/权威 PostgreSQL readiness gate，成功后才写 tenant-scoped temporary immutable object并校验 digest；随后以 PostgreSQL 事务 CAS 发布 metadata 并写正式 Audit。gate 失败时 ObjectStore 调用数为 0；gate 后故障时旧引用不变，临时对象进入 TTL 清理；只有 SQL metadata 指向的对象可读。
5. **Knowledge index**：先通过 Audit/权威 PostgreSQL readiness gate，再由 PostgreSQL 写 `PENDING_INDEX` metadata 与 Audit；替身向量端按 tenant upsert，成功后 CAS 为 `INDEXED` 并写 Audit。初始 gate 失败时 VectorStore 调用数为 0；后续失败项保持不可搜索并可幂等重试。
6. **Raw content read**：在返回 Event/Memory/Summary/Artifact/Knowledge 原文前写 access Audit；外部对象读取后若最终审计确认失败，不得向调用方返回已取得内容。
7. **Migration cutover**：迁移状态、authority 指针和校验摘要在 PostgreSQL CAS；首笔 cutover 后新写与 `rollback_eligible=false` 在同一事务完成。

## Event and Summary State Machines

### Session Event

```text
CANDIDATE
  ├─ same event_id + same digest ─► REPLAYED(existing)
  ├─ same event_id + other digest ─► IDEMPOTENCY_CONFLICT
  ├─ sequence != watermark + 1 ────► SEQUENCE_GAP (no write)
  └─ valid + audit available ──────► COMMITTED(event + watermark + audit)
```

`sequence_gap` 同时覆盖跳跃和回退；Repository 不建立 pending buffer。事务提交后响应丢失，重试会走 `REPLAYED`。

### Summary

```text
PROPOSED
  ├─ watermark > confirmed event watermark ─► SEQUENCE_GAP
  ├─ watermark < current ────────────────────► VERSION_CONFLICT
  ├─ same watermark + same digest ──────────► REPLAYED(existing)
  ├─ same watermark + other digest ─────────► SUMMARY_CONFLICT
  └─ higher valid watermark + audit ─────────► COMMITTED
```

Summary 永不宣称覆盖尚未提交的 Event，重建任务从已提交 Summary watermark 之后读取连续事件。

## Migration State Machine

```text
PLANNED → PAUSING → SNAPSHOT_LOCKED → COPYING → VERIFYING → CUTOVER_READY
   │          │             │             │           │
   └──────── failure before cutover ──────┴──────────► ROLLED_BACK / RETRYABLE

CUTOVER_READY → ACTIVE_ROLLBACK_ELIGIBLE
                   ├─ no post-cutover write + rollback ─► LEGACY_ACTIVE
                   └─ first PG write (same tx) ─────────► ACTIVE_FORWARD_ONLY
                                                           └─ fault ─► FORWARD_REPAIR_REQUIRED
```

- `PAUSING` 起该 tenant/stream 的业务写入返回 `migration_write_paused`，其他 tenant/stream 正常服务。
- `SNAPSHOT_LOCKED` 保存 Redis source watermark、源摘要和 fence generation；复制可从最后确认 checkpoint 幂等继续。
- `VERIFYING` 必须比较记录数、版本、水位和规范化 digest；任何差异都不能切换 authority。
- `ACTIVE_FORWARD_ONLY` 后禁止把 Redis 恢复成写入权威；故障只能暂停并补齐 PostgreSQL。

## Error Semantics

| Stable code | Retryable | Side effect | Meaning |
|---|---:|---|---|
| `tenant_scope_invalid` | No | none | tenant 缺失、不可信或资源 scope 不匹配 |
| `sequence_gap` | Yes | none | Event 非连续或 Summary 超过已确认 Event 水位 |
| `idempotency_conflict` | No | none | 相同 event_id 的 digest 不同 |
| `version_conflict` | Re-read | none | Memory/Summary/metadata CAS 版本过期 |
| `summary_conflict` | No | none | Summary 同 watermark 但 digest 不同 |
| `content_too_large` | No | none | Memory 超过上限，调用方需显式改用 Artifact |
| `digest_mismatch` | No | temp only | Artifact 或迁移校验摘要不一致 |
| `tenant_filter_unsupported` | No | none | Knowledge 后端无法查询前租户过滤 |
| `migration_write_paused` | Yes | none | 当前 tenant/stream 正在一致性迁移 |
| `migration_conflict` | Re-inspect | none | authority/state/fence CAS 失败 |
| `forward_repair_required` | Operator action | none/new writes paused | 已越过安全回滚点，只能前向修复 |
| `audit_unavailable` | Later | none committed/returned | 不允许未审计写入、迁移或原文读取 |
| `state_backend_unavailable` | Later | none claimed | 权威 PostgreSQL 不可用；Redis 不得旁路 |
| `stale_fence` | Re-acquire | none | 旧节点或旧 generation 的写入被拒绝 |

错误对外只返回稳定 code、retryable 和 trace_id，不包含 SQL、对象 key、原文、tenant 明文或供应商响应。

## Cross-Node Failure Recovery

- **事务前/中断**：PostgreSQL 回滚，不产生 Event、watermark、Memory/Summary 或正式 Audit 的部分状态。
- **提交后响应丢失**：按 tenant + stable id + digest 查询原结果，不重做 Agent、Summary 或 Artifact 发布。
- **旧节点恢复**：所有写操作携带 generation/fence；旧 fence 即使持有缓存也不能推进权威状态。
- **Artifact 中断**：metadata 未发布时对象不可读；TTL 清理器仅删除未被 metadata 引用且超过阈值的临时对象，删除幂等。
- **Knowledge 中断**：`PENDING_INDEX` 不进入查询；恢复器按 document_id/digest 幂等补索引，不返回跨租户候选。
- **迁移中断**：从持久化 state/checkpoint 继续；切换前可回滚，首笔新写后只允许 forward repair。
- **Audit 故障**：业务写入和迁移不开始或同事务回滚；原文读取不返回。metadata/digest/status 诊断只记录最小本地 operational event。

## Project Structure

### Documentation

```text
specs/007-memory-summary-backend-flow/
├── spec.md
├── clarification-decisions.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── repository-contracts.md
│   └── migration-recovery-contracts.md
└── tasks.md             # 由后续 $speckit-tasks 重新生成
```

### Source Code

```text
trpc_service/
├── storage/
│   ├── contracts.py
│   ├── data_models.py
│   ├── memory.py
│   ├── data_service.py
│   ├── sync.py
│   ├── object_store.py
│   ├── vector_store.py
│   ├── shared.py
│   └── postgres/
│       ├── database.py
│       ├── models.py
│       ├── data_repositories.py
│       └── migrations/006_memory_summary.sql
├── gateway/service.py
├── worker/service.py
├── recovery/reconciler.py
├── audit/models.py
└── metrics/

tests/
├── unit/data/
├── contract/data/
├── integration/data/
├── e2e/data/
└── security/
```

**Structure Decision**: 延续单一 Python 包。数据领域与端口继续位于 `storage`，真实 SQL 细节只在 `storage/postgres`；Gateway/Worker 只做组合接入。现有顶层 Data 测试在实施时迁入分层目录，保留 Git 历史，不建立第二套服务。

## Test Strategy

严格测试先行：每项行为先记录 RED 命令和预期失败，再实现并运行相同命令得到 GREEN，最后执行相关回归。

- **Unit**：规范化 digest、模型冻结、大小限制、稳定错误、Event/Memory/Summary/Artifact/Knowledge/迁移状态机。
- **Contract**：同一 suite 验证 InMemory 与 PostgreSQL；对象和向量替身验证端口、tenant key、pre-filter capability 和故障语义。
- **Transaction integration**：故障注入 Event insert、watermark、Audit 任一点，证明全回滚；Memory/Summary CAS 与 access Audit 同理。
- **Concurrency integration**：两个节点并发追加相同/不同 Event、更新 Memory/Summary，只允许契约规定的胜者。
- **Migration integration**：tenant/stream 停写、水位锁定、digest 校验、切换、切换前回滚、首写关闭回滚及 forward repair。
- **Content backend tests**：Artifact 上传/CAS/GC；Knowledge 查询前 filter；均明确使用 fixture，不宣称真实后端。
- **Audit outage tests**：写入、迁移、原文读取的底层业务调用数为 0 或事务全回滚；metadata 诊断不泄露原文。
- **E2E**：双 Worker 在真实 Redis/PostgreSQL 下完成 Event→Memory→Summary→Runner 连续会话和双租户隔离。
- **Regression/Security**：第二、三、五、六阶段及全量 pytest；源码、diff、日志、Audit 样本原文/Secret 扫描 0 命中。

共享后端缺失只允许通过 `shared_backend` marker 明确 skip；最终验收必须在 Docker profile 下产生真实 PASS 记录。

## Delivery Phases

1. **Phase 0 — Research**：固定权威存储、UoW、状态机、迁移、Audit 和替身边界。
2. **Phase 1 — Contracts & Models**：先写 RED contract/unit tests，再实现领域模型、错误、Protocol 和 InMemory oracle。
3. **Phase 2 — PostgreSQL Authority**：migration 006、真实 Event/Memory/Summary/Audit UoW、共享契约 GREEN。
4. **Phase 3 — Content Adapters**：Artifact/Knowledge metadata、对象/向量 fixture、发布/索引/清理恢复。
5. **Phase 4 — Migration**：tenant/stream authority 与 Redis legacy→PG cutover/rollback/forward repair。
6. **Phase 5 — Composition**：接入 Gateway/Worker/Runner，验证跨节点可见性且 Worker 无状态。
7. **Phase 6 — Evidence**：故障矩阵、性能、敏感扫描、真实 Docker 验收、全量回归和文档。

## Post-Design Constitution Check

| Principle | Result | Design evidence |
|---|---|---|
| Framework-First | PASS | 官方 Runner/Session/Memory/Knowledge 继续作为上游能力，平台仅加 tenant/data adapter |
| Tenant Isolation | PASS | SQL key、对象 key、向量 filter、迁移 state 与 Audit 均以可信 tenant 开头 |
| Stateless Workers | PASS | PostgreSQL/Redis 保存全部权威与恢复事实；无 sticky session 或进程内旁路 |
| Contract-First | PASS | InMemory/PG 同套契约，对象/向量端口以确定性 fixture 证明替换边界 |
| Security by Default | PASS | Audit、tenant pre-filter、权威后端或 fence 不可判定时全部安全拒绝 |
| Observability | PASS | 所有 commit/state transition 有正式 Audit 或明确的最小 operational event |
| Spec-Driven Evidence | PASS | DEC-001～005 均落到事务、状态机、错误、恢复与测试 |

**Post-design gate result**: PASS。无高严重度问题或宪法例外，可以进入 `$speckit-tasks`。

## Complexity Tracking

无需要豁免的复杂度或宪法违反。对象/向量采用契约替身是已明确的阶段范围，不是生产能力声明。

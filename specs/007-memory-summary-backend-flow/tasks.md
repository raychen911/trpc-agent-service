# Tasks: 完整数据抽象与同步策略

**Feature**: `007-memory-summary-backend-flow`

**Input**: [spec.md](./spec.md)、[clarification-decisions.md](./clarification-decisions.md)、[plan.md](./plan.md)、[research.md](./research.md)、[data-model.md](./data-model.md)、[contracts/](./contracts/)、[quickstart.md](./quickstart.md)

**Method**: 严格测试先行。每组实现开始前必须先写测试并运行得到预期 RED；实现完成后运行同一命令得到 GREEN，并把命令、退出码、通过/失败/跳过数量及必要的脱敏摘要追加到 `validation-results.md`。

## Checklist Format

- `[P]`：可与相邻任务并行，且不会修改同一文件或依赖尚未完成的实现。
- `[USn]`：对应 `spec.md` 的用户故事。
- 每项任务都标注关联 FR、人工决策及验收证据；`DEC-N/A` 表示仅为不改变决策的工程准备。
- 共享后端未配置造成的 skip 只能记录环境原因，不能作为 GREEN 或阶段完成证据。
- 对象存储和向量库结果必须标注 deterministic fake/fixture，不得表述为真实后端验证。

---

## Phase 1: Setup（任务与证据基线）

**Purpose**：固定现有实现基线、测试目录、共享环境入口和证据格式；不改变业务行为。

- [X] T001 记录当前 Git 分支、工作区状态、Python/依赖版本和现有 Data 骨架文件清单到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-032, FR-034] [DEC-005] [Evidence: baseline inventory]
- [X] T002 将旧验证记录重构为带日期、任务号、RED/GREEN、命令、退出码、结果摘要、环境和备注列的模板，保留既有事实并明确标注旧骨架不算本计划验收，文件为 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-033] [DEC-005] [Evidence: validation template review]
- [X] T003 [P] 创建第七阶段分层测试包与 `__init__.py`：`tests/unit/data/`、`tests/contract/data/`、`tests/integration/data/`、`tests/e2e/data/`、`tests/security/data/`。[FR-002, FR-032] [DEC-005] [Evidence: pytest collection]
- [X] T004 [P] 在 `tests/data_support.py` 定义两个租户、两个节点、固定 UTC、规范化内容、稳定 trace/fence 和无真实 Secret 的 fixture builders。[FR-001, FR-027, FR-032] [DEC-004, DEC-005] [Evidence: fixture self-test]
- [X] T005 [P] 在 `tests/conftest.py` 扩展第七阶段共享 PostgreSQL/Redis marker 与唯一 namespace fixture，确保缺环境时给出明确 skip 原因且不回显 DSN。[FR-019, FR-033] [DEC-005] [Evidence: collection/skip reason]

**Checkpoint**：测试目录和证据模板可用，未修改生产语义。

---

## Phase 2: Foundational（阻塞所有用户故事）

**Purpose**：先通过 RED 测试固定规范化、模型、稳定错误、公共异步端口和 schema v6，再实现共同基础。

**⚠️ CRITICAL**：T006–T016 未完成前不得开始任何用户故事实现。

### Foundational tests — 必须先 RED

- [X] T006 [P] 为 canonical JSON、SHA-256 digest、UTC/version/sequence 验证和 Memory 字节上限编写 RED 单元测试到 `tests/unit/data/test_canonical_and_models.py`。[FR-001, FR-004, FR-008, NFR-003, NFR-005] [DEC-002, DEC-004] [Evidence: targeted RED]
- [X] T007 [P] 为全部稳定 domain error 的 code、retryable、无后端详情 envelope 编写 RED 单元测试到 `tests/unit/data/test_data_errors.py`。[FR-024, FR-031] [DEC-001, DEC-003, DEC-005] [Evidence: targeted RED]
- [X] T008 [P] 为 `DataScope`、DataUnitOfWork、Event/Memory/Summary/Artifact/Knowledge/Migration/Object/Vector 异步 Protocol 及禁止可选 tenant 编写 RED 契约测试到 `tests/contract/data/test_repository_ports.py`。[FR-001, FR-002, FR-017] [DEC-001, DEC-004, DEC-005] [Evidence: port shape RED]
- [X] T009 [P] 为现有 PostgreSQL schema v5 原地升级到 v6、重复初始化幂等、未来版本拒绝和旧数据保留编写 RED 集成测试到 `tests/integration/data/test_schema_upgrade.py`。[FR-019, FR-034] [DEC-001, DEC-005] [Evidence: v5→v6 RED]
- [X] T010 运行 T006–T009 的精确 pytest 命令，确认失败原因仅为尚未实现的新契约，并将命令和失败摘要记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-002, FR-033] [DEC-001, DEC-004, DEC-005] [Evidence: foundational RED record]

### Foundational implementation — RED 后实施

- [X] T011 [P] 在 `trpc_service/storage/canonical.py` 实现确定性 JSON 规范化、digest 和安全大小计算，拒绝 NaN/Infinity 与不可序列化 Secret。[FR-008, FR-029, NFR-005] [DEC-002, DEC-004] [Evidence: T006 GREEN]
- [X] T012 [P] 重构 `trpc_service/storage/data_models.py`，实现不可变 `DataScope`、SessionStream/Event、Memory、Summary、ArtifactMetadata/Upload、KnowledgeDocument、MigrationState 及 metadata view。[FR-001, FR-004, FR-008, FR-011, FR-013, FR-015, FR-020] [DEC-001, DEC-002, DEC-003, DEC-004] [Evidence: T006/T008 GREEN]
- [X] T013 [P] 在 `trpc_service/storage/data_errors.py` 实现 `tenant_scope_invalid`、`sequence_gap`、`idempotency_conflict`、`version_conflict`、`summary_conflict`、`content_too_large`、`digest_mismatch`、`tenant_filter_unsupported`、`migration_write_paused`、`migration_conflict`、`forward_repair_required`、`audit_unavailable`、`state_backend_unavailable` 和 `stale_fence`。[FR-024, FR-031] [DEC-001, DEC-002, DEC-003, DEC-004, DEC-005] [Evidence: T007 GREEN]
- [X] T014 重构 `trpc_service/storage/contracts.py` 的第七阶段端口，增加 `DataUnitOfWorkFactory`、audited content/metadata 分离接口和 capability 声明，同时保持旧 Session/Channel/Governance Protocol 兼容。[FR-002, FR-017, FR-034] [DEC-001, DEC-004, DEC-005] [Evidence: T008 plus legacy contract GREEN]
- [X] T015 在 `trpc_service/storage/postgres/migrations/006_memory_summary.sql`、`trpc_service/storage/postgres/models.py` 和 `trpc_service/storage/postgres/database.py` 添加前向 schema v6、tenant-scoped 唯一约束/索引及 schema gate，禁止清表式升级。[FR-004, FR-008, FR-011, FR-013, FR-015, FR-020, FR-028, FR-034] [DEC-001, DEC-003, DEC-004, DEC-005] [Evidence: T009 GREEN]
- [X] T016 运行 T006–T009 同一组命令和既有 storage/schema 回归，确认 GREEN；将通过/跳过数量及 schema version=6 证据记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-002, FR-033, FR-034] [DEC-001, DEC-004, DEC-005] [Evidence: foundational GREEN record]

**Checkpoint**：公共模型、错误、端口和数据库结构完成，用户故事实现可以开始。

---

## Phase 3: US1 — 租户隔离的数据模型和 Repository（P1）🎯 MVP

**Goal**：同名资源在两个租户中完全隔离；上层只依赖统一异步 Repository。

**Independent Test**：两个租户写入相同 Memory key、Artifact id 和 Knowledge document_id；InMemory 与 PostgreSQL metadata 查询均只返回当前租户对象，缺失/伪造 scope 默认拒绝。

### Tests for US1 — 必须先 RED

- [X] T017 [P] [US1] 在 `tests/unit/data/test_tenant_scope.py` 编写可信 scope、缺 tenant、scope mismatch、不可变领域对象和安全 tenant digest 的 RED 测试。[FR-001, FR-003, FR-027, FR-029] [DEC-004, DEC-005] [Evidence: scope RED]
- [X] T018 [P] [US1] 在 `tests/contract/data/repository_contracts.py` 建立可复用的 tenant isolation、async API、immutable result 和 stable metadata contract suite。[FR-001, FR-002, FR-003, FR-017] [DEC-004, DEC-005] [Evidence: reusable contract RED]
- [X] T019 [P] [US1] 在 `tests/contract/data/test_inmemory_repository.py` 对 InMemory factory 套用 T018，并覆盖两个租户同 key 不覆盖。[FR-002, FR-003] [DEC-004] [Evidence: InMemory isolation RED]
- [X] T020 [P] [US1] 在 `tests/contract/data/test_postgres_repository.py` 对 PostgreSQL factory 套用 T018，覆盖真实数据库 tenant predicate 和无全局 fallback。[FR-002, FR-003, FR-019, FR-033] [DEC-001, DEC-005] [Evidence: PostgreSQL isolation RED]
- [X] T021 [US1] 运行 T017–T020，确认 InMemory 与共享实现均出现预期 RED；共享环境缺失时只记录 skip，随后必须在 Docker 环境补跑 RED，记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-002, FR-033] [DEC-005] [Evidence: US1 RED record]

### Implementation for US1

- [X] T022 [P] [US1] 在 `trpc_service/storage/data_scope.py` 实现可信 `DataScope` 构造/校验和只输出摘要的诊断方法，禁止由消息正文或普通 metadata 创建 tenant。[FR-001, FR-003, FR-027, FR-029] [DEC-004, DEC-005] [Evidence: T017 GREEN]
- [X] T023 [P] [US1] 重构 `trpc_service/storage/memory.py` 为 tenant-scoped InMemory reference adapter，使用异步锁和复合键，不实现任何跨租户扫描。[FR-001, FR-002, FR-003] [DEC-004] [Evidence: T019 GREEN]
- [X] T024 [P] [US1] 在 `trpc_service/storage/postgres/data_repositories.py` 实现共享 tenant predicate、metadata projection 和 Repository factory 基础，不把 SQLAlchemy Row 泄漏到领域层。[FR-002, FR-003, FR-017, FR-019] [DEC-001, DEC-004, DEC-005] [Evidence: T020 GREEN]
- [X] T025 [US1] 在 `trpc_service/storage/data_service.py` 建立 `AuditedDataAccess` 外观及 metadata/content 分离入口，Gateway/Worker 不得直接接触具体 adapter。[FR-002, FR-017, FR-029, FR-030] [DEC-004, DEC-005] [Evidence: facade contract GREEN]
- [X] T026 [US1] 运行 T017–T020 和 `tests/contract/test_adapter_substitutability.py`，确认 US1 GREEN 并记录两个租户、两个实现和真实共享后端结果到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-002, FR-003, FR-017, FR-033, FR-034] [DEC-004, DEC-005] [Evidence: US1 GREEN record]

**Checkpoint**：US1 可独立演示 tenant-scoped Repository 与后端可替换边界。

---

## Phase 4: US2 — Session Event 顺序与幂等（P1）

**Goal**：PostgreSQL 作为唯一权威，严格连续地原子提交 Event、watermark 和 Audit。

**Independent Test**：两个节点并发提交连续、重复和乱序 Event；只有合法连续事实提交，响应丢失重试返回原记录，任一事务故障不留下部分状态。

### Tests for US2 — 必须先 RED

- [X] T027 [P] [US2] 在 `tests/unit/data/test_event_state_machine.py` 编写初始水位、`current+1`、gap/rollback、同 event_id 同/异 digest 和 stable error 的 RED 测试。[FR-004, FR-005, FR-006, FR-007] [DEC-001, DEC-002] [Evidence: event state RED]
- [X] T028 [P] [US2] 在 `tests/contract/data/test_event_repository.py` 编写 InMemory/PostgreSQL 共用 append/replay/conflict/list metadata/raw read 契约 RED 测试。[FR-002, FR-004, FR-005, FR-006, FR-007] [DEC-001, DEC-002, DEC-005] [Evidence: event contract RED]
- [X] T029 [P] [US2] 在 `tests/integration/data/test_event_atomicity.py` 对 Event insert、watermark 更新、Audit append 和 commit-response 各点注入故障，断言前三者全有或全无。[FR-005, FR-024, FR-028, FR-030] [DEC-001, DEC-005] [Evidence: atomicity RED]
- [X] T030 [P] [US2] 在 `tests/integration/data/test_event_concurrency.py` 编写双节点同 sequence、同 ID 重放、异 digest 冲突、旧 fence 和 Redis 不可作为 PG 旁路的 RED 测试。[FR-004, FR-005, FR-006, FR-007, FR-026] [DEC-001, DEC-002] [Evidence: concurrency RED]
- [X] T031 [US2] 运行 T027–T030 并记录精确失败点到 `specs/007-memory-summary-backend-flow/validation-results.md`，确认 gap 后数据库和 Redis 均无 pending business Event。[FR-005, FR-033] [DEC-001, DEC-002] [Evidence: US2 RED record]

### Implementation for US2

- [X] T032 [P] [US2] 在 `trpc_service/storage/memory.py` 实现与最终规则一致的 InMemory Event/watermark/Audit 原子临界区及 replay/conflict outcome。[FR-004, FR-005, FR-006, FR-007, FR-028] [DEC-001, DEC-002, DEC-005] [Evidence: T027/T028 InMemory GREEN]
- [X] T033 [US2] 在 `trpc_service/storage/postgres/data_repositories.py` 实现 SessionStream 行锁、双唯一约束判定、严格 sequence 和 Event metadata/content 读取。[FR-004, FR-005, FR-006, FR-007] [DEC-001, DEC-002] [Evidence: T028/T030 repository GREEN]
- [X] T034 [US2] 在 `trpc_service/storage/postgres/data_uow.py` 实现共享 AsyncConnection 的 Event insert+watermark CAS+mutation Audit 单事务及 rollback/failure injection seam。[FR-005, FR-028, FR-030] [DEC-001, DEC-005] [Evidence: T029 GREEN]
- [X] T035 [US2] 在 `trpc_service/storage/data_service.py` 接入 Event append、metadata/raw read、trace 和 error envelope；PG 不可用时禁止 Redis/InMemory fallback。[FR-004, FR-024, FR-027, FR-030, FR-031] [DEC-001, DEC-005] [Evidence: facade event GREEN]
- [X] T036 [US2] 在 `trpc_service/storage/postgres/data_repositories.py` 为 cutover 后首笔 Event 新写预留同事务关闭 `rollback_eligible` 的钩子，旧 generation 返回 `stale_fence`。[FR-022, FR-026] [DEC-003] [Evidence: first-write hook contract]
- [X] T037 [US2] 运行 T027–T030 及既有 shared Event/Session 回归，确认真实 PostgreSQL US2 GREEN，将事务故障矩阵、双节点结果和零 Redis 权威写证据记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-004–FR-007, FR-033, FR-034] [DEC-001, DEC-002, DEC-005] [Evidence: US2 GREEN record]

**Checkpoint**：US2 可独立证明 Event 顺序、幂等、事务原子性和 PostgreSQL 唯一权威。

---

## Phase 5: US3 — 跨节点 Memory 可见性（P1）

**Goal**：Memory 以规范化 JSON、digest 和 version 在 PostgreSQL CAS 提交，任意节点读取 committed version。

**Independent Test**：节点 A 提交 Memory 后节点 B 读取相同 canonical content/version/digest；并发 CAS 只有一个胜者，超限内容不隐式写 Artifact。

### Tests for US3 — 必须先 RED

- [X] T038 [P] [US3] 在 `tests/unit/data/test_memory_state_machine.py` 编写 canonical digest、create/update/replay、stale version、大小边界和 source event watermark 的 RED 测试。[FR-008, FR-009] [DEC-004] [Evidence: Memory state RED]
- [X] T039 [P] [US3] 在 `tests/contract/data/test_memory_repository.py` 编写 InMemory/PostgreSQL 共用 CAS、metadata/raw read、Audit gate 和 tenant isolation 契约 RED 测试。[FR-002, FR-003, FR-008, FR-009, FR-030] [DEC-004, DEC-005] [Evidence: Memory contract RED]
- [X] T040 [P] [US3] 在 `tests/integration/data/test_memory_cross_node.py` 编写两个独立 Repository/Worker 节点的 committed visibility、并发 CAS、旧 fence 和后端断连 RED 测试。[FR-009, FR-010, FR-018, FR-024, FR-026] [DEC-004, DEC-005] [Evidence: Memory cross-node RED]
- [X] T041 [P] [US3] 在 `tests/integration/data/test_memory_size_boundary.py` 断言超限返回 `content_too_large`、Memory/Artifact/Audit mutation 调用均为 0 且无隐式 storage_ref。[FR-008, FR-013, FR-030] [DEC-004, DEC-005] [Evidence: size-boundary RED]
- [X] T042 [US3] 运行 T038–T041 并将预期 RED、共享环境和失败摘要记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-008–FR-010, FR-033] [DEC-004, DEC-005] [Evidence: US3 RED record]

### Implementation for US3

- [X] T043 [P] [US3] 在 `trpc_service/storage/memory.py` 实现 InMemory Memory canonicalization、大小限制、version CAS、digest replay 和 tenant-scoped metadata/content 读取。[FR-002, FR-003, FR-008, FR-009] [DEC-004] [Evidence: T038/T039 InMemory GREEN]
- [X] T044 [US3] 在 `trpc_service/storage/postgres/data_repositories.py` 实现 PostgreSQL Memory CAS、规范化 JSON/digest/version 和 tenant predicate。[FR-008, FR-009, FR-010, FR-019] [DEC-004, DEC-005] [Evidence: T039/T040 PostgreSQL GREEN]
- [X] T045 [US3] 在 `trpc_service/storage/postgres/data_uow.py` 将 Memory CAS、mutation Audit 和 cutover 首写 rollback closure 纳入同一事务。[FR-009, FR-022, FR-028, FR-030] [DEC-003, DEC-004, DEC-005] [Evidence: Memory UoW atomicity]
- [X] T046 [US3] 在 `trpc_service/storage/data_service.py` 接入 Memory metadata/raw read Audit gate、安全错误和无本地旁路逻辑。[FR-010, FR-018, FR-024, FR-027, FR-030, FR-031] [DEC-004, DEC-005] [Evidence: Memory facade GREEN]
- [X] T047 [US3] 运行 T038–T041 与既有跨节点 Session/治理回归，确认 US3 GREEN，并记录两个节点的 version/digest、一胜一冲突及 Audit 故障零写证据到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-008–FR-010, FR-018, FR-033, FR-034] [DEC-004, DEC-005] [Evidence: US3 GREEN record]

**Checkpoint**：US3 可独立证明 Memory 跨节点一致可见、并发不丢更新且无隐式 Artifact。

---

## Phase 6: US4 — Summary watermark 和状态一致性（P1）

**Goal**：Summary 只覆盖已确认的连续 Event，水位不回退，同水位由 digest 明确判定幂等或冲突。

**Independent Test**：已确认 Event watermark=5 时，Summary 4→5 成功、5+同 digest 重放、5+异 digest 冲突、6 拒绝；任何失败均保留旧 Summary。

### Tests for US4 — 必须先 RED

- [X] T048 [P] [US4] 在 `tests/unit/data/test_summary_state_machine.py` 编写水位前进/回退、超过 Event 水位、同水位同/异 digest 和恢复起点 RED 测试。[FR-011, FR-012] [DEC-002] [Evidence: Summary state RED]
- [X] T049 [P] [US4] 在 `tests/contract/data/test_summary_repository.py` 编写 InMemory/PostgreSQL 共用 CAS、replay/conflict、metadata/raw read 和 Audit gate 契约 RED 测试。[FR-002, FR-011, FR-012, FR-030] [DEC-002, DEC-005] [Evidence: Summary contract RED]
- [X] T050 [P] [US4] 在 `tests/integration/data/test_summary_concurrency.py` 编写两个节点同水位不同 digest、一胜一冲突、旧 fence 和 Event/Summary 锁定顺序 RED 测试。[FR-011, FR-012, FR-026] [DEC-002] [Evidence: Summary concurrency RED]
- [X] T051 [P] [US4] 在 `tests/integration/data/test_summary_atomicity.py` 注入 Summary CAS、Audit 和 commit 故障，断言旧 Summary 始终有效且不会宣称覆盖缺失 Event。[FR-011, FR-012, FR-024, FR-028, FR-030] [DEC-002, DEC-005] [Evidence: Summary atomicity RED]
- [X] T052 [US4] 运行 T048–T051 并记录预期 RED 到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-011, FR-012, FR-033] [DEC-002, DEC-005] [Evidence: US4 RED record]

### Implementation for US4

- [X] T053 [P] [US4] 在 `trpc_service/storage/memory.py` 实现 InMemory Summary watermark/digest/version 状态机和旧值保留语义。[FR-011, FR-012] [DEC-002] [Evidence: T048/T049 InMemory GREEN]
- [X] T054 [US4] 在 `trpc_service/storage/postgres/data_repositories.py` 实现锁定 Event watermark 与当前 Summary、验证覆盖范围和 version CAS 的 PostgreSQL Repository。[FR-011, FR-012, FR-019] [DEC-001, DEC-002] [Evidence: T049/T050 PostgreSQL GREEN]
- [X] T055 [US4] 在 `trpc_service/storage/postgres/data_uow.py` 原子提交 Summary+mutation Audit，并接入 cutover 首写回滚资格关闭钩子。[FR-011, FR-022, FR-028, FR-030] [DEC-002, DEC-003, DEC-005] [Evidence: T051 GREEN]
- [X] T056 [US4] 在 `trpc_service/storage/data_service.py` 实现 Summary metadata/raw read、重建起点和安全错误映射。[FR-011, FR-012, FR-027, FR-030, FR-031] [DEC-002, DEC-005] [Evidence: Summary facade GREEN]
- [X] T057 [US4] 运行 T048–T051 和 Event/Memory 回归，确认 US4 GREEN；记录同水位 replay/conflict、缺失 Event 拒绝和事务回滚证据到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-011, FR-012, FR-033, FR-034] [DEC-002, DEC-005] [Evidence: US4 GREEN record]

**Checkpoint**：US4 可独立证明 Summary 水位与 Event 权威流一致。

---

## Phase 7: US5 — Artifact 与 Knowledge 后端适配（P1）

**Goal**：以可恢复的 metadata 状态管理大对象和知识索引；对象/向量端只用供应商无关 deterministic fake，Knowledge 必须查询前 tenant filter。

**Independent Test**：两个租户使用相同 Artifact/Document 标识仍完全隔离；Artifact 中断保留旧引用并清理孤儿；不支持 tenant pre-filter 的 Vector fake 在查询调用前拒绝。

### Tests for US5 — 必须先 RED

- [X] T058 [P] [US5] 在 `tests/unit/data/test_artifact_state_machine.py` 编写 STAGED/VERIFIED/PUBLISHED/ORPHANED/DELETED、digest、metadata version 和 TTL 规则 RED 测试。[FR-013, FR-014] [DEC-004] [Evidence: Artifact state RED]
- [X] T059 [P] [US5] 在 `tests/unit/data/test_knowledge_state_machine.py` 编写 PENDING_INDEX/INDEXED/FAILED_RETRYABLE、digest/version 和 only-indexed-visible RED 测试。[FR-015, FR-016] [DEC-004] [Evidence: Knowledge state RED]
- [X] T060 [P] [US5] 在 `tests/contract/data/test_object_store_fake.py` 编写 tenant-scoped temporary key、immutable put/read/delete、故障注入和 deterministic fake 标识契约 RED 测试。[FR-013, FR-017, FR-019, FR-032] [DEC-004, DEC-005] [Evidence: Object fake RED]
- [X] T061 [P] [US5] 在 `tests/contract/data/test_vector_store_fake.py` 编写 pre-filter capability、tenant 必填、upsert 幂等、有限结果和 deterministic fake 标识契约 RED 测试。[FR-015, FR-016, FR-017, FR-019, FR-032] [DEC-004, DEC-005] [Evidence: Vector fake RED]
- [X] T062 [P] [US5] 在 `tests/contract/data/test_artifact_repository.py` 编写 publish/get metadata/raw read/CAS/digest/Audit readiness 契约 RED 测试，断言初始 Audit gate 失败时 ObjectStore 调用为 0。[FR-013, FR-014, FR-030] [DEC-004, DEC-005] [Evidence: Artifact contract RED]
- [X] T063 [P] [US5] 在 `tests/contract/data/test_knowledge_repository.py` 编写 stage/index/search/Audit readiness 契约 RED 测试，断言不支持 pre-filter 或 Audit gate 失败时 VectorStore 调用为 0。[FR-015, FR-016, FR-030] [DEC-004, DEC-005] [Evidence: Knowledge contract RED]
- [X] T064 [P] [US5] 在 `tests/integration/data/test_artifact_publication.py` 注入 upload、digest verify、metadata CAS、Audit 和响应丢失故障，验证旧引用、不可见临时对象和幂等恢复。[FR-013, FR-014, FR-024, FR-025] [DEC-004, DEC-005] [Evidence: Artifact recovery RED]
- [X] T065 [P] [US5] 在 `tests/integration/data/test_artifact_orphan_gc.py` 编写 TTL、仍被引用对象保护、旧 fence、重复清理和双租户 GC RED 测试。[FR-013, FR-026] [DEC-004] [Evidence: orphan GC RED]
- [X] T066 [P] [US5] 在 `tests/integration/data/test_knowledge_tenant_filter.py` 断言 tenant filter 在相似度计算前进入后端、跨租户候选从未返回到应用和后端不支持时 fail closed。[FR-003, FR-016, FR-024] [DEC-004, DEC-005] [Evidence: query pre-filter RED]
- [X] T067 [US5] 运行 T058–T066 并记录预期 RED，确保报告明确写明 Object/Vector 为 fixture 而非真实服务，记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-032, FR-033] [DEC-004, DEC-005] [Evidence: US5 RED record]

### Implementation for US5

- [X] T068 [P] [US5] 在 `trpc_service/storage/object_store.py` 实现 tenant-scoped deterministic ObjectStore fake、temporary immutable ref、digest readback、故障开关和幂等 delete。[FR-013, FR-017, FR-032] [DEC-004, DEC-005] [Evidence: T060 GREEN]
- [X] T069 [P] [US5] 在 `trpc_service/storage/vector_store.py` 实现声明 pre-filter capability 的 deterministic VectorStore fake、tenant-scoped upsert/search 和有限结果。[FR-015, FR-016, FR-017, FR-032] [DEC-004, DEC-005] [Evidence: T061 GREEN]
- [X] T070 [US5] 在 `trpc_service/storage/postgres/data_repositories.py` 实现 ArtifactMetadata/ArtifactUpload 与 KnowledgeDocument tenant-scoped CAS/status Repository。[FR-013, FR-014, FR-015, FR-019] [DEC-004, DEC-005] [Evidence: T062/T063 metadata GREEN]
- [X] T071 [US5] 在 `trpc_service/storage/data_service.py` 实现 Audit readiness→temporary upload→digest verify→metadata CAS+Audit 的 Artifact publish/read 编排，任何失败保持旧引用。[FR-013, FR-014, FR-017, FR-024, FR-030] [DEC-004, DEC-005] [Evidence: T062/T064 GREEN]
- [X] T072 [US5] 在 `trpc_service/storage/data_service.py` 实现 Knowledge PENDING_INDEX→tenant upsert→INDEXED 编排、pre-filter capability gate、metadata digest 验证和安全 search。[FR-015, FR-016, FR-017, FR-024, FR-030] [DEC-004, DEC-005] [Evidence: T063/T066 GREEN]
- [X] T073 [US5] 在 `trpc_service/recovery/reconciler.py` 增加 tenant-scoped Artifact orphan GC 和 Knowledge pending-index 幂等恢复，所有推进携带 generation/fence。[FR-013, FR-025, FR-026] [DEC-004] [Evidence: T065 and pending-index recovery GREEN]
- [X] T074 [US5] 运行 T058–T066 及 tenant isolation/Audit 回归，确认 US5 GREEN；记录旧引用保持、GC 删除计数、pre-filter 调用参数和 fixture 声明到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-013–FR-017, FR-032–FR-034] [DEC-004, DEC-005] [Evidence: US5 GREEN record]

**Checkpoint**：US5 可独立演示 Artifact 发布/清理和 Knowledge 查询前租户过滤，但不宣称真实对象/向量后端。

---

## Phase 8: US6 — 多后端迁移（P1）

**Goal**：按 tenant/stream 短暂停写，将 Redis 遗留权威数据迁移并校验到 PostgreSQL，安全切换且明确回滚终点。

**Independent Test**：锁定源 watermark 后复制、校验和切换；切换前可回滚，首笔 PG 新写后只能 forward repair；一个租户迁移不影响另一个租户。

### Tests for US6 — 必须先 RED

- [X] T075 [P] [US6] 在 `tests/unit/data/test_migration_state_machine.py` 编写全部合法/非法转换、generation CAS、rollback eligibility 和首写关闭规则 RED 测试。[FR-020, FR-021, FR-022, FR-023, FR-026] [DEC-003] [Evidence: migration state RED]
- [X] T076 [P] [US6] 在 `tests/contract/data/test_migration_repository.py` 编写 tenant/stream get/transition/checkpoint/activate/first-write 契约 RED 测试。[FR-002, FR-020, FR-021, FR-022, FR-023] [DEC-003, DEC-005] [Evidence: migration repository RED]
- [X] T077 [P] [US6] 在 `tests/integration/data/test_migration_pause_scope.py` 编写仅目标 tenant/stream 返回 `migration_write_paused`、其他作用域继续写入的 RED 测试。[FR-020, FR-024] [DEC-003] [Evidence: scoped pause RED]
- [X] T078 [P] [US6] 在 `tests/integration/data/test_redis_postgres_migration.py` 编写 source watermark lock、批量 checkpoint、计数/version/digest 双读校验、重复执行和 authority CAS RED 测试。[FR-019, FR-020, FR-021, FR-023] [DEC-003, DEC-005] [Evidence: migration happy/replay RED]
- [X] T079 [P] [US6] 在 `tests/integration/data/test_migration_cutover_rollback.py` 编写切换前节点中断/回滚、首笔 PG 写同事务关闭回滚和之后 `forward_repair_required` RED 测试。[FR-021, FR-022, FR-023, FR-024] [DEC-003] [Evidence: rollback boundary RED]
- [X] T080 [P] [US6] 在 `tests/integration/data/test_migration_conflicts.py` 编写目标较新、digest 不同、旧 generation、未校验水位和禁止反向覆盖 RED 测试。[FR-021, FR-022, FR-023, FR-026] [DEC-003] [Evidence: migration conflict RED]
- [X] T081 [US6] 运行 T075–T080，确认现有 `DualReadMigrator` 宽松实现无法通过，并把预期 RED、真实 Redis/PG 环境和失败阶段记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-020–FR-023, FR-033] [DEC-003, DEC-005] [Evidence: US6 RED record]

### Implementation for US6

- [X] T082 [P] [US6] 重构 `trpc_service/storage/sync.py`，用显式 MigrationState/Authority/Watermark/Digest 和稳定 transition 取代现有仅比较 Memory 的 `DualReadMigrator`。[FR-020, FR-021, FR-022, FR-023] [DEC-003] [Evidence: T075 GREEN]
- [X] T083 [US6] 在 `trpc_service/storage/postgres/data_repositories.py` 实现 MigrationRepository 的 tenant/stream CAS、checkpoint、activate、rollback 和 forward-repair 状态。[FR-020, FR-021, FR-022, FR-023, FR-026] [DEC-003, DEC-005] [Evidence: T076 GREEN]
- [X] T084 [US6] 在 `trpc_service/storage/migration.py` 实现复用 Redis lease/fence 的 pause→snapshot→copy→verify→cutover Coordinator，Redis 只作为 legacy source 且不接收新权威写。[FR-019, FR-020, FR-021, FR-024, FR-026] [DEC-001, DEC-003, DEC-005] [Evidence: T077/T078 GREEN]
- [X] T085 [US6] 在 `trpc_service/storage/migration.py` 实现切换前安全回滚、首笔 PG 新写后的 write pause/forward repair 和不可反向转换保护。[FR-022, FR-023, FR-024, FR-025] [DEC-003] [Evidence: T079/T080 GREEN]
- [X] T086 [US6] 在 `trpc_service/storage/data_service.py` 对所有 Event/Memory/Summary 写入增加 authority/migration gate，并确保首笔 cutover 写与关闭 rollback eligibility 共用 PostgreSQL 事务。[FR-020, FR-021, FR-022, FR-024] [DEC-003] [Evidence: write-gate integration GREEN]
- [X] T087 [US6] 在 `trpc_service/recovery/reconciler.py` 增加从持久化 checkpoint 接管迁移、校验后推进和 `FORWARD_REPAIR_REQUIRED` 运维状态，不跨越未校验水位。[FR-023, FR-025, FR-026] [DEC-003] [Evidence: migration takeover GREEN]
- [X] T088 [US6] 运行 T075–T080 和 Event/Memory/Summary 回归，确认 US6 GREEN；记录作用域停写、source/target digest、authority generation、切换前回滚和首写后前向修复证据到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-019–FR-026, FR-033, FR-034] [DEC-001, DEC-003, DEC-005] [Evidence: US6 GREEN record]

**Checkpoint**：US6 可独立证明迁移无双权威、可接管且不会丢弃 cutover 后新事实。

---

## Phase 9: US7 — 后端故障、冲突和恢复（P1）

**Goal**：任一节点或后端故障都有安全、可解释且可接管的后继，不重复业务事实、不启用本地旁路。

**Independent Test**：在 Event、Memory、Summary、Artifact、Knowledge 和 Migration 各阶段中断，另一节点只能根据 durable marker/fence 推进合法后继；旧节点写入为 0。

### Tests for US7 — 必须先 RED

- [X] T089 [P] [US7] 在 `tests/unit/data/test_recovery_decisions.py` 编写 pre-commit、committed-response-lost、pending-index、orphan、migration checkpoint、outcome-unknown 和 review 决策表 RED 测试。[FR-024, FR-025, FR-026] [DEC-001, DEC-003, DEC-004, DEC-005] [Evidence: recovery decision RED]
- [X] T090 [P] [US7] 在 `tests/integration/data/test_backend_outages.py` 注入 PostgreSQL、Redis coordination、Object fake 和 Vector fake 超时/断连，验证稳定错误、有限重试和无本地权威旁路。[FR-018, FR-024, NFR-004] [DEC-001, DEC-004, DEC-005] [Evidence: backend outage RED]
- [X] T091 [P] [US7] 在 `tests/integration/data/test_data_recovery_matrix.py` 覆盖 contracts 中列出的 Event/Memory/Summary/Artifact/Knowledge/Migration 全部故障点和幂等补偿 RED 测试。[FR-024, FR-025, FR-026] [DEC-001, DEC-002, DEC-003, DEC-004, DEC-005] [Evidence: recovery matrix RED]
- [X] T092 [P] [US7] 在 `tests/integration/data/test_data_fencing_takeover.py` 编写双节点 lease 到期、generation 接管、旧 owner 恢复后写入为 0 和新 owner 终态最多一次 RED 测试。[FR-025, FR-026] [DEC-001, DEC-003, DEC-004] [Evidence: fencing takeover RED]
- [X] T093 [P] [US7] 在 `tests/e2e/data/test_worker_restart_data_flow.py` 编写 Worker 全部重启后 Session Event、Memory、Summary 和 trace 从共享后端继续的 RED 测试。[FR-010, FR-018, FR-024, FR-026, FR-027] [DEC-001, DEC-004, DEC-005] [Evidence: restart E2E RED]
- [X] T094 [US7] 运行 T089–T093 并记录预期 RED、故障注入点和无真实外部依赖事实到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-024–FR-026, FR-032, FR-033] [DEC-001, DEC-003, DEC-004, DEC-005] [Evidence: US7 RED record]

### Implementation for US7

- [X] T095 [P] [US7] 在 `trpc_service/storage/data_models.py` 和 `trpc_service/storage/postgres/models.py` 补齐 DataRecoveryMarker、confirmed stage、result digest、review reason 和 generation 持久化模型。[FR-025, FR-026] [DEC-001, DEC-003, DEC-004] [Evidence: T089 GREEN]
- [X] T096 [US7] 在 `trpc_service/storage/postgres/data_repositories.py` 实现 tenant-scoped DataRecoveryRepository 的 create-once、claim-by-generation、mark-complete/review 和诊断查询。[FR-025, FR-026, FR-027] [DEC-001, DEC-003, DEC-004, DEC-005] [Evidence: recovery repository contract]
- [X] T097 [US7] 在 `trpc_service/recovery/reconciler.py` 编排只补齐已确认结果/metadata/Audit/索引/清理/迁移 checkpoint 的恢复路径，结果未知的非幂等操作进入 review，不重新调用 Agent 或生成内容。[FR-023, FR-024, FR-025, FR-026] [DEC-001, DEC-003, DEC-004, DEC-005] [Evidence: T091/T092 GREEN]
- [X] T098 [US7] 在 `trpc_service/storage/shared.py` 和 `trpc_service/config/settings.py` 注入 Data UoW、Repository、Object/Vector fixture、超时/有限重试配置，任何真实共享依赖缺失时拒绝启动共享模式而不降级。[FR-018, FR-019, FR-024, FR-031] [DEC-001, DEC-004, DEC-005] [Evidence: T090 GREEN]
- [X] T099 [US7] 在 `trpc_service/gateway/service.py` 与 `trpc_service/worker/service.py` 接入 `AuditedDataAccess` 和共享恢复标识，保持现有 Channel/Governance/Runner 契约且禁止 Worker 业务副本。[FR-017, FR-018, FR-027, FR-034] [DEC-001, DEC-004, DEC-005] [Evidence: T093 plus legacy E2E GREEN]
- [X] T100 [US7] 运行 T089–T093、既有 shared/channel/governance recovery suites 和双 Worker E2E，确认 US7 GREEN；把各故障点终态、接管 generation、重试次数与 review 数记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-024–FR-027, FR-033, FR-034] [DEC-001, DEC-003, DEC-004, DEC-005] [Evidence: US7 GREEN record]

**Checkpoint**：US7 可独立证明跨节点恢复、安全失败和 Worker 无状态。

---

## Phase 10: US8 — 数据访问审计与敏感信息保护（P2）

**Goal**：所有数据变更、迁移和原文读取均可按 trace 解释；Audit 故障时没有未审计业务行为，所有可观察面原文命中为 0。

**Independent Test**：让手机号、邮箱、token、secret marker 穿过所有数据路径并模拟 Audit 断连；写入/迁移/原文读取调用为 0 或全事务回滚，metadata 诊断仍安全可用。

### Tests for US8 — 必须先 RED

- [X] T101 [P] [US8] 在 `tests/contract/data/test_data_audit_repository.py` 编写不可变 mutation/access Audit、tenant/trace/session/resource 查询和只含 digest/version/state 契约 RED 测试。[FR-027, FR-028, FR-029] [DEC-001, DEC-005] [Evidence: Audit contract RED]
- [X] T102 [P] [US8] 在 `tests/integration/data/test_audit_failure_boundary.py` 断言初始 Audit readiness 失败时 Event/Memory/Summary/Object/Vector/Migration 调用为 0，事务内 Audit 失败时状态全回滚，原文不返回。[FR-028, FR-030] [DEC-001, DEC-004, DEC-005] [Evidence: Audit fail-closed RED]
- [X] T103 [P] [US8] 在 `tests/unit/data/test_operational_event.py` 编写 Audit 故障时允许的最小 operational event 字段白名单及禁止冒充正式 Audit 的 RED 测试。[FR-029, FR-030, FR-031] [DEC-005] [Evidence: operational event RED]
- [X] T104 [P] [US8] 在 `tests/integration/data/test_data_traceability.py` 编写三类 trace 跨 Facade/UoW/Repository/恢复一致传播和 Audit 查询 RED 测试。[FR-027, FR-028] [DEC-001, DEC-005] [Evidence: trace RED]
- [X] T105 [P] [US8] 在 `tests/security/data/test_sensitive_material.py` 注入测试手机号、邮箱、token 和 secret marker，扫描日志、错误、trace、metrics、Audit、对象 ref、测试输出和 Git fixture 的 RED 测试。[FR-029, FR-032, NFR-005] [DEC-004, DEC-005] [Evidence: sensitive scan RED]
- [X] T106 [US8] 运行 T101–T105 并记录预期 RED 和 marker 范围到 `specs/007-memory-summary-backend-flow/validation-results.md`，禁止把 marker 原值复制进正式记录。[FR-027–FR-030, FR-033] [DEC-001, DEC-004, DEC-005] [Evidence: US8 RED record]

### Implementation for US8

- [X] T107 [US8] 在 `trpc_service/audit/models.py` 与 `trpc_service/storage/postgres/data_repositories.py` 实现 DataAuditRecord 扩展、不可变最小字段及 tenant/trace/session/resource metadata 查询。[FR-027, FR-028, FR-029] [DEC-001, DEC-005] [Evidence: T101 GREEN]
- [X] T108 [US8] 在 `trpc_service/storage/data_service.py` 实现 mutation/raw-read Audit readiness gate、PostgreSQL 同事务 Audit、外部内容 pre-audit/final gate 和 metadata-only 诊断分支。[FR-028, FR-030] [DEC-001, DEC-004, DEC-005] [Evidence: T102 GREEN]
- [X] T109 [P] [US8] 在 `trpc_service/log/__init__.py` 和 `trpc_service/web/errors.py` 添加 Data domain 错误脱敏与最小 OperationalEvent 输出，禁止 SQL、DSN、对象 ref 和原文。[FR-029, FR-030, FR-031] [DEC-005] [Evidence: T103/T105 GREEN]
- [X] T110 [P] [US8] 在 `trpc_service/metrics/models.py` 与 `trpc_service/metrics/shared.py` 增加 resource_type/backend_type/operation/outcome 低基数指标，禁止 tenant/user/session/message/trace 标签。[FR-027, FR-029] [DEC-005] [Evidence: metrics label GREEN]
- [X] T111 [US8] 运行 T101–T105、既有 Audit/observability/security suites，确认 US8 GREEN；把 Audit outage 调用计数、trace 查询和敏感 marker 0 命中记录到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-027–FR-030, FR-032–FR-034] [DEC-001, DEC-004, DEC-005] [Evidence: US8 GREEN record]

**Checkpoint**：US8 可独立证明数据访问可追踪、Audit 分级拒绝和零原文泄露。

---

## Phase 11: Polish & Cross-Cutting Validation

**Purpose**：完成官方框架兼容、性能、双节点纵向链路、文档和全量回归证据。

- [X] T112 [P] 在 `tests/sdk_validation/test_data_adapter_compatibility.py` 增加 `trpc-agent-py==1.1.19` Event/Session/Memory/Knowledge 公开对象映射和官方 Runner 多轮会话兼容测试。[FR-017, FR-034] [DEC-004] [Evidence: SDK compatibility PASS]
- [X] T113 [P] 在 `tests/integration/data/test_data_performance.py` 增加确定性 InMemory 普通操作 p95<50ms 和真实共享后端延迟只记录不冒充容量结论的测试，覆盖 NFR-001/NFR-002。[FR-019, FR-033] [DEC-005] [Evidence: percentile report]
- [X] T114 在 `tests/e2e/data/test_dual_im_data_flow.py` 增加飞书/企业微信 SDK 替身→Gateway/Governance→双 Worker→官方 Runner→Event/Memory/Summary/Audit 的双租户多轮纵向验收。[FR-003, FR-010, FR-018, FR-027, FR-034] [DEC-001, DEC-004, DEC-005] [Evidence: two-channel/two-worker E2E]
- [X] T115 在 `tests/e2e/data/test_migration_recovery_flow.py` 增加迁移中断、节点接管、cutover 首写和 forward repair 的完整纵向验收。[FR-020–FR-026] [DEC-003, DEC-005] [Evidence: migration E2E]
- [X] T116 按 `specs/007-memory-summary-backend-flow/quickstart.md` 从干净 v5 schema 执行 Docker 启动、v6 初始化、contract/integration/e2e 命令并把真实 PASS/skip 数写入 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-019, FR-033, FR-034] [DEC-001, DEC-005] [Evidence: quickstart transcript]
- [X] T117 [P] 更新 `README.md` 的第七阶段架构、权威存储、迁移边界、真实/fixture 后端声明、本地测试命令和范围外事项。[FR-019, FR-032, FR-034, SC-009] [DEC-001, DEC-003, DEC-004, DEC-005] [Evidence: README review]
- [X] T118 [P] 在 `specs/007-memory-summary-backend-flow/阶段成果记录.md` 总结八个用户故事、DEC-001～005 的人工判断、实现结果、真实后端证据和 fixture 限制，覆盖 SC-001/006/008/009 并说明范围外事项。[FR-032, FR-033, FR-034] [DEC-001, DEC-002, DEC-003, DEC-004, DEC-005] [Evidence: defense-ready summary]
- [X] T119 运行第七阶段全部 Unit/Contract/Integration/E2E/SDK/Security 测试及第二、三、五、六阶段回归，把命令、退出码、pass/fail/skip 和 Docker 健康状态写入 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-033, FR-034, SC-001, SC-008] [DEC-005] [Evidence: full regression PASS]
- [X] T120 扫描源码、测试、Git diff、运行日志、错误样本和 PostgreSQL Audit 样本，确认 Secret/DSN/原文 marker 0 命中；执行 `git diff --check` 并记录结果到 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-029, FR-032, SC-007] [DEC-004, DEC-005] [Evidence: zero-hit scan and clean diff]
- [X] T121 对照 `spec.md` 的 FR-001～FR-034、SC-001～SC-009 和 `clarification-decisions.md` 的 DEC-001～DEC-005 完成逐项可追踪性复核，将最终证据链接写入 `specs/007-memory-summary-backend-flow/validation-results.md`。[FR-001–FR-034, SC-001–SC-009] [DEC-001–DEC-005] [Evidence: final traceability matrix]

**Final Checkpoint**：无失败、真实 PostgreSQL/Redis 测试不是 skip、对象/向量明确标注 fixture、敏感扫描为 0、文档与代码命名一致。

---

## Dependencies & Execution Order

### Phase Dependencies

```text
Phase 1 Setup
   ↓
Phase 2 Foundational（阻塞全部故事）
   ├─→ US1 Tenant-scoped Repository（MVP）
   │      ├─→ US2 Event authority ─→ US4 Summary
   │      ├─→ US3 Memory ──────────┘
   │      └─→ US5 Artifact/Knowledge
   └─────────────────────────────────────┐
US2 + US3 + US4 ─→ US6 Migration        │
US2..US6 ─────────→ US7 Recovery         │
US1..US7 ─────────→ US8 Audit/Security ◄─┘
US1..US8 ─────────→ Phase 11 Final Validation
```

- Phase 1 无依赖。
- Phase 2 依赖 Phase 1，并阻塞所有用户故事。
- US1 依赖 Phase 2，是建议 MVP。
- US2、US3、US5 在 US1 后可并行；US4 依赖 US2 的 confirmed Event watermark。
- US6 依赖 US2、US3、US4；不依赖 US5 的外部真实后端，因为其对象/向量只有 fixture，但迁移文档需复用 US5 契约。
- US7 依赖 US2–US6 的 durable states。
- US8 的 RED 测试可在 US1 后先写，但最终 GREEN 依赖 US2–US7 均接入 Audit gate。
- Phase 11 依赖全部八个用户故事。

### Within Each Story

1. 编写标记为“必须先 RED”的测试。
2. 运行该故事全部新测试并记录预期 RED；不得在未得到 RED 证据前写生产实现。
3. 按 models/contracts → repository/adapter → service/composition → recovery 顺序实施。
4. 运行同一命令获得 GREEN，再运行相关前序回归。
5. 记录证据后才进入下一故事 checkpoint。

## Parallel Opportunities

- T003–T005 可并行；T006–T009 可并行；T011–T013 可并行。
- 每个故事中标记 `[P]` 的 Unit、Contract、Integration 测试位于不同文件，可同时编写，但必须在共享实现开始前统一运行 RED。
- US2、US3、US5 在 US1 完成后可由不同执行者并行；US4 等待 US2，US6/US7/US8 按依赖收敛。
- ObjectStore fake（T068）与 VectorStore fake（T069）可并行。
- 文档、SDK 兼容和性能工作（T112、T113、T117、T118）在对应功能 GREEN 后可并行，最终回归 T119–T121 串行收口。

## Parallel Execution Examples

### US2 Event

```text
并行编写：T027 event state、T028 repository contract、T029 atomicity、T030 concurrency
汇合：T031 统一运行 RED
串行实施：T032 → T033 → T034 → T035 → T036
验收：T037 GREEN + 回归
```

### US5 Artifact / Knowledge

```text
Artifact 轨：T058 + T060 + T062 + T064 + T065
Knowledge 轨：T059 + T061 + T063 + T066
统一 RED：T067
并行 adapters：T068 + T069
汇合 metadata/service/recovery：T070 → T071/T072 → T073 → T074
```

### US8 Audit / Security

```text
并行编写：T101 Audit contract、T102 failure boundary、T103 operational event、T104 trace、T105 security
汇合：T106 RED
实施：T107/T109/T110 可在不修改同一文件时并行，T108 依赖 Audit contract
验收：T111 GREEN + security regression
```

## Implementation Strategy

### MVP First

1. 完成 Phase 1 与 Phase 2。
2. 完成 US1（T017–T026），证明统一领域模型、可信 tenant scope、InMemory/PostgreSQL 契约骨架和跨租户拒绝。
3. 停止并独立运行 US1 tests；只有真实 PostgreSQL suite 通过才可把 MVP 标记为完成。

### Incremental Delivery

1. US2：先建立最关键的 Event PostgreSQL 权威与事务原子性。
2. US3 + US4：补齐跨节点 Memory 和 Summary 一致性。
3. US5：引入可替换对象/向量边界及 fixture 证据。
4. US6：完成 legacy Redis→PostgreSQL 迁移和安全切换。
5. US7：汇总跨后端故障恢复与双 Worker 接管。
6. US8：以 Audit/Security 横切验证收口。
7. Phase 11：真实共享环境、全量回归、敏感扫描与答辩记录。

## Traceability Summary

| Story/Phase | Task range | Primary requirements | Decisions | Independent evidence |
|---|---|---|---|---|
| Setup/Foundation | T001–T016 | FR-001/002/008/017/019/024/031–034 | DEC-001–005 | model/port/schema RED→GREEN |
| US1 | T017–T026 | FR-001–003/017/019/027/029/030/033/034 | DEC-004/005 | two-tenant InMemory+PG contract |
| US2 | T027–T037 | FR-004–007/022/024/026–031/033/034 | DEC-001/002/003/005 | event atomicity + concurrency |
| US3 | T038–T047 | FR-002/003/008–010/013/018/019/022/024/026–034 | DEC-003/004/005 | cross-node Memory CAS |
| US4 | T048–T057 | FR-002/011/012/019/022/024/026–034 | DEC-001/002/003/005 | Summary watermark/digest |
| US5 | T058–T074 | FR-003/013–017/019/024–026/030/032–034 | DEC-004/005 | Artifact recovery + tenant pre-filter fixture |
| US6 | T075–T088 | FR-002/019–026/033/034 | DEC-001/003/005 | scoped migration/cutover/rollback |
| US7 | T089–T100 | FR-010/017–019/023–027/031–034 | DEC-001/003/004/005 | failure matrix + takeover E2E |
| US8 | T101–T111 | FR-027–034 | DEC-001/004/005 | Audit outage + zero-hit scan |
| Final | T112–T121 | FR-003/010/017–034, SC-001–009 | DEC-001–005 | SDK/E2E/full regression/traceability |

## Notes

- 所有任务均从未执行状态开始；旧 `tasks.md` 的 `[X]` 不代表新 DEC 语义已实现。
- 每个测试任务都必须先失败，且失败原因应是缺少目标行为，不是语法、fixture、端口或凭证错误。
- 同一生产文件被多个任务修改时按任务号顺序执行，避免并发覆盖。
- 每个 logical group 建议独立 commit；禁止提交 DSN、Secret、完整对象 ref、原始用户内容或测试运行日志中的敏感值。
- 若实现发现需要改变 DEC-001～DEC-005，立即停止并回到 clarify，不得在代码中静默改变人工决策。

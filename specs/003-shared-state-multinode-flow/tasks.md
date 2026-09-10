# Tasks: 003 Shared-State Multi-Node Flow

**Input**: spec.md、clarification-decisions.md、plan.md、research.md、data-model.md 与 contracts/
**Prerequisite**: 第二阶段 002 本地消息闭环及其 101 项基线测试保持通过。
**Method**: 严格测试先行。每个故事先完成并运行失败测试（RED），再做最小实现（GREEN），最后保存阶段验收证据。

每项任务的 Trace 关联用户故事（US）、功能需求（FR）和澄清决策（D）；Evidence 指定可审查的验收证据。

## Phase 1: Setup（共享后端开发与验证环境）

**Purpose**: 建立可离线复现的 Redis、PostgreSQL、双 Worker 和测试基础设施，不改变第二阶段默认运行方式。

- [X] T001 在 pyproject.toml 声明并锁定 redis 8.1.0、SQLAlchemy 2.0.52、asyncpg 0.31.0 直接依赖及 shared_backend 测试标记，更新 uv.lock（Trace: US1-US5; FR-002/006/017/030; D-003-001-D-003-005; Evidence: uv sync --group dev 输出与 lockfile diff）
- [X] T002 [P] 创建 deploy/local-shared/compose.yaml，固定 redis:7.4.11-alpine3.21、postgres:17.11-alpine3.24、loopback 端口、健康检查和测试卷（Trace: US1-US5; FR-002/006/007/017/030; D-003-001/003/004/005; Evidence: docker compose config 输出）
- [X] T003 [P] 建立 trpc_service/storage/redis_scripts/__init__.py、trpc_service/storage/postgres/__init__.py、trpc_service/recovery/__init__.py 和 tests/integration/shared/__init__.py 模块边界（Trace: US1-US5; FR-004/025/026; D-003-001-D-003-005; Evidence: import smoke test）
- [X] T004 [P] 编写 deploy/local-shared/README.md，说明端口、运行时 secret reference、初始化/停止步骤和禁止 InMemory 回退（Trace: US4/US5; FR-017/028/030; D-003-005; Evidence: 文档审阅记录）
- [X] T005 [P] 在 tests/integration/shared/conftest.py 建立 Redis、PostgreSQL、双节点、故障注入与按唯一 namespace 清理的 fixture 骨架（Trace: US1-US5; FR-002/003/005/030; D-003-001-D-003-005; Evidence: pytest collection 输出）
- [X] T006 运行第二阶段完整基线，把命令、环境、通过数和耗时写入 specs/003-shared-state-multinode-flow/validation-results.md 的 Baseline 节（Trace: US1-US5; FR-001/025/026/030; D-003-001-D-003-005; Evidence: 101 项基线测试结果）

---

## Phase 2: Foundational（所有用户故事的阻塞前置）

**Purpose**: 用测试固定异步 Repository/Adapter 契约、共享状态模型、错误语义和运行时组合根。

### Tests first

- [X] T007 [P] 在 tests/unit/test_shared_settings.py 编写失败测试，覆盖 shared profile、node_id、租约默认值、运行时凭据引用、非法配置和禁止共享后端失败时回退 InMemory（Trace: US1-US5; FR-002/014/017/028/029; D-003-001/005; Evidence: RED 测试输出）
- [X] T008 [P] 在 tests/unit/test_shared_state_models.py 编写失败测试，固定 MessageExecution、Lease、FenceGeneration、ExecutionPhase、RecoveryMarker、AuditOutcome 状态转换与非法转换（Trace: US2-US5; FR-008-FR-016/021-FR-024; D-003-001-D-003-004; Evidence: RED 测试输出）
- [X] T009 [P] 在 tests/contract/test_repository_ports_async.py 编写失败的异步端口契约，覆盖配置、幂等、会话、事件、租约、审计和恢复的稳定返回值与错误（Trace: US1-US5; FR-005/019/020/023/025/026; D-003-001-D-003-005; Evidence: RED 契约输出）
- [X] T010 [P] 在 tests/contract/support/repository_contracts.py 提取可同时驱动 InMemory 与 shared adapter 的厂商无关契约套件，禁止断言 Redis key 或 SQL 表细节（Trace: US5; FR-025/026/030; D-003-001-D-003-005; Evidence: contract suite collection）

### Minimal implementation

- [X] T011 根据 RED 测试扩展 trpc_service/storage/models.py，实现 scope、generation、execution phase、recovery marker 和显式状态转换校验（Trace: US2-US5; FR-008-FR-016/021-FR-024; D-003-001-D-003-004; Evidence: T008 转绿）
- [X] T012 将 trpc_service/storage/contracts.py 升级为异步 Protocol，加入 lease、fence、recovery、query 能力和稳定领域错误（Trace: US1-US5; FR-004/017/019/020/025/026/029; D-003-001-D-003-005; Evidence: T009 类型与运行时契约转绿）
- [X] T013 [P] 更新 trpc_service/storage/inmemory.py，使第二阶段 InMemory 配置、幂等、审计和恢复实现满足新异步契约（Trace: US1/US2/US4/US5; FR-001/025/026; D-003-003/004/005; Evidence: 既有和共享契约套件转绿）
- [X] T014 [P] 更新 trpc_service/storage/locks.py 和现有 InMemory Session adapter，使其满足异步 lease/fence 端口并保持单进程语义（Trace: US1/US3/US5; FR-012-FR-014/025/026; D-003-001/002; Evidence: InMemory 契约转绿）
- [X] T015 更新 trpc_service/gateway/service.py、trpc_service/worker/service.py 和 trpc_service/web/app.py，统一 await 新端口并保持 002 HTTP、Gateway、Runner 与回复契约（Trace: US1-US5; FR-001/004/025; D-003-001-D-003-005; Evidence: 002 回归通过）
- [X] T016 实现 trpc_service/config/settings.py 的 local/shared profile、node_id、Redis/SQL DSN reference、租约和 Agent timeout 校验（Trace: US1-US5; FR-002/014/017/028/029; D-003-001/005; Evidence: T007 转绿）
- [X] T017 [P] 创建 trpc_service/storage/postgres/models.py 和 trpc_service/storage/postgres/migrations/001_shared_state.sql，定义 Tenant、Agent、ChannelBinding、AuditLog、RecoveryMarker 约束、索引与版本（Trace: US1/US4/US5; FR-005/019-FR-022; D-003-002-D-003-005; Evidence: 空库与重复迁移测试）
- [X] T018 [P] 在 trpc_service/storage/redis_codec.py 和 trpc_service/storage/redis_scripts/loader.py 实现 tenant/agent/session key 编码、SCRIPT LOAD/EVALSHA 与 NOSCRIPT 恢复（Trace: US1-US4; FR-005/008/012/014/017; D-003-001-D-003-004; Evidence: key 隔离与 loader 单测）
- [X] T019 [P] 在 trpc_service/storage/postgres/database.py 实现 async engine、transaction boundary、连接健康检查和不泄露 DSN 的异常转换（Trace: US1/US4/US5; FR-017/019-FR-022/028/029; D-003-004/005; Evidence: 数据库生命周期测试）
- [X] T020 在 trpc_service/storage/shared.py 建立 shared adapter 组合根，在 trpc_service/_cli.py 暴露 trpc-agent-shared-init 与 trpc-agent-shared-serve 骨架，local profile 保持默认（Trace: US1-US5; FR-001-FR-004/017/025/030; D-003-001-D-003-005; Evidence: CLI help 与 local 回归）
- [X] T021 运行 Phase 2 单元、契约和 002 回归，把 RED→GREEN 命令与结果追加到 specs/003-shared-state-multinode-flow/validation-results.md 的 Foundational 节（Trace: US1-US5; FR-001/025/026/030; D-003-001-D-003-005; Evidence: Phase 2 checkpoint）

**Checkpoint**: local profile 无回归；shared profile 可初始化依赖和装配端口，但尚未宣称用户故事完成。

---

## Phase 3: User Story 1 - 任意健康节点继续租户会话（Priority: P1） 🎯 MVP

**Goal**: 两个独立 Worker 任意路由且不使用 sticky session，仍能读取同一 tenant-scoped Session 的多轮上下文并隔离不同租户。

**Independent Test**: 节点 A 写首轮、节点 B 读取并回复后续轮；同名 session/external ID 在另一租户不可见；重启节点后已确认上下文仍可读取。

### Tests first

- [X] T022 [P] [US1] 在 tests/contract/test_shared_configuration_repository.py 编写失败契约，覆盖 Tenant/Agent/Binding 版本、secret_ref、未知租户与错误 Binding 默认拒绝（Trace: US1; FR-005/018/019/025/026; D-003-005; Evidence: RED 契约输出）
- [X] T023 [P] [US1] 在 tests/contract/test_shared_session_repository.py 编写失败契约，覆盖 tenant+agent+session scope、事件顺序/唯一、跨实例读取和 restart 后读取（Trace: US1; FR-004-FR-007/023/025/026; D-003-002; Evidence: RED 契约输出）
- [X] T024 [P] [US1] 在 tests/integration/shared/test_cross_node_session.py 编写失败测试，覆盖 A→B 20 轮交替、同租户多轮、跨租户/Agent 隔离和随机路由（Trace: US1; FR-002-FR-007/023/024; D-003-002/005; Evidence: RED 集成输出）
- [X] T025 [US1] 在 tests/e2e/test_two_worker_processes.py 编写失败进程测试，启动 8001/8002 两节点，验证无 sticky session、节点重启后继续会话和 HTTP 契约不变（Trace: US1; FR-001-FR-003/007/024; D-003-002/005; Evidence: RED E2E 输出）

### Minimal implementation

- [X] T026 [P] [US1] 在 trpc_service/storage/postgres/repositories.py 实现 Tenant、Agent、ChannelBinding 权威查询、版本和 secret_ref 访问，不缓存授权结论（Trace: US1; FR-005/018/019/025; D-003-005; Evidence: T022 转绿）
- [X] T027 [P] [US1] 在 trpc_service/storage/redis_scripts/session_append.lua、session_read.lua 和 trpc_service/storage/redis_session.py 实现有序唯一 Session/Event 共享存储（Trace: US1; FR-004/006/007/023/025; D-003-002; Evidence: T023 转绿）
- [X] T028 [US1] 在 trpc_service/storage/redis_session.py 实现面向官方 BaseSessionService 的 FencedRedisSessionService adapter，保持 Runner/Event/Session 边界（Trace: US1; FR-001/004/006/023/025; D-003-002; Evidence: 官方 Runner 集成通过）
- [X] T029 [US1] 更新 trpc_service/worker/service.py 与 trpc_service/web/app.py，按 profile 注入共享配置和 Session adapter，Worker 不保留业务状态（Trace: US1; FR-002-FR-007/025; D-003-002/005; Evidence: T024 转绿）
- [X] T030 [US1] 完成 trpc_service/_cli.py 的双进程 node_id/port 启动、依赖初始化和优雅关闭（Trace: US1; FR-002/003/007/017/030; D-003-005; Evidence: T025 转绿）
- [X] T031 [US1] 运行 US1 契约、20 轮交替、跨租户隔离和进程重启测试，把输出追加到 specs/003-shared-state-multinode-flow/validation-results.md 的 US1 节（Trace: US1; FR-001-FR-007/018/019/023-FR-026; D-003-002/005; Evidence: US1 checkpoint）

**Checkpoint**: 不依赖 sticky session 的跨节点多轮会话与租户隔离可独立演示。

---

## Phase 4: User Story 2 - 跨节点重复投递只执行一次（Priority: P1）

**Goal**: 相同 idempotency scope 被不同节点并发接收时只有一个 owner，Agent、Session Event 和回复投递最多发生一次。

**Independent Test**: 对 50 组同 tenant+binding+external_message_id 并发投递到 A/B；每组仅一个 owner/execution/event/delivery，其余返回 processing 或缓存结果；不同内容返回 conflict。

### Tests first

- [X] T032 [P] [US2] 在 tests/contract/test_shared_idempotency_repository.py 编写失败契约，固定 scope、content fingerprint、原子 claim、generation、processing/completed/conflict 和 trace 字段（Trace: US2; FR-008-FR-011/024-FR-026; D-003-003; Evidence: RED 契约输出）
- [X] T033 [P] [US2] 在 tests/integration/shared/test_cross_node_idempotency.py 编写失败测试，覆盖 100 次顺序重复、50 组跨节点并发重复和不同租户同 external ID（Trace: US2; FR-002/003/005/008-FR-011/024; D-003-003; Evidence: RED 集成输出）
- [X] T034 [US2] 在 tests/integration/shared/test_duplicate_http_contract.py 编写失败 HTTP 测试，验证 processing、cached、conflict 的稳定状态码/错误码且不泄露后端细节（Trace: US2; FR-001/011/029; D-003-003; Evidence: RED HTTP 输出）

### Minimal implementation

- [X] T035 [P] [US2] 在 trpc_service/storage/redis_scripts/idempotency_claim.lua、idempotency_complete.lua 和 idempotency_inspect.lua 实现原子 claim/CAS/读取和 owner generation（Trace: US2; FR-008-FR-011/017; D-003-003; Evidence: Lua 并发契约转绿）
- [X] T036 [US2] 在 trpc_service/storage/redis_idempotency.py 实现 Repository、content fingerprint、owner/execution trace 保存和稳定领域错误映射（Trace: US2; FR-008-FR-011/024/025/029; D-003-003; Evidence: T032 转绿）
- [X] T037 [US2] 更新 trpc_service/gateway/service.py，在 Session 与 Agent 前执行 message claim，实现 owner、processing、cached、conflict 四条分支（Trace: US2; FR-008-FR-011/024; D-003-003; Evidence: T033/T034 核心断言转绿）
- [X] T038 [US2] 更新 trpc_service/worker/service.py 和 trpc_service/storage/redis_session.py，以 idempotency scope 抑制重复 Agent、Session Event 与回复提交（Trace: US2; FR-009/011/023/024; D-003-002/003; Evidence: 每组执行/事件/投递计数均为 1）
- [X] T039 [US2] 运行 US2 契约、100 次顺序重复、50 组跨节点并发和 HTTP 错误测试，把输出追加到 specs/003-shared-state-multinode-flow/validation-results.md 的 US2 节（Trace: US2; FR-001-FR-003/008-FR-011/023-FR-026/029; D-003-002/003; Evidence: US2 checkpoint）

**Checkpoint**: 跨节点重复请求最多执行一次，冲突和缓存结果具有稳定外部语义。

---

## Phase 5: User Story 3 - 跨节点保持会话顺序（Priority: P2）

**Goal**: 同一 tenant+agent+session 跨节点串行，不同会话并行；租约续期和 fencing 阻止过期 owner 写入业务状态。

**Independent Test**: 同会话 50 组竞争峰值并发为 1；两个不同会话确实重叠；旧 generation 对 Session、幂等终态和业务 Audit 的写入全部被拒绝。

### Tests first

- [X] T040 [P] [US3] 在 tests/contract/test_shared_lease_repository.py 编写失败契约，覆盖 Redis PTTL 权威、原子 acquire/renew/release、仅当前且未过期 generation 可续期（Trace: US3; FR-012/014/025/026; D-003-001; Evidence: RED 租约契约）
- [X] T041 [P] [US3] 在 tests/contract/test_fenced_business_writes.py 编写失败契约，覆盖旧 generation 对 Session/Event、幂等终态、业务 Audit 的写拒绝及当前平台身份追加诊断审计（Trace: US3; FR-013/020/023/025/026; D-003-002; Evidence: RED fencing 契约）
- [X] T042 [P] [US3] 在 tests/integration/shared/test_session_serialization.py 编写失败测试，覆盖同会话跨节点串行、不同会话并行和无 sticky session（Trace: US3; FR-002/003/012/027; D-003-001; Evidence: RED 并发输出）
- [X] T043 [US3] 在 tests/integration/shared/test_fencing.py 编写失败测试，覆盖续期竞态、租约过期、新 generation 接管及旧 owner 全部晚到写（Trace: US3; FR-013-FR-016/020/023/024; D-003-001/002/003; Evidence: RED 故障时序）

### Minimal implementation

- [X] T044 [P] [US3] 在 trpc_service/storage/redis_scripts/lease_acquire.lua、lease_renew.lua 和 lease_release.lua 实现 PTTL 驱动的原子租约与单调 generation（Trace: US3; FR-012/014/017; D-003-001; Evidence: T040 转绿）
- [X] T045 [US3] 在 trpc_service/storage/redis_leases.py 实现 acquire wait、10 秒默认租期、3 秒 heartbeat、取消和 lease-lost 信号，测试可注入短参数（Trace: US3; FR-012/014/027; D-003-001; Evidence: 续期/超时测试）
- [X] T046 [US3] 在 trpc_service/storage/redis_session.py 和 trpc_service/storage/redis_idempotency.py 为所有业务写增加 generation CAS（Trace: US3; FR-013/023/025; D-003-002; Evidence: T041 业务写拒绝转绿）
- [X] T047 [US3] 在 trpc_service/storage/postgres/repositories.py 实现带 generation 校验的业务 Audit 和由当前平台身份追加的 immutable stale-write 诊断审计（Trace: US3; FR-013/020/024; D-003-002; Evidence: T041 审计断言转绿）
- [X] T048 [US3] 更新 trpc_service/gateway/service.py，按 tenant+agent+session 获取租约、启动 heartbeat、传播 fence context，并在失租后隔离旧 owner（Trace: US3; FR-012-FR-016/024; D-003-001/002/003; Evidence: T042/T043 转绿）
- [X] T049 [US3] 在 trpc_service/metrics.py 增加 node/tenant/backend/session 匿名维度的 lease wait、renew failure、stale write 和并发度指标（Trace: US3; FR-005/027; D-003-001/002; Evidence: 指标单元断言）
- [X] T050 [US3] 运行 US3 租约/fencing 契约、50 组同会话串行、20 组不同会话并行和 stale write 测试，把输出追加到 specs/003-shared-state-multinode-flow/validation-results.md 的 US3 节（Trace: US3; FR-002/003/012-FR-016/020/023-FR-027; D-003-001-D-003-003; Evidence: US3 checkpoint）

**Checkpoint**: 同会话严格串行、不同会话并行，过期 generation 无法污染业务状态。

---

## Phase 6: User Story 4 - 节点和共享后端故障可安全恢复（Priority: P2）

**Goal**: 节点中断、租约超时、Redis/SQL 短暂不可用和部分提交有明确 fail-closed、takeover 或 outcome_unknown 语义，恢复不重跑 Agent。

**Independent Test**: 在 claim 后、EXECUTION_STARTED 前后、Agent 返回后、SQL 提交后和 Redis 终态前注入故障；仅持久化 pre-start 证据允许接管，其余未知/已开始状态不重放；恢复器只补终态。

### Tests first

- [X] T051 [P] [US4] 在 tests/integration/shared/test_node_takeover.py 编写失败 kill-point 测试，证明 heartbeat/失联不是执行证据，只有 durable EXECUTION_STARTED 决定可否接管（Trace: US4; FR-010/014-FR-016/024; D-003-001/003; Evidence: RED 接管矩阵）
- [X] T052 [P] [US4] 在 tests/integration/shared/test_configuration_outage.py 编写失败测试，覆盖 SQL 不可用、未知租户、Binding 版本变化和已有正缓存时仍禁止授权（Trace: US4; FR-017-FR-019/029; D-003-005; Evidence: RED fail-closed 输出）
- [X] T053 [P] [US4] 在 tests/integration/shared/test_backend_outages.py 编写失败测试，覆盖 Redis/SQL 在 claim、lock、session、audit 阶段短暂不可用，无 InMemory fallback 且返回稳定安全错误（Trace: US4; FR-017/021/028/029; D-003-004/005; Evidence: RED outage matrix）
- [X] T054 [P] [US4] 在 tests/integration/shared/test_partial_commit_recovery.py 编写失败测试，覆盖 final Audit+RecoveryMarker SQL 事务成功但 Redis terminal CAS 失败时 outcome_unknown，恢复器复用结果/引用且不调用 Agent（Trace: US4; FR-015/016/020-FR-022/024; D-003-003/004; Evidence: RED partial-commit 输出）
- [X] T055 [P] [US4] 在 tests/integration/shared/test_worker_restart_recovery.py 编写失败测试，覆盖 20 轮 pre-start/post-start 节点中断、lease expiry、新节点检查共享证据和 restart 恢复（Trace: US4; FR-007/014-FR-017/021/022; D-003-001/003/004; Evidence: RED restart 输出）
- [X] T056 [P] [US4] 在 tests/contract/test_shared_error_semantics.py 编写失败契约，固定 backend_unavailable、authorization_unavailable、processing、conflict、outcome_unknown、audit_incomplete 安全 HTTP 映射（Trace: US4; FR-011/016-FR-018/021/029; D-003-003-D-003-005; Evidence: RED 错误契约）

### Minimal implementation

- [X] T057 [US4] 扩展 trpc_service/storage/redis_idempotency.py，在 Agent 前持久化 PRE_START/EXECUTION_STARTED，仅 durable PRE_START 明确存在时允许新 generation 接管（Trace: US4; FR-010/015/016/024; D-003-003; Evidence: T051 转绿）
- [X] T058 [US4] 在 trpc_service/storage/postgres/repositories.py 实现 final Audit 与 TERMINAL_PENDING RecoveryMarker 同事务提交、结果或稳定 result_ref 保存和幂等状态转换（Trace: US4; FR-020-FR-022/024; D-003-004; Evidence: T054 事务断言转绿）
- [X] T059 [US4] 在 trpc_service/recovery/reconciler.py 实现 RecoveryMarker 扫描、当前 generation 校验、Redis terminal CAS 和完成标记，代码路径不得引用 AgentExecutor（Trace: US4; FR-016/021/022; D-003-003/004; Evidence: 恢复器 Agent 调用计数为 0）
- [X] T060 [US4] 更新 trpc_service/gateway/service.py，实现 Agent 前 durable execution phase、final Audit→RecoveryMarker→Redis terminal 顺序和 audit_incomplete/outcome_unknown 分支（Trace: US4; FR-015/016/020-FR-022/024; D-003-003/004; Evidence: T054/T056 转绿）
- [X] T061 [US4] 在 trpc_service/config/cache.py 实现仅作性能提示的版本化缓存；权威 SQL 读取失败时拒绝授权，缓存不得成为授权来源（Trace: US4; FR-017-FR-019/029; D-003-005; Evidence: T052 转绿）
- [X] T062 [US4] 在 trpc_service/web/app.py 添加 /readyz 共享依赖检查和恢复器生命周期，保留 /healthz 存活语义（Trace: US4; FR-017/027/029; D-003-004/005; Evidence: readiness/outage 测试）
- [X] T063 [US4] 在 tests/integration/shared/faults.py 完成可重复 kill point、Redis/SQL proxy failure 和 Agent spy 控制器，确保每个故障可证明注入位置（Trace: US4; FR-007/014-FR-017/021/030; D-003-001/003/004/005; Evidence: 故障注入自检）
- [X] T064 [US4] 运行 US4 接管、20 轮节点中断、共享后端 outage matrix、部分提交恢复和错误语义测试，把输出追加到 specs/003-shared-state-multinode-flow/validation-results.md 的 US4 节（Trace: US4; FR-007/010/011/014-FR-022/024/029/030; D-003-001-D-003-005; Evidence: US4 checkpoint）

**Checkpoint**: 每个故障点均能解释为拒绝、接管、处理中、结果未知或补终态之一，并有共享证据支持，Agent 最多执行一次。

---

## Phase 7: User Story 5 - 跨节点追踪、审计与替换边界验证（Priority: P3）

**Goal**: 运维人员可按 tenant/session/trace 跨节点查询成功、重复、冲突、接管、stale write 和 outage；同一契约套件验证 InMemory 与 shared adapter。

**Independent Test**: 从另一节点查询每类结果，first trace_id、owner_trace_id、execution_trace_id 和 generation 可串联；两套 adapter 运行相同厂商无关契约。

### Tests first

- [X] T065 [P] [US5] 在 tests/contract/test_shared_audit_repository.py 编写失败契约，覆盖 immutable append、tenant/agent/session/trace 查询、诊断审计关联和 RecoveryMarker 查询（Trace: US5; FR-005/013/020-FR-026; D-003-002/004; Evidence: RED 审计契约）
- [X] T066 [P] [US5] 在 tests/integration/shared/test_cross_node_traceability.py 编写失败测试，覆盖 success/duplicate/conflict/takeover/stale-write/outage 的三类 trace、generation、node_id 与状态关联（Trace: US5; FR-010/020/024/027; D-003-001-D-003-005; Evidence: RED 追踪输出）
- [X] T067 [P] [US5] 在 tests/contract/test_adapter_substitutability.py 编写参数化失败测试，对 InMemory 与 Redis/PostgreSQL adapter 运行相同 Repository/Adapter 契约且不含厂商断言（Trace: US5; FR-025/026/030; D-003-001-D-003-005; Evidence: 两 backend 同名测试报告）
- [X] T068 [P] [US5] 在 tests/unit/test_observability_security.py 编写失败测试，验证 metrics/audit/error 不输出 DSN、secret_ref 值、消息原文或 Redis/SQL 内部细节（Trace: US5; FR-005/027-FR-030; D-003-002/005; Evidence: RED 安全断言）

### Minimal implementation

- [X] T069 [US5] 扩展 trpc_service/storage/postgres/repositories.py，实现 tenant/agent/session/trace scope 的 immutable Audit 与 RecoveryMarker 查询并关联 stale generation（Trace: US5; FR-005/013/020-FR-022/024; D-003-002/004; Evidence: T065/T066 转绿）
- [X] T070 [US5] 扩展 trpc_service/metrics.py 和 trpc_service/audit/models.py，统一 node_id、匿名 tenant、backend、lease、first/owner/execution trace 标签并脱敏（Trace: US5; FR-005/010/020/024/027-FR-029; D-003-001-D-003-005; Evidence: T066/T068 转绿）
- [X] T071 [US5] 修正 trpc_service/storage/inmemory.py 与 shared adapters 的契约差异，使 tests/contract/test_adapter_substitutability.py 全通过且不削弱 shared 安全语义（Trace: US5; FR-025/026/030; D-003-001-D-003-005; Evidence: T067 两套 adapter 全绿）
- [X] T072 [US5] 运行 US5 审计、跨节点 trace、adapter 替换和可观测性安全测试，把输出追加到 specs/003-shared-state-multinode-flow/validation-results.md 的 US5 节（Trace: US5; FR-005/010/013/020-FR-030; D-003-001-D-003-005; Evidence: US5 checkpoint）

**Checkpoint**: 每类业务与故障结果都可跨节点追踪，InMemory/shared 可替换边界有同套契约证据。

---

## Phase 8: Polish & Cross-Cutting Validation

**Purpose**: 收敛全量回归、可复现实验、答辩证据和范围声明，不新增业务语义。

- [X] T073 运行 tests/unit/、tests/contract/、tests/integration/、tests/e2e/ 和 tests/sdk_validation/ 全量测试，把精确命令、通过数、耗时和环境写入 specs/003-shared-state-multinode-flow/validation-results.md（Trace: US1-US5; FR-001-FR-030; D-003-001-D-003-005; Evidence: Full Regression）
- [X] T074 [P] 执行 tests/integration/shared/test_cross_node_session.py 的 200 次随机路由及同/不同会话并发场景，把 p95、错误数和执行计数写入 specs/003-shared-state-multinode-flow/validation-results.md（Trace: US1-US3; FR-002/003/006/008/009/012/027; D-003-001-D-003-003; Evidence: SC-001/002/003/009）
- [X] T075 [P] 重跑 tests/sdk_validation/，确认官方 tRPC-Agent Runner/Event/Session 接入未被 shared adapter 绕过，把结果写入 specs/003-shared-state-multinode-flow/validation-results.md（Trace: US1-US5; FR-001/004/025/026; D-003-002; Evidence: SDK boundary regression）
- [X] T076 更新 specs/003-shared-state-multinode-flow/quickstart.md，用实际命令完成 10 分钟双节点、故障恢复、清理演示和结果判读（Trace: US1-US5; FR-002/003/017/028-FR-030; D-003-001-D-003-005; Evidence: SC-010 walkthrough）
- [X] T077 [P] 编写 specs/003-shared-state-multinode-flow/阶段成果记录.md，摘要记录阶段目标、五项人类决策、实现边界、测试数字、失败语义和排除范围（Trace: US1-US5; FR-001-FR-030; D-003-001-D-003-005; Evidence: 答辩材料）
- [X] T078 更新 README.md 的第三阶段本地运行与验证入口，明确不含真实企业微信、向量库、Kubernetes、管理后台、真实模型 API 和生产级 Telemetry（Trace: US1-US5; FR-030; D-003-001-D-003-005; Evidence: scope review）
- [X] T079 [P] 对 trpc_service/、deploy/local-shared/ 和 specs/003-shared-state-multinode-flow/ 执行 secret/DSN/消息泄漏和“生产就绪”误导声明扫描，把命令与结论写入 specs/003-shared-state-multinode-flow/validation-results.md（Trace: US4/US5; FR-028-FR-030; D-003-005; Evidence: SC-011/012）
- [X] T080 运行第三阶段一致性与收敛检查，逐项核对 FR-001-FR-030、SC-001-SC-012、D-003-001-D-003-005 和所有任务证据，把未通过项修复或登记到 specs/003-shared-state-multinode-flow/validation-results.md（Trace: US1-US5; FR-001-FR-030; D-003-001-D-003-005; Evidence: final traceability/convergence report）

---

## Dependencies & Execution Order

### Phase dependencies

- Phase 1 → Phase 2：先建立可复现依赖和测试环境。
- Phase 2 → 所有用户故事：异步端口、状态模型、错误语义和组合根是共同门禁。
- US1 与 US2 在 Phase 2 后可并行推进；分别证明共享会话与跨节点幂等。
- US3 依赖 US1 的共享 Session 和 US2 的 message claim，才能对完整业务写实施租约与 fencing。
- US4 依赖 US2、US3 的 owner generation、execution phase 和 lease-lost 语义。
- US5 契约可在 Phase 2 后先写；完整追踪验收依赖 US1-US4 产生全部状态样本。
- Phase 8 依赖五个用户故事 checkpoint 全部通过。

### User story dependency graph

~~~text
Phase 1 Setup
      |
Phase 2 Foundational
      |
  +---+---+
  |       |
 US1     US2
  |       |
  +---+---+
      |
     US3
      |
     US4
      |
     US5
      |
Phase 8 Polish
~~~

### Test-first gate for every story

1. 完成本故事全部 Tests first 任务。
2. 运行测试并确认因能力缺失而失败，而不是 fixture、语法或环境错误。
3. 将 RED 命令与最小失败摘要写入 validation-results.md。
4. 执行 Minimal implementation，禁止提前实现后续故事。
5. 运行故事独立测试与必要回归，记录 GREEN 后再进入下一阶段。

---

## Parallel Execution Examples

### US1

- T022、T023、T024 可并行编写；T025 在双进程 fixture 接口固定后进行。
- T026 PostgreSQL 配置仓储与 T027 Redis Session 仓储可并行；T028 依赖 T027。

### US2

- T032、T033、T034 可并行编写。
- T035 后 T036 固定 Repository；T037/T038 再顺序整合 Gateway 与 Worker。

### US3

- T040、T041、T042 可并行编写；T043 复用前述 fixture。
- T044/T045 租约实现与 T047 PostgreSQL 诊断审计可并行；T046/T048 在端口稳定后集成。

### US4

- T051-T056 的故障矩阵可并行编写，但共享 fixture 修改需协调。
- T058 SQL 事务、T059 恢复器与 T061 配置缓存可并行；T060 最后串联提交顺序。

### US5

- T065-T068 可并行编写。
- T069 查询实现与 T070 指标/脱敏可并行；T071 最后处理两套 adapter 契约差异。

---

## Implementation Strategy

### MVP first

1. 完成 Phase 1、Phase 2，保持 002 全部回归通过。
2. 完成 US1，交付“两个节点无 sticky session 的跨节点多轮会话”演示。
3. 完成 US2，形成 P1 最小安全闭环：共享 Session + 跨节点最多执行一次。
4. 通过 P1 checkpoint 后再进入租约/fencing 与故障恢复。

### Incremental delivery

- Increment 1：共享配置和 Session，可演示跨节点上下文。
- Increment 2：共享幂等，可演示并发重复最多执行一次。
- Increment 3：会话租约与 fencing，可演示串行、并行和 stale write 拒绝。
- Increment 4：执行证据、恢复标记和 fail-closed，可演示节点/后端故障。
- Increment 5：审计、三类 trace、同套 adapter 契约与答辩证据。

## Completion Criteria

- 所有任务符合 checklist/ID/可选并行标记/用户故事标签格式。
- 每个用户故事都能用其 Independent Test 单独验收。
- 每项实现任务之前已有失败测试；每阶段都有 RED 与 GREEN 证据。
- FR-001-FR-030 和 D-003-001-D-003-005 均至少被一个测试任务及一个实现/验证任务覆盖。
- 第二阶段外部契约与官方 tRPC-Agent SDK 边界持续通过回归。
- 无 Redis/SQL 失败时 InMemory 回退；无配置正缓存授权；恢复器不重跑 Agent。

---

description: "第六阶段租户治理与工具安全策略的测试先行任务清单"
---

# Tasks: 租户治理与工具安全策略

**Feature**: `006-tenant-governance-tool-policy`
**Git Branch**: `feature/luwenjie`
**Input**: `spec.md`、`clarification-decisions.md`、`plan.md`、`research.md`、`data-model.md`、`contracts/`、`quickstart.md`
**Method**: 严格测试先行。每个实现批次开始前必须先运行对应新测试并确认因“能力尚未实现”而失败（RED），不得以导入错误、环境错误或错误断言冒充 RED；实现后运行相同测试确认通过（GREEN），命令和结果持续记录到 `specs/006-tenant-governance-tool-policy/validation-results.md`。

## Format: `[ID] [P?] [Story] Description`

- `[P]`：可与同阶段其他标记任务并行，且不修改相同文件。
- `[US1]`…`[US6]`：对应 `spec.md` 的用户故事；Setup、Foundational 和最终收敛任务按规则不带故事标签。
- 每项均标明目标文件、FR、DEC（不适用写 N/A）和验收证据。
- 未形成 RED/GREEN 记录的实现任务不得勾选完成；skip 必须写明原因，不能把共享后端连接失败算作通过。

## Phase 1: Setup（测试与证据框架）

**Purpose**: 建立本阶段目录、确定性测试替身和可持续追加的验证记录，不改变现有消息闭环语义。

- [X] T001 创建包含环境、commit、命令、RED、GREEN、通过/失败/skip、US/FR/DEC 和证据字段的记录模板 `specs/006-tenant-governance-tool-policy/validation-results.md`（关联：Shared；FR-031、FR-033；DEC：N/A；证据：模板可逐任务追加且无凭据字段）
- [X] T002 运行 `uv run pytest -q` 建立第二/三/五阶段回归基线，并把完整统计与环境条件写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（关联：Shared；FR-033；DEC：N/A；证据：baseline 无失败，所有 skip 有原因）
- [X] T003 [P] 创建治理源码与测试包骨架 `trpc_service/governance/__init__.py`、`tests/unit/governance/__init__.py`、`tests/contract/governance/__init__.py`、`tests/integration/governance/__init__.py`、`tests/e2e/governance/__init__.py`（关联：Shared；FR-004、FR-030；DEC：N/A；证据：pytest 可发现新目录且未新增业务逻辑）
- [X] T004 [P] 建立双租户、双节点、确定性策略/Agent/工具/用量/时钟测试构造器 `tests/governance_support.py`（关联：Shared；FR-031、FR-032；DEC：ACA；证据：fixture 不依赖真实模型、真实危险工具或真实 IM 凭据）

**Checkpoint**: 基线结果已记录，测试基础可用，尚未改变生产行为。

---

## Phase 2: Foundational（所有用户故事的阻塞基础）

**Purpose**: 先用失败测试固定公共模型、错误、Repository 端口、schema 和官方 SDK 接入边界，再实现最小基础。

**⚠️ CRITICAL**: T005–T008 必须全部获得有效 RED 证据后，才可执行 T009–T012；Phase 2 未 GREEN 前不得进入用户故事。

### Tests First — RED

- [X] T005 [P] 编写租户作用域、不可变策略、主体摘要、工具描述、确认/预算状态和稳定错误的失败单元测试 `tests/unit/governance/test_models_and_errors.py`，运行并记录 RED（关联：Shared；FR-002、FR-006、FR-012、FR-014、FR-017、FR-023；DEC-001/002/003；证据：模型不变量和 fail-closed 错误断言）
- [X] T006 [P] 编写 Policy、Grant、Budget、Confirmation、GovernanceRecovery Repository 端口的失败契约测试 `tests/contract/governance/test_repository_ports.py`，运行并记录 RED（关联：Shared；FR-004、FR-027、FR-030；DEC-001/002/003；证据：异步端口、tenant scope、幂等/fencing 行为均有断言）
- [X] T007 [P] 编写从既有 schema 无损升级到治理表和可空审计/恢复字段的失败测试 `tests/integration/governance/test_schema_upgrade.py`，运行并记录 RED（关联：Shared；FR-004、FR-024、FR-027、FR-033；DEC-001/003；证据：旧数据保留、重复 migration 幂等）
- [X] T008 [P] 编写固定版本 `trpc-agent-py==1.1.19` 的 AgentContext metadata、model/tool callback 与官方 Tool 执行顺序兼容性失败测试 `tests/sdk_validation/test_governance_callbacks.py`，运行并记录 RED（关联：Shared；FR-009、FR-011、FR-034；DEC-001；证据：before-tool 可在工具函数前阻断且没有自建 Runner）

### Minimal Foundation — GREEN

- [X] T009 实现不可变治理值对象、枚举、状态转换和稳定 domain error `trpc_service/governance/models.py`、`trpc_service/governance/errors.py`，使 T005 GREEN（关联：Shared；FR-002、FR-006、FR-012、FR-014、FR-017、FR-023；DEC-001/002/003；证据：T005 全通过）
- [X] T010 实现策略、授权、预算、确认与治理恢复的异步 Protocol 及 fail-closed 异常映射 `trpc_service/storage/contracts.py`，使 T006 的端口检查 GREEN（关联：Shared；FR-004、FR-020、FR-029、FR-030；DEC-001/002/003；证据：端口测试不依赖具体后端）
- [X] T011 新增治理 schema、约束、索引及 ORM/row 映射 `trpc_service/storage/postgres/migrations/005_governance.sql`、`trpc_service/storage/postgres/models.py`，覆盖不可变策略、active 指针、授权、预算和恢复字段（关联：Shared；FR-002、FR-004、FR-017、FR-024、FR-027；DEC-001/003；证据：T007 schema 断言通过且旧表数据不丢失）
- [X] T012 更新迁移加载和共享存储初始化 `trpc_service/storage/postgres/database.py`、`trpc_service/storage/shared.py`，固定官方 callback 兼容入口而不复制 SDK `trpc_service/worker/service.py`（关联：Shared；FR-027、FR-030、FR-033、FR-034；DEC-001；证据：T007/T008 GREEN，重复初始化成功）
- [X] T013 运行 Phase 2 单元、契约、schema 和 SDK validation 测试并把 RED→GREEN 命令、统计和失败修复摘要写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（关联：Shared；FR-030、FR-031、FR-034；DEC：ACA；证据：T005–T008 对应测试全通过）

**Checkpoint**: 公共治理模型、端口、schema 与官方 SDK 接入边界完成；所有用户故事可在此基础上开发。

---

## Phase 3: User Story 1 — 租户策略隔离与默认拒绝（Priority: P1）🎯 MVP

**Goal**: 两个租户只使用自己的不可变有效策略；缺失、禁用、跨租户或不可读取时默认拒绝；策略收紧在提交后对新判断和工具前最终检查立即生效。

**Independent Test**: 两租户配置相反策略并从两个节点发起相同请求；验证无跨租户策略读取，未知策略拒绝，激活收紧版本后旧缓存和未执行操作均不能继续获准。

### Tests First — RED

- [X] T014 [P] [US1] 编写策略文档校验、不可变版本、`DRAFT→ACTIVE→SUPERSEDED/DISABLED`、active generation CAS 和默认拒绝的失败单元测试 `tests/unit/governance/test_policy_lifecycle.py`，运行并记录 RED（FR-002、FR-003、FR-005；DEC-001；证据：非法状态和宽松 fallback 全部失败）
- [X] T015 [P] [US1] 编写 InMemory/PostgreSQL 共用的 Policy Repository 失败契约套件 `tests/contract/governance/test_policy_repository.py`，运行并记录 RED（FR-002、FR-004、FR-005、FR-030；DEC-001；证据：tenant scope、不可变版本、CAS 和 active 读取一致）
- [X] T016 [P] [US1] 编写缓存仅作解析提示、每次授权权威确认 active 指针的失败测试 `tests/unit/governance/test_authoritative_policy_cache.py`，运行并记录 RED（FR-003、FR-005；DEC-001；证据：旧宽松缓存不能授权）
- [X] T017 [US1] 编写双节点、双租户、策略提交后立即生效及治理存储不可用的失败集成测试 `tests/integration/governance/test_policy_isolation_and_invalidation.py`，运行并记录 RED（FR-001–FR-005、FR-029、FR-032；DEC-001；证据：跨租户错误授权为 0，故障时 Runner 调用为 0）

### Implementation — GREEN

- [X] T018 [P] [US1] 实现策略 schema 解析、作用域合并、生命周期与稳定决定 `trpc_service/governance/policy.py`（FR-002、FR-003、FR-005；DEC-001；证据：T014 GREEN）
- [X] T019 [P] [US1] 实现 tenant-scoped InMemory Policy Repository `trpc_service/storage/inmemory.py`（FR-004、FR-030；DEC-001；证据：T015 的 InMemory 参数组 GREEN）
- [X] T020 [US1] 实现不可变版本、active 指针 CAS、禁用与权威读取的 PostgreSQL Policy Repository `trpc_service/storage/postgres/repositories.py`（FR-002、FR-004、FR-005；DEC-001；证据：T015 的 PostgreSQL 参数组 GREEN）
- [X] T021 [US1] 实现只缓存同版本解析结果的权威策略读取器并接入共享 composition root `trpc_service/governance/policy.py`、`trpc_service/storage/shared.py`（FR-003、FR-005、FR-027；DEC-001；证据：T016 GREEN）
- [X] T022 [US1] 实现 Gateway 治理准入骨架，忽略入站 tenant/policy/permission 声明并在 Runner 前 fail closed `trpc_service/governance/service.py`、`trpc_service/gateway/service.py`（FR-001、FR-003、FR-009、FR-033；DEC-001；证据：T017 仅因后续未实现能力之外的策略场景 GREEN）
- [X] T023 [US1] 运行 US1 单元、契约和双节点集成测试，将 RED→GREEN、采用的 policy version、旧缓存拒绝和跨租户 0 串用证据写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（FR-001–FR-005、FR-032；DEC-001；证据：US1 独立测试全通过）

**Checkpoint**: US1 可独立演示，是本阶段最小 MVP；未配置 active policy 的现有租户会明确拒绝而非隐式放行。

---

## Phase 4: User Story 2 — IM 用户身份授权（Priority: P1）

**Goal**: 只允许当前可信 tenant/channel/binding 下获得有效授权的稳定渠道主体进入 Agent，且拒绝不会创建或读取业务 Session。

**Independent Test**: 在同一 binding 下测试授权、未授权、禁用、过期、身份缺失和跨租户主体，并用飞书/企业微信相同字符串 ID 证明两者不共享授权。

### Tests First — RED

- [X] T024 [P] [US2] 编写 `ChannelPrincipal` 规范化、tenant-scoped 摘要、同字符串跨渠道隔离和禁止显示名授权的失败单元测试 `tests/unit/governance/test_channel_principal.py`，运行并记录 RED（FR-006、FR-008、FR-022；DEC：N/A；证据：不可信字段无法构造授权主体）
- [X] T025 [P] [US2] 编写 InMemory/PostgreSQL 共用的 Principal Grant 失败契约套件 `tests/contract/governance/test_principal_grant_repository.py`，运行并记录 RED（FR-004、FR-007、FR-008、FR-030；DEC-001；证据：tenant/Agent/binding 交集、过期与禁用语义一致）
- [X] T026 [US2] 编写双 IM 主体授权在 Session 创建/读取之前拒绝的失败集成测试 `tests/integration/governance/test_principal_authorization.py`，运行并记录 RED（FR-006–FR-009、FR-032、FR-033；DEC-001；证据：拒绝主体的 Session 与 Runner 写入计数均为 0）

### Implementation — GREEN

- [X] T027 [P] [US2] 实现稳定渠道主体规范化、租户内不可逆摘要和授权交集求值 `trpc_service/governance/principal.py`（FR-006–FR-008、FR-022；DEC：N/A；证据：T024 GREEN）
- [X] T028 [P] [US2] 实现 Principal Grant 的 tenant-scoped InMemory Repository `trpc_service/storage/inmemory.py`（FR-004、FR-007、FR-030；DEC-001；证据：T025 InMemory 参数组 GREEN）
- [X] T029 [US2] 实现 Principal Grant 的 PostgreSQL Repository、有效期/禁用读取和跨租户拒绝 `trpc_service/storage/postgres/repositories.py`（FR-004、FR-007、FR-008；DEC-001；证据：T025 PostgreSQL 参数组 GREEN）
- [X] T030 [US2] 从飞书/企业微信已验证 SDK 身份字段构造可信主体并禁止 display name fallback `trpc_service/channels/identity.py`、`trpc_service/channels/feishu.py`、`trpc_service/channels/wecom.py`（FR-006、FR-008、FR-033；DEC：N/A；证据：T024 与现有 Channel mapping 回归 GREEN）
- [X] T031 [US2] 将 principal authorization 放在 Session 读取/创建及预算准入之前 `trpc_service/governance/service.py`、`trpc_service/gateway/service.py`（FR-008、FR-009、FR-029；DEC-001；证据：T026 GREEN，授权存储不可用时 fail closed）
- [X] T032 [US2] 运行 US2 单元、契约、双 IM 集成和既有 Session 回归，将 RED→GREEN 与授权/拒绝/跨渠道隔离证据写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（FR-006–FR-009、FR-032–FR-033；DEC-001；证据：US2 独立测试全通过）

**Checkpoint**: US1 与 US2 均可独立验证；可信 binding 决定租户，稳定渠道主体决定是否有权使用该租户 Agent。

---

## Phase 5: User Story 3 — 工具白名单与危险操作确认（Priority: P1）

**Goal**: 只有当前策略明确允许的工具可以通过官方 Tool callback；危险操作必须由原主体通过文本编号或最小按钮消费同一确认事实，并保证副作用最多一次。

**Independent Test**: 用无真实副作用的计数工具触发允许、禁止、待确认、文本/按钮交叉重放、过期、错身份/Session/参数、策略收紧和节点接管，断言每个操作工具执行最多一次。

### Tests First — RED

- [X] T033 [P] [US3] 编写 ToolDescriptor 默认拒绝、风险分类和官方 before-tool callback 最终授权的失败单元/SDK 测试 `tests/unit/governance/test_tool_policy.py`、`tests/sdk_validation/test_governed_tool_callback.py`，运行并记录 RED（FR-009、FR-011、FR-012、FR-034；DEC-001；证据：禁用/旧策略下计数工具调用为 0）
- [X] T034 [P] [US3] 编写 PendingConfirmation 身份/Session/操作/参数/策略/TTL 绑定、明文禁止、状态转换及与唯一 budget reservation 绑定的失败单元测试 `tests/unit/governance/test_confirmation_state_machine.py`，运行并记录 RED（FR-013–FR-017a、FR-022；DEC-002、DEC-004；证据：非法转换、参数变化、跨租户消费和确认后重复预占均拒绝）
- [X] T035 [P] [US3] 编写 InMemory/Redis 共用的一次性 create/claim/executing/complete/cancel/fencing 失败契约套件 `tests/contract/governance/test_confirmation_repository.py`，运行并记录 RED（FR-004、FR-013–FR-015、FR-027、FR-030；DEC-002；证据：两实现返回相同终态与稳定冲突）
- [X] T036 [P] [US3] 编写飞书与企业微信文本编号、可信按钮回调到同一 `ConfirmationIntent` 及统一回复最小按钮映射的失败契约测试 `tests/contract/governance/test_confirmation_channel_contract.py`，运行并记录 RED（FR-013、FR-014、FR-023、FR-033；DEC-002；证据：两入口引用同一 opaque confirmation，payload 无权限字段）
- [X] T037 [US3] 编写两节点“文本→按钮”和“按钮→文本”各至少 10 次并发/重放的失败集成测试 `tests/integration/governance/test_dual_confirmation_idempotency.py`，运行并记录 RED（FR-013–FR-017a、FR-028、FR-032；DEC-002、DEC-004；证据：每个操作副作用和 reservation/settlement 计数均为 1）
- [X] T038 [US3] 编写 CLAIMED 执行前接管、EXECUTING 结果未知禁止重放、确认过期释放 reservation 及确认后策略立即收紧的失败集成测试 `tests/integration/governance/test_confirmation_takeover.py`，运行并记录 RED（FR-005、FR-015、FR-017a、FR-027–FR-029；DEC-001、DEC-002、DEC-004；证据：旧 generation 写入、自动二次执行和重复预占均为 0）

### Implementation — GREEN

- [X] T039 [P] [US3] 实现稳定 ToolDescriptor、风险/副作用分类与可计数的确定性治理工具 `trpc_service/tool/models.py`、`trpc_service/tool/deterministic.py`（FR-011、FR-012、FR-031；DEC-001；证据：T033 模型与替身部分 GREEN）
- [X] T040 [US3] 通过官方 `LlmAgent.before_tool_callback`/Filter 实现策略、授权、工具白名单和参数内容的执行前最终门禁 `trpc_service/tool/callbacks.py`、`trpc_service/worker/service.py`（FR-005、FR-009、FR-011、FR-034；DEC-001；证据：T033 全部 GREEN且 SDK 原工具调用路径未复制）
- [X] T041 [P] [US3] 实现 confirmation 创建、散列绑定、统一 intent 校验及稳定错误 `trpc_service/governance/confirmation.py`（FR-013–FR-015、FR-022–FR-023；DEC-002；证据：T034 GREEN）
- [X] T042 [P] [US3] 实现 tenant-scoped InMemory PendingConfirmation Repository `trpc_service/storage/inmemory.py`（FR-004、FR-015、FR-030；DEC-002；证据：T035 InMemory 参数组 GREEN）
- [X] T043 [US3] 实现 Redis 原子 claim/状态推进/fencing/TTL `trpc_service/storage/redis_confirmations.py`、`trpc_service/storage/redis_scripts/confirmation_claim.lua`、`trpc_service/storage/redis_scripts/loader.py`（FR-013–FR-015、FR-027–FR-030；DEC-002；证据：T035 Redis 参数组 GREEN）
- [X] T044 [US3] 为既有统一回复增加向后兼容的可选 confirmation 载荷并保持纯文本契约 `trpc_service/channels/contracts.py`（FR-013、FR-023、FR-033；DEC-002；证据：T036 模型契约与第五阶段回复回归 GREEN）
- [X] T045 [US3] 实现飞书/企业微信文本确认 parser、可信按钮 callback 和最小确认按钮发送映射 `trpc_service/channels/feishu.py`、`trpc_service/channels/wecom.py`（FR-013–FR-015、FR-023；DEC-002；证据：T036 全 GREEN，不扩展通用卡片）
- [X] T046 [US3] 串联 Gateway、Worker 和 Recovery 的 confirmation execution_id、租约、generation、结果复用与 outcome-unknown 语义 `trpc_service/governance/service.py`、`trpc_service/gateway/service.py`、`trpc_service/recovery/reconciler.py`（FR-015、FR-024、FR-027–FR-029；DEC-001/002；证据：T037/T038 GREEN）
- [X] T047 [US3] 运行 US3 unit/SDK/contract/双节点集成与双 IM 既有回归，把 RED→GREEN、两种顺序各 10 次、工具最多一次和接管状态写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（FR-009、FR-011–FR-015、FR-028、FR-034；DEC-001/002；证据：US3 独立测试全通过）

**Checkpoint**: 未确认危险操作不会产生副作用；文本与按钮只是同一确认事实的两个入口。

---

## Phase 6: User Story 4 — 租户预算与调用配额（Priority: P1）

**Goal**: 每次受控执行在 Runner 前完整预占租户四维单次最大额度，实际用量只结算一次并释放差额；并发、重复投递、节点中断和回复失败均不能超额或重复扣减。

**Independent Test**: 两个 Worker 的 20 个请求竞争临界额度，只有可完整预占最大值的请求进入执行；actual 小于 maximum 后差额可复用，所有恢复/重复路径实际结算仍为一次。

### Tests First — RED

- [X] T048 [P] [US4] 编写 `REQUESTED/RESERVED/SETTLED/RELEASED/REVIEW_REQUIRED`、四维全有或全无预占、actual≤maximum 和终态不可逆的失败单元测试 `tests/unit/governance/test_budget_state_machine.py`，运行并记录 RED（FR-016–FR-020；DEC-003；证据：所有非法转换与部分预占均失败）
- [X] T049 [P] [US4] 编写 InMemory/PostgreSQL 共用的 reserve/mark-started/settle/release/get/fencing 失败契约套件 `tests/contract/governance/test_budget_repository.py`，运行并记录 RED（FR-004、FR-017–FR-020、FR-030；DEC-003；证据：同 execution_id 重复调用不重复占用/结算）
- [X] T050 [US4] 编写两个节点至少 20 个临界并发、租户隔离和预算后端不可判定的失败集成测试 `tests/integration/governance/test_strict_budget_concurrency.py`，运行并记录 RED（FR-016–FR-020、FR-029、FR-032；DEC-003；证据：`settled + reserved <= hard_limit` 始终成立）
- [X] T051 [P] [US4] 编写剩余额度小于 maximum 拒绝、actual 差额释放、重复消息和回复失败不重复结算的失败集成测试 `tests/integration/governance/test_budget_settlement.py`，运行并记录 RED（FR-017–FR-020、FR-024；DEC-003；证据：低 actual 不能绕过预占，相同 execution 结算一次）
- [X] T052 [US4] 编写预占后/执行后/结果持久后/结算后节点中断和旧 generation 恢复的失败测试 `tests/integration/governance/test_budget_recovery.py`，运行并记录 RED（FR-019、FR-027–FR-029；DEC-003；证据：执行前可释放，执行后未知进入 REVIEW_REQUIRED，已有结果只补结算）

### Implementation — GREEN

- [X] T053 [P] [US4] 实现预算维度、最大/实际用量校验、reservation/settlement 状态机与稳定预算错误 `trpc_service/governance/budget.py`（FR-016–FR-020、FR-023；DEC-003；证据：T048 GREEN）
- [X] T054 [P] [US4] 实现 tenant/execution-scoped InMemory Budget Repository `trpc_service/storage/inmemory.py`（FR-004、FR-017–FR-020、FR-030；DEC-003；证据：T049 InMemory 参数组 GREEN）
- [X] T055 [US4] 实现 PostgreSQL 四维单事务严格预占、幂等结算/释放、唯一键与 fencing Budget Repository `trpc_service/storage/postgres/repositories.py`、`trpc_service/storage/postgres/migrations/005_governance.sql`（FR-017–FR-020、FR-027、FR-030；DEC-003；证据：T049 PostgreSQL 参数组和 T050 GREEN）
- [X] T056 [P] [US4] 将策略 maximum 传入官方 Runner/Tool 限制并从确定性执行结果产生 actual usage `trpc_service/worker/service.py`、`trpc_service/tool/callbacks.py`（FR-016–FR-018、FR-031、FR-034；DEC-003；证据：actual 永不超过 reservation maximum）
- [X] T057 [US4] 在 Gateway 中实现“授权/内容→原子预占→pre-audit→mark-started→执行→结算→出站/交付”的固定顺序 `trpc_service/governance/service.py`、`trpc_service/gateway/service.py`（FR-009、FR-017–FR-020、FR-024；DEC-003；证据：T051 GREEN）
- [X] T058 [US4] 扩展共享恢复器只释放执行前占用、只补齐确定结果结算，并将执行后未知转为 REVIEW_REQUIRED `trpc_service/recovery/reconciler.py`、`trpc_service/storage/shared.py`（FR-019、FR-027–FR-029；DEC-003；证据：T052 GREEN且危险操作/Agent 重执行为 0）
- [X] T059 [US4] 运行 US4 单元、契约和共享后端集成测试，将 RED→GREEN、20 并发、差额释放、重复结算为 0 与 fencing 结果写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（FR-016–FR-020、FR-027–FR-032；DEC-003；证据：US4 测试全通过）
- [X] T060 [US4] 独立执行预算 quickstart 场景并查询 PostgreSQL 账户/reservation 不变量，将脱敏查询摘要写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（FR-018–FR-020、FR-025；DEC-003；证据：批准用量不超预算、相同 execution 只有一份结算）

**Checkpoint**: 四个 P1 故事完成；权限、工具、确认和预算都在产生受控成本或副作用前形成不可突破的门禁。

---

## Phase 7: User Story 5 — 敏感信息保护（Priority: P2）

**Goal**: 同一租户内容策略覆盖入站、工具参数、Agent 输出、渠道回复、日志和审计；支持稳定脱敏与拒绝，检查失败默认拒绝且任何可观察面不出现原始测试敏感值。

**Independent Test**: 用测试手机号、邮箱、token 与 secret 标记穿过所有内容边界，验证 redact/reject、出站阻断、渠道长度处理和日志/持久化 0 原文命中。

### Tests First — RED

- [X] T061 [P] [US5] 编写五类 ContentBoundary、租户规则、稳定占位符、reject 和检查异常 fail-closed 的失败单元测试 `tests/unit/governance/test_content_policy.py`，运行并记录 RED（FR-009、FR-010、FR-021、FR-029；DEC：N/A；证据：命中原文不进入 finding）
- [X] T062 [P] [US5] 编写入站→工具参数→Agent 输出→统一回复完整边界的失败集成测试 `tests/integration/governance/test_content_boundaries.py`，运行并记录 RED（FR-009、FR-010、FR-021、FR-032–FR-033；DEC：N/A；证据：每个边界的 allow/redact/reject 次数明确）
- [X] T063 [P] [US5] 编写源码/日志/trace/指标/错误/审计/数据库样本中原始测试 Secret 为 0 的失败安全测试 `tests/integration/governance/test_sensitive_material.py`，运行并记录 RED（FR-014、FR-021、FR-022、FR-025–FR-026；DEC-002；证据：未脱敏 marker 搜索期望 0）
- [X] T064 [US5] 编写 Agent 输出拒绝、脱敏后渠道长度限制及双 IM 安全错误回复的失败测试 `tests/integration/governance/test_safe_outbound_reply.py`，运行并记录 RED（FR-010、FR-021、FR-023、FR-033；DEC：N/A；证据：不安全原文发送次数为 0）

### Implementation — GREEN

- [X] T065 [P] [US5] 实现租户内容规则、确定性 matcher、redact/reject 和无原文 `RedactionFinding` `trpc_service/governance/content.py`（FR-021、FR-022、FR-029；DEC：N/A；证据：T061 GREEN）
- [X] T066 [US5] 将内容检查接入 Gateway 入站、官方 Tool callback 参数、Worker 输出和统一回复前边界 `trpc_service/governance/service.py`、`trpc_service/tool/callbacks.py`、`trpc_service/worker/service.py`（FR-009、FR-010、FR-021、FR-034；DEC-001；证据：T062 GREEN）
- [X] T067 [P] [US5] 统一 domain error、结构化日志和审计的最小披露/脱敏转换 `trpc_service/governance/errors.py`、`trpc_service/audit/models.py`、`trpc_service/log/__init__.py`（FR-021–FR-025；DEC：N/A；证据：T063 核心可观察面 GREEN）
- [X] T068 [US5] 在飞书与企业微信 Adapter 发送前执行安全回复映射、脱敏后长度限制且禁止回显后端细节 `trpc_service/channels/feishu.py`、`trpc_service/channels/wecom.py`、`trpc_service/channels/service.py`（FR-010、FR-021、FR-023、FR-033；DEC-002；证据：T064 GREEN且既有文本消息回归通过）
- [X] T069 [US5] 运行 US5 单元/集成/双 IM 回归与敏感值扫描，将 RED→GREEN、各边界决定和 0 原文命中证据写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（FR-010、FR-021–FR-023、FR-029、FR-032–FR-033；DEC：N/A；证据：US5 独立测试全通过）

**Checkpoint**: 内容只能以允许或脱敏形式传播；无法确认安全时停止传播而不是绕过。

---

## Phase 8: User Story 6 — 治理审计与可解释性（Priority: P2）

**Goal**: 每个允许、拒绝、待确认、确认、预算与故障决定都能按 trace 解释策略版本、主体摘要、工具、成本和节点角色，同时不泄露敏感值或制造高基数指标。

**Independent Test**: 从两个节点产生六类治理结果，按 tenant/trace/session/agent 查询不可变审计；验证首次 owner、waiter、cached replay 和唯一实际执行可区分，审计不可用时高风险操作不执行。

### Tests First — RED

- [X] T070 [P] [US6] 编写 GovernanceAuditEvent 必填字段、不可变性、三类 trace 与治理指标低基数标签的失败单元测试 `tests/unit/governance/test_audit_and_metrics.py`，运行并记录 RED（FR-024–FR-026；DEC：ACA；证据：tenant/user/session/message/trace 不进入 metric label）
- [X] T071 [P] [US6] 扩展 InMemory/PostgreSQL Audit Repository 的 tenant/trace/session/agent/tool/decision 查询失败契约 `tests/contract/governance/test_governance_audit_repository.py`，运行并记录 RED（FR-004、FR-024、FR-025、FR-030；DEC：ACA；证据：不可变 append 与租户隔离查询一致）
- [X] T072 [US6] 编写允许、拒绝、待确认、确认成功、预算不足、治理故障的跨节点 trace 失败集成测试 `tests/integration/governance/test_governance_traceability.py`，运行并记录 RED（FR-024–FR-028、FR-032；DEC-001/002/003；证据：每类结果可定位唯一策略/执行事实）
- [X] T073 [US6] 编写高风险 pre-audit 不可用默认拒绝、普通失败不记成功及重复消息/确认 owner-waiter-cache 分类的失败测试 `tests/integration/governance/test_governance_audit_failures.py`，运行并记录 RED（FR-024、FR-028、FR-029；DEC-002/003；证据：审计失败时工具调用为 0，实际执行审计最多一份）

### Implementation — GREEN

- [X] T074 [P] [US6] 扩展审计 domain/row 模型与 InMemory/PostgreSQL 不可变 Repository，写入 policy、principal、confirmation、reservation 和 usage 摘要 `trpc_service/audit/models.py`、`trpc_service/storage/inmemory.py`、`trpc_service/storage/postgres/repositories.py`（FR-024、FR-025、FR-030；DEC：ACA；证据：T070/T071 GREEN）
- [X] T075 [US6] 实现治理 pre/post 审计编排、owner/waiter/cached/executed 分类和三类 trace 传播 `trpc_service/governance/service.py`、`trpc_service/gateway/service.py`、`trpc_service/recovery/reconciler.py`（FR-024、FR-025、FR-027–FR-029；DEC：ACA；证据：T072/T073 治理决定部分 GREEN）
- [X] T076 [P] [US6] 实现允许/拒绝、预算、工具耗时/错误的低基数指标模型和共享 recorder `trpc_service/metrics/models.py`、`trpc_service/metrics/shared.py`（FR-025、FR-026；DEC：ACA；证据：T070 metrics GREEN，禁止标签集合通过）
- [X] T077 [US6] 扩展 trace 诊断 CLI/查询输出治理版本、决定、预算和恢复摘要且默认脱敏 `trpc_service/_cli.py`、`trpc_service/web/errors.py`（FR-023–FR-026；DEC：ACA；证据：T072 可用 trace 查询复核，不显示主体原值或后端异常）
- [X] T078 [US6] 运行 US6 单元、契约和双节点集成测试，把 RED→GREEN、六类决定、三类 trace、唯一执行和审计不可用拒绝证据写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（FR-024–FR-029、FR-032；DEC：ACA；证据：US6 独立测试全通过）

**Checkpoint**: 六个用户故事均完成，并能以不可变、脱敏证据解释每个治理结果。

---

## Phase 9: Polish & Cross-Cutting Validation

**Purpose**: 将六个故事合成双 IM、双租户、双节点纵向链路，完成性能、安全、回归、文档与一致性门禁。

- [X] T079 [P] 编写并运行飞书/企业微信替身下双租户“授权→内容→预算→Runner→工具/确认→回复→审计”的组合端到端测试 `tests/e2e/governance/test_dual_im_governed_flow.py`；若首次运行失败先记录有效 RED，若已通过则记录 first-run PASS，禁止制造失败（关联：Shared；FR-001–FR-034；DEC：ACA；证据：两租户 Session/策略/确认/成本/审计串用为 0）
- [X] T080 [P] 编写并运行策略/Redis/PostgreSQL/审计/节点/回复故障点的端到端恢复矩阵 `tests/e2e/governance/test_governance_recovery_matrix.py`，记录每个安全后继与禁止动作（关联：Shared；FR-005、FR-015、FR-019、FR-027–FR-029；DEC：ACA；证据：Agent/危险工具/实际结算均最多一次）
- [X] T081 完成组合链路的 composition root、共享后端 wiring 和 T079/T080 揭示的合法集成缺口 `trpc_service/storage/shared.py`、`trpc_service/_shared_server.py`、`trpc_service/_cli.py`，再运行相同测试 GREEN（关联：Shared；FR-027–FR-034；DEC：ACA；证据：双节点纵向链路全通过，无旁路治理）
- [X] T082 运行 `uv run pytest -q` 及第二/三/五阶段关键回归，将总通过/失败/skip 与 skip 理由记录到 `specs/006-tenant-governance-tool-policy/validation-results.md`（关联：Shared；FR-033、FR-034；DEC：N/A；证据：无失败，现有真实 IM 文本行为兼容）
- [X] T083 [P] 编写并运行不含 Runner/IM 网络耗时的治理准入 p95 与并发预算基准 `tests/integration/governance/test_governance_performance.py`，先记录基线或有效 RED（关联：Shared；FR-018、FR-026；DEC-003；证据：目标 p95<100ms、20 并发不超额）
- [X] T084 仅针对 T083 证明的瓶颈优化权威读取、策略解析或批量审计路径 `trpc_service/governance/service.py`、`trpc_service/governance/policy.py`、`trpc_service/storage/postgres/repositories.py`，保持每次授权的权威校验并记录 GREEN（关联：Shared；FR-005、FR-018；DEC-001/003；证据：性能达标且旧策略不能授权）
- [X] T085 [P] 创建并运行源码、Git diff、日志、错误、审计和数据库样本敏感信息扫描 `specs/006-tenant-governance-tool-policy/scripts/scan-sensitive-material.ps1`，结果写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（关联：Shared；FR-014、FR-022、FR-026；DEC-002；证据：真实凭据与测试敏感原文 0 命中）
- [X] T086 创建可重复执行分层测试、ACA 场景和共享后端检查的验证脚本 `specs/006-tenant-governance-tool-policy/scripts/run-automated-validation.ps1`，按 `specs/006-tenant-governance-tool-policy/quickstart.md` 从干净进程执行并记录结果（关联：Shared；FR-030–FR-033；DEC：ACA；证据：无需真实模型/危险工具/IM Secret 可重复验收）
- [X] T087 更新阶段成果与 README 验收映射 `specs/006-tenant-governance-tool-policy/阶段成果记录.md`、`README.md`，明确官方 tRPC-Agent 复用、平台新增边界、真实/替身验证范围和 ACA 人工决策（关联：Shared；FR-031、FR-033、FR-034；DEC：ACA；证据：文档不夸大生产能力且可用于答辩）
- [X] T088 执行 `$speckit-analyze`、任务格式检查、`git diff --check` 和最终全量测试，将 FR/SC/DEC 完成矩阵、未完成项、限制及最终命令写入 `specs/006-tenant-governance-tool-policy/validation-results.md`（关联：Shared；FR-001–FR-034；DEC：ACA；证据：无 HIGH/CRITICAL 文档遗漏、无格式错误、全量测试无失败）

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 Setup**：无依赖，可立即开始。
- **Phase 2 Foundational**：依赖 Phase 1；阻塞所有用户故事。
- **US1 / Phase 3**：依赖 Phase 2，是策略和默认拒绝 MVP。
- **US2 / Phase 4**：依赖 Phase 2；可用 Policy stub 独立开发，合并顺序建议在 US1 后。
- **US3 / Phase 5**：依赖 US1 的 active policy 与 US2 的 principal authorization。
- **US4 / Phase 6**：依赖 US1；Repository 与并发测试可和 US2/US3 并行，Gateway 最终 wiring 在它们之后。
- **US5 / Phase 7**：依赖 US1；内容引擎可并行，完整边界集成在 US3/US4 后。
- **US6 / Phase 8**：审计模型可在 Phase 2 后开始；完整追踪验收依赖 US1–US5。
- **Phase 9**：依赖计划纳入本次交付的全部用户故事。

### User Story Dependency Graph

```text
Setup → Foundation → US1 ─┬─→ US2 ─→ US3 ─┐
                          ├─→ US4 ─────────┤
                          └─→ US5 ─────────┤
                                           └─→ US6 → Final Validation
```

### Within Each User Story

1. 先完成该阶段所有 Tests First 任务并获得真实 RED。
2. Domain model/service 在 Repository Adapter 之前或并行实现。
3. InMemory 契约先 GREEN，再实现 PostgreSQL/Redis 参数组。
4. Gateway/Worker/Channel/Recovery wiring 在组件契约 GREEN 后进行。
5. 运行独立验收并更新 `validation-results.md` 后才进入下一 checkpoint。

## Parallel Opportunities

- **Setup**：T003 与 T004 可并行。
- **Foundation**：T005–T008 可并行编写失败测试；T009/T010 可在不同文件并行。
- **US1**：T014–T016 测试可并行；T018/T019 可并行。
- **US2**：T024/T025 可并行；T027/T028 可并行。
- **US3**：T033–T036 可并行；T039/T041/T042 可并行；Redis、Channel 与 Tool callback 在契约稳定后可分工。
- **US4**：T048/T049/T051 可并行；T053/T054/T056 可并行。
- **US5**：T061–T063 可并行；T065/T067 可并行。
- **US6**：T070/T071 可并行；T074/T076 可并行。
- **跨故事**：US4 Repository 与 US5 内容引擎可在 US1 后并行；最终共享文件 wiring 必须串行合并。

## Parallel Examples by User Story

### US1

```text
Parallel RED: T014 policy lifecycle | T015 repository contract | T016 cache authority
Parallel GREEN: T018 policy service | T019 in-memory repository
```

### US2

```text
Parallel RED: T024 principal normalization | T025 grant repository contract
Parallel GREEN: T027 principal evaluator | T028 in-memory grant repository
```

### US3

```text
Parallel RED: T033 tool callback | T034 confirmation state | T035 repository | T036 channel contract
Parallel GREEN: T039 tool fixture | T041 confirmation service | T042 in-memory confirmation
```

### US4

```text
Parallel RED: T048 budget state | T049 repository contract | T051 settlement semantics
Parallel GREEN: T053 budget service | T054 in-memory budget | T056 Runner usage adapter
```

### US5

```text
Parallel RED: T061 content unit | T062 boundary integration | T063 sensitive scan
Parallel GREEN: T065 content engine | T067 error/log/audit sanitizer
```

### US6

```text
Parallel RED: T070 audit/metrics model | T071 repository contract
Parallel GREEN: T074 audit repository | T076 metrics recorder
```

## Requirement and Decision Traceability

| Scope | Primary FR coverage | Decisions | Acceptance evidence |
|-------|---------------------|-----------|---------------------|
| Foundation | FR-004、FR-009、FR-012、FR-017、FR-023、FR-027、FR-030、FR-031、FR-033、FR-034 | DEC-001/002/003 | 公共模型、端口、schema、SDK callback compatibility |
| US1 | FR-001–FR-005、FR-009、FR-029、FR-032–FR-034 | DEC-001 | 双租户策略隔离、立即失效、旧缓存拒绝 |
| US2 | FR-006–FR-009、FR-022、FR-029、FR-032–FR-033 | DEC-001 | 未授权 pre-Session 拒绝、跨渠道身份隔离 |
| US3 | FR-005、FR-009、FR-011–FR-015、FR-022–FR-024、FR-027–FR-034 | DEC-001/002 | 双入口共用状态、10 次重放、工具最多一次、接管边界 |
| US4 | FR-009、FR-016–FR-020、FR-023–FR-025、FR-027–FR-032、FR-034 | DEC-003 | 20 并发不超额、差额释放、唯一结算、故障恢复 |
| US5 | FR-009–FR-010、FR-014、FR-021–FR-023、FR-025–FR-026、FR-029、FR-032–FR-034 | DEC-001/002 | 六类可观察面敏感原文 0 命中 |
| US6 | FR-004、FR-022–FR-030、FR-032–FR-034 | DEC-001/002/003 | 六类治理决定、三类 trace、审计 fail-closed |
| Final | FR-001–FR-034 | ACA | 双 IM/双租户/双节点纵向验收、回归、性能、安全扫描 |

## Implementation Strategy

### MVP First

1. 完成 Phase 1 Setup。
2. 完成 Phase 2 Foundational。
3. 完成 Phase 3 US1。
4. 停止并独立验证策略隔离、默认拒绝和 DEC-001 立即生效。
5. MVP 通过后再加入主体、工具、确认、预算、内容和审计能力。

### Incremental Delivery

1. **MVP**：Setup + Foundation + US1，证明策略是可信租户安全边界。
2. **Identity gate**：加入 US2，证明“找到机器人”不等于获得 Agent 权限。
3. **Side-effect gate**：加入 US3，证明官方 Tool 边界和双确认入口最多执行一次。
4. **Cost gate**：加入 US4，证明跨节点预算不可突破且可恢复。
5. **Content gate**：加入 US5，证明敏感内容不会跨边界传播。
6. **Evidence gate**：加入 US6 和 Phase 9，形成可追踪、可演示、可答辩的完整证据。

## Notes

- `[P]` 只表示文件和前置依赖允许并行，不允许跳过同阶段 RED 门禁。
- 所有测试必须使用确定性替身；真实 IM 可作为补充人工验收，但不能替代自动化证据。
- 共享测试需要 healthy Redis/PostgreSQL；环境失败必须修复或明确记录，不能用 skip 掩盖。
- 不提交 `.env`、凭据、response URL、明文确认码、完整敏感参数或未经脱敏的日志。
- 每个任务或逻辑小组完成后建议形成清晰 Git commit；Phase checkpoint 前必须复查工作区和 `validation-results.md`。

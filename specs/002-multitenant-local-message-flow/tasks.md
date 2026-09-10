# Tasks: 多租户本地消息闭环

**Input**: Design documents from `/specs/002-multitenant-local-message-flow/`

**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`,
`contracts/`, `quickstart.md`, `决策记录.md`

**Tests**: 本功能明确要求测试先行。每个用户故事必须先完成测试任务和 Red Gate，
确认新测试因目标能力尚未实现而失败，之后才能执行实现任务；Green Gate 通过后才能
进入下一阶段。

**Trace semantics**: `[USn]` 仅标注用户故事阶段任务；Setup、Foundation 和 Polish
任务使用 `Trace: Shared`，表示它们是 US1–US4 的共同前置或共同验收工作。每个任务
同时列出适用的 FR、SC 与 D 编号，形成“用户故事—功能需求—成功标准—人工决策—交付
证据”链路。D-001–D-003 来自规格澄清，D-004–D-008 来自一致性分析后的人工批准。

**Trace format**: 每项任务末尾的 `Trace` 映射到用户故事、功能需求和人工决定。
`D-001` 为同会话串行，`D-002` 为失败重试边界，`D-003` 为 HMAC 认证，
`D-004` 为审计/指标作用域，`D-005` 为审计与终态顺序，`D-006` 为 Agent 开始/
超时边界，`D-007` 为 Agent-scoped 会话，`D-008` 为重复终态响应与 trace 语义。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可与同组任务并行，文件不同且不依赖尚未完成的同组任务。
- **[Story]**: 对应 `spec.md` 中的用户故事。
- Setup、Foundation、Polish 不使用 Story 标签，但仍在描述中提供 Trace。

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 固定依赖、建立代码与测试目录，并确认第一阶段基线未回退。

- [x] T001 在 `pyproject.toml` 直接固定 Starlette、Uvicorn、Pydantic 和 httpx 的计划版本并更新 `uv.lock`，保留 `trpc-agent-py==1.1.19`（Trace: Shared; FR-008, FR-021, FR-022; D-001–D-003）
- [x] T002 [P] 按 `specs/002-multitenant-local-message-flow/plan.md` 的权威文件树创建 `trpc_service/audit/models.py`、`trpc_service/channels/contracts.py`、`trpc_service/channels/hmac_auth.py`、`trpc_service/channels/local_http.py`、`trpc_service/config/settings.py`、`trpc_service/gateway/service.py`、`trpc_service/log/__init__.py`、`trpc_service/metrics/contracts.py`、`trpc_service/metrics/inmemory.py`、`trpc_service/metrics/models.py`、`trpc_service/storage/contracts.py`、`trpc_service/storage/inmemory.py`、`trpc_service/storage/locks.py`、`trpc_service/storage/models.py`、`trpc_service/storage/session_backend.py`、`trpc_service/tenant/models.py`、`trpc_service/tenant/session_identity.py`、`trpc_service/worker/service.py`、`trpc_service/web/app.py` 及对应包 `__init__.py` 的可导入 API 骨架；公开方法仅抛出 `NotImplementedError`，不得实现业务行为（Trace: Shared; FR-021, FR-028; D-004–D-008）
- [x] T003 [P] 在 `tests/conftest.py` 和 `tests/support.py` 建立运行时随机秘密、固定 UTC 时钟、消息工厂、调用计数器及外部网络阻断 fixture，禁止提交明文测试凭据（Trace: Shared; FR-014, FR-017, FR-022, FR-025; D-003）
- [x] T004 运行 `tests/sdk_validation/` 的 18 项基线测试并把命令、通过数和耗时写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: Shared; FR-008, FR-022; D-001–D-003）

**Checkpoint**: 依赖可同步，新目录可导入，第一阶段 SDK 验证保持通过。

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 先用失败测试固定共享领域模型、错误类型、租户键和配置边界；本阶段完成
前不得开始任何用户故事实现。

### Foundation Tests — write first

- [x] T005 [P] 在 `tests/unit/test_channel_contract_models.py` 编写入站消息、统一回复、VerifiedBindingScope 不可由请求构造、UUID、枚举、未知字段和 1–4000 Unicode 字符边界的失败测试（Trace: Shared; FR-001–FR-004, FR-013, FR-014, FR-018, FR-025–FR-027; D-003）
- [x] T006 [P] 在 `tests/unit/test_tenant_and_audit_models.py` 编写 Tenant、AgentApplication、ChannelBinding、TenantContext、Agent-scoped SessionIdentity、TenantScope/PreAuthScope、AuditRecord 和 MetricSnapshot 的归属、不可变、伪名化、计数约束与秘密字段拒绝测试（Trace: Shared; FR-002–FR-007, FR-015–FR-017, FR-025, FR-028; D-001, D-003, D-004, D-007）
- [x] T007 [P] 在 `tests/unit/test_settings.py` 编写两个演示租户、独立 secret_ref、环境秘密缺失/为空以及模型凭据不参与启动的失败测试（Trace: Shared; FR-003, FR-022, FR-025; D-003）
- [x] T008 运行 `tests/unit/test_channel_contract_models.py`、`tests/unit/test_tenant_and_audit_models.py` 和 `tests/unit/test_settings.py`，确认测试已正常收集且仅因 API 骨架无业务行为而失败，并把 Red Gate 结果写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: Shared; FR-001–FR-007, FR-013–FR-018, FR-022, FR-025, FR-028; D-001, D-003, D-004, D-007）

### Foundation Implementation — only after T008

- [x] T009 [P] 在 `trpc_service/channels/contracts.py` 实现严格的 InboundMessage、OutboundReply、ErrorDetail、VerifiedBindingScope、状态与 delivery_action 契约（Trace: Shared; FR-001–FR-004, FR-013, FR-014, FR-018, FR-025–FR-027; D-003）
- [x] T010 [P] 在 `trpc_service/tenant/models.py` 和 `trpc_service/tenant/session_identity.py` 实现 Tenant、AgentApplication、ChannelBinding、VerifiedTenantContext、SessionIdentity 及 tenant+agent 作用域摘要规则（Trace: Shared; FR-002–FR-007, FR-023, FR-025; D-001, D-003, D-007）
- [x] T011 [P] 在 `trpc_service/audit/models.py` 和 `trpc_service/metrics/models.py` 实现 TenantScope、PreAuthScope、AuditRecord、AuditDecision、MetricSnapshot、伪名 user_id、binding/message 摘要和禁止秘密/正文序列化的校验（Trace: Shared; FR-015–FR-017, FR-019, FR-028; D-003, D-004）
- [x] T012 [P] 在 `trpc_service/storage/contracts.py` 和 `trpc_service/metrics/contracts.py` 定义 BindingAuthRegistry、SecretResolver、TenantDirectory、IdempotencyRepository、SessionLockManager、SessionBackendFactory、tenant-scoped AuditRepository、MetricsRecorder 端口及稳定类型错误（Trace: Shared; FR-002–FR-007, FR-009–FR-012, FR-015, FR-019–FR-021, FR-023–FR-028; D-001–D-008）
- [x] T013 [P] 在 `trpc_service/config/settings.py` 实现两个非敏感演示租户/Agent/Binding 的配置装配，只保存环境变量名称形式的 secret_ref（Trace: Shared; FR-003, FR-020, FR-022, FR-025; D-003）
- [x] T014 运行 Phase 2 三个测试文件并确认全部通过，同时运行 `tests/sdk_validation/` 回归并记录 Green Gate 到 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: Shared; FR-001–FR-007, FR-013–FR-018, FR-022, FR-025, FR-028; D-001, D-003, D-004, D-007）

**Checkpoint**: 共享模型、端口和配置可独立验证；业务状态不能绕过 tenant scope。

---

## Phase 3: User Story 1 — 发送一条可审计的租户消息 (Priority: P1) 🎯 MVP

**Goal**: 一个有效签名的本地消息通过 Binding → Tenant → Gateway → Worker →
官方 Runner → Session → Reply/Audit 返回非空最终回复。

**Independent Test**: 使用一个活动租户、Agent、绑定和运行时随机秘密发送有效签名
消息；得到 HTTP 200、`status=succeeded`、`delivery_action=deliver`、非空
trace/session/final text，并能从 AuditRepository 查询同 trace 的成功路径。

### Tests for User Story 1 — write first

- [x] T015 [P] [US1] 在 `tests/unit/test_hmac_auth.py` 编写 v1 规范串、原始正文 SHA-256、有效 HMAC 和恰好 ±300 秒边界的成功测试（Trace: US1; FR-025–FR-027; D-003）
- [x] T016 [P] [US1] 在 `tests/contract/test_agent_executor.py` 编写 prepare/RUNNING 边界、真实官方 Runner/Event/Session 的最终回复选择、事件计数和无外部模型调用成功契约测试（Trace: US1; FR-008, FR-013, FR-022, FR-024; D-003, D-006）
- [x] T017 [P] [US1] 在 `tests/contract/test_platform_adapters.py` 编写活动 Binding/Tenant/Agent 解析、首次幂等 claim、TenantScope 审计 append/update/query、MetricsRecorder 成功快照和无竞争 session lease 的成功契约测试（Trace: US1; FR-002, FR-003, FR-015, FR-020, FR-021, FR-028; D-001, D-003, D-004）
- [x] T018 [P] [US1] 在 `tests/contract/test_local_message_http_contract.py` 编写 `GET /healthz` 与有效 `POST /v1/local/messages` 的请求/响应 envelope 和状态码测试（Trace: US1; FR-001, FR-013, FR-014, FR-018, FR-026; D-003）
- [x] T019 [P] [US1] 在 `tests/integration/test_multitenant_message_flow.py` 编写单租户首条消息从 ASGI 到官方 Runner、SDK Session、统一回复、租户作用域审计和指标快照的完整失败测试（Trace: US1; FR-001–FR-003, FR-008, FR-013–FR-016, FR-020–FR-022, FR-025–FR-028; SC-009; D-003, D-004）
- [x] T020 [US1] 运行 T015–T019 新增测试并确认测试正常收集且仅因 HMAC、Adapter、Worker、Gateway 和 HTTP 行为尚未实现而失败，把 Red Gate 输出写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: US1; FR-001–FR-003, FR-008, FR-013–FR-016, FR-020–FR-022, FR-024–FR-028; SC-009; D-003, D-004, D-006）

### Implementation for User Story 1 — only after T020

- [x] T021 [P] [US1] 在 `trpc_service/channels/hmac_auth.py` 实现 v1 签名生成/验证、原始正文摘要、±300 秒校验、恒定时间比较、环境 SecretResolver，并仅在成功后产生 VerifiedBindingScope（Trace: US1; FR-002–FR-004, FR-025–FR-027; D-003）
- [x] T022 [P] [US1] 在 `trpc_service/storage/inmemory.py` 和 `trpc_service/metrics/inmemory.py` 实现活动目录、tenant-scoped AuditRepository、MetricsRecorder、首次 claim 和无竞争 lease 的 InMemory 成功路径（Trace: US1; FR-002, FR-003, FR-015, FR-020, FR-021, FR-028; D-001, D-003, D-004）
- [x] T023 [P] [US1] 在 `trpc_service/worker/service.py` 实现 AgentExecutor prepare/execute Adapter，复用 DeterministicValidationModel、LlmAgent、Runner、Event 和 InMemorySessionService，在首次请求 Event 前建立开始边界并只选正式最终 Event（Trace: US1; FR-008, FR-013, FR-022, FR-024; D-003, D-006）
- [x] T024 [US1] 在 `trpc_service/gateway/service.py` 实现有效消息的 TenantContext、Agent-scoped SessionIdentity、首次 claim、TenantScope 审计预写、Worker prepare、RUNNING、最终审计、幂等终态和指标记录编排（Trace: US1; FR-002–FR-008, FR-013–FR-016, FR-019–FR-024, FR-028; D-001, D-003–D-007）
- [x] T025 [US1] 在 `trpc_service/channels/local_http.py`、`trpc_service/web/app.py` 和 `trpc_service/_cli.py` 实现传输映射、应用生命周期、`/healthz`、`/v1/local/messages` 与 `trpc-agent-local-serve` 入口（Trace: US1; FR-001, FR-013, FR-014, FR-018, FR-026; D-003）
- [x] T026 [US1] 运行 T015–T019、全部 `tests/sdk_validation/` 和一次本地健康/成功请求，断言首次演示不超过 5 分钟且 MetricsSnapshot 对应成功结果，确认 Green Gate 并记录到 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: US1; FR-001–FR-003, FR-008, FR-013–FR-016, FR-020–FR-022, FR-025–FR-028; SC-001, SC-009; D-003, D-004）

**Checkpoint**: User Story 1 可独立演示，是本功能 MVP；尚不宣称跨租户、多轮和完整
重复投递语义完成。

---

## Phase 4: User Story 2 — 保持会话连续且跨租户不串话 (Priority: P1)

**Goal**: 同租户同会话可连续两轮，两个租户使用完全相同外部标识仍拥有不同 Session、
不同上下文和不同审计归属，跨租户平台会话引用被拒绝。

**Independent Test**: alpha 和 beta 使用相同 user/conversation，分别保存 ALPHA 和
BRAVO，再各自 recall；只召回本租户标记且 session_id 不同。随后在内部
SessionBackend/Worker 契约测试中组合另一租户的 VerifiedTenantContext 与
SessionIdentity，验证在 SDK 调用前拒绝且无状态变化。

### Tests for User Story 2 — write first

- [x] T027 [P] [US2] 在 `tests/unit/test_session_identity.py` 编写相同输入稳定、tenant/agent/binding/user/conversation 任一变化即不同、Agent 改绑创建新会话、无分隔符碰撞及原始标识不泄露测试（Trace: US2; FR-005–FR-007; D-001, D-003, D-007）
- [x] T028 [P] [US2] 在 `tests/contract/test_session_backend.py` 编写 SDK app/user/session 的租户与 Agent 作用域、后端生命周期，以及错配 VerifiedTenantContext/SessionIdentity 在 SDK 调用前被拒绝的测试（Trace: US2; FR-005–FR-008, FR-020, FR-021; D-001, D-007）
- [x] T029 [P] [US2] 扩展 `tests/integration/test_multitenant_message_flow.py`，加入两个租户相同外部标识的交替双轮、独立 session_id、上下文隔离和审计归属测试（Trace: US2; FR-002–FR-008, FR-014–FR-017, FR-020–FR-023, FR-025–FR-027; D-001, D-003, D-004, D-007）
- [x] T030 [US2] 运行 T027–T029 测试并确认仅因多租户 Session Backend 路由、上下文/会话身份所有权校验尚未完成而失败，将 Red Gate 输出写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: US2; FR-002–FR-008, FR-014–FR-017, FR-020–FR-023, FR-025–FR-027; D-001, D-003, D-004, D-007）

### Implementation for User Story 2 — only after T030

- [x] T031 [P] [US2] 完成 `trpc_service/tenant/session_identity.py` 的长度无歧义编码、tenant+agent-scoped session/app/user 摘要和平台 session 所有权校验（Trace: US2; FR-005–FR-007, FR-023; D-001, D-007）
- [x] T032 [P] [US2] 在 `trpc_service/storage/session_backend.py` 实现按 tenant_id/agent_id 作用域管理官方 InMemorySessionService 的 SessionBackendFactory 和关闭生命周期（Trace: US2; FR-006–FR-008, FR-020, FR-021; D-001, D-007）
- [x] T033 [US2] 更新 `trpc_service/worker/service.py` 和 `trpc_service/gateway/service.py`，强制使用 VerifiedTenantContext 与 SessionIdentity 路由 SDK，并在 SDK 调用前拒绝租户上下文与会话身份所有权错配（Trace: US2; FR-002–FR-008, FR-014, FR-020–FR-023; D-001, D-003, D-007）
- [x] T034 [US2] 更新 `trpc_service/storage/inmemory.py` 的 TenantDirectory 和 AuditRepository，使所有读取/查询验证 tenant scope 并隔离两个演示租户（Trace: US2; FR-002–FR-007, FR-015–FR-017, FR-020, FR-021; D-001, D-003, D-004）
- [x] T035 [US2] 运行 T027–T029、US1 契约/集成测试及 `tests/sdk_validation/`，确认 Green Gate 并把双租户、Agent 改绑 session_id 与 recall 证据写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: US2; FR-002–FR-008, FR-014–FR-017, FR-020–FR-023, FR-025–FR-027; SC-002; D-001, D-003, D-007）

**Checkpoint**: US1 和 US2 均可演示；Session 连续性和租户隔离具有自动化证据。

---

## Phase 5: User Story 3 — 安全处理重复投递 (Priority: P2)

**Goal**: 顺序和并发重复最多执行一次；同键异内容冲突；同会话不同消息串行、不同
会话并行；只有 Agent 开始前失败允许相同标识重试。

**Independent Test**: 对一条消息顺序投递 100 次并进行 20 组并发重复，每组
Agent 调用计数均为 1；重复返回 current/original trace 和 suppress；冲突返回 409；
执行前失败可 reclaim，RUNNING 后失败及结果未知保持终态。

### Tests for User Story 3 — write first

- [x] T036 [P] [US3] 在 `tests/unit/test_idempotency_state_machine.py` 编写 PENDING、RUNNING、SUCCEEDED、FAILED_PRE_START、FAILED_POST_START、OUTCOME_UNKNOWN 的合法/非法转换，first_claim/owner/execution trace、owner_token、attempt、最终审计先于终态及终态不可变测试（Trace: US3; FR-009–FR-012, FR-014, FR-019, FR-024; D-002, D-005, D-008）
- [x] T037 [P] [US3] 在 `tests/contract/test_idempotency_repository.py` 编写同键原子 claim、processing owner trace、completed execution trace、异指纹 conflict、pre-start reclaim 和成功/失败/不确定终态缓存且不重放契约测试（Trace: US3; FR-009–FR-012, FR-014, FR-020, FR-021, FR-024; D-002, D-005, D-008）
- [x] T038 [P] [US3] 在 `tests/contract/test_session_lock_manager.py` 编写同 session 串行、不同 session 并行、异常/取消释放和无全局锁测试（Trace: US3; FR-007, FR-021, FR-023; D-001）
- [x] T039 [P] [US3] 扩展 `tests/integration/test_multitenant_message_flow.py`，加入 100 次顺序重复、20 组并发重复、同键异内容、同会话顺序、不同会话并行、prepare 失败、RUNNING 后失败/超时/取消、审计终态失败和终态缓存响应测试（Trace: US3; FR-009–FR-012, FR-014–FR-016, FR-018–FR-024; D-001, D-002, D-005, D-006, D-008）
- [x] T040 [US3] 运行 T036–T039 并确认测试正常收集且仅因原子状态机、per-session lock、Agent 生命周期和 Gateway 重复分支尚未实现而失败，把 Red Gate 输出写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: US3; FR-009–FR-012, FR-018–FR-024; D-001, D-002, D-005, D-006, D-008）

### Implementation for User Story 3 — only after T040

- [x] T041 [P] [US3] 在 `trpc_service/storage/models.py` 实现 IdempotencyKey、ContentFingerprint、含 first_claim/owner/execution trace 的 IdempotencyRecord、ExecutionResult、状态枚举和条件转换规则（Trace: US3; FR-009–FR-014, FR-024; D-002, D-005, D-008）
- [x] T042 [US3] 完成 `trpc_service/storage/inmemory.py` 的原子 claim、指纹冲突、owner_token compare-and-set、pre-start reclaim、terminal result 和调用计数实现（Trace: US3; FR-009–FR-012, FR-020, FR-021, FR-024; D-002）
- [x] T043 [P] [US3] 在 `trpc_service/storage/locks.py` 实现按 tenant-scoped platform_session_id 管理的异步 lease、独立会话并行和 finally/cancellation 安全释放（Trace: US3; FR-007, FR-021, FR-023; D-001）
- [x] T044 [US3] 更新 `trpc_service/gateway/service.py`，按 claim → session lease → audit pre-write → Worker prepare → mark RUNNING/execution trace → timed execute → final audit → terminal CAS 顺序实现 duplicate、processing、conflict、失败、超时和重试边界（Trace: US3; FR-009–FR-012, FR-014–FR-016, FR-018–FR-024; D-001, D-002, D-005, D-006, D-008）
- [x] T045 [US3] 更新 `trpc_service/channels/contracts.py` 和 `trpc_service/channels/local_http.py`，使 processing 返回 owner trace，成功 duplicate 返回缓存文本，缓存 FAILED_POST_START/OUTCOME_UNKNOWN 保留 502/503 安全错误，全部缓存终态使用 execution trace 与 `delivery_action=suppress`，conflict 映射 HTTP 409（Trace: US3; FR-010–FR-015, FR-018, FR-024; D-002, D-008）
- [x] T046 [US3] 运行 T036–T039、US1/US2 全部测试和 `tests/sdk_validation/`，确认 Green Gate 并将次数、调用计数、owner/execution trace、prepare/timeout/cancel、终态审计顺序和耗时写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: US3; FR-009–FR-024; SC-003; D-001, D-002, D-005, D-006, D-008）

**Checkpoint**: D-001 与 D-002 均有自动化证据；重复投递和会话并发语义可独立验收。

---

## Phase 6: User Story 4 — 拒绝非法入口并追踪完整链路 (Priority: P2)

**Goal**: 未知绑定、错误/超时签名、禁用或跨租户绑定、输入错误和执行/审计故障都
默认拒绝、无未授权 Session 副作用，并以安全错误、trace 和脱敏审计完整呈现。

**Independent Test**: 对每类拒绝及失败注入发起请求，验证 400/401/403/502/503
稳定映射、未知绑定和错误签名公开响应完全一致、Worker 调用为 0（执行前拒绝）、
同一 delivery trace 贯穿，日志/审计/响应没有正文、签名或秘密。

### Tests for User Story 4 — write first

- [x] T047 [P] [US4] 扩展 `tests/unit/test_hmac_auth.py`，加入未知 binding、缺失 secret、错误版本/长度/hex、篡改正文、超出 ±300 秒和恒定公开错误测试（Trace: US4; FR-002–FR-004, FR-017, FR-025–FR-027; D-003）
- [x] T048 [P] [US4] 扩展 `tests/contract/test_local_message_http_contract.py`，加入缺字段、未知字段、空白/超长正文、非法 trace 以及 400/401/403/502/503 安全 envelope 测试（Trace: US4; FR-001–FR-004, FR-014, FR-017–FR-019, FR-025–FR-027; D-003）
- [x] T049 [P] [US4] 在 `tests/contract/test_audit_repository.py` 和 `tests/contract/test_metrics_recorder.py` 编写 TenantScope/PreAuthScope 隔离、按 tenant/session/trace 查询、current/owner/execution trace、指标计数/延迟/not_applicable、伪名化、秘密拒绝和故障注入测试；断言指标故障产生脱敏 `metrics_incomplete` 运维事件且不改写业务终态/认证响应，并用最小 Fake Adapter 复用同一契约（Trace: US4; FR-007, FR-014–FR-017, FR-019–FR-021, FR-025, FR-028; SC-005, SC-006, SC-008, SC-009; D-003, D-004）
- [x] T050 [P] [US4] 扩展 `tests/integration/test_multitenant_message_flow.py`，加入未知租户、禁用/归属错误绑定、Agent prepare 异常、RUNNING 后异常/超时/取消、无最终 Event、audit pre/final 失败、无 Session 副作用、指标归属和全链路 trace 测试（Trace: US4; FR-002–FR-004, FR-007, FR-014–FR-019, FR-022, FR-024–FR-028; D-002–D-006, D-008）
- [x] T051 [P] [US4] 在 `tests/integration/test_offline_security.py` 编写清除常见模型凭据、阻断外部 socket、扫描 stdout/stderr/响应/审计中秘密与完整正文的安全测试（Trace: US4; FR-017, FR-022, FR-025; SC-006, SC-007; D-003）
- [x] T052 [US4] 运行 T047–T051 并确认测试仅因拒绝分支、审计/指标故障语义、脱敏和错误映射尚未完整而失败，把 Red Gate 输出写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: US4; FR-001–FR-004, FR-007, FR-014–FR-019, FR-022, FR-024–FR-028; D-002–D-006, D-008）

### Implementation for User Story 4 — only after T052

- [x] T053 [P] [US4] 加固 `trpc_service/channels/hmac_auth.py`，统一所有认证失败、先验证时间窗和格式、恒定时间比较且任何异常不包含 binding/secret/signature 原值（Trace: US4; FR-002–FR-004, FR-017, FR-025–FR-027; D-003）
- [x] T054 [P] [US4] 完成 `trpc_service/storage/inmemory.py` 和 `trpc_service/metrics/inmemory.py` 的 TenantScope/PreAuthScope AuditRepository、MetricsRecorder、tenant/session/trace 安全查询、字段脱敏、owner/execution trace、故障注入和指标快照（Trace: US4; FR-007, FR-014–FR-017, FR-019–FR-021, FR-025, FR-028; D-002–D-005）
- [x] T055 [US4] 更新 `trpc_service/gateway/service.py`，实现默认拒绝、拒绝前无租户业务状态访问、audit pre-write fail closed、RUNNING 时最终审计失败 → FAILED_POST_START/audit_incomplete、终态持久化不确定 → OUTCOME_UNKNOWN，且两者禁止自动重放（Trace: US4; FR-002–FR-004, FR-007, FR-014–FR-019, FR-024–FR-028; D-002–D-006, D-008）
- [x] T056 [P] [US4] 更新 `trpc_service/channels/local_http.py`、`trpc_service/log/__init__.py` 和 `trpc_service/metrics/inmemory.py`，集中映射稳定 HTTP 错误、生成/继承 UUID trace、记录租户作用域请求/错误/延迟/投递指标，并输出不含秘密、正文、堆栈和完整用户标识的结构化日志（Trace: US4; FR-014, FR-017–FR-019, FR-025–FR-028; D-003, D-004, D-008）
- [x] T057 [US4] 运行 T047–T051、US1–US3 全部测试和 `tests/sdk_validation/`，确认 Green Gate 并将错误矩阵、无副作用、审计查询隔离、owner/execution trace、超时/取消、指标快照与秘密扫描结果写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: US4; FR-001–FR-004, FR-007, FR-014–FR-019, FR-022, FR-024–FR-028; SC-004–SC-009; D-002–D-008）

**Checkpoint**: 四个用户故事均可独立验收；D-003 的认证与不泄露要求有正反向证据。

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: 完成 CLI、规模验收、文档证据、全量回归和最终安全门禁。

- [x] T058 [P] 在 `tests/contract/test_local_cli.py` 先编写 `trpc-agent-local-send` 参数、secret-env、原始 JSON 字节签名和不打印秘密的失败契约测试（Trace: Shared/US1/US4; FR-001, FR-017, FR-025–FR-027; D-003）
- [x] T059 在 `trpc_service/_cli.py` 和 `pyproject.toml` 实现 `trpc-agent-local-send` 与最终 `trpc-agent-local-serve` 入口并更新 `uv.lock`，运行 T058 至通过（Trace: Shared/US1/US4; FR-001, FR-013, FR-017, FR-022, FR-025–FR-027; D-003）
- [x] T060 [P] 在 `tests/integration/test_acceptance_scale.py` 固化两个租户 20 次交替双轮、100 次顺序重复、20 组并发重复和不同会话并行的 SC-002/SC-003 验收测试，并断言 Agent 改绑产生新 session（Trace: Shared/US2/US3; FR-005–FR-012, FR-023, FR-024; SC-002, SC-003; D-001, D-002, D-007）
- [x] T061 [P] 更新 `README.md`，加入第二阶段范围、启动/发送/测试命令、复用与新增边界，并明确 InMemory、单进程、离线、本地 MetricsSnapshot 和未完成真实 IM/共享后端/生产 Telemetry（Trace: Shared; FR-008, FR-020–FR-022, FR-028; D-001–D-008）
- [x] T062 [P] 更新 `specs/002-multitenant-local-message-flow/决策记录.md`，为 D-001–D-008 填入实现文件、测试名称、测试结果和“AI 建议—人类决策—交付证据”追踪表，并保持与 `specs/002-multitenant-local-message-flow/一致性分析修订记录.md` 对应（Trace: Shared; FR-014–FR-028; D-001–D-008）
- [x] T063 按 `specs/002-multitenant-local-message-flow/quickstart.md` 从干净依赖状态执行健康、成功、双租户、多轮、重复、冲突、错误签名、超时/取消、审计查询和指标快照演示，修正文档与实际命令的任何差异（Trace: Shared; FR-001–FR-028; SC-001–SC-009; D-001–D-008）
- [x] T064 运行 `tests/unit/`、`tests/contract/`、`tests/integration/`、`tests/sdk_validation/` 和全量 `tests/`，把通过数、耗时、SC/FR/D 覆盖矩阵写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: Shared; FR-001–FR-028; SC-001–SC-009; D-001–D-008）
- [x] T065 执行 `uv run python -m compileall trpc_service tests`、`git diff --check`、生成文件检查和秘密模式扫描，并将零泄漏或发现项写入 `specs/002-multitenant-local-message-flow/validation-results.md`（Trace: Shared; FR-017, FR-022, FR-025, FR-028; SC-006, SC-009; D-003, D-004）
- [x] T066 创建 `specs/002-multitenant-local-message-flow/阶段成果记录.md`，浓缩背景、目标、实现链路、人工决策、测试结果、演示步骤、已知限制和下一阶段接口（Trace: Shared; FR-001–FR-028; SC-001–SC-009; D-001–D-008）
- [x] T067 对 `specs/002-multitenant-local-message-flow/spec.md`、`plan.md`、`tasks.md`、`research.md`、`data-model.md`、`contracts/`、实现和测试执行 `$speckit-converge`，阻断项清零后才标记功能完成（Trace: Shared; FR-001–FR-028; SC-001–SC-009; D-001–D-008）
- [x] T068 在 `feature/luwenjie` 上审查并提交第二阶段相关源码、测试和 `specs/002-multitenant-local-message-flow/`，确认生成文件与秘密未暂存后推送到 `origin/feature/luwenjie`（Trace: Shared; FR-001–FR-028; SC-001–SC-009; D-001–D-008）

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 Setup**: 无依赖，可立即开始。
- **Phase 2 Foundation**: 依赖 Phase 1，阻断所有用户故事。
- **Phase 3 US1**: 依赖 Foundation，完成后形成可运行 MVP。
- **Phase 4 US2**: SessionIdentity 与 SessionBackend 单元/契约工作可在 Foundation
  后并行；完整双租户集成与 Green Gate 依赖 US1。
- **Phase 5 US3**: IdempotencyRepository 与 SessionLockManager 单元/契约工作可在
  Foundation 后并行；Gateway/HTTP 集成与 Green Gate 依赖 US1，最终回归包含 US2。
- **Phase 6 US4**: HMAC 负向、Audit 契约和离线安全测试可在 Foundation 后并行；
  完整拒绝链路与 Green Gate 依赖 US1，最终回归包含 US2/US3。
- **Phase 7 Polish**: 依赖四个用户故事 Green Gate。

### User Story Dependency Graph

```text
Setup → Foundation → US1 (MVP)
                    ├──→ US2 ──┐
                    ├──→ US3 ──┼──→ Polish → Converge → Git push
                    └──→ US4 ──┘
```

US2、US3、US4 的专属模型和契约测试可以并行，但修改共享
`gateway/service.py`、`storage/inmemory.py` 或
`integration/test_multitenant_message_flow.py` 时必须按 T 编号顺序合并，避免
同文件冲突。

### Within Each User Story

1. 完成该故事全部测试文件。
2. 执行 Red Gate，确认失败原因是目标能力缺失而非语法、导入或环境错误。
3. 才能执行 Implementation 任务。
4. 执行 Green Gate，并包含以前阶段回归。
5. 将命令、数量、耗时和关键证据记录到 validation-results.md。

---

## Parallel Opportunities

### Foundation

```text
T005 channel model tests
T006 tenant/audit model tests
T007 settings tests

After T008:
T009 channel models
T010 tenant/session models
T011 audit models
T012 storage ports
T013 settings
```

### User Story 1

```text
T015 HMAC happy-path tests
T016 AgentExecutor contract
T017 platform adapter contract
T018 HTTP contract
T019 end-to-end integration test

After T020:
T021 HMAC implementation
T022 InMemory happy-path adapters
T023 Worker Adapter
```

### User Story 2

```text
T027 SessionIdentity tests
T028 SessionBackend contract
T029 tenant-isolation integration scenarios

After T030:
T031 SessionIdentity implementation
T032 SessionBackend implementation
```

### User Story 3

```text
T036 idempotency state tests
T037 repository contract
T038 session lock contract
T039 concurrency integration scenarios

After T040:
T041 idempotency models
T043 session lock implementation
```

### User Story 4

```text
T047 HMAC rejection tests
T048 HTTP error contract
T049 audit contract
T050 rejection/failure integration scenarios
T051 offline security tests

After T052:
T053 HMAC hardening
T054 AuditRepository hardening
T056 HTTP/log mapping
```

---

## Traceability Summary

| Story / Shared | Requirements | Human Decisions | Primary Test Evidence |
|---|---|---|---|
| Setup / Shared (US1–US4) | FR-008, FR-014, FR-017, FR-021, FR-022, FR-025, FR-028 | D-001–D-008 | T001–T004 |
| Foundation | FR-001–FR-007, FR-013–FR-018, FR-021, FR-022, FR-025, FR-028 | D-001, D-003, D-004, D-007 | T005–T008, T014 |
| US1 | FR-001–FR-003, FR-008, FR-013–FR-016, FR-018–FR-022, FR-024–FR-028 | D-003–D-007 | T015–T020, T026 |
| US2 | FR-002–FR-008, FR-014–FR-017, FR-020–FR-023, FR-025–FR-027 | D-001, D-003, D-004, D-007 | T027–T030, T035 |
| US3 | FR-009–FR-024 | D-001, D-002, D-005, D-006, D-008 | T036–T040, T046 |
| US4 | FR-001–FR-004, FR-007, FR-014–FR-019, FR-022, FR-024–FR-028 | D-002–D-006, D-008 | T047–T052, T057 |
| Final acceptance / Shared (US1–US4) | FR-001–FR-028, SC-001–SC-009 | D-001–D-008 | T058–T068 |

## Implementation Strategy

### MVP First

1. 完成 T001–T014。
2. 完成 T015–T020 并保留失败证据。
3. 完成 T021–T026。
4. 停止扩展范围，独立演示 US1 成功消息闭环。

### Incremental Delivery

1. **MVP**: US1 有效签名消息、官方 Runner、统一回复和审计。
2. **Isolation**: US2 两租户相同外部标识仍完全隔离。
3. **Consistency**: US3 原子幂等、失败重试边界和会话串行。
4. **Security/Operations**: US4 默认拒绝、脱敏、trace 和审计失败。
5. **Evidence**: Quickstart、规模验收、决策证据、Converge、Git 提交。

每个增量都必须保持前序 Green Gate 和 `tests/sdk_validation/` 通过。

## Notes

- 所有任务必须按 T 编号执行；只有明确标记 `[P]` 的任务可以并行。
- 测试任务不得为了“先通过”而跳过、xfail 或降低断言。
- Red Gate 只接受“目标能力尚未实现”的预期失败，不接受导入错误、拼写错误或环境
  故障作为测试先行证据。
- 不得把 InMemory 结果表述为多节点或生产持久化验证。
- D-001–D-003 的最终证据必须回填到 `决策记录.md`，体现 AI 提议、人的决定和
  交付验证之间的闭环。
- Phase 7 前不得接入真实 IM、真实模型、Redis、SQL、Kubernetes 或管理后台。

---

## Phase 8: Convergence Remediation

**Purpose**: 根据首次 `$speckit-converge` 对实现、测试和验收证据的复核，补齐仍未被
直接证明的拒绝矩阵与故障隔离证据；不扩大第二阶段功能范围。

- [x] T069 [US4] 扩展 `tests/contract/test_platform_adapters.py`，用禁用 Tenant、禁用 Agent、禁用 Binding 和错误 Binding ownership 分别证明 TenantDirectory 默认拒绝且不产生业务状态（Trace: US4; FR-002–FR-004, FR-007; SC-004; D-003, D-004）
- [x] T070 [US4] 扩展 `tests/contract/test_local_message_http_contract.py`，直接验证未知 Binding、非法 trace 自动替换、Agent execution 失败 502、audit pre-write 失败 503 的安全 envelope 和执行边界（Trace: US4; FR-004, FR-014, FR-018–FR-019, FR-027; SC-004–SC-006; D-003, D-005, D-006）
- [x] T071 [US4] 扩展 `tests/unit/test_hmac_auth.py` 与 `tests/integration/test_failure_boundaries.py`，证明签名后的正文篡改必定拒绝，且 MetricsRecorder 故障只产生脱敏 `metrics_incomplete` 运维事件、不改变认证拒绝响应（Trace: US4; FR-017, FR-025–FR-028; SC-006, SC-009; D-003, D-004）
- [x] T072 修复 T069–T071 暴露的实现缺口，运行新增测试、四类测试目录与全量测试，并将命令、数量、耗时和结论写入 `validation-results.md`（Trace: Shared/US4; FR-002–FR-004, FR-007, FR-014, FR-017–FR-019, FR-025–FR-028; SC-004–SC-006, SC-009; D-003–D-006）
- [x] T073 再次执行 `$speckit-converge`，确认 Constitution、spec、plan、research、data-model、contracts、tasks、实现、测试和运行证据无阻断项后，完成 T067 并进入 T068 Git 交付（Trace: Shared; FR-001–FR-028; SC-001–SC-009; D-001–D-008）

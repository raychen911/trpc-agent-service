# Tasks: 生产可观测性与运维收敛

**Feature**: `008-observability-operations-flow`

**Input**: [spec.md](./spec.md)、[clarification-decisions.md](./clarification-decisions.md)、[plan.md](./plan.md)、[research.md](./research.md)、[data-model.md](./data-model.md)、[contracts/observability-contracts.md](./contracts/observability-contracts.md)、[contracts/release-operations-contracts.md](./contracts/release-operations-contracts.md)、[quickstart.md](./quickstart.md)

**Method**: 严格测试先行。每组实现开始前必须先写测试并运行得到预期 RED；实现完成后运行同一命令得到 GREEN，并把命令、退出码、通过/失败/跳过数量及必要的脱敏摘要追加到 `specs/008-observability-operations-flow/validation-results.md`。

## Checklist Format

- `[P]`：可与相邻任务并行，且不会修改同一文件或依赖尚未完成的实现。
- `[USn]`：对应 `spec.md` 的用户故事。
- 每项任务都标注关联 FR/NFR/SC、人工决策（DEC-001～DEC-005）及验收证据。
- 共享后端未配置造成的 skip 只能记录环境原因，不能作为 GREEN 或阶段完成证据。
- 本地容量、Compose 和故障演练结果必须标注 `local_evidence`，不得表述为生产 SLA 或生产 HA 验证。
- 普通遥测（可丢诊断）与正式 Audit（fail closed）是两个故障域；任何任务不得用前者替代后者。

---

## Phase 1: Setup（任务与证据基线）

**Purpose**：固定现有实现基线、测试目录、共享环境入口和证据格式；不改变业务行为。

- [X] T001 记录当前 Git 分支、工作区状态、Python/依赖版本（含 `trpc-agent-py==1.1.19` 与计划锁定的 OpenTelemetry 版本）和现有 `trpc_service/metrics/`、`trpc_service/log/`、`trpc_service/observability/` 骨架文件清单到 `specs/008-observability-operations-flow/validation-results.md`。[FR-033, FR-036] [DEC-001] [Evidence: baseline inventory]
- [X] T002 将 `specs/008-observability-operations-flow/validation-results.md` 重构为带日期、任务号、RED/GREEN、命令、退出码、结果摘要、环境和备注列的模板，明确标注旧骨架不算本计划验收。[FR-033, FR-036] [DEC-005] [Evidence: validation template review]
- [X] T003 [P] 创建第八阶段分层测试包与 `__init__.py`：`tests/unit/observability/`、`tests/unit/operations/`、`tests/contract/observability/`、`tests/contract/operations/`、`tests/integration/observability/`、`tests/integration/operations/`、`tests/e2e/observability/`、`tests/e2e/operations/`，并核对 `tests/performance/`、`tests/security/` 可复用结构。[FR-033, FR-034] [DEC-005] [Evidence: pytest collection]
- [X] T004 [P] 在 `tests/observability_support.py` 定义两个租户、两个节点、固定 UTC、稳定 trace/digest/fence/generation、预置敏感标记（Secret、Token、response URL、手机号、邮箱、正文样本）和无真实凭证的 fixture builders。[FR-001, FR-031, FR-032] [DEC-001, DEC-005] [Evidence: fixture self-test]
- [X] T005 [P] 在 `tests/conftest.py` 扩展第八阶段共享 PostgreSQL/Redis marker 与唯一 namespace fixture，确保缺环境时给出明确 skip 原因且不回显 DSN 或密码。[FR-033, FR-034] [DEC-005] [Evidence: collection/skip reason]

**Checkpoint**：测试目录和证据模板可用，未修改生产语义。

---

## Phase 2: Foundational（阻塞所有用户故事）

**Purpose**：先通过 RED 测试固定中央枚举、稳定错误契约、领域模型、异步端口、OpenTelemetry 依赖锁定和 PostgreSQL schema v7，再实现共同基础。

**⚠️ CRITICAL**：T006–T018 未完成前不得开始任何用户故事实现。

### Foundational tests — 必须先 RED

- [X] T006 [P] 为中央 taxonomy 编写 RED 单元测试到 `tests/unit/observability/test_taxonomy.py`：统一阶段枚举（`adapter.receive → binding.resolve → gateway.accept → idempotency.claim → session.lock → governance.evaluate → worker.dispatch → runner.invoke → data.access → reply.compose → delivery.queue/attempt/result → recovery.reconcile`）、outcome 枚举、`not_applicable` 语义、组件/依赖/结果注册表拒绝未知值。[FR-002, FR-003, FR-007, NFR-007] [DEC-001] [Evidence: targeted RED]
- [X] T007 [P] 为全部稳定运维错误编写 RED 单元测试到 `tests/unit/operations/test_operations_errors.py`：`telemetry_unavailable`、`telemetry_dropped`、`telemetry_adapter_incompatible`、`diagnostic_access_denied`、`health_state_unknown`、`release_not_found`、`release_not_authorized`、`release_conflict`、`stale_release_fence`、`snapshot_invalid`、`snapshot_digest_mismatch`、`configuration_incompatible`、`quality_gate_paused`、`hard_gate_triggered`、`rollback_target_unavailable`、`release_state_unavailable`、`drain_timeout` 的 code、retryable 与无后端详情 envelope。[FR-007, FR-021, NFR-007] [DEC-002, DEC-003, DEC-004] [Evidence: targeted RED]
- [X] T008 [P] 为可观测与运维领域模型编写 RED 单元测试到 `tests/unit/observability/test_models.py` 和 `tests/unit/operations/test_models.py`：`TrustedCorrelationContext`、`DiagnosticSpan`、`TelemetryEnvelope`、`CriticalDiagnosticSummary`、`MetricDefinition`、`DependencyObservation`、`RoleReadinessSnapshot`、`PlatformHealthSnapshot`、`AlertIncident`、`ConfigurationSnapshot`、`CanaryRelease`、`TenantConfigRoute`、`ExecutionConfigPin`、`ReleaseGateSignal`、`RollbackDecision`、`CapacityScenario/Run/Comparison`、`DrainSnapshot` 的不变量、联合键与禁字段。[FR-001, FR-002, FR-003, FR-005, FR-012, FR-017, FR-023, FR-028] [DEC-001, DEC-004, DEC-005] [Evidence: targeted RED]
- [X] T009 [P] 为全部异步端口 Protocol 编写 RED 契约测试到 `tests/contract/observability/test_port_shapes.py` 和 `tests/contract/operations/test_port_shapes.py`：`CorrelationContextPort`、`TelemetryRecorderPort`、`TelemetryExporterPort`、`SamplingPolicyPort`、`TelemetryBufferPort`、`HealthProbePort`、`AlertRepository`、`AlertNotifierPort`、`DiagnosticQueryPort`、`ConfigurationSnapshotRepository`、`TenantConfigRouteRepository`、`ReleaseRepository`/`ReleaseCoordinator`、`GateEvaluationPort`、`CapacityHarnessPort`、`DrainControllerPort` 的方法签名、异步性与稳定错误。[FR-033] [DEC-001, DEC-002, DEC-003, DEC-004] [Evidence: port shape RED]
- [X] T010 [P] 为 OpenTelemetry 依赖锁定编写 RED 验证到 `tests/unit/observability/test_otel_dependency_lock.py`：`opentelemetry-api`、`opentelemetry-sdk`、`opentelemetry-exporter-otlp-proto-http` 固定版本可直接 import、与 `uv.lock` 一致、官方 `trpc_agent_sdk.telemetry` span 公开形状快照（版本锁定契约）。[FR-033, NFR-005] [DEC-001] [Evidence: dependency lock RED]
- [X] T011 [P] 为 PostgreSQL schema v6 原地升级到 v7 编写 RED 集成测试到 `tests/integration/operations/test_schema_v7_upgrade.py`：`007_observability_operations.sql` 幂等、未来版本拒绝、旧数据保留、8 张新表全部 tenant 联合键首列、不可变表无 update 业务路径、mutable projection 带 revision/fence。[FR-017, FR-022, FR-033, FR-034] [DEC-004] [Evidence: v6→v7 RED]
- [X] T012 运行 T006–T011 的精确 pytest 命令，确认失败原因仅为尚未实现的新契约，并把命令和失败摘要记录到 `specs/008-observability-operations-flow/validation-results.md`。[FR-033, FR-034] [DEC-001, DEC-002, DEC-003, DEC-004] [Evidence: foundational RED record]

### Foundational implementation — RED 后实施

- [X] T013 [P] 在 `trpc_service/observability/taxonomy.py` 实现中央阶段、结果、组件、依赖和错误分类注册表，任何 Adapter 不得私造互相矛盾命名。[FR-002, FR-003, FR-007, NFR-007] [DEC-001] [Evidence: T006 GREEN]
- [X] T014 [P] 在 `trpc_service/observability/models.py` 实现可信关联、DiagnosticSpan、TelemetryEnvelope、CriticalDiagnosticSummary、MetricDefinition 与健康/告警模型；在 `trpc_service/operations/models.py` 实现配置快照、灰度发布、路由、执行 pin、门槛信号、回滚决策、容量与排空模型。[FR-001, FR-002, FR-003, FR-005, FR-012, FR-017, FR-023, FR-028] [DEC-001, DEC-004, DEC-005] [Evidence: T008 GREEN]
- [X] T015 [P] 在 `trpc_service/observability/contracts.py` 和 `trpc_service/operations/contracts.py` 实现全部异步端口，保持与第七阶段既有端口兼容。[FR-033] [DEC-001, DEC-002, DEC-003, DEC-004] [Evidence: T009 GREEN]
- [X] T016 [P] 在 `trpc_service/operations/operations_errors.py` 实现 T007 全部稳定错误及其 retryable 与安全 envelope 语义。[FR-007, FR-021] [DEC-002, DEC-003, DEC-004] [Evidence: T007 GREEN]
- [X] T017 [P] 在 `trpc_service/storage/postgres/migrations/007_observability_operations.sql`、`trpc_service/storage/postgres/models.py` 和 `trpc_service/storage/postgres/database.py` 添加前向 schema v7：`configuration_snapshots`、`configuration_releases`、`release_targets`、`tenant_config_routes`、`execution_config_pins`、`release_gate_signals`、`release_transition_events`、`alert_incidents`，禁止清表式升级。[FR-017, FR-022, FR-033, FR-034] [DEC-004] [Evidence: T011 GREEN]
- [X] T018 运行 T006–T011 同一组命令和既有 storage/schema 回归，确认 GREEN；将通过/跳过数量及 schema version=7 证据记录到 `specs/008-observability-operations-flow/validation-results.md`。[FR-033, FR-034] [DEC-001, DEC-004] [Evidence: foundational GREEN record]

**Checkpoint**：中央枚举、错误契约、领域模型、端口和数据库结构完成，用户故事实现可以开始。

---

## Phase 3: US1 — 端到端链路诊断（Priority: P1）🎯 MVP

**Goal**：任一外部请求可用同一可信关联标识还原完整处理轨迹，官方 Runner span 在出口前完成脱敏，避免“链路可查但把正文送出进程”。

**Independent Test**：分别从飞书、企业微信和签名 HTTP 入口发送成功、拒绝与失败请求，按关联标识查询完整阶段记录，核对回复、审计与处理终态；重复投递只关联一个业务终态。

### Tests for US1 — 必须先 RED

- [X] T019 [P] [US1] 在 `tests/unit/observability/test_correlation_context.py` 编写 RED 测试：`start_root` 对非法外部 trace 重建、消息正文与不可信 metadata 不得覆盖标识、`bind_tenant` scope 不匹配拒绝、`link_attempt` 只追加可信关联、`inject`/`extract` 仅限内部 transport、`trace_digest` 安全引用格式。[FR-001, FR-004, FR-032] [DEC-001] [Evidence: correlation RED]
- [X] T020 [P] [US1] 在 `tests/contract/observability/test_sanitizing_processor.py` 编写 RED 测试：`SanitizingSpanProcessor` 保留 trace/span/parent id、时间、status、scope，只保留白名单属性，丢弃 events/links 原文与 `*.input`、`*.output`、`state.*`、URL、Secret、用户/消息/tenant 原值，不修改 `ReadableSpan` 私有字段，SDK 形状不兼容时 fail closed 为 `telemetry_adapter_incompatible`。[FR-002, FR-031, NFR-003] [DEC-001] [Evidence: sanitizing RED]
- [X] T021 [P] [US1] 在 `tests/integration/observability/test_runner_span_nesting.py` 编写 RED 测试：Gateway 激活平台根上下文后调用官方 Runner，`trpc.python.agent` 等 span 成为同一 trace 的子节点，`gen_ai.*` metric 复用同一 Provider，不改 SDK 私有实现。[FR-002, FR-005, FR-033] [DEC-001] [Evidence: runner nesting RED]
- [X] T022 [P] [US1] 在 `tests/e2e/observability/test_end_to_end_trace.py` 编写 RED 测试：双 IM 与 HTTP 入口的成功/拒绝/失败请求全阶段可按关联标识还原，重复投递区分首次执行与幂等命中且只有一个业务终态，未进入阶段显式 `not_applicable`，IM 回复记录排队/尝试/成功/失败/限流/未知。[FR-001, FR-002, FR-003, FR-004, FR-011, FR-034] [DEC-001] [Evidence: E2E trace RED]
- [X] T023 [P] [US1] 在 `tests/contract/observability/test_diagnostic_query.py` 编写 RED 测试：`DiagnosticQueryPort` 先授权并写最小 access Audit 再查询，tenant scope 只返回同 scope 数据，跨租户返回空集合，telemetry outage 时返回 `partial_telemetry` 而非伪造成完整 trace。[FR-016, FR-032] [DEC-002] [Evidence: diagnostic query RED]
- [X] T024 [US1] 运行 T019–T023 并把精确失败点记录到 `specs/008-observability-operations-flow/validation-results.md`，共享环境缺失时只记录 skip 并在 Docker 环境补跑。[FR-033, FR-034] [DEC-001, DEC-002] [Evidence: US1 RED record]

### Implementation for US1

- [X] T025 [P] [US1] 在 `trpc_service/observability/context.py` 实现 `TrustedCorrelationContext`、W3C trace context 传播、可信 `TenantScope` 绑定与 `trace_digest`，`first_claim_trace_id`/`owner_trace_id`/`execution_trace_id` 只追加不覆盖。[FR-001, FR-004, FR-032] [DEC-001] [Evidence: T019 GREEN]
- [X] T026 [P] [US1] 在 `trpc_service/observability/sanitizing.py` 实现 `SanitizingSpanProcessor` 与 `SafeSpanEnvelope` 构造，进程出口前完成脱敏，原始 span 不交给网络 exporter。[FR-002, FR-031, NFR-003] [DEC-001] [Evidence: T020 GREEN]
- [X] T027 [P] [US1] 在 `trpc_service/observability/otel.py` 实现 `TelemetryBootstrap`：进程启动时幂等创建一次 TracerProvider/MeterProvider，并先于所有 Runner 实例安装。[FR-002, FR-005] [DEC-001] [Evidence: T021 GREEN]
- [X] T028 [US1] 在 `trpc_service/gateway/service.py`、`trpc_service/worker/service.py` 和 `trpc_service/channels/` 接入统一阶段 span 与 `DiagnosticSpan` 记录：入口、租户绑定、幂等、Session、治理、执行、数据访问、投递与恢复；Delivery 状态含排队、尝试、成功、明确失败、限流与结果未知，重试复用既有投递幂等语义。[FR-002, FR-003, FR-004, FR-011] [DEC-001] [Evidence: T022 stage GREEN]
- [X] T029 [P] [US1] 在 `trpc_service/observability/service.py` 实现 `TelemetryRecorderPort` 与 `DiagnosticQueryPort`，并扩展 `trpc-agent-trace-diagnose` CLI 只输出安全 trace reference、阶段图、稳定错误、generation 与 `partial_telemetry` 标记。[FR-002, FR-003, FR-016] [DEC-001, DEC-002] [Evidence: US1 GREEN record]

**Checkpoint**：US1 可独立演示按关联标识还原完整链路，且官方 span 与正文不出进程。

---

## Phase 4: US2 — 安全的租户级指标与日志（Priority: P1）

**Goal**：请求量、错误率、延迟、投递、用量和后端状态可观察；租户数据不串查、标签低基数、日志零泄露；关键异常 100% 保留、普通成功比例采样、普通遥测 fail-open。

**Independent Test**：两个租户使用相同用户、Session 和消息标识产生不同结果，验证查询隔离、敏感标记零命中和标签值集合有上限。

### Tests for US2 — 必须先 RED

- [X] T030 [P] [US2] 在 `tests/unit/observability/test_metric_registry.py` 编写 RED 测试：`MetricRegistry` 固定 name/unit/instrument/labels 与有限枚举域，拒绝动态名称和未注册 label；核心指标（`trpc.requests`、`trpc.stage.duration`、`trpc.runner.duration`、`trpc.tool.duration`、`trpc.channel.delivery`、`trpc.state.operation.duration`、`trpc.recovery`、`trpc.telemetry.dropped`、`trpc.release.transition`）全部注册；确定性 Runner 下 Token/成本标记 `not_applicable`。[FR-005, FR-006, NFR-007] [DEC-001] [Evidence: registry RED]
- [X] T031 [P] [US2] 在 `tests/unit/observability/test_sampling_policy.py` 编写 RED 测试：关键分类（错误、安全拒绝、恢复、跨租户尝试、结果未知）100% `keep_full` 且租户不可调低；普通成功按 trace+scope digest+config version 稳定 hash 决策；默认 10%、平台 ceiling 25%，非法 override 拒绝配置；同一 trace 全 span 决策一致。[FR-008] [DEC-001] [Evidence: sampling RED]
- [X] T032 [P] [US2] 在 `tests/unit/observability/test_telemetry_buffer.py` 编写 RED 测试：`offer` 非阻塞、关键保留区不少于 20%、普通满淘汰最旧、最多 3 次重试并受 TTL 限制、分类 drop counter、完整关键空间耗尽时生成固定大小 `CriticalDiagnosticSummary`、禁止写入磁盘/Redis/PostgreSQL、队列默认容量 10,000 有界。[FR-009, NFR-004] [DEC-002] [Evidence: buffer RED]
- [X] T033 [P] [US2] 在 `tests/contract/observability/test_exporter_adapter.py` 编写 RED 测试：`OtlpHttpExporterAdapter` 只接受已验证安全 envelope，结果为 `success | retryable_failure | permanent_failure`，有限重试/退避/抖动与超时，异常折叠为稳定结果不穿透业务。[FR-009, NFR-004, NFR-005] [DEC-002] [Evidence: exporter RED]
- [X] T034 [P] [US2] 在 `tests/contract/observability/test_diagnostic_isolation.py` 编写 RED 测试：双租户同名会话与相同外部消息标识下，日志、指标、追踪、诊断查询互不串用；tenant/user/session/message/trace 原值或 digest 不作为指标标签；高基数输入进入标签边界被拒绝。[FR-006, FR-032] [DEC-001] [Evidence: isolation RED]
- [X] T035 [P] [US2] 在 `tests/security/observability/test_no_sensitive_leak.py` 编写 RED 测试：预置 Secret、Token、response URL、手机号、邮箱和消息正文经过成功与失败路径后，在普通日志、指标、追踪、错误详情、运行事件、告警正文和 Git 候选文件中未脱敏命中为 0；Audit 不可用时既有 fail-closed 边界不变，普通遥测故障不改变业务终态。[FR-009, FR-010, FR-031, SC-003] [DEC-002] [Evidence: security RED]
- [X] T036 [US2] 运行 T030–T035 并记录 RED 到 `specs/008-observability-operations-flow/validation-results.md`。[FR-033, FR-034] [DEC-001, DEC-002] [Evidence: US2 RED record]

### Implementation for US2

- [X] T037 [P] [US2] 在 `trpc_service/observability/sampling.py` 实现 `OutcomeAwareSamplingPolicy`：root 结束后对整棵 trace 作结果感知决策，普通成功稳定 hash、租户 override 夹在平台 ceiling 内。[FR-008] [DEC-001] [Evidence: T031 GREEN]
- [X] T038 [P] [US2] 在 `trpc_service/observability/buffer.py` 实现 `PriorityTelemetryBuffer`：双优先级有界内存队列、关键保留区、有限重试、TTL 丢弃与分类计数，只处理安全 envelope。[FR-009, NFR-004] [DEC-002] [Evidence: T032 GREEN]
- [X] T039 [P] [US2] 将 `trpc_service/metrics/` 现有 recorder 收敛到中央 `MetricRegistry`（低基数标签、稳定枚举），并将 `trpc_service/log/` 的 raw `trace_id` 日志替换为 safe envelope + `trace_digest` 引用。[FR-005, FR-006, FR-007, NFR-007] [DEC-001] [Evidence: T030/T034 GREEN]
- [X] T040 [US2] 在 `trpc_service/observability/service.py` 接入 OTLP exporter 适配、出口健康与 drop counter 暴露，运行 T030–T035 全组命令确认 US2 GREEN 并记录双租户隔离与零泄露证据到 `specs/008-observability-operations-flow/validation-results.md`。[FR-009, FR-033, FR-034] [DEC-002] [Evidence: US2 GREEN record]

**Checkpoint**：US2 可独立演示安全、隔离、低基数且 fail-open 的指标与日志链路。

---

## Phase 5: US3 — 健康、告警与依赖故障处置（Priority: P1）

**Goal**：角色级 readiness 矩阵、路径级平台聚合、去重告警与恢复通知；关键依赖故障 30 秒内反映状态，恢复稳定 60 秒内关闭活动告警。

**Independent Test**：依次注入关键依赖断连、认证失败、超时、恢复和重复抖动，验证就绪变化、业务语义、告警合并及恢复通知。

### Tests for US3 — 必须先 RED

- [X] T041 [P] [US3] 在 `tests/unit/observability/test_role_readiness.py` 编写表驱动 RED 测试：Gateway、Worker、Feishu Adapter、WeCom Adapter、Recovery/Operator 五角色的关键依赖矩阵；关键依赖 missing/unknown 即 `unready`；普通 telemetry 与单一渠道只导致 `degraded`；`liveness` 独立于外部依赖，不得触发重启风暴；过期观察为 `unknown` 不无限延期。[FR-012, FR-013] [DEC-003] [Evidence: readiness RED]
- [X] T042 [P] [US3] 在 `tests/unit/observability/test_alert_state_machine.py` 编写 RED 测试：`PENDING → FIRING → RECOVERING → RESOLVED` 状态机、触发持续窗口未满足保持 PENDING、恢复稳定窗口未满足保持 RECOVERING、指纹（rule/severity/role/component/scope digest/stable reason）去重合并、CAS `state_version`、`notification_id=fingerprint:state_version` 至少一次通知语义、告警正文不含租户原值/内容/Secret。[FR-014, FR-015] [DEC-003] [Evidence: alert RED]
- [X] T043 [P] [US3] 在 `tests/contract/observability/test_health_endpoints.py` 编写 RED 测试：`GET /health/live` 仅进程不可推进时非 200；`GET /health/ready` ready 200 / unready 503 并带稳定 reason code；`GET /health/status` 未授权 403 并形成最小 Audit，授权返回路径级汇总；响应不含 DSN、host、Secret 或 tenant 原值。[FR-012, FR-016, FR-031] [DEC-003] [Evidence: endpoints RED]
- [X] T044 [P] [US3] 在 `tests/integration/observability/test_dependency_faults.py` 编写 RED 测试：PostgreSQL 权威 down → Gateway/Operator unready 且新执行 fail closed；Redis lease/fence down → 受影响角色 unready；单 IM 渠道断线 → 渠道路径 unready、平台 degraded、另一渠道继续；Audit down → 受保护写 fail closed 零副作用；同一依赖持续抖动 → 告警去重；恢复稳定 → 一次 resolved 通知；30 秒状态变化与 60 秒恢复关闭时限。[FR-010, FR-012, FR-013, FR-015, SC-005] [DEC-003] [Evidence: fault matrix RED]
- [X] T045 [US3] 运行 T041–T044 并记录 RED 到 `specs/008-observability-operations-flow/validation-results.md`。[FR-033, FR-034] [DEC-003] [Evidence: US3 RED record]

### Implementation for US3

- [X] T046 [P] [US3] 在 `trpc_service/observability/health.py` 实现 `HealthProbePort`：独立短超时探针、角色矩阵评估与 `PlatformHealthSnapshot` 路径级聚合，未知关键依赖不伪造 ready。[FR-012, FR-013] [DEC-003] [Evidence: T041 GREEN]
- [X] T047 [P] [US3] 在 `trpc_service/observability/alerts.py` 实现 `AlertRepository` CAS 状态机与 `AlertNotifierPort`，通知只含影响范围摘要、稳定原因、安全证据引用和建议操作。[FR-014, FR-015] [DEC-003] [Evidence: T042 GREEN]
- [X] T048 [P] [US3] 在 `trpc_service/storage/postgres/operations_repositories.py` 实现 `alert_incidents` 跨节点 CAS、指纹合并与恢复关闭，多节点相同指纹只有一个逻辑 state version。[FR-015, FR-033] [DEC-003] [Evidence: T042 PG GREEN]
- [X] T049 [P] [US3] 在 `trpc_service/web/app.py` 实现三个健康端点与授权边界，接入 `trpc_service/_shared_server.py` 启动路径。[FR-012, FR-016] [DEC-003] [Evidence: T043 GREEN]
- [X] T050 [US3] 将依赖探针接入各组件（channels 连接状态、PostgreSQL/Redis 可达、telemetry 出口健康、Runner 初始化），运行 T041–T044 全组命令确认 US3 GREEN 并记录故障矩阵证据到 `specs/008-observability-operations-flow/validation-results.md`。[FR-013, FR-034, SC-005] [DEC-003] [Evidence: US3 GREEN record]

**Checkpoint**：US3 可独立演示健康矩阵、依赖故障降级与去重告警。

---

## Phase 6: US4 — 租户级灰度发布与配置回滚（Priority: P1）

**Goal**：不可变配置版本、租户 cohort 灰度、硬门槛自动回滚、质量门槛暂停人工决定；回滚只切换新请求边界，历史事实不被改写。

**Independent Test**：创建旧版本、新版本和两个租户灰度范围，验证范围外租户保持旧版本、失败后停止推进、回滚后新请求使用旧版本且历史审计不被改写。

### Tests for US4 — 必须先 RED

- [X] T051 [P] [US4] 在 `tests/contract/operations/test_snapshot_repository.py` 编写 RED 测试：`ConfigurationSnapshotRepository.create` 只允许不可变插入，相同 tenant/id/digest 返回原值、不同 digest 冲突；Secret 只允许 `secret_ref`；`verify_compatible` 按 contract version 判定；scope、digest、兼容性或 Audit 不可验证时无写入。[FR-017, FR-021] [DEC-004] [Evidence: snapshot RED]
- [X] T052 [P] [US4] 在 `tests/unit/operations/test_release_state_machine.py` 编写 RED 测试：`DRAFT → VALIDATED → CANARY → COMPLETED` 全转换及 `PAUSED_QUALITY`、`PAUSED_INSUFFICIENT_SAMPLE`、`ROLLING_BACK → ROLLED_BACK`、`FAILED`、`FAILED_REQUIRES_REPAIR` 分支；所有转换要求 `expected_revision` 与不低于已见值的 release fence；非法转换无副作用；相同 `command_id` 重试返回首次结果。[FR-018, FR-019, FR-022] [DEC-004] [Evidence: release machine RED]
- [X] T053 [P] [US4] 在 `tests/contract/operations/test_route_resolution.py` 编写 RED 测试：`resolve_for_new_execution` 在同一 tenant scope 内读取权威 route 并创建/读取不可变 pin，重复 key+相同 fingerprint 返回同一 pin、不同 fingerprint 为冲突；PostgreSQL 不可用、route 缺失、禁用、摘要不符或不兼容时 fail closed，绝不回退进程默认配置或 Redis 缓存；范围外 tenant 不可见 candidate。[FR-020, FR-021] [DEC-004] [Evidence: route RED]
- [X] T054 [P] [US4] 在 `tests/unit/operations/test_gate_evaluation.py` 编写 RED 测试：`HARD_STOP` 任一跨租户、未授权副作用、数据一致性或配置不兼容信号首次出现即 latch 并阻止候选新请求；`QUALITY_PAUSE` 需达到观察窗口+最小样本后越线才暂停；`INSUFFICIENT_SAMPLE` 窗口结束样本不足禁止静默推进；`PASS` 才允许 CAS 推进；硬信号必须来自持久 enforcement point 而非可丢 telemetry。[FR-018, FR-019] [DEC-004] [Evidence: gate RED]
- [X] T055 [P] [US4] 在 `tests/integration/operations/test_release_transactions.py` 编写 RED 测试：release state、tenant routes、transition events、`RollbackDecision` 与正式 Audit 同一 PostgreSQL 事务，Audit 失败整体回滚；提交前崩溃无变化、提交后响应丢失按 `command_id` 返回原结果；旧 fence 写入被拒、新节点从最后 committed revision 接管；Redis 缓存失败不影响 PG 权威。[FR-019, FR-020, FR-022, FR-034] [DEC-004] [Evidence: release tx RED]
- [X] T056 [P] [US4] 在 `tests/e2e/operations/test_tenant_canary_release.py`、`tests/e2e/operations/test_hard_gate_rollback.py`、`tests/e2e/operations/test_quality_gate_pause.py` 编写 RED 测试：范围外租户始终使用 stable 版本；硬门槛首次命中即停止 candidate 新请求并自动回滚；质量门槛达到最小样本后越线只暂停等待授权；回滚后目标租户新请求统一使用已知良好版本；在途执行固定原 pin 不混用；未执行副作用按当前治理重新授权；混合版本节点不支持的配置使其退出就绪。[FR-018, FR-019, FR-020, FR-021, FR-022, FR-034, SC-006] [DEC-004] [Evidence: canary E2E RED]
- [X] T057 [US4] 运行 T051–T056 并记录 RED 到 `specs/008-observability-operations-flow/validation-results.md`。[FR-033, FR-034] [DEC-004] [Evidence: US4 RED record]

### Implementation for US4

- [X] T058 [P] [US4] 在 `trpc_service/operations/release.py` 实现 `ReleaseCoordinator`：八类命令、状态机、事务边界、Redis lease/fence 与 Redis 缓存提交后刷新。[FR-017, FR-018, FR-019, FR-022] [DEC-004] [Evidence: T052/T055 GREEN]
- [X] T059 [P] [US4] 在 `trpc_service/storage/postgres/operations_repositories.py` 实现 snapshot、route、pin、gate signal、transition event 与 rollback decision 的 PostgreSQL 仓库，含行锁与 CAS。[FR-017, FR-020, FR-022, FR-033] [DEC-004] [Evidence: T051/T053/T055 PG GREEN]
- [X] T060 [US4] 在 `trpc_service/gateway/service.py` 为新执行固定 `ExecutionConfigPin`，在 `trpc_service/worker/service.py` 实现 snapshot-aware Runner cache key，并让 `trpc_service/recovery/reconciler.py` 沿用原 pin 恢复。[FR-020, FR-022] [DEC-004] [Evidence: T053 GREEN]
- [X] T061 [US4] 在 `trpc_service/governance/` 实现持久硬门槛信号：enforcement point 在安全/一致性违规时与 Audit 同事务写 latch，route resolver 看到 latch 即选择 last-good，不依赖可丢 telemetry。[FR-019, FR-010] [DEC-004] [Evidence: T054 GREEN]
- [X] T062 [US4] 运行 T051–T056 全组命令及第二、三、五、六阶段既有回归，确认 US4 GREEN，把双租户范围隔离、双节点 CAS/fence、各崩溃点恢复与回滚后 5 分钟统一旧版本证据记录到 `specs/008-observability-operations-flow/validation-results.md`。[FR-019, FR-034, FR-035, SC-006] [DEC-004] [Evidence: US4 GREEN record]

**Checkpoint**：US4 可独立演示租户级灰度、硬门槛自动回滚与安全回滚边界。

---

## Phase 7: US5 — 可重复的容量评估（Priority: P2）

**Goal**：固定业务画像的容量场景与双门禁判定，产生明确标记 `local_evidence` 的机器可读容量报告。

**Independent Test**：在相同环境使用固定租户、会话、消息大小与并发梯度重复运行，比较结果差异并生成包含环境、负载、分位延迟、错误率和资源峰值的报告。

### Tests for US5 — 必须先 RED

- [X] T063 [P] [US5] 在 `tests/unit/operations/test_capacity_scenario.py` 编写 RED 测试：`CapacityScenario` manifest 不可变且固定 2 tenant、2 Worker、100 并发 Session、每 Session 10 条有序消息（共 1,000 条）、固定 seed、消息大小桶、重复比例、Tool 比例、数据读写比例、warm-up 与测量轮次。[FR-023] [DEC-005] [Evidence: scenario RED]
- [X] T064 [P] [US5] 在 `tests/unit/operations/test_capacity_comparison.py` 编写 RED 测试：`CapacityComparison` 先判正确性零容忍（丢失、跨租户串用、不可解释重复任一非零即失败），再判相对性能（吞吐下降或 p50/p95/p99 增幅任一超过 10% 即失败）；环境不等价标记 invalid 不放宽阈值；报告区分已测量事实、推算结果和未覆盖因素，不设置生产绝对吞吐/延迟/SLA。[FR-024, FR-025, NFR-001, NFR-006] [DEC-005] [Evidence: comparison RED]
- [X] T065 [P] [US5] 在 `tests/performance/test_observability_capacity_gate.py` 编写 RED 测试：同机同拓扑同数据初态，warm-up → telemetry off baseline → telemetry on，记录吞吐、p50/p95/p99、CPU/内存与 Redis/PostgreSQL 压力峰值，输出不含正文或 Secret 的机器可读报告。[FR-023, FR-024, FR-025, SC-007, SC-008] [DEC-005] [Evidence: capacity gate RED]
- [X] T066 [US5] 运行 T063–T065 并记录 RED 到 `specs/008-observability-operations-flow/validation-results.md`。[FR-033] [DEC-005] [Evidence: US5 RED record]

### Implementation for US5

- [X] T067 [P] [US5] 在 `trpc_service/operations/capacity.py` 实现 `CapacityHarnessPort`：`prepare`/`run`/`compare`、确定性负载生成、资源采样与瓶颈判断，环境指纹不含敏感值。[FR-023, FR-024, FR-025, NFR-001, NFR-006] [DEC-005] [Evidence: T063/T064 GREEN]
- [X] T068 [US5] 在共享后端就绪的环境运行正式容量门禁（2 tenant、2 Worker、100 并发 Session、1,000 消息），生成 `specs/008-observability-operations-flow/capacity-results.md` 及机器可读结果，标记 `local_evidence` 且明确排除真实模型延迟、真实 IM 限流和生产基础设施 SLA，把命令与退出码记录到 `specs/008-observability-operations-flow/validation-results.md`。[FR-024, FR-025, SC-007, SC-008] [DEC-005] [Evidence: capacity run transcript]

**Checkpoint**：US5 可独立重复运行并产出双层门禁容量证据。

---

## Phase 8: US6 — 最小部署与安全停机（Priority: P2）

**Goal**：`deploy/local-observable/` 最小可观察部署从干净环境启动、验证、排空并停止；Worker 停机先撤就绪、完成或移交在途任务。

**Independent Test**：从干净环境启动最小拓扑，完成跨节点消息，然后依次停止一个 Worker、渠道 Adapter 和共享依赖，验证排空、接管、状态与恢复说明。

### Tests for US6 — 必须先 RED

- [X] T069 [P] [US6] 在 `tests/unit/operations/test_drain_lifecycle.py` 编写 RED 测试：`accepting → draining → drained | timed_out` 状态只能前进；`begin` 原子撤销 readiness 并停止新 claim；重复 SIGTERM/stop 幂等；deadline 后不能证明结果的任务标记 outcome unknown，禁止自动重放非幂等副作用；排空快照含在途/完成/移交/未知计数。[FR-028] [DEC-003] [Evidence: drain RED]
- [X] T070 [P] [US6] 在 `tests/e2e/operations/test_worker_drain_takeover.py` 编写 RED 测试：Worker-A 先撤 readiness 不再接新 claim，在途执行完成、由更高 fence 接管或明确标记 unknown；Worker-B 持续服务同一租户会话；租约到期接管不产生重复业务结果。[FR-026, FR-028, FR-034, SC-008] [DEC-003] [Evidence: drain E2E RED]
- [X] T071 [P] [US6] 在 `tests/e2e/observability/test_local_observable_deployment.py` 编写 RED 测试：叠加层启动后核心服务 `/health/live`、`/health/ready` ready，两个节点可接续同一租户会话；Collector debug 输出能按安全 trace reference 定位 Adapter/Gateway/Worker/官方 Runner/数据/投递阶段且不含测试敏感标记。[FR-026, FR-034, SC-009] [DEC-002, DEC-003] [Evidence: deployment RED]
- [X] T072 [P] [US6] 在 `tests/e2e/observability/test_exporter_outage.py` 编写 RED 测试：停止 otel-collector 后消息业务仍成功、平台 degraded、缓冲不超过上限、drop counter 可见；恢复后健康状态在规格时限内恢复；正式 Audit 行为不得改变。[FR-009, FR-012, NFR-004, SC-004] [DEC-002] [Evidence: outage RED]
- [X] T073 [US6] 运行 T069–T072 并记录 RED 到 `specs/008-observability-operations-flow/validation-results.md`，Docker 权限被拦截时明确记录环境原因，不绕过权限。[FR-033, FR-034] [DEC-002, DEC-003] [Evidence: US6 RED record]

### Implementation for US6

- [X] T074 [P] [US6] 在 `trpc_service/operations/drain.py` 实现 `DrainControllerPort`，并在 `trpc_service/worker/service.py` 与进程信号处理接入排空生命周期。[FR-028] [DEC-003] [Evidence: T069 GREEN]
- [X] T075 [P] [US6] 新增 `deploy/local-observable/compose.yaml`：schema-init、Gateway、Worker-A/B、OTel Collector，复用 `deploy/local-shared/compose.yaml` 的 Redis/PostgreSQL；核心服务 healthcheck 使用 `/health/ready`；真实飞书/企业微信 Adapter 放入显式 profile；唯一 Compose project name；Secret 仅通过当前进程或 `.env.local` 注入。[FR-026] [DEC-002] [Evidence: T071 GREEN]
- [X] T076 [P] [US6] 新增 `deploy/local-observable/otel-collector.yaml`：与平台采样语义一致的 tail policy（关键全保留、普通成功默认 10%）、trace-affinity 聚合说明与 debug exporter，作为网络侧第二道防线。[FR-008, NFR-005] [DEC-001] [Evidence: collector config review]
- [X] T077 [US6] 按 `specs/008-observability-operations-flow/quickstart.md` 第 5 节在真实 Compose 环境运行 T069–T072 全组命令确认 US6 GREEN，把排空快照、接管、出口故障与恢复证据记录到 `specs/008-observability-operations-flow/validation-results.md`。[FR-026, FR-028, FR-034, SC-009] [DEC-002, DEC-003] [Evidence: US6 GREEN record]

**Checkpoint**：US6 可独立演示最小部署启动、跨节点会话、安全排空与出口故障降级。

---

## Phase 9: US7 — 生产推荐拓扑与故障演练（Priority: P2）

**Goal**：供应商中立的生产推荐部署文档、至少八项结构化风险登记和至少四项可重复故障演练，恢复后租户隔离与幂等不变量零破坏。

**Independent Test**：按拓扑和故障矩阵逐项走查节点、网络、数据库、缓存、渠道、配置、遥测和密钥故障，确认每项都有检测、影响、处置、恢复和证据责任人。

### Tests for US7 — 必须先 RED

- [X] T078 [P] [US7] 在 `tests/e2e/operations/test_fault_exercise.py` 编写 RED 测试：至少四项可重复故障演练——OTLP/Collector down、PostgreSQL 配置权威 down 后新执行 fail closed、普通 telemetry outage 期间硬门槛仍 latch 并让新请求走 last-good、Worker 终止排空接管、时钟偏移下单调时长与 wall-time 标记；每次恢复后验证零跨租户泄露与零不可解释重复副作用。[FR-029, FR-034, SC-010] [DEC-002, DEC-003, DEC-004] [Evidence: exercise RED]

### Implementation for US7

- [X] T079 [P] [US7] 实现故障演练注入与恢复验证 harness：复用第三、五、七阶段故障注入 seam 与 fencing/恢复机制，时钟偏移使用单调时长并标记 wall-time 异常，不修改官方 SDK。[FR-029, FR-034] [DEC-003] [Evidence: T078 GREEN]
- [X] T080 [P] [US7] 编写 `specs/008-observability-operations-flow/deployment-topology.md`：最小可观察部署与生产推荐拓扑（LB + 多 Gateway、每渠道冗余 Adapter、多 Worker、独立 Recovery/Operator、外部 HA Redis/PostgreSQL、两层 Collector trace-affinity tail sampling、外部 Secret Provider、发布顺序、备份恢复、扩缩容信号、故障域）；组件名称与 plan.md 架构图和前七阶段一致；明确本地演示与生产建议的差距和补齐路径，不声明生产 HA/SLA。[FR-027, FR-036, SC-011] [Evidence: topology review]
- [X] T081 [P] [US7] 编写 `specs/008-observability-operations-flow/risk-register.md`：至少八项生产风险，每项含触发条件、影响范围、检测信号、预防措施、处置步骤、恢复验证和剩余风险；对应 README 风险清单交付物并可追溯到演练证据。[FR-030, SC-010, SC-011] [Evidence: risk register review]
- [X] T082 [US7] 运行 T078 全组演练命令确认 GREEN，把至少四项演练的实际检测、处置与恢复证据写入 `specs/008-observability-operations-flow/validation-results.md` 并在 `risk-register.md` 中引用，环境缺失的演练项明确标注外部条件与补测步骤。[FR-029, FR-030, SC-010] [DEC-002, DEC-003, DEC-004] [Evidence: US7 GREEN record]

**Checkpoint**：US7 交付生产推荐拓扑、风险登记和可重复演练证据。

---

## Phase 10: US8 — 最终交付与验收追踪（Priority: P2）

**Goal**：README 每项要求可追溯到规格、架构、代码、测试和真实/模拟证据；前七阶段与第八阶段全量回归无未解释失败。

**Independent Test**：随机选择 README 验收项沿追踪表找到对应产物、命令和结果，核对范围声明与实际证据一致。

### Implementation for US8

- [X] T083 [P] [US8] 更新 `README.md` 第八阶段章节：可观测性与运维能力说明、启动与验收命令（对齐 `specs/008-observability-operations-flow/quickstart.md`）、明确本阶段范围声明（不含真实模型 API、管理 UI、生产 K8s 与生产 SLA）。[FR-036] [Evidence: README review]
- [X] T084 [P] [US8] 在 `README.md` 或 `specs/008-observability-operations-flow/` 建立 README 总体验收项追踪矩阵：七项总体验收标准 → 阶段 → FR/NFR/SC → 设计文档 → 测试 → 证据，模拟、真实和仅设计状态均有明确标识，覆盖率为 100%。[FR-036, SC-011] [Evidence: traceability matrix]
- [X] T085 [P] [US8] 编写 `specs/008-observability-operations-flow/阶段成果记录.md`：框架复用（官方 Runner/OTel instrumentation）与平台新增（边界、脱敏、采样、健康、告警、发布、容量）对照、验证环境边界声明和答辩摘要。[FR-036, SC-011] [Evidence: stage summary]
- [X] T086 [US8] 运行前七阶段与第八阶段全量回归 `uv run pytest -q`，把 passed/failed/skipped、每个 skip 的外部环境原因和补测条件记录到 `specs/008-observability-operations-flow/validation-results.md`；存在未解释失败时不得声明本阶段完成。[FR-035, SC-012] [DEC-005] [Evidence: full regression PASS]
- [X] T087 [P] [US8] 运行安全门禁收口：`uv run pytest tests/security -q`、`git diff --check`、`git status --short` 与敏感材料扫描，确认预置敏感标记零命中、无 Secret 进入 Git 候选，并把结果记录到 `specs/008-observability-operations-flow/validation-results.md`。[FR-031, SC-003] [DEC-002] [Evidence: security gate]
- [X] T088 [US8] 按 `specs/008-observability-operations-flow/quickstart.md` 第 2～8 节在真实共享环境完整执行一遍验收命令链（契约、集成、最小部署、灰度回滚、容量、回归与安全），把命令、退出码与结果写入 `specs/008-observability-operations-flow/validation-results.md`；不得用替身结果冒充真实平台结果。[FR-033, FR-034, SC-001–SC-009] [DEC-001, DEC-002, DEC-003, DEC-004, DEC-005] [Evidence: quickstart transcript]
- [X] T089 [US8] 最终收口 `specs/008-observability-operations-flow/validation-results.md`：全部 RED/GREEN 记录、跳过原因、外部条件、补测步骤和证据边界，与 `tasks.md` 勾选状态一致。[FR-035, SC-012] [Evidence: final validation record]

**Checkpoint**：README 验收项 100% 可追踪，最终证据链完整且边界诚实。

---

## Phase 11: Polish & Cross-Cutting Validation

**Purpose**：跨故事一致性、配置边界和最终审查。

- [X] T090 [P] 在 `trpc_service/config/settings.py` 增加有界观测与发布设置：普通成功默认采样率与租户 ceiling、缓冲容量与关键保留区、告警窗口、容量参数、OTLP endpoint；非法值（如 ceiling 超上限）拒绝配置而非静默修正。[FR-008, FR-009, FR-018, NFR-007] [DEC-001, DEC-002] [Evidence: settings validation]
- [X] T091 [P] 执行跨文档一致性检查：`specs/008-observability-operations-flow/` 内无 `NEEDS CLARIFICATION` 残留；README、plan.md、data-model.md、contracts、quickstart、deployment-topology.md 与 risk-register.md 的组件名称、状态语义和错误码一致；每项 Evidence 边界（自动化/真实环境/设计建议）标注正确。[FR-036] [Evidence: consistency review]
- [X] T092 运行 `$speckit-analyze` 风格最终审查：核对任务勾选与 validation-results 证据一一对应、无 HIGH 级遗漏、宪法七原则复查（Framework-First、租户隔离、无状态 Worker、契约优先、默认安全、端到端可观测、Spec 证据），然后把 `specs/008-observability-operations-flow/` 纳入 Git 提交；不提交 DSN、Secret、正文或运行日志中的敏感值。[FR-033, FR-035, FR-036, SC-011, SC-012] [DEC-001–DEC-005] [Evidence: final review]

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup（Phase 1）**：无依赖，可立即开始。
- **Foundational（Phase 2）**：依赖 Phase 1；阻塞所有用户故事（T006–T018 完成前不得开始任何 US 实现）。
- **US1（Phase 3）**：依赖 Foundational；MVP，其余故事都复用其 correlation/stage/sanitizing 基础。
- **US2（Phase 4）**：依赖 Foundational；采样与缓冲依赖 US1 的 envelope 与 outcome 分类。
- **US3（Phase 5）**：依赖 Foundational；健康矩阵引用 US2 的 telemetry 出口健康信号。
- **US4（Phase 6）**：依赖 Foundational；release 事务与硬门槛依赖 US1 的 Audit 关联和阶段记录。
- **US5（Phase 7）**：依赖 US1–US3（容量 A/B 需要完整可观测链路与健康就绪）。
- **US6（Phase 8）**：依赖 US1–US4（部署需 release pin、健康端点与排空语义）。
- **US7（Phase 9）**：依赖 US3、US4、US6（演练复用其故障注入与恢复机制）。
- **US8（Phase 10）与 Polish（Phase 11）**：依赖全部故事完成，串行收口。

### Within Each Story

- 每个 US 的测试任务必须先运行并记录 RED；RED 原因必须是缺少目标行为，不是语法、fixture 或凭证错误。
- 模型/端口在服务之前；服务在接入（Gateway/Worker/web）之前；实现 GREEN 后再运行故事级全组命令。
- 同一生产文件被多个任务修改时按任务号顺序执行，避免并发覆盖。

## Parallel Opportunities

- Phase 1 的 T003/T004/T005 可并行（不同文件）。
- Phase 2 的 RED 测试 T006–T011 可并行编写；实现 T013–T017 在端口契约冻结后可并行（不同文件）。
- US1 内 T019–T023 五个测试文件可并行；T025/T026/T027 实现互不依赖可并行。
- US2 内 T030–T035 可并行；T037/T038/T039 可并行。
- US3 内 T041–T044 可并行；T046–T049 可并行。
- US4 内 T051–T056 可并行；T058/T059 可并行。
- US7 的 T080/T081 文档任务可与 T079 实现并行。
- 跨故事：US2 与 US3 在各自 RED 冻结后可由不同执行者并行推进（不修改同一文件）。

## Parallel Execution Examples

### US1 链路诊断

```text
并行 RED：T019 correlation、T020 sanitizing、T021 runner nesting、T022 E2E trace、T023 diagnostic query
汇合记录：T024
并行实现：T025 context、T026 sanitizing、T027 otel bootstrap
串行接入：T028 gateway/worker/channels → T029 service + CLI
```

### US4 灰度回滚

```text
并行 RED：T051 snapshot、T052 release machine、T053 route、T054 gate、T055 tx、T056 E2E
汇合记录：T057
并行实现：T058 coordinator、T059 PG repositories
串行接入：T060 gateway pin / worker cache → T061 governance hard signal
验收：T062 GREEN + 既有阶段回归
```

### US6 最小部署

```text
RED：T069 drain、T070 drain E2E、T071 deployment、T072 outage
汇合记录：T073
并行实现：T074 drain、T075 compose、T076 collector config
验收：T077 quickstart 第 5 节真实环境 GREEN
```

## Implementation Strategy

### MVP First

1. 完成 Phase 1 与 Phase 2（中央枚举、错误契约、端口、OTel 锁定、schema v7）。
2. 完成 US1（T019–T029），证明同一关联标识可还原完整链路且官方 Runner span 出口前脱敏。
3. 停止并独立运行 US1 测试；只有契约与集成 suite 通过（共享环境缺失时明确记录 skip 与补测条件）才可把 MVP 标记为完成。

### Incremental Delivery

1. US2：补齐安全指标、采样与 fail-open 缓冲，收敛旧 metrics/log。
2. US3：健康矩阵、依赖故障降级与去重告警。
3. US4：配置快照、租户灰度、硬门槛自动回滚。
4. US5：容量 harness 与双门禁报告。
5. US6：最小可观察部署、排空与出口故障演练。
6. US7：生产推荐拓扑、风险登记与故障演练汇总。
7. US8 + Phase 11：README 追踪、全量回归、安全门禁与最终一致性收口。

## Traceability Summary

| Story/Phase | Task range | Primary requirements | Decisions | Independent evidence |
|---|---|---|---|---|
| Setup/Foundation | T001–T018 | FR-001/002/003/005/007/012/017/021/022/023/028/031–034, NFR-004/005/007 | DEC-001–004 | taxonomy/model/port/schema RED→GREEN |
| US1 | T019–T029 | FR-001–004, FR-011, FR-016, FR-031/032/034, NFR-003 | DEC-001, DEC-002 | 三入口全阶段 trace + span 脱敏 |
| US2 | T030–T040 | FR-005–010, FR-031/033/034, NFR-004/005/007 | DEC-001, DEC-002 | 隔离/采样/缓冲/零泄露 |
| US3 | T041–T050 | FR-010, FR-012–016, FR-031/034, SC-005 | DEC-003 | readiness 矩阵 + 告警去重恢复 |
| US4 | T051–T062 | FR-017–022, FR-010/033–035, SC-006 | DEC-004 | cohort 隔离 + 硬门槛回滚 + 事务原子性 |
| US5 | T063–T068 | FR-023–025, NFR-001/006, SC-007/008 | DEC-005 | 双门禁容量报告（local_evidence） |
| US6 | T069–T077 | FR-008/009/012/026/028, NFR-004, SC-004/009 | DEC-001–003 | Compose 部署 + 排空 + 出口故障 |
| US7 | T078–T082 | FR-027/029/030/034/036, SC-010/011 | DEC-002–004 | ≥8 风险 + ≥4 可重复演练 |
| US8/Final | T083–T092 | FR-031/033–036, SC-001–012, NFR-002/003 | DEC-001–005 | README 追踪 + 全量回归 + 安全门禁 |

## Notes

- 所有任务均从未执行状态开始；`trpc_service/observability/` 现有骨架与 `metrics/`、`log/` 旧实现不算本计划验收。
- 每个测试任务都必须先失败，且失败原因应是缺少目标行为，不是语法、fixture、端口或凭证错误。
- 普通遥测（可采样、可丢弃、fail-open）与正式 Audit（不可变、fail-closed）不得互相替代或混淆边界。
- 本地容量、Compose 和演练结果一律标注 `local_evidence`；不得据此宣称生产 SLA、HA 或绝对容量。
- 共享后端凭证只通过当前进程或 `.env.local` 注入；不得把 DSN、密码、Token、response URL、消息正文或完整对象引用写入代码、文档、截图或 Git。
- 若实现发现需要改变 DEC-001～DEC-005，立即停止并回到 clarify，不得在代码中静默改变人工决策。
- 每个 logical group 建议独立 commit；`specs/008-observability-operations-flow/` 当前未跟踪，T092 统一收口提交。

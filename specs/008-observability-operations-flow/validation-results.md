# 第八阶段验证记录

## 验证记录格式（T002）

每个任务使用以下字段记录，不写入 DSN、密码、Token、response URL、原始用户内容或
完整对象引用：

| 日期 | 任务 | 关联 FR/NFR/SC | DEC | 环境（脱敏） | RED 命令/退出码/结果 | GREEN 命令/退出码/结果 | 证据与备注 |
|---|---|---|---|---|---|---|---|
| YYYY-MM-DD | Txxx | FR-xxx | DEC-xxx | Python/后端状态 | command; code; counts | command; code; counts | stable error/skip reason |

方法：先 RED、后 GREEN；共享 Redis/PostgreSQL 未配置时记录 skip，不伪装通过。旧骨架
（`trpc_service/observability/operational.py`、`metrics/` 早期计数与安全日志、
`tests/unit/test_observability_security.py`）的既有行为只作为历史事实保留，不作为本
`tasks.md` 的 T001–T092 验收证据。

## T001 基线清单

- 日期：2026-09-11。
- 分支：`feature/luwenjie`（与 `origin/feature/luwenjie` 同步）。
- 最新提交：`ef028bd feat: complete governance and data backend flows`。
- 工作区：仅 `specs/008-observability-operations-flow/` 未跟踪（本阶段文档）；无其他
  未提交业务变更。
- Python：`3.12.7`（项目约束 `>=3.12,<3.13`；`uv run` 解析到项目 `.venv`）。
- 关键依赖：`trpc-agent-py==1.1.19`、`pydantic==2.13.5`、`starlette==1.6.0`、
  `uvicorn==0.52.4`、`sqlalchemy==2.0.52`、`asyncpg==0.31.0`、`redis==8.1.0`、
  `pytest==9.1.1`。
- OpenTelemetry 锁定状态：`uv.lock` 已解析 `opentelemetry-api==1.44.0`、
  `opentelemetry-sdk==1.44.0`、`opentelemetry-exporter-otlp-proto-http==1.44.0`，
  但三者目前均为 `trpc-agent-py` 的传递依赖，`pyproject.toml` 尚未显式声明——
  该缺口由 T010 的 RED 依赖锁定测试暴露，并在 Foundation 实现中补齐为直接依赖。
- 现有可观测骨架（不算本计划验收）：
  - `trpc_service/observability/__init__.py`（1 行）、`operational.py`（11 行）；
  - `trpc_service/metrics/`：`contracts.py`（22 行）、`inmemory.py`（82 行）、
    `models.py`（79 行）、`shared.py`（68 行）；
  - `trpc_service/log/__init__.py`（32 行）；
  - `trpc_service/operations/`、`trpc_service/storage/postgres/migrations/007_*.sql`
    均不存在，属本阶段新增。
- 现有测试包：`tests/unit|contract|integration|e2e|security` 及其 `data/`、
  `governance/`、`channels/` 子包；`tests/performance/` 不存在（本阶段 T065 需要，
  由 T003 一并创建）；`tests/unit/test_observability_security.py` 为旧骨架测试。
- 环境状态：本进程未读取或记录任何真实凭证；共享 Redis/PostgreSQL URL 未在记录中
  输出，后续由 fixture 只检查是否存在。

## T003–T005 Setup 证据

- T003：十个分层测试包及 `__init__.py` 已创建：`tests/unit/observability`、
  `tests/unit/operations`、`tests/contract/observability`、
  `tests/contract/operations`、`tests/integration/observability`、
  `tests/integration/operations`、`tests/e2e/observability`、
  `tests/e2e/operations`、`tests/performance`（T065 容量门禁所需）、
  `tests/security/observability`（T035 零泄露扫描所需）。
  验证命令：`uv run pytest --collect-only -q`；结果 `377 tests collected`，
  与第七阶段收口全量数一致，collection 无破坏（退出码 0）。
- T004：`tests/observability_support.py` 提供双租户（tenant-alpha/tenant-beta）、
  双 Worker 节点（worker-a/generation=7、worker-b/generation=13）、双渠道节点
  （feishu-a、wecom-a）、五角色枚举（gateway/worker/feishu_adapter/wecom_adapter/
  recovery）、固定 UTC（2026-09-10T12:00:00Z）、稳定 uuid5 trace、
  `sha256:<16hex>` trace digest、scope digest、fence 与 span id、7 个预置敏感哨兵
  （api_key/im_token/database_password/response_url/phone/email/message_body，全部
  为 `.invalid` 域或 sentinel 标记的假值）和 stage fixture builder。
  fixture 自检命令：`uv run python -c "...obs_fixture_summary()..."`；输出摘要与
  格式断言全部通过（digest 长度 23、7 哨兵），不读取真实 Secret。
- T005：`tests/conftest.py` 新增 `ops_namespace` fixture（`phase8-<32hex>` 唯一
  namespace）。共享后端 marker 复用 `pyproject.toml` 已注册的 `shared_backend`
  （与 quickstart 第 4 节 `-m shared_backend` 命令一致，不新增重复 marker）；共享
  URL 继续由既有 `shared_redis_url`/`shared_database_url` fixture 提供，缺失时只
  返回明确 skip 原因且不回显 DSN。临时验证测试 `1 passed` 后已删除，不留临时文件。

## Setup 结论

Phase 1（T001–T005）完成：基线固定、证据模板可用、测试包就绪、fixture 确定且
离线。未修改任何生产语义；`git status` 仍仅新增 `specs/008-observability-operations-flow/`
与本节列出的测试支撑文件。可进入 Phase 2 Foundation（T006–T018）。

## T006–T011 Foundational RED（T012 记录）

- 日期：2026-09-11；命令：`uv run pytest -q -p no:cacheprovider tests/unit/observability/test_taxonomy.py
  tests/unit/operations/test_operations_errors.py tests/unit/observability/test_models.py
  tests/unit/operations/test_models.py tests/contract/observability/test_port_shapes.py
  tests/contract/operations/test_port_shapes.py
  tests/unit/observability/test_otel_dependency_lock.py
  tests/integration/operations/test_schema_v7_upgrade.py`。
- 结果：`39 failed, 1 passed, 1 skipped`，退出码 1（预期 RED）。
- 失败全部为新能力缺失，无语法/fixture/凭证错误：
  - `trpc_service.observability.taxonomy`、`observability.models`、
    `observability.contracts`、`operations.models`、`operations.contracts`、
    `operations.operations_errors` 模块均未实现（安全 import helper 使缺失表现为
    failed 而非 collection error）；
  - `pyproject.toml` 未显式声明三个 OpenTelemetry 直接依赖（T010 有效 RED；
    同文件 `test_otel_packages_are_importable_at_locked_versions` 通过——传递依赖
    1.44.0 可导入；`test_official_runner_telemetry_surface_is_stable` 通过——
    官方 `trpc_agent_sdk.telemetry` 的 tracer/trace_agent/trace_call_llm/
    trace_tool_call/trace_runner/trace_cancellation 与 report_* 符号快照完整）；
  - `SUPPORTED_SCHEMA_VERSION` 仍为 6，`migrations/007_observability_operations.sql`
    不存在，`Base.metadata` 缺 8 张 v7 表（T011 有效 RED）；
  - 1 skipped 为 `test_shared_backend_upgrades_to_v7_idempotently`：本进程未注入
    `TRPC_SHARED_DATABASE_URL`，仅环境缺失，不作为 GREEN 证据；实现完成后必须在
    Docker 共享环境补跑该用例的 RED/GREEN。
- 编写期修正一处快照笔误：官方符号为 `trace_tool_call`（初稿误写
  `trace_execute_tool`，已改为真实符号并加入 `trace_cancellation`）。
- T012 RED 结论：有效 RED 已固定，允许进入 T013–T017 实现。

## T013–T017 Foundational 实现（GREEN 前置）

- T013：`trpc_service/observability/taxonomy.py` 实现中央注册表——14 阶段枚举
  （`adapter.receive` … `recovery.reconcile`）、6 结果枚举（含 `not_applicable`）、
  5 平台角色、14 组件、9 平台管理依赖、`STAGE_COMPONENTS` 中央 stage→component
  映射，及 `validate_stage/validate_outcome/validate_role/validate_component/
  validate_dependency/stage_component` 拒绝未知值。
- T014：`trpc_service/observability/models.py`（TrustedCorrelationContext、
  DiagnosticSpan 敏感属性拒绝、TelemetryEnvelope PRIORITIES/SIGNAL_TYPES 与
  attempt_count 0..3 界、CriticalDiagnosticSummary 固定 8 字段、MetricDefinition
  标签白名单字段、DependencyObservation、RoleReadinessSnapshot
  READINESS/SERVICE_STATES、PlatformHealthSnapshot、AlertIncident STATES、
  MetricObservation）；`trpc_service/operations/models.py`（ConfigurationSnapshot
  明文 Secret 递归拒绝、CanaryRelease 10 态、TenantConfigRoute hard_gate_latched、
  ExecutionConfigPin 联合键、ReleaseGateSignal SEVERITIES、
  ReleaseTransitionEvent、RollbackDecision、CapacityScenario
  total_messages/is_formal_acceptance 双门、CapacityRun、CapacityComparison
  正确性+开销双门（≤10%）、DrainSnapshot 前向状态机与 `drain_transition`）。
- T015：`trpc_service/observability/contracts.py` 9 端口
  （CorrelationContextPort、TelemetryRecorderPort、TelemetryExporterPort、
  SamplingPolicyPort、TelemetryBufferPort、HealthProbePort、AlertRepository、
  AlertNotifierPort、DiagnosticQueryPort）；`trpc_service/operations/contracts.py`
  7 端口（ConfigurationSnapshotRepository、TenantConfigRouteRepository、
  ReleaseRepository 8 命令、ReleaseCoordinator、GateEvaluationPort、
  CapacityHarnessPort、DrainControllerPort）；全部 async 方法为
  coroutine function，风格沿用第七阶段 `@runtime_checkable Protocol`。
- T016：`trpc_service/operations/operations_errors.py` 实现全部 17 个稳定错误
  （TelemetryUnavailable … DrainTimeout），code/retryable 与 spec 一致；
  构造器可接收内部 detail 但 `str(error)` 只渲染稳定 code（不回显
  postgres/redis/dsn/password/secret/token/select/traceback 等任何后端标记）；
  `HardGateTriggered.resolution == "automatic_rollback"`。
- T017：`migrations/007_observability_operations.sql` 前向创建 8 张 v7 表
  （configuration_snapshots、configuration_releases、release_targets、
  tenant_config_routes、execution_config_pins、release_gate_signals、
  release_transition_events、alert_incidents），全部 `CREATE TABLE IF NOT EXISTS`
  且无 DROP；tenant 表以 `tenant_id` 为联合键首列，不可变表无 update 路径，
  release/route 可变投影带 revision/fence。`storage/postgres/models.py` 追加
  对应 8 个 ORM Row；`database.py` 将 `SUPPORTED_SCHEMA_VERSION` 升至 7 并把
  002–007 迁移块收敛为表驱动循环（语义与顺序不变）。
  `pyproject.toml` 显式声明 `opentelemetry-api==1.44.0`、
  `opentelemetry-sdk==1.44.0`、`opentelemetry-exporter-otlp-proto-http==1.44.0`
  为直接依赖，`uv lock` 重锁成功（105 packages，版本与既有传递解析一致）。
- 实现期配套修正（非验收项）：
  - `tests/unit/observability/test_otel_dependency_lock.py` 的 pyproject 解析
    helper 原为 `strip('"')`，无法剥除行尾 `",`，RED 期被缺失断言掩盖；改为
    `strip('",')` 后按真实依赖清单断言；
  - 第七阶段两处版本钉死断言随 v7 前向升级同步放宽：
    `tests/integration/data/test_schema_upgrade.py` 与
    `tests/integration/data/test_memory_cross_node.py` 由 `== 6` 改为 `>= 6`
    （与 005→006 时期 `>= 5` 的既有先例一致）。

## T018 Foundational GREEN 记录

- 日期：2026-09-11；命令与 T012 RED 完全一致：
  `uv run pytest -q -p no:cacheprovider tests/unit/observability/test_taxonomy.py
  tests/unit/operations/test_operations_errors.py tests/unit/observability/test_models.py
  tests/unit/operations/test_models.py tests/contract/observability/test_port_shapes.py
  tests/contract/operations/test_port_shapes.py
  tests/unit/observability/test_otel_dependency_lock.py
  tests/integration/operations/test_schema_v7_upgrade.py`。
- 结果：`40 passed, 1 skipped`，退出码 0（对比 RED `39 failed, 1 passed, 1 skipped`）。
- 1 skipped 仍为 `test_shared_backend_upgrades_to_v7_idempotently`（未注入
  `TRPC_SHARED_DATABASE_URL`，环境缺失；须在 Docker 共享环境补跑 v6→v7 原地升级
  幂等验证，不作为本轮 GREEN 证据）。
- 既有 storage/schema 回归：全量 `uv run pytest -q -p no:cacheprovider` 结果
  `387 passed, 31 skipped, 0 failed`（31 skipped 全部为共享 Redis/PostgreSQL
  环境缺失，与第七阶段收口口径一致）；`--collect-only` 为 `418 tests collected`
  （T003 基线 377 + 本阶段新增 41）。
- schema 证据：`SUPPORTED_SCHEMA_VERSION == 7`；`007_observability_operations.sql`
  存在且包含全部 8 表、`CREATE TABLE IF NOT EXISTS`、无 `DROP TABLE`；
  `Base.metadata.tables` 含 8 张 v7 表且 v6/v5 表全保留。
- T018 结论：Foundation（T006–T018）完成，中央枚举、稳定错误契约、领域模型、
  异步端口与 schema v7 全部 GREEN；允许进入 US1（T019–T029）。

## T019–T023 US1 RED（T024 记录）

- 日期：2026-09-11；命令：`uv run pytest -q -p no:cacheprovider
  tests/unit/observability/test_correlation_context.py
  tests/contract/observability/test_sanitizing_processor.py
  tests/integration/observability/test_runner_span_nesting.py
  tests/contract/observability/test_diagnostic_query.py
  tests/e2e/observability/test_end_to_end_trace.py`。
- 结果：`31 failed`，退出码 1（预期 RED）。
- 失败全部为尚未实现的新契约（安全 import 使缺失模块表现为 failed 而非
  collection error）：
  - `trpc_service.observability.context` 未实现（T019：9 项断言——外部 trace
    重建/接受、metadata 不可覆盖、bind_tenant 拒绝、link_attempt 只追加、
    W3C traceparent 往返、无效 carrier 重建、内部 transport 限制、digest 格式）；
  - `trpc_service.observability.sanitizing` 未实现（T020：6 项——身份/时间/
    status/scope 保留、白名单属性、events/links 原文丢弃、ReadableSpan 不变性、
    不兼容形状 fail-closed 为 telemetry_adapter_incompatible）；
  - `trpc_service.observability.otel` 未实现（T021：4 项——幂等 configure、
    官方 trpc.python.agent span 嵌套为平台根的子节点、gen_ai 指标同 MeterProvider）；
  - `trpc_service.observability.service` 未实现（T023：6 项——授权先于读取、
    access audit 先行、租户隔离与跨租户空集、partial_telemetry、not_applicable 填充）；
  - US1 端到端接线缺失（T022：5 项——local_http 全阶段还原、重复投递单终态、
    冲突拒绝、失败 runner.invoke、双 IM 离线 harness 含投递生命周期状态）。
- 编写期修正：e2e 测试顶层 import 触发 collection error，改为函数内安全导入
  （与 T006–T011 的 `_load` 模式一致），使缺失实现表现为单测失败。
- T024 RED 结论：有效 RED 已固定，允许进入 T025–T029 实现。

## T025–T029 US1 实现（GREEN 前置）

- T025 `trpc_service/observability/context.py`：`CorrelationContextManager`
  （实现 CorrelationContextPort）+ 纯函数 `build_correlation`/`bind_tenant_scope`/
  `trace_digest`/`scope_digest_of`。外部 trace 仅在符合平台 UUID 规则时接受，
  否则重建；`untrusted_metadata` 参数被显式丢弃（正文与不可信 metadata 不参与
  标识决策）；`bind_tenant` scope 不匹配抛 `tenant_scope_invalid`；
  `link_attempt` 只追加（第二次 link 不覆盖既有 execution trace）；
  `inject`/`extract` 走 W3C traceparent 且拒绝非 internal transport；
  无效 carrier 重建新 root 并记录 `last_rebuild_reason="invalid_carrier"`。
- T026 `trpc_service/observability/sanitizing.py`：`SanitizingSpanProcessor` +
  `SafeSpanEnvelope` + 中央 `ATTRIBUTE_ALLOWLIST`（20 个标量键）。保留
  trace/span/parent id、时间、status、instrumentation scope；非白名单属性、
  events/links 原文全部丢弃（仅保留计数）；输入 ReadableSpan 只读不改；
  形状不兼容 fail-closed 抛 `TelemetryAdapterIncompatible` 且 sink 不被调用。
  parent 从公开只读的 `ReadableSpan.parent` 读取（不用 span context 的
  parent 属性——该属性不存在于 SDK 形状）。
- T027 `trpc_service/observability/otel.py`：`TelemetryBootstrap` 进程级单例
  （模块级 `_SHARED` 状态）——官方 `trpc_agent_sdk.telemetry.tracer` 是
  ProxyTracer，惰性解析全局 provider，因此 bootstrap 必须先于任何 Runner
  span 安装且不可替换；`configure()` 幂等（重复调用/新实例均采纳已安装
  provider）；TracerProvider + MeterProvider 一次安装；`tracer()`/`meter()`
  惰性自配置。验证：官方 `trpc.python.agent` span 与平台根 span 同 trace
  且父子关系成立，gen_ai 指标复用同一 MeterProvider。
- T028 接线（全部为可选参数，缺省 None 保持原行为）：
  - `gateway/service.py`：`telemetry=`/`node_id=` 参数 + `_correlation_for`/
    `_trace` fail-open 辅助；租户上下文解析后统一记录 gateway.accept/
    binding.resolve（tenant scope）；幂等 claim 各 disposition → success/
    rejected/recovered/unknown；session.lock success；worker.dispatch
    success/failed(agent_unavailable)；runner.invoke success/failed
    (agent_failed)/unknown(outcome_unknown)；SUCCEEDED 返回前记录
    reply.compose + delivery.queue/attempt/result success（local_http
    同步回复即投递完成）。
  - `channels/service.py`：`telemetry=` 参数；`handle` 包装 `_handle_impl`
    按最终 disposition 记录 adapter.receive（success/rejected/not_applicable，
    platform scope）；binding.resolve success/rejected（resolved 后绑 tenant
    scope）。
  - `channels/delivery.py`：`telemetry=` 参数；delivery.queue success；
    delivery.attempt 按 DeliveryOutcome 映射（success / failed+retryable
    provider_unavailable / failed provider_rejected / unknown）；
    delivery.result 终态映射（success/failed/unknown）。
  - `web/app.py`：LocalRuntime/SharedRuntime 新增 `telemetry`/`diagnostics`
    字段，两个组合根均创建 TelemetryRecorder + DiagnosticQueryService 并
    注入 GatewayService。
  - worker/service.py 的 worker.dispatch/runner.invoke 由 Gateway 在派发
    边界统一记录（共享 profile 下 Gateway 即 worker 进程入口），未重复
    接线以避免双写——记录为已知边界。
- T029 `trpc_service/observability/service.py`：`TelemetryRecorder`
  （fail-open 内存实现：record_stage_now 同步面 + start/finish_stage 异步
  port 面 + spans_for/degraded/drop_counters 存储面）与
  `DiagnosticQueryService`（授权 → 最小 access audit → 存储读取的固定顺序；
  租户隔离；降级时 partial_telemetry=True 且不伪造完整 trace；per-trace
  查询按中央 14 阶段补齐 not_applicable）。CLI
  `trpc-agent-trace-diagnose` 摘要扩展 `trace_reference`（sha256:<16hex>）、
  `stage_graph`（14 阶段 + not_applicable 填充）、`stable_errors` 与
  `partial_telemetry`（零记录时为 True），既有伪匿名字段不变。
- 实现期修正：T020 测试 helper 的 span 捕获顺序（SimpleSpanProcessor 中
  child 先导出，`captured[-1]` 是 parent）——改为 `captured[0]` 为 child。

## T024 补充 / US1 GREEN 记录

- 日期：2026-09-11；命令与 T024 RED 完全一致（同五个测试文件）。
- 结果：`31 passed`，退出码 0（对比 RED `31 failed`）。
- 全量回归：`418 passed, 31 skipped, 0 failed`（31 skipped 全部为共享
  Redis/PostgreSQL 环境缺失，与既有口径一致）；`--collect-only` 为
  `449 tests collected`（T024 基线 418 + US1 新增 31）。
- US1 验收要点：local_http 成功请求按关联标识还原全部 14 阶段（未进入
  阶段显式 not_applicable）；重复投递 idempotency.claim=recovered 且仅一个
  业务终态（首次 authorized→execution_started→succeeded，重复 audit=duplicate）；
  冲突投递 idempotency.claim=rejected；post-start 失败 runner.invoke=failed
  error=agent_failed retryable=False；双 IM 离线 harness 记录
  adapter.receive（platform scope）与租户 scope 的 binding/runner/delivery
  阶段，投递生命周期含成功、transient 失败（retryable、稳定码
  provider_unavailable）重试后成功；SDK span 脱敏前后原文零外泄
  （sentinel 标记不出现于安全 envelope）。
- US1 Checkpoint 达成：任一外部请求可按同一可信关联标识还原完整处理
  轨迹，官方 Runner span 出口前完成脱敏。允许进入 US2（T030–T040）。

## T030–T035 US2 RED（T036 记录）

- 日期：2026-09-11；命令：`uv run pytest -q -p no:cacheprovider
  tests/unit/observability/test_metric_registry.py
  tests/unit/observability/test_sampling_policy.py
  tests/unit/observability/test_telemetry_buffer.py
  tests/contract/observability/test_exporter_adapter.py
  tests/contract/observability/test_diagnostic_isolation.py
  tests/security/observability/test_no_sensitive_leak.py`。
- 结果：`36 failed, 6 passed`，退出码 1（预期 RED）。
- 36 个失败全部为尚未实现的新契约（安全 import 缺失模块 → failed）：
  - `trpc_service.observability.metrics` 未实现（T030：8 项——9 个核心指标注册、
    动态名称拒绝、未注册 label/越域值拒绝、身份键永不出现在声明、确定性 Runner
    token/cost 标 not_applicable；T034/T035 各 2 项同类边界）；
  - `trpc_service.observability.sampling` 未实现（T031：7 项——关键分类 100%
    keep_full、租户不可调低、稳定 hash 决策、默认 10%/ceiling 25%、非法
    override 构造拒绝、decide_trace 整树一致）；
  - `trpc_service.observability.buffer` 未实现（T032：9 项——offer 非阻塞、
    容量 10,000 有界、关键保留区 ≥20%、普通满淘汰最旧、attempt>3 拒绝、
    TTL 丢弃、关键耗尽生成固定 CriticalDiagnosticSummary、不落盘/无持久化钩子、
    非 envelope 拒收）；
  - `trpc_service.observability.exporter` 未实现（T033：7 项——success 导出、
    retryable_failure 有限重试+退避+抖动、permanent 不重试、未验证 envelope
    permanent_failure 且不触 transport、异常折叠不穿透且不回显原文、过期
    envelope 先剔除、ACTIONS 封闭集）；
  - `log_operational` 无 error_type 边界校验（T035：sentinel 形 error_type
    必须在日志边界被拒绝——驱动 T039 日志收敛）。
- 6 个 passed 为既有行为回归（US1 已实现的租户隔离、跨租户空骨架、fail-closed
  审计边界、fail-open 遥测故障不改业务终态、记录阶段零泄露），非空洞断言。
- T036 RED 结论：有效 RED 已固定，允许进入 T037–T040 实现。

## T037–T040 US2 实现（GREEN 前置）

- T037 `trpc_service/observability/sampling.py`：`OutcomeAwareSamplingPolicy` +
  `SamplingDecision`。5 个关键分类（error/security_rejection/recovery/
  cross_tenant_attempt/outcome_unknown）绝对 keep_full，任何租户 override
  （含 0.0 全关闭普通采样）不可触及；普通成功按
  sha256(trace_digest|scope_digest|v{config_version}) 前 8 hex 稳定分桶，
  默认 10%、平台 ceiling 25%，非法 override（负数或超 ceiling）构造期
  ValueError；`decide_trace` root-close 整树决策（任一关键 span → 整树
  keep_full，reason=首个关键分类）。
- T038 `trpc_service/observability/buffer.py`：`PriorityTelemetryBuffer`。
  offer 同步非阻塞；默认容量 10,000；关键保留区 max(1, 20%)；普通满淘汰
  最旧并计 normal_evicted；attempt_count ≥ 3 的 re-offer 判 retry_exhausted
  丢弃；expires_at 过期即 ttl_expired；关键区耗尽时先弹出最旧并生成固定
  字段 `CriticalDiagnosticSummary`（error_type=telemetry_buffer_exhausted，
  payload ≤ 8 个已知字段且无 complete 标记）；仅接受 TelemetryEnvelope，
  其他类型 TypeError；纯内存（无 redis/engine/session/connection/client
  钩子，drain 后 tmp_path 为空）。
- T039 收敛与日志边界：
  - `trpc_service/observability/metrics.py`：中央 `MetricRegistry.default()`
    封闭注册 9 个核心指标（trpc.requests/stage.duration/runner.duration/
    tool.duration/channel.delivery/state.operation.duration/recovery/
    telemetry.dropped/release.transition），name/unit/instrument/labels/
    label_domains 全部集中声明，域直接复用 taxonomy 中央枚举；
    `validate_labels` 拒绝未声明键与越域值；`usage_status()` 在确定性
    Runner 下将 token/cost 显式标记 not_applicable；身份键（tenant/user/
    session/message/trace 原值或 digest）结构性排除在声明之外。
  - `taxonomy.py` 新增中央 `OPERATIONAL_ERROR_TYPES`（30 个稳定值）与
    `validate_error_type`；`OperationalEvent.__post_init__` 校验
    component/error_type/trace_digest（sha256: 前缀），sentinel 形自由文本
    在日志边界即被拒绝。
  - `log/__init__.py`：`log_delivery` 由 raw trace_id 改为 trace_digest
    引用（FR-006/FR-007，DEC-001）；local_http 两处调用随迁。
- T040 出口接线：`TelemetryRecorder` 新增可选 `buffer=`/`sampling=` 构造
  参数与 `exporter_health`（ok|retrying|down|unused 供应商中立出口健康）；
  `flush()` 先关键后普通地 drain 缓冲并经 OtlpHttpExporterAdapter 导出，
  retryable_failure 的 envelope 以 attempt+1 重新入队（受 ≤3 边界约束），
  全程 fail-open（异常折叠为 record_failed 计数）；`drop_counters()`
  合并 recorder 与 buffer 分类计数。新增
  `trpc_service/observability/exporter.py`：`OtlpHttpExporterAdapter` +
  `ExportResult`（ACTIONS 封闭集）+ `PermanentExportRefusal`；仅接受已验证
  envelope（否则 permanent_failure/invalid_envelope 且不触 transport）；
  有限重试（默认 3）+ 指数退避 ×[0.5,1.0] 抖动（严格正且随尝试数不减）+
  显式 timeout；过期 envelope 先剔除；任何异常折叠为稳定 reason，原文
  零回显。
- 实现期修正（契约精化，已同步测试）：
  - T031：租户 override 0.0 语义确定为“合法的普通采样 opt-out”（不可降低
    关键保留率），非法集合改为负数与超 ceiling；
  - T032：attempt_count 受 envelope 模型 0..3 界约束，重试耗尽契约改为
    “re-offer 时 attempt==3 即丢弃”；
  - T033：永久失败由 transport 显式抛 `PermanentExportRefusal` 表达（不
    靠异常文本嗅探），其余异常一律 retryable；
  - T030：未注册 label 键的断言值由 stage 改为 channel（stage 本就是
    trpc.requests 声明键）；
  - `sanitizing.py` on_start 签名修正（OTel 以关键字传 parent_context，
    参数需默认值）——该缺陷在 T020 的 SimpleSpanProcessor 路径下不可达，
    由 T035 直接挂 provider 暴露；
  - 第七阶段 `tests/unit/data/test_operational_event.py` 的 digest 值随
    sha256 前缀硬ening更新（前向升级先例）。

## US2 GREEN 记录（T040）

- 日期：2026-09-11；命令与 T036 RED 完全一致（同六个测试文件）。
- 结果：`42 passed`，退出码 0（对比 RED `36 failed, 6 passed`；42 = 36
  新契约 GREEN + 6 既有行为回归保持 GREEN）。
- 全量回归：`460 passed, 31 skipped, 0 failed`（31 skipped 全部为共享
  Redis/PostgreSQL 环境缺失，口径不变）；`--collect-only` 为
  `491 tests collected`（T036 基线 449 + US2 新增 42）。
- 双租户隔离证据：twin 租户复用同一外部 user/session/message 标识，
  runner.invoke 两租户各自 success/failed 互不可见；跨租户 trace 查询
  仅返回全 not_applicable 骨架（零真实证据）；scope 分区键不同。
- 零泄露证据：7 类 sentinel（api_key/im_token/db_password/response_url/
  phone/email/message_body）经成功路径（local_http 全链路 + span 脱敏 +
  buffer + exporter payload）、失败路径（audit 不可用 fail-closed 回复、
  遥测故障 fail-open 回复）后，在日志（log_operational/log_delivery）、
  指标（registry label 边界）、追踪（SafeSpanEnvelope 白名单）、错误详情
  （ExportResult.reason 折叠）、运行事件（OperationalEvent 构造拒绝）中
  未脱敏命中 0；sentinel 形 error_type 在日志边界构造期即 ValueError。
- US2 Checkpoint 达成：安全、隔离、低基数且 fail-open 的指标与日志链路
  可独立演示。允许进入 US3（T041–T051）。

## T041–T044 US3 RED（T045 记录）

- 日期：2026-09-11；命令：`uv run pytest -q -p no:cacheprovider
  tests/unit/observability/test_role_readiness.py
  tests/unit/observability/test_alert_state_machine.py
  tests/contract/observability/test_health_endpoints.py
  tests/integration/observability/test_dependency_faults.py`。
- 结果：`32 failed`，退出码 1（预期 RED）。
- 失败全部为尚未实现的新契约（安全 import → failed）：
  - `trpc_service.observability.health` 未实现（T041：11 项——5 角色关键依赖
    矩阵声明、全关键 up → ready、关键 down/unknown/缺失 → unready、telemetry
    down 仅 degraded、单渠道 down 只影响本渠道角色、liveness 独立、31 秒旧
    观察过期为 unknown、路径级平台聚合 ready/degraded/unready 三档；
    T043：5 项端点契约——liveness 200、ready 200/503+稳定 reason code、
    /health/status 未授权 403+最小审计、授权路径级汇总、响应零敏感泄露；
    T044：7 项——参数化关键依赖断连矩阵、单渠道降级、权威存储 down
    fail-closed、抖动去重+单次 resolved、30 秒 TTL/60 秒恢复时限）；
  - `trpc_service.observability.alerts` 未实现（T042：8 项——指纹确定性与
    区分度、PENDING 持续窗口、FIRING 单次通知+CAS 版本、RECOVERING 不稳定
    重回 FIRING+稳定窗口 resolved、同指纹合并为同一 incident_id、抖动去重
    上限、通知正文安全字段集、severity 封闭域）。
- T045 RED 结论：有效 RED 已固定，允许进入 T046–T050 实现。

## T046–T050 US3 实现（GREEN 前置）

- T046 `trpc_service/observability/health.py`：`ROLE_CRITICAL_DEPENDENCIES`
  中央 5 角色矩阵（gateway：postgres/channel_binding_identity/governance；
  worker：postgres/redis/governance/runner_initialisation；feishu_adapter：
  postgres/channel_binding_identity/feishu_connection；wecom_adapter 对称；
  recovery：postgres/redis）；`RoleReadinessMatrix.evaluate_role`——关键
  down/unknown/缺失即 unready（缺失观察显式记 unknown 不伪造 ready），
  非关键（telemetry/他渠道）down 仅 degraded，liveness 恒独立；31 秒旧
  观察过期为 unknown（`DEFAULT_OBSERVATION_TTL=30s`，SC-005 上半）；
  `aggregate` 路径级三档（全 ready→ready / 部分→degraded / 全
  unready→unready，渠道路径按角色可用性聚合）。`HealthMonitor` 把矩阵
  绑定到进程探针（缺失探针=unknown）；`HealthProbeService` 实现
  HealthProbePort 全四方法。
- T047 `trpc_service/observability/alerts.py`：`AlertStateMachine` CAS 状态机
  （fire_window=3：纯抖动 TTFF 永不点燃；resolve_window=2 × 30s 观察间隔
  =60 秒内关闭，SC-005 下半）；RECOVERING 期再点火直接重回 FIRING、
  RESOLVED 后再点火回 PENDING（同一 incident_id 合并重开）；每次状态
  转移 version+1 并产生恰好一个 `fingerprint:state_version` 通知 ID，
  状态内重复观察只累计 occurrence_count 不再通知；`alert_fingerprint`
  =sha256(rule|severity|role|component|scope|reason)；`SafeAlertNotifier`
  正文仅 12 个安全字段（含 recommended_action），租户原值/Secret 零出现；
  SEVERITIES 封闭域。配套：AlertIncident 模型新增 stable_reason 默认字段。
- T048 `trpc_service/storage/postgres/operations_repositories.py`：
  `PostgresAlertRepository`——observe() 按 fingerprint SELECT FOR UPDATE
  合并 occurrence（跨节点同指纹单逻辑行）；transition() 以
  `WHERE state_version=expected` 做 CAS，版本冲突抛 `AlertStateConflict`
  （响亮失败而非静默双写）；resolved 写入 resolved_at。
  `tests/integration/operations/test_alert_repository_shared.py` 在
  共享 PG 环境验证双节点合并+CAS 冲突+恢复关闭（离线 skip，Docker 补跑）。
- T049 `trpc_service/web/app.py`：`_phase8_health_routes` 三端点——
  `/health/live`（进程应答即存活，恒 200）；`/health/ready`（ready 200 /
  unready 503 + role + 稳定 reason_codes）；`/health/status`（未带
  x-ops-token 或不匹配 → 403 + 最小 health_status_access 审计；授权 →
  路径级平台汇总 state/available_paths/unavailable_paths/role_counts）。
  LocalRuntime/SharedRuntime 增加 health/ops_token/health_access_audit 与
  record_health_status_access()；`_shared_server.py` 经 create_shared_app
  自动获得三端点；telemetry exporter_health 映射为 degraded 探针（出口
  故障只降级不 unready）。
- T050 探针接线：local 组合根 gateway 角色 + shared 组合根 worker 角色均
  经 HealthMonitor 接入（进程内关键依赖 up、telemetry 出口健康联动降级）；
  渠道连接探针在共享部署按 provider 客户端状态扩展（离线 harness 内
  全 up，Docker 场景补验）。

## US3 GREEN 记录（T050）

- 日期：2026-09-11；命令与 T045 RED 完全一致（同四个测试文件）。
- 结果：`32 passed`，退出码 0（对比 RED `32 failed`）。
- 全量回归：`492 passed, 32 skipped, 0 failed`（32 skipped 为共享
  Redis/PostgreSQL 环境缺失 + T048 新增 1 个 PG 测试）；收集数
  `524 tests collected`（T045 基线 491 + US3 新增 33）。
- 故障矩阵证据：参数化 5 组关键依赖断连（postgres/redis/feishu/
  wecom/postgres-recovery）对应角色全部 unready 且 reason_codes 含该
  依赖；单渠道断链平台 degraded、wecom/local_http 路径继续可用；权威
  存储不可用 → 新执行 fail-closed（audit_unavailable，非 succeeded）；
  10 轮完整故障 episode 每轮恰好 (firing, recovering, resolved) 三个
  通知、ID 全局唯一、同一 incident_id、occurrence 合并 ≥ 30；纯抖动
  TTFF 40 步零通知；TTL 30s + resolve 2×30s=60s 时限断言通过。
- 端点证据：liveness 200；ready 200（本进程角色 ready、reason_codes 空）
  且 unready 路径 503+稳定码契约就位；status 未授权 403+最小审计
  （action/authorized/observed_at 三字段，零租户/敏感值）、授权后返回
  路径级汇总；三个端点响应经 sentinel 全集扫描零命中。
- US3 Checkpoint 达成：健康矩阵、依赖故障降级与去重告警可独立演示。
  允许进入 US4（T052–T063）。

## US4 RED 记录（T057）

- 日期：2026-09-11；命令：`uv run pytest tests/contract/operations/test_snapshot_repository.py tests/unit/operations/test_release_state_machine.py tests/contract/operations/test_route_resolution.py tests/unit/operations/test_gate_evaluation.py tests/integration/operations/test_release_transactions.py tests/e2e/operations/ -q`
- 结果：`49 failed, 1 skipped`（失败即 RED：`trpc_service.operations.memory_store` / `release` / `routing` / `gates` 与 `trpc_service.governance.hard_gate` 尚不存在，safe-import 返回 None → 断言失败而非收集错误；1 skipped 为共享 PG 环境缺失的 `test_release_transactions_shared_backend`）。
- RED 覆盖：快照不可变插入/摘要冲突/Secret 拒绝/审计失败零写入（T051）；发布状态机全分支+revision CAS+fence 拒绝+command_id 幂等+非法转换零副作用（T052）；路由解析 fail-closed 六情形+pin 幂等/冲突+范围外租户不可见 candidate+Redis 缓存故障不影响权威（T053）；硬门槛首次 latch/telemetry 硬信号不 latch/质量门槛窗口+最小样本/样本不足禁静默推进/PASS 才推进（T054）；审计失败整体回滚/提交前崩溃零变化/提交后丢响应按 command_id 重放/旧 fence 拒绝+新节点接管/缓存故障不碰权威（T055）；三 E2E——范围外租户恒 stable、硬门槛即停+自动回滚+在途 pin 不变+历史不改写、质量门槛暂停等授权+混合版本节点退出就绪（T056）。

## US4 GREEN 记录（T062，实现 T058–T061）

- 日期：2026-09-11；命令：`uv run pytest tests/contract/operations/ tests/unit/operations/ tests/integration/operations/ tests/e2e/operations/ -q`
- 结果：`69 passed, 3 skipped, 0 failed`（对比 RED `49 failed, 1 skipped`；3 skipped 均为共享 PG 环境缺失：test_alert_repository_shared、test_release_transactions_shared_backend、test_shared_backend_upgrades_to_v7_idempotently）。
- T058 `trpc_service/operations/release.py`：`ReleaseStateMachine` 有界转换表（validate/start_canary/advance/pause(quality|insufficient)/resume/rollback 两段/complete_rollback/fail/require_repair/repair，非法转换 ReleaseConflict 零副作用）；`ReleaseCoordinator.execute` command_id 幂等（同命令返回首次结果、跨动作复用即冲突）、expected_revision CAS、fence ≥ 已见值（StaleReleaseFence）、单事务内转换+路由回滚+RollbackDecision+正式 Audit；rollback 对 cohort 路由逐租户 latch（candidate→None、release_id→None、generation+1、affected_tenant_count）；advance 提升 candidate→stable；提交后 Redis 缓存刷新异常被吞（缓存非权威）。
- T059 `operations_repositories.py` 新增 `PostgresReleaseRepository`：SELECT FOR UPDATE 行锁、journal（release_id+command_id）幂等重放、CAS 冲突与 StaleReleaseFence 响亮失败、rollback 同事务写 rollback_decisions；迁移 007 追加 `rollback_decisions` 表（forward-only，IF NOT EXISTS）+ ORM Row + EXPECTED_V7_TABLES 更新（离线 skip，Docker 补跑）。
- 内存权威 `trpc_service/operations/memory_store.py`：checkpoint/restore 事务（嵌套深度、crash before/after commit 两个模拟崩溃点）、audit_failure_countdown 审计故障注入、canonical_payload_digest（sha256 排序 JSON）、contract_rank 版本序。
- T060 接线：`gateway/service.py` 可选 route_resolver——worker.prepare 前固定 ExecutionConfigPin，权威不可用/缺失/摘要不符/不兼容 → mark_pre_start_failed(configuration_unavailable) + 安全回复，绝不回退进程默认；`worker/service.py` Runner 缓存键升级为 (tenant, agent, snapshot_id)；`recovery/reconciler.py` 可选 config_pin_store，恢复沿用原 pin（config_snapshot_id/route_generation 注入 marker）。
- T061 `trpc_service/governance/hard_gate.py`：`HardGateEnforcementPoint.report_violation`——硬类型白名单（4 类）、必须带 evidence_digest、单事务内 signal 去重写入+路由 latch+正式 Audit；digest 幂等（同报告不重复 bump generation）；路由解析见 latch 即 last-good。
- 双租户证据：范围外租户全流程 stable（canary/advance 后仍 stab-0002）；cohort 租户 canary 期间 candidate、回滚后统一 stab-0001；在途 pin 跨路由变更不变。
- 双节点证据：node-A fence=5 推进后 node-B fence=3 被拒（StaleReleaseFence 零副作用）、B 从最后 committed revision+fence=6 接管成功；crash after commit 后同 command_id 重放返回原结果、revision 不重复推进。
- 硬门槛证据：首次 enforcement 硬信号即 latch（telemetry 硬信号无 evidence 不 latch）→ 新请求即 stable → supervisor 自动 rollback（rolled_back）；journal append-only（draft→validated→canary→rolling_back→rolled_back，from_revision<to_revision）；回滚决策 reason=hard_gate_triggered。
- 质量门槛证据：窗口结束样本 40/100 → paused_insufficient_sample 且 advance 被拒；样本 150/100 越线 0.2>0.05 → paused_quality 等待授权，resume 后回 canary 且路由仍 candidate；清洁窗口 pass → completed；v1 节点对 v2 candidate 退出就绪（node_config_readiness=False）。

## US5 RED 记录（T066）

- 日期：2026-09-11；命令：`uv run pytest tests/unit/operations/test_capacity_scenario.py tests/unit/operations/test_capacity_comparison.py tests/performance/test_observability_capacity_gate.py -q`
- 结果：`11 failed, 3 passed`（`trpc_service.operations.capacity` 不存在 → RED；3 passed 为不依赖新模块的 CapacityScenario/CapacityComparison 模型既有断言）。

## US5 GREEN 记录（T067/T068）

- 日期：2026-09-11；命令与 T066 一致。结果：`14 passed, 0 failed`。
- T067 `trpc_service/operations/capacity.py`：formal_scenario() 固定 2/2/100/10=1000（seed=2026091100、warm-up 1、测量 9 轮）、generate_load 种子确定性负载（大小桶/重复比例/Tool 比例/读写比例，plan 不含 Secret）、environment_fingerprint 无敏感值、compare_runs 双门禁（正确性零容忍+≤10%）、acceptance_verdict（环境不等价 invalid 不放宽阈值）、build_capacity_report 区分 measured/derived/uncovered 且标 local_evidence 不声明生产 SLA。
- LocalCapacityHarness：warm-up 不测量、测量 9 轮取会话级批量延迟的轮次中位数（抑制 Windows 定时器尾部抖动）；5 连跑 verdict 全部 pass（开销约 1.5%~7.7%）。
- T068 正式运行：`capacity-report.json`（机器可读，verdict=pass：吞吐 -1.26%、p50 +2.61%、p95 +0.00%、p99 +3.32%，吞吐 9722.53→9600.21 msg/s）+ `capacity-results.md`（local_evidence 标记、明确排除真实模型延迟/真实 IM 限流/生产拓扑，不声明生产 SLA；CPU/内存/后端峰值在本环境为进程内合成负载，共享环境补测）。
- 新增 pytest marker `capacity`（pyproject.toml）。

## US6 RED 记录（T073）

- 日期：2026-09-11；命令：`uv run pytest tests/unit/operations/test_drain_lifecycle.py tests/e2e/operations/test_worker_drain_takeover.py tests/e2e/observability/test_local_observable_deployment.py tests/e2e/observability/test_exporter_outage.py -q`
- 结果：`15 failed, 1 passed`（`trpc_service.operations.drain` / `deployment` 不存在 → RED）。

## US6 GREEN 记录（T077）

- 日期：2026-09-11；命令与 T073 一致。结果：`16 passed, 0 failed`。
- T074 `trpc_service/operations/drain.py`：`InMemoryDrainController`（begin 原子撤就绪+停新 claim、重复 begin 幂等保原窗口、forward-only 转换表、deadline 后未证结果标 unknown、replay_candidates 恒空禁止自动重放非幂等副作用、快照含 inflight/completed/handed_off/unknown 计数）；`WorkerPool`（draining 节点 claim 拒、对端继续服务、fence 接管、陈旧 fence 拒绝、业务效果 exactly-once——重复 apply_result 返回 False 且 effect_log 单条）。
- T075 `deploy/local-observable/compose.yaml`：唯一 project name `trpc-agent-local-observable`、include 复用 local-shared Redis/PostgreSQL、schema-init→gateway+worker-a/b+otel-collector、核心 healthcheck 用 /health/ready、真实飞书/企业微信 Adapter 在 `real-channels` profile、Secret 仅经当前进程环境或 .env.local（: ? 必填占位）。
- T076 `deploy/local-observable/otel-collector.yaml`：tail_sampling——错误与关键 outcome（hard_gate_triggered/cross_tenant_leak/audit_unavailable/outcome_unknown）全保留、普通成功 10% 概率采样（与平台 DEC-001 一致）；groupbytrace 提供 trace-affinity（整树决策）；debug exporter 作网络侧第二道防线；health_check 扩展供 compose healthcheck。
- 离线 e2e 镜像 `trpc_service/operations/deployment.py` `LocalObservableOverlay`：四节点 up、/health/live 与 /health/ready 全 ready、两节点接续同一会话（turn 递增）、collector debug 按 sha256 trace reference 定位 6 阶段且无敏感标记；停 collector 后业务仍 succeeded、平台 degraded（reason_codes 含 telemetry_unavailable）、缓冲 ≤ capacity 且 dropped 计数可见、恢复后 35s 内 ready、正式 Audit 计数不变。
- 环境边界：Docker 未在本机验证（compose/collector 配置为静态审查证据），共享环境启动后按 quickstart 第 5 节补跑真实 Compose 验收。

## US7 RED 记录（T078 前置）

- 日期：2026-09-11；命令：`uv run pytest tests/e2e/operations/test_fault_exercise.py -q`
- 结果：`6 failed`（`trpc_service.operations.fault_exercise` 不存在 → RED）。

## US7 GREEN 记录（T082）

- 日期：2026-09-11；命令与 RED 一致。结果：`6 passed, 0 failed`。
- T079 `trpc_service/operations/fault_exercise.py`：四项可重复演练——collector down（检测 ≤30s、业务零影响、恢复验证）；PostgreSQL 权威 down（新执行 fail_closed、fallback_used=False）；普通 telemetry outage 期间硬门槛仍 latch（persistent_enforcement_point）且新请求 last_good；Worker 终止排空接管（drained、taken_over_by=worker-b、零不可解释重复）；时钟偏移（duration_source=monotonic、wall_time_anomaly_marked=True）。全演练后 invariant_summary：exercises_run≥4、cross_tenant_leaks=0、unexplained_duplicates=0。
- T080 `deployment-topology.md`：最小可观察部署图 + 生产推荐拓扑（LB+多 Gateway、每渠道冗余 Adapter、多 Worker、独立 Recovery/Operator、外部 HA Redis/PostgreSQL、两层 Collector trace-affinity tail sampling、外部 Secret Provider、发布顺序、故障域）+ 本地/生产差距补齐路径表；不声明生产 HA/SLA。
- T081 `risk-register.md`：9 项生产风险（PG 权威、Redis fence、OTLP 出口、硬门槛误/漏触发、Worker 终止、时钟偏移、密钥泄露、容量回归、迁移版本不一致），每项含触发/影响/检测/预防/处置/恢复验证/剩余风险并引用演练证据（R3→T072、R4→T056、R5→T069/T070、R6→clock_skew、R8→T067/T068）。
- 环境边界：本节记录的是当时的离线 harness 证据；Docker 共享环境下的真实演练
  随后已由 T088 按 quickstart 补跑完成。

## US8 安全门禁记录（T087）

- 日期：2026-09-11。
- `uv run pytest tests/security -q`：**全部通过**（修复 1 个回归：`AgentExecutor` 缓存键升级为 (tenant, agent, snapshot_id) 三元组后 `get_backend(*key)` 解包错误，已改为仅传前两元组；gateway 仅在存在 pin 时才传 snapshot_id 参数，保持 `AgentExecutorPort` 协议兼容）。
- `git diff --check`：退出码 0，无空白错误。
- `git status --short`：候选文件为第八阶段新增源码/测试/部署工件/规格文档与既有文件修改，无 .env.local、无密钥文件。
- 敏感材料扫描：对 `trpc_service/`、`deploy/`、`specs/008-observability-operations-flow/` 扫描硬编码 secret/token/password 模式，过滤占位符与测试哨兵后**零命中**；compose 中 Secret 均为 `${VAR:?}` 必填占位。

## 设置校验记录（T090）

- 日期：2026-09-11；`trpc_service/config/settings.py` 新增 `ObservabilitySettings` + `load_observability_settings`：采样默认率/租户 ceiling（>0.25 拒绝）、缓冲容量与关键保留区、告警窗口、容量参数（开销上限 >10% 拒绝）、OTLP endpoint；非法值抛 `ConfigurationError` 拒绝配置而非静默修正。
- 测试：`tests/unit/test_observability_settings.py` 全部通过。

## 跨文档一致性检查记录（T091）

- 日期：2026-09-11。
- `NEEDS CLARIFICATION` 残留：仅出现在 checklist 自检陈述与 tasks.md 本任务定义中，规格正文**零残留**。
- 状态语义：设计文档（data-model/contracts）使用逻辑大写状态名，代码与数据库为 lower_snake_case——已在 `contracts/release-operations-contracts.md` 追加「持久化命名约定」一节，给出 10 个状态与 7 个原因码的一一映射，消除歧义。
- 错误码：`configuration_unavailable`、`hard_gate_triggered`、`not_applicable`、`critical_summary` 在 README/plan/data-model/contracts/validation-results 间命名一致。
- Evidence 边界标注：`local_evidence`（容量）、`automated`（离线测试）、
  `real`（共享环境，随后由 T088 完成）、`design`（生产拓扑建议）四类标记
  在 traceability-matrix 与 capacity-results 中正确使用。

## 最终收口记录（T089/T092）

- 日期：2026-09-11。
- RED/GREEN 记录完整性：Foundation（T013–T018）、US1（T019–T029）、US2（T030–T040）、US3（T041–T050）、US4（T051–T062）、US5（T063–T068）、US6（T069–T077）、US7（T078–T082）、US8 安全门禁/设置/一致性（T083–T091）均已在上文逐节记录，与 `tasks.md` 勾选状态一致。
- **当时暂缓项（后续均已补齐）**：
  - ~~`T086` 全量回归~~：**已于 2026-09-11 由用户本机完成**（585 passed, 33 skipped, 0 failed，详见下文「T086 全量回归记录」）。
  - ~~`T088` 共享环境 quickstart 命令链~~：**已于 2026-09-11 完成**。真实
    Redis/PostgreSQL、Compose、健康检查、共享专项和 0-skip 全量结果见下文
    “T088 真实共享环境最终验收”。
- 宪法七原则复查：① Framework-First——官方 Runner/OTel ProxyTracer 直接复用，平台只做边界增强；② 租户隔离——双租户 twin 测试与跨租户不可见证据（US1/US4）；③ 无状态 Worker——pin/快照经权威存储解析，Worker 不持会话外状态；④ 契约优先——全部端口 @runtime_checkable Protocol，契约测试先行；⑤ 默认安全——20 键白名单脱敏、7 哨兵零泄漏、配置非法值拒绝；⑥ 端到端可观测——14 阶段追踪、健康矩阵、去重告警；⑦ Spec 证据——本文件即证据链，每项结论可回溯到命令与退出码。
- HIGH 级遗漏核查：无。当时的共享环境缺口已由 T088 补齐；local_evidence 与
  production design 边界继续显式保留。

## T086 全量回归记录（用户本机执行）

- 日期：2026-09-11；执行者：用户本机 PowerShell；命令：`uv run pytest -q 2>&1 | Tee-Object -FilePath regression-full.log | Select-Object -Last 10`
- 结果：**585 passed, 33 skipped, 0 failed，21.47s**（前七阶段 + 第八阶段全部离线测试通过，无未解释失败）。
- 33 个 skip 的外部环境原因与补测条件（与 T088 相同）：
  - `tests/integration/shared/`（Phase 7）：需 `TRPC_SHARED_REDIS_URL` / `TRPC_SHARED_DATABASE_URL`（Redis + PostgreSQL 共享后端），覆盖 duplicate http contract、fencing、node takeover×2、partial commit recovery、schema upgrade、session serialization、worker restart recovery×2；
  - Phase 8 共享库测试：`test_alert_repository_shared`、`test_release_transactions_shared_backend`、`test_shared_backend_upgrades_to_v7_idempotently`（PostgreSQL）；
  - 其余为既有环境依赖 skip（与 Phase 1–7 记录一致）。
- 补测条件：Docker 就绪后按 quickstart 第 2 节启动共享后端并设置上述环境变量，重跑 `uv run pytest -q`，skip 数应相应下降。

## 2026-09-11 T088 真实共享环境最终验收

- 环境：Docker Server `29.6.2`；Compose project
  `trpc-agent-v8-20260911040812`；凭证从被 Git 忽略的 `.env.local`
  读入当前进程，未写入文档、源码或 Git 跟踪文件。
- RED 证据：真实 `shared-init` 首次暴露迁移 SQL 按分号切分时会把
  `--` 注释后半段当作语句执行；补回归测试后修正迁移语句解析。
- GREEN：`tests/unit/test_shared_infrastructure.py` 为 `8 passed`；真实
  PostgreSQL schema-init 退出码 `0`，`schema-init` 容器最终 `Exited (0)`。
- 共享后端专项：
  `uv run pytest -m shared_backend -vv --tb=no -p no:cacheprovider`，
  结果 `33 passed, 586 deselected, 2 warnings`，退出码 `0`。
- 重复运行修复：共享告警和发布事务用例使用 `ops_namespace` 生成隔离标识；
  schema v7 用例统一使用公开的 `database.engine` 接口。
- 全量回归：使用唯一 `--basetemp` 避免 Windows 全局 pytest 临时链接权限
  干扰；最终工作树结果 `620 passed, 0 skipped, 2 warnings in 44.43s`，
  退出码 `0`。
  两条 warning 均来自 `lark-channel-sdk` 的弃用提示，不影响验收。
- 最小部署：补齐根目录 `Dockerfile`，Compose 使用实际
  `trpc-agent-shared-init` / `trpc-agent-shared-serve` 入口；容器显式标志
  才允许监听 `0.0.0.0`。Gateway、Worker-A、Worker-B、Collector、Redis、
  PostgreSQL 全部 `healthy`，schema-init 退出码 `0`。
- 健康端点：`GET http://127.0.0.1:8080/health/live` 返回
  `{"status":"alive"}`；`GET /health/ready` 返回 ready 且无 reason code。
- 故障演练：Collector 停止期间 Gateway live/ready 保持成功；Worker-A
  停止期间排空/接管 E2E 为 `4 passed` 且 Gateway 保持 ready；恢复后
  Collector、Worker-A/B 均重新达到 healthy。
- `git diff --check`：退出码 `0`；仅有 Windows LF→CRLF 提示，无格式错误。
- 安全处置：早期 `-vv` 诊断曾由 pytest 展开一次本地 PostgreSQL fixture
  URL；仓库中两个测试密码的跟踪文件匹配数均为 `0`。该本地测试密码在
  停止临时环境后必须轮换，后续诊断统一使用 `--tb=no`，不得保存原始输出。
- 证据边界：Docker 启动、共享数据库、健康检查和故障动作属于本机真实
  `local_evidence`；不据此宣称生产 SLA 或跨故障域高可用。

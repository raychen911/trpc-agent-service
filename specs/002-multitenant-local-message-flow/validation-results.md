# Validation Results: 多租户本地消息闭环

**Feature**: `002-multitenant-local-message-flow`

本文件只记录实际执行过的命令和结果。未运行的测试不得标记为通过。

## Phase 1 — Setup

- `uv lock`：成功，解析 99 个包；直接依赖固定为 httpx 0.28.1、Pydantic 2.13.5、
  Starlette 1.6.0、Uvicorn 0.52.4，保留 tRPC-Agent SDK 1.1.19。
- `uv sync --group dev`：成功，项目可编辑安装完成。
- 首次 `uv run pytest tests/sdk_validation -q`：被测试基础设施导入错误阻断，原因是
  根 `tests/` 缺少包标记；补充 `tests/__init__.py` 后重新执行。
- 最终 `uv run pytest tests/sdk_validation -q`：`18 passed in 6.42s`。

结论：T001–T004 完成，第一阶段 SDK 基线无回退。

## Phase 2 — Foundation

### Red Gate（T005–T008）

- 命令：`uv run pytest tests/unit/test_channel_contract_models.py tests/unit/test_tenant_and_audit_models.py tests/unit/test_settings.py -q`
- 结果：`23 failed in 0.40s`。
- 收集情况：23 个测试全部正常收集；没有语法、模块导入或环境错误。
- 预期失败原因：Channel、Tenant、Audit、Metric、SessionIdentity 和 Settings API
  仍为 `NotImplementedError` 骨架或缺少预期枚举/工厂行为。

结论：Red Gate 有效，可以开始 T009–T013；此结果不代表功能失败。

### Green Gate（T009–T014）

- 命令：`uv run pytest tests/unit/test_channel_contract_models.py tests/unit/test_tenant_and_audit_models.py tests/unit/test_settings.py -q`
- 结果：`23 passed in 0.21s`。
- SDK 回归命令：`uv run pytest tests/sdk_validation -q`
- SDK 回归结果：`18 passed in 4.60s`。
- 已实现：严格且不可变的消息契约、租户资源归属校验、Agent 作用域会话摘要、伪名化审计与指标模型、可替换存储/指标端口，以及仅引用环境变量名称的双租户演示配置。

结论：T009–T014 完成，Phase 2 基础门禁通过；可以进入 Phase 3，但尚未实现 HTTP、HMAC、Gateway、Worker 或完整消息闭环。

### 前两阶段收尾检查

- 全量测试：`uv run pytest -q` → `41 passed in 3.91s`。
- 编译检查：`uv run python -m compileall -q trpc_service tests` → 通过。
- 差异格式检查：`git diff --check` → 通过；仅出现 Git 的 LF/CRLF 转换提示，不属于内容错误。

## Phase 3 — User Story 1 / MVP

- 有效 Red Gate：HMAC、HTTP、Platform Adapter、Worker 和端到端测试正常收集，
  `7 failed in 0.43s`，失败原因为目标行为尚未实现。
- Green Gate：US1 新增测试 `7 passed in 3.66s`；SDK 回归 `18 passed in 3.69s`。
- 结果：有效签名请求经 Gateway 和官方 Runner 得到唯一非空最终 Event，形成统一回复、
  tenant-scoped 审计和指标；外部模型调用数为 0。

## Phase 4 — User Story 2 / Tenant Isolation

- Red Gate：新增隔离断言 `1 failed, 4 passed in 4.25s`，失败点为缺少显式 Session
  ownership 校验。
- Green Gate：US1、US2 和 SDK 合并回归 `29 passed in 3.85s`。
- 后续安全复核又以失败测试证明伪造 SDK app/user identity 与跨租户 user pseudonym
  原先未被充分约束；修复后相应测试全部通过。
- 结果：tenant、agent、binding、channel、user、conversation 共同决定 Session；同外部
  标识在 alpha/beta 中产生不同 session、SDK identity 和审计 user digest。

## Phase 5 — User Story 3 / Idempotency & Ordering

- Red Gate：状态机、Repository、会话锁和并发测试 `6 failed, 2 passed in 5.83s`。
- Green Gate：本阶段测试 `8 passed in 3.75s`；当时全量 `58 passed in 3.89s`。
- 加固 Red/Green：prepare 取消、RUNNING 后取消、重复/冲突指标最初
  `3 failed, 4 passed`，修复后 `7 passed`；owner/execution trace 非法转换先失败，
  改为 `ConditionalWriteFailed` 后通过。
- 结果：顺序重复、并发重复均最多执行一次；同 session 串行、不同 session 并行；
  pre-start 失败可 reclaim，post-start/unknown 为不可重放终态。

## Phase 6 — User Story 4 / Rejection & Observability

- Red Gate：HTTP 错误映射和审计故障测试 `2 failed, 14 passed in 4.38s`。
- Green Gate：本阶段测试 `16 passed in 3.73s`。
- 加固 Red/Green：服务端字段注入与 execution-start 审计故障 `4 failed`；修复后通过。
  PreAuth 审计与终态写入不确定性 `2 failed, 7 passed`；修复后 `9 passed`。
  Audit/Metric scope 绕过与敏感标签测试先 `2 failed`，修复后 `2 passed`。
- 结果：400/401/403/409/502/503 稳定映射；拒绝请求写受限 PreAuthScope；审计、
  指标和错误结果不保存秘密或完整正文；final audit 与 terminal CAS 故障均显式可见。

## Phase 7 — CLI, Scale & Final Acceptance

- CLI 测试经历 Red Gate `2 failed`，初次 Green `3 passed`；加固 loopback、隐藏签名/
  正文 repr 和非法响应后为 `4 passed in 3.67s`。
- 真实进程首次演示发现 `httpx` 继承系统代理，健康检查成功但消息得到空正文 502；
  CLI 改用 `httpx.Client(trust_env=False)` 后重跑通过。这是实际运行发现并闭环的问题。
- Uvicorn loopback 演示：health 200；alpha `stored/recalled:ALPHA`；beta
  `stored/recalled:BRAVO`；两租户 session 不同；重复请求返回 `duplicate + suppress`
  且 original_trace_id 指向首次执行；不同内容复用 ID 返回 409 conflict。修复后完整
  演示耗时不足 1 分钟，满足 SC-001 的 5 分钟限制。
- 分类验收：
  - `uv run pytest tests/unit -q --tb=short` → `35 passed in 0.20s`
  - `uv run pytest tests/contract -q --tb=short` → `17 passed in 4.24s`
  - `uv run pytest tests/integration -q --tb=short` → `19 passed in 4.39s`
  - `uv run pytest tests/sdk_validation -q --tb=short` → `18 passed in 4.22s`
- 当时全量：`uv run pytest -q --tb=short` → `89 passed in 4.23s`；后续 converge
  补充测试后的最终结果见下节。
- SC-002：20 轮双租户交替双轮，串话与错误审计归属均为 0。
- SC-003：100 次顺序重复及 20 组并发重复均每组只执行一次。
- SC-004–SC-009：由 HTTP 拒绝、故障注入、离线安全、Fake Adapter、审计/指标快照
  测试覆盖；具体映射见 `决策记录.md` 和测试文件。

## 证据边界

以上结果只证明本地单进程、InMemory、确定性离线模型链路。没有验证跨进程锁、共享
Redis/SQL、真实 IM、真实模型、生产 Telemetry 或 Kubernetes；不得据此宣称生产级
exactly-once 或跨节点一致性。

## Convergence Remediation（T069–T073）

首次 converge 发现 3 类直接证据缺口：禁用/错配资源矩阵、HTTP 502/503 与未知绑定
矩阵、HMAC 篡改和指标故障隔离。T069–T071 新增目标测试 `28 passed in 3.74s`；它们
均在既有实现上通过，证明属于证据缺失而非新行为缺陷。随后补充审计 update/failure、
签名后非法输入矩阵和 state_backend 指标，其中 state_backend 先出现 1 项预期失败，
实现记录后 `11 passed`。HTTP `processing.data.retryable` 契约先 `1 failed`，实现后
HTTP 契约 `8 passed`。

最终分类结果：

- Unit：`36 passed in 0.21s`
- Contract：`27 passed`（最终全量中的分类数量）
- Integration：`20 passed in 4.40s`
- SDK baseline：`18 passed in 4.23s`
- Full：`101 passed in 4.19s`

最终静态与安全门禁：`compileall` 通过；`git diff --check` 仅有 Windows LF/CRLF 提示、
无空白错误；秘密模式扫描 0 命中；无生成文件待提交；端口 8765/8766 无遗留 listener。

### FR / SC / D 覆盖矩阵

| 范围 | 主要自动化证据 | 决策/标准 |
|---|---|---|
| FR-001, FR-013, FR-014, FR-018 | `test_local_message_http_contract.py` | SC-001, SC-005; D-008 |
| FR-002–FR-004, FR-025–FR-027 | `test_hmac_auth.py`, `test_platform_adapters.py` | SC-004, SC-006; D-003 |
| FR-005–FR-008, FR-023 | `test_session_identity.py`, `test_session_backend.py`, `test_agent_executor.py`, `test_acceptance_scale.py` | SC-002, SC-007; D-001, D-006, D-007 |
| FR-009–FR-012, FR-024 | `test_idempotency_state_machine.py`, `test_idempotency_repository.py`, `test_failure_boundaries.py` | SC-003; D-002, D-005, D-008 |
| FR-015–FR-017, FR-019–FR-021 | `test_audit_repository.py`, `test_offline_security.py`, Fake Adapter contract | SC-005, SC-006, SC-008; D-004, D-005 |
| FR-022, FR-028 | `tests/sdk_validation/`, `test_metrics_recorder.py`, message-flow metrics assertions | SC-007, SC-009; D-004 |

第二次 converge 结论：文档、实现、测试与运行证据一致，Critical/High/Medium 阻断项均为 0。

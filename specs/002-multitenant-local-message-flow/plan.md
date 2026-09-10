# Implementation Plan: 多租户本地消息闭环

**Branch**: `feature/luwenjie` | **Date**: 2026-09-05 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/002-multitenant-local-message-flow/spec.md`

## Summary

实现一个单进程、本地可运行但边界可替换的消息纵向切片：Starlette HTTP Channel
Adapter 验证每个绑定的 HMAC-SHA256 签名，建立可信租户上下文；Gateway 完成租户
和 Agent 绑定校验、消息幂等、租户作用域会话路由与同会话串行化；Agent Worker
通过适配器复用 `trpc-agent-py==1.1.19` 的 `LlmAgent → Runner → Event →
  InMemorySessionService`；最终返回统一回复，写入脱敏审计，并通过租户作用域的
  InMemory MetricsRecorder 形成最小可观测证据。

本功能仅证明平台边界和业务语义。平台 Repository 使用 InMemory 实现，SDK Session
也使用官方 InMemory 服务；生产共享存储、真实 IM、真实模型、多节点和 Kubernetes
仍明确未完成。

## Technical Context

**Language/Version**: Python 3.12（开发基线 3.12.7）

**Primary Dependencies**:

- `trpc-agent-py==1.1.19`：复用 Agent、Runner、Event、Content 和 Session。
- `starlette==1.6.0`：本地 ASGI HTTP 入口与生命周期管理。
- `uvicorn==0.52.4`：本地服务进程。
- `pydantic==2.13.5`：入口、出口和配置契约验证。
- Python 标准库 `hmac`、`hashlib`、`asyncio`、`uuid`：签名、摘要、并发与
  链路标识。
- 开发依赖 `pytest>=8.3,<10`、`pytest-asyncio>=0.25,<2`、
  `httpx==0.28.1`。

所有直接导入的第三方依赖必须在 `pyproject.toml` 中直接声明，并由 `uv.lock`
固定完整解析版本，不依赖偶然存在的传递依赖。

**Storage**:

- Agent 会话：官方 `InMemorySessionService`，通过 Session Backend Factory 注入
  Worker。
- 租户目录、通道绑定、幂等、审计和会话锁：平台 `Protocol` 端口加 InMemory
  Adapter。
- 请求、错误、阶段/Agent/状态后端延迟、通道投递、token 与成本：平台
  MetricsRecorder 端口加 InMemory Adapter；不提供生产 Telemetry 承诺。
- 所有状态仅在单进程生命周期内有效；服务重启后清空。

**Testing**: pytest 单元测试、Repository/Channel 契约测试、ASGI 集成测试、真实
SDK Runner 离线集成测试、并发与安全失败注入测试。

**Target Platform**: 首要验收平台为 Windows PowerShell 本地开发环境；服务保持
标准 ASGI 可移植性，不依赖 Windows 专属业务逻辑。

**Project Type**: Python 单服务项目，包含 HTTP Channel Adapter、Gateway、
Agent Worker 和可替换的 Storage Adapter。

**Performance Goals**:

- 本地启动到完成首次演示不超过 5 分钟。
- 20 次双租户交替双轮验证无上下文泄漏。
- 100 次顺序重复和 20 组并发重复均只执行一次业务动作。
- 不同会话可并行；同一会话严格串行且不存在死锁。

**Constraints**:

- 全流程离线，无真实 IM、模型 API Key 或外部模型调用。
- HMAC 时间戳允许偏差为 ±300 秒。
- 文本为 1 至 4000 个 Unicode 字符。
- 未授权请求不得访问租户业务数据；未知绑定与错误签名返回不可区分结果。
- Agent 开始后失败或结果不确定不得使用相同消息标识自动重放。
- Worker 可失败准备必须发生在 RUNNING 前；RUNNING 后执行默认限时 30 秒，超时或
  取消进入 OUTCOME_UNKNOWN 并释放会话锁。
- 审计写入失败不得被报告为完整成功。

**Scale/Scope**: 两个预置租户、每租户一个 Agent 应用和本地绑定；单进程；验收负载
以规格中的 20 次双轮、100 次顺序重复和 20 组并发重复为界，不宣称生产吞吐。

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Pre-Research Gate

| Principle / Gate | Status | Plan Evidence |
|---|---|---|
| I. Framework-First | PASS | Worker 通过单一 Adapter 使用固定版官方 Runner/Event/Session；不复制上游源码 |
| II. Tenant Isolation | PASS | HMAC 验证后建立 TenantContext；所有键和端口显式包含 tenant_id；含跨租户拒绝测试 |
| III. Stateless Workers | PASS WITH LOCAL LIMIT | Worker 不保存租户业务状态；状态经端口访问。InMemory 仅限单进程演示且明确不可用于生产 |
| IV. Contract-First | PASS | 先定义统一消息、HTTP 和 Storage/Worker 端口契约，再实现 Adapter |
| V. Security by Default | PASS | 每绑定独立密钥引用、HMAC、默认拒绝、恒定时间比较、日志脱敏和秘密扫描 |
| VI. Observability | PASS | 每次投递生成当前 trace_id，审计按租户/会话/trace 安全查询；MetricsRecorder 覆盖请求量、错误量、链路/Agent/后端延迟、投递、token 与成本 |
| VII. Spec-Driven Evidence | PASS | Spec、澄清、决策记录、Plan、契约、数据模型和测试证据连续追踪 |
| Test Gate | PASS | 计划覆盖单元、契约、集成、隔离、并发、幂等、失败和秘密扫描 |
| Scope Gate | PASS | 真实 IM、共享后端、多节点和生产部署均排除且不会伪称完成 |

无宪法例外或需记录的违规项。

### Framework Reuse vs Platform Ownership

| 能力 | 归属 | 本阶段处理 |
|---|---|---|
| LlmAgent、Runner、Event、Content、Session Service | tRPC-Agent-Python | 直接复用固定版公开接口 |
| 确定性离线模型 | 第一阶段验证资产 | 复用，后续可由模型 Factory 替换 |
| TenantContext、Binding 验证、HMAC | 平台新增 | 本阶段实现 |
| Gateway 路由、幂等状态机、SessionKey | 平台新增 | 本阶段实现 |
| Worker 生命周期与 SDK Adapter | 平台新增边界 | 本阶段实现，内部调用官方 SDK |
| Repository 端口、InMemory Adapter、会话锁 | 平台新增 | 本阶段实现 |
| 统一回复、错误映射、审计、MetricsRecorder 与 trace 传播 | 平台新增 | 本阶段实现 |
| Redis/SQL/真实 IM/生产 Telemetry | 后续平台功能 | 本阶段不实现 |

## Project Structure

### Documentation (this feature)

```text
specs/002-multitenant-local-message-flow/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── 决策记录.md
├── 一致性分析修订记录.md
├── tasks.md
├── checklists/
│   └── requirements.md
└── contracts/
    ├── local-message-http.md
    └── platform-ports.md
```

### Source Code (repository root)

```text
trpc_service/
├── _cli.py
├── agent/
│   ├── _deterministic_validation_model.py
│   └── sdk_validation.py
├── audit/
│   ├── __init__.py
│   └── models.py
├── channels/
│   ├── __init__.py
│   ├── contracts.py
│   ├── hmac_auth.py
│   └── local_http.py
├── config/
│   ├── __init__.py
│   └── settings.py
├── gateway/
│   ├── __init__.py
│   └── service.py
├── log/
│   └── __init__.py
├── metrics/
│   ├── __init__.py
│   ├── contracts.py
│   ├── inmemory.py
│   └── models.py
├── storage/
│   ├── __init__.py
│   ├── contracts.py
│   ├── inmemory.py
│   ├── locks.py
│   ├── models.py
│   └── session_backend.py
├── tenant/
│   ├── __init__.py
│   ├── models.py
│   └── session_identity.py
├── web/
│   ├── __init__.py
│   └── app.py
└── worker/
    ├── __init__.py
    └── service.py

tests/
├── conftest.py
├── support.py
├── contract/
│   ├── __init__.py
│   ├── test_agent_executor.py
│   ├── test_audit_repository.py
│   ├── test_idempotency_repository.py
│   ├── test_local_cli.py
│   ├── test_local_message_http_contract.py
│   ├── test_metrics_recorder.py
│   ├── test_platform_adapters.py
│   ├── test_session_backend.py
│   └── test_session_lock_manager.py
├── integration/
│   ├── __init__.py
│   ├── test_acceptance_scale.py
│   ├── test_multitenant_message_flow.py
│   └── test_offline_security.py
├── unit/
│   ├── __init__.py
│   ├── test_channel_contract_models.py
│   ├── test_hmac_auth.py
│   ├── test_idempotency_state_machine.py
│   ├── test_session_identity.py
│   ├── test_settings.py
│   └── test_tenant_and_audit_models.py
└── sdk_validation/
    └── ...
```

**Structure Decision**: 保持现有单 Python 包布局，在 `trpc_service` 内按平台边界
新增小型模块。HTTP 层只负责传输与契约转换，Gateway 负责用例编排，Worker 负责
SDK 调用，Repository 端口与 InMemory Adapter 分离；测试按 unit、contract、
integration 和既有 sdk_validation 分组。上述树与 `tasks.md` 使用同一份权威文件
清单；Setup 只创建可导入但无业务行为的 API 骨架，避免 Red Gate 因 collection 或
import error 失真。

## Processing Design

### Successful First Delivery

1. Channel Adapter 接收原始请求，限制正文大小并解析必填字段。
2. Trace 服务继承合法 UUID，否则生成新 UUID。
3. Binding Auth Registry 仅读取验证签名所需的最小绑定元数据和密钥引用。
4. HMAC Verifier 校验时间窗、规范字符串和签名；失败统一返回 `unauthorized`；成功产生
   不可由请求构造的 VerifiedBindingScope。
5. Tenant Service 仅凭 VerifiedBindingScope 检查绑定、租户和 Agent 的启用与归属，
   形成不可变 TenantContext。
6. SessionIdentity Factory 由 tenant、agent、binding、channel、user、conversation
   type 和 conversation 生成确定、不可碰撞的作用域标识；绑定改绑 Agent 后生成
   新会话。
7. Idempotency Repository 对 `(tenant_id, binding_id, external_message_id)` 原子
   claim，并校验规范化消息指纹。
8. 首次请求取得 session lock；同会话等待，不同会话使用不同锁。
9. 在 Agent 调用前使用显式 TenantScope 写入审计开始记录。失败则释放 claim 并按
   执行前失败返回。
10. Worker 完成 Runner/Session/请求对象准备；准备失败进入 FAILED_PRE_START。
11. Idempotency 状态切换为 RUNNING 并保存 execution_trace_id；Worker 在 30 秒
    timeout 内首次请求 Runner Event，此后超时或取消均进入 OUTCOME_UNKNOWN。
12. Worker 只接受正式最终 Event。结果产生后 Idempotency 保持 RUNNING，先可靠写入
    最终审计；审计失败进入 FAILED_POST_START/audit_incomplete。
13. 最终审计成功后条件提交 SUCCEEDED、FAILED_POST_START 或 OUTCOME_UNKNOWN；
    终态写入不确定时返回 outcome_unknown，终态后不再执行审计更新。
14. MetricsRecorder 记录结果与阶段耗时，释放 session lock；记录失败必须输出脱敏的
    `metrics_incomplete` 运维事件，但不得改写已提交的幂等终态或改变认证拒绝响应；
    Channel Adapter 返回当前投递 trace_id 对应的统一响应。

### Duplicate and Concurrent Delivery

- 已完成成功重复：不进入 session lock 或 Worker；返回缓存业务结果，状态为
  `duplicate`、`delivery_action=suppress`，original_trace_id 指向 execution trace。
- 已完成失败或不确定重复：返回首次保存的 502/503 安全错误，携带 execution trace，
  `delivery_action=suppress` 且不可用相同 ID 重试。
- 首次仍在处理：返回 `processing` 与可重试提示，original_trace_id 指向当前 owner
  attempt 的 trace，不启动第二次执行。
- 相同键不同指纹：返回 `idempotency_conflict`，不覆盖首次记录。
- 执行前失败：记录可重试状态，下一次相同消息可重新 claim。
- RUNNING 之后失败或结果未知：保存终态，相同标识不得自动重放。

### Session Ordering

- Lock Key 使用完整平台 session_id，而非外部 conversation_id。
- platform_session_id 包含 tenant_id 和 agent_id；绑定改绑 Agent 后不会复用旧锁或
  旧 SDK Session。
- 锁在幂等 claim 后、审计预写和 Worker 执行前获取，并在 `finally` 中释放。
- 锁只控制同一会话的不同消息；同一幂等消息由原子 claim 控制。
- 本阶段使用单进程异步锁；后续共享后端必须保持相同的串行业务语义。
- 不采用 sticky session；生产迁移目标是共享 Session 后端和分布式并发控制。

## Security and Privacy Design

- 签名版本为 `v1`，算法 HMAC-SHA256；使用原始请求字节摘要避免 JSON 重排歧义。
- 规范串固定包含版本、Unix 秒时间戳、binding_id、external_message_id 和正文
  SHA-256；详细格式由 HTTP 契约唯一规定。
- 时间戳偏差超过 ±300 秒直接拒绝；时间窗内重放由消息幂等阻止业务重复。
- Binding 只保存环境变量名称形式的 secret_ref；密钥解析器按需读取秘密，不缓存到
  可序列化模型，不进入异常文本。
- 签名使用恒定时间比较；未知绑定、密钥缺失和错误签名统一为未授权。
- 审计只保存消息摘要和伪名化 user_id，不保存完整正文、签名或密钥。
- 测试清除常见模型凭据并阻断外部 socket，延续第一阶段离线安全门禁。

## Data Consistency and Failure Semantics

| Failure Point | Side Effect Boundary | Result | Same-ID Retry |
|---|---|---|---|
| 输入或 HMAC 失败 | 未访问业务会话 | rejected | 修正请求后可提交 |
| Tenant/Binding 禁用 | 未 claim、未执行 | rejected | 配置恢复后可提交 |
| Idempotency claim 前失败 | 无执行权 | failed_pre_start | 允许 |
| 审计开始记录失败 | Agent 未开始 | audit_unavailable | 允许 |
| Worker 准备失败 | 尚未首次请求 Runner Event | failed_pre_start | 允许 |
| Runner 开始后失败 | Session 可能已有事件 | failed_post_start | 禁止自动重放 |
| RUNNING 后超时或取消 | Session 副作用不确定 | outcome_unknown | 禁止自动重放 |
| 最终 Event 缺失 | Session 可能已有事件 | failed_post_start | 禁止自动重放 |
| 结果持久化状态不确定 | 副作用未知 | outcome_unknown | 禁止自动重放 |
| RUNNING 状态下最终审计写入失败 | 执行结果已知但链路不完整 | failed_post_start / audit_incomplete | 禁止自动重放 |

InMemory Adapter 的方法仍必须表达原子 claim、条件状态转换和 compare-and-set 语义，
以便第三阶段实现 Redis/SQL 时不改变 Gateway 决策逻辑。

## Error Contract

| Result Category | HTTP Status | Retry Meaning |
|---|---:|---|
| succeeded / duplicate | 200 | duplicate 不再次投递业务回复 |
| processing | 202 | 相同消息稍后查询或重试 |
| invalid_request | 400 | 修正字段后使用新请求 |
| unauthorized | 401 | 不披露绑定存在性 |
| access_denied | 403 | 已验证绑定无可用租户或 Agent |
| idempotency_conflict | 409 | 必须使用新的外部消息标识 |
| agent_failed | 502 | 相同标识不可自动重放 |
| agent_unavailable | 503 | 仅 execution_started=false 时允许相同标识重试 |
| audit_incomplete / unavailable | 503 | 是否可重试由 pre-start 标志决定 |
| outcome_unknown | 503 | 相同标识不可自动重放 |

所有错误使用统一 envelope，包含当前 `trace_id`、稳定错误码和安全消息，不返回堆栈。

## Observability and Audit

- 每次 HTTP 投递都有当前 trace_id；processing 使用 owner attempt trace，已完成缓存
  结果使用 execution trace 作为 original_trace_id。
- 审计阶段：RECEIVED、AUTHORIZED/REJECTED、DUPLICATE/CONFLICT、EXECUTION_STARTED、
  SUCCEEDED/FAILED/AUDIT_INCOMPLETE。
- 最小字段：tenant_id（未知时为空）、channel、binding_id、伪名 user_id、
  session_id、agent_name、decision、latency_ms、error_type、cost、trace_id、
  external_message_digest、created_at。
- 认证后审计 append/update/query 均使用 TenantScope，并支持 tenant/session/trace；
  预认证拒绝仅使用 PreAuthScope。本阶段不增加公开审计 HTTP 接口。
- MetricsRecorder 按 TenantScope 或 PreAuthScope 记录请求量、错误量、阶段/Agent/
  状态后端耗时、投递量、token 和成本，并通过内部快照契约验收；真实模型、工具和
  IM 不适用项为零或 not_applicable。
- 指标写入故障以脱敏 `metrics_incomplete` 运维事件显式暴露；指标不是正确性账本，
  因而不反向修改已提交业务终态，也不改变未知绑定等认证拒绝的公开响应。
- 结构化日志复用相同 trace_id，但不能打印正文、签名、密钥或完整外部用户标识。

## Test Strategy

1. **测试先行**：Setup 仅创建可导入 API 骨架；每项用户故事先写行为失败测试，
   Red Gate 不接受 collection/import error，再实现最小代码。
2. **Unit**：HMAC 正常/篡改/超时、SessionKey 稳定与隔离、幂等状态转换、脱敏。
3. **Contract**：HTTP schema/status/terminal cached error envelope；所有 InMemory
   Repository、Audit 与 Metrics Adapter 运行同一组行为测试，为后续共享后端复用。
4. **Integration**：真实 ASGI → Gateway → Worker → 官方 Runner → SDK Session →
   Reply/Audit，不 mock 被验证的 SDK 链路。
5. **Concurrency**：同消息原子 claim、同会话串行、不同会话可并行、锁异常释放，
   以及 RUNNING 后超时/取消转 OUTCOME_UNKNOWN。
6. **Security**：跨租户访问、未知绑定与错误签名不可区分、秘密扫描、外部网络阻断。
7. **Regression**：保留并运行第一阶段 `tests/sdk_validation` 全部测试。

## Delivery and Rollback

- 实现以小提交推进：contracts/models → auth/isolation → idempotency/locks →
  Worker/SDK → HTTP/integration → docs/evidence。
- 本阶段不修改上游 SDK；依赖升级不在范围内。
- 若 HTTP 切片失败，可回退第二阶段代码提交而不影响第一阶段 SDK 验证入口。
- InMemory Adapter 明确标记 local-only；任何生产部署文档不得引用它作为共享后端。
- 完成时更新 README 运行方式、验收结果和 `决策记录.md` 的证据链接。

## Phase Outputs

- Phase 0: [research.md](./research.md)
- Phase 1: [data-model.md](./data-model.md)
- Phase 1 contracts: [local-message-http.md](./contracts/local-message-http.md)、
  [platform-ports.md](./contracts/platform-ports.md)
- Phase 1 validation guide: [quickstart.md](./quickstart.md)
- Human decision evidence: [决策记录.md](./决策记录.md)
- Analysis remediation evidence: [一致性分析修订记录.md](./一致性分析修订记录.md)
- Phase 2 tasks: [tasks.md](./tasks.md)

### Post-Design Constitution Re-check

Phase 1 设计产物不得改变上述 Gate 结论。检查重点如下：

- 数据模型的每个业务键都包含 tenant scope。
- HTTP 契约先验签再建立 TenantContext，且签名失败不泄露绑定存在性。
- Platform Ports 明确原子 claim、条件状态转换、同会话锁和审计失败语义。
- Audit 与 Metrics 端口显式携带 TenantScope/PreAuthScope，并提供本地指标证据。
- Worker 准备、RUNNING、首次 Runner Event、30 秒超时和取消边界定义一致。
- SDK 仅通过 Worker Adapter 使用，且固定版本与第一阶段验证保持一致。
- Quickstart 明确 InMemory、单进程、离线和未完成的生产能力。

复核结果：**PASS**，无 Constitution 违规或例外。

## Complexity Tracking

无需要豁免的宪法违规。Repository/Adapter、幂等状态机和会话锁均为宪法明确要求，
不是额外架构层级。

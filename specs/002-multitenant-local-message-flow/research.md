# Phase 0 Research: 多租户本地消息闭环

**Feature**: `002-multitenant-local-message-flow`
**Date**: 2026-09-05
**Status**: Complete — no unresolved `NEEDS CLARIFICATION`

## Research Basis

研究以项目宪法、第二阶段规格、第一阶段 SDK 验证代码和本地锁定依赖为依据。固定版
SDK 的实际签名已核对：`Runner.run_async(user_id, session_id, new_message, ...)`
返回异步 Event 流，`InMemorySessionService` 提供 session 创建、读取和事件追加。

## Decision 1: Local HTTP Runtime

- **Decision**: 使用 Starlette ASGI 应用和 Uvicorn，Pydantic 验证外部契约；将当前
  lock 中已验证兼容的版本声明为直接依赖。
- **Rationale**: Starlette 已存在于锁定依赖图，能以较小表面积完成本地 HTTP 入口、
  生命周期和 ASGI 测试；Pydantic 提供稳定字段错误；Uvicorn 提供可演示启动方式。
- **Alternatives considered**:
  - FastAPI：开发体验更完整，但当前环境未安装，为本切片增加新解析组合没有必要。
  - 标准库 HTTP Server：异步生命周期和测试支持较弱，会产生更多自研传输代码。
  - 直接复用 SDK Server：固定版 SDK 中未发现适合本功能多租户入口的公开通用接口。

## Decision 2: Verified Tenant Identity

- **Decision**: 以经过 HMAC 验证的 Channel Binding 作为租户身份根；请求正文不接受
  可直接授权的 tenant_id。Binding 解析到唯一 Tenant 与 Agent Application。
- **Rationale**: 调用方自报 tenant_id 不能构成授权。绑定是 IM 接入中的自然信任
  边界，能把身份验证和业务租户解析分离。
- **Alternatives considered**:
  - 信任 tenant_id：违反默认拒绝和租户隔离原则。
  - 本地网络即可信：无法测试认证边界，也不能平滑迁移到真实 IM。
  - 独立用户登录系统：超出本阶段范围。

## Decision 3: HMAC Contract

- **Decision**: 每绑定独立 secret_ref；HMAC-SHA256 规范串包含版本、Unix 秒时间戳、
  binding_id、external_message_id 和原始正文 SHA-256。允许偏差 ±300 秒，使用恒定
  时间比较。
- **Rationale**: 原始字节摘要避免 JSON 空白或字段顺序造成签名歧义；时间窗限制旧
  请求重放；external_message_id 将认证与后续幂等身份绑定。
- **Alternatives considered**:
  - 固定 Bearer Token：只能验证持有者，不能验证正文完整性。
  - 仅签正文：缺少绑定和时间上下文，重放边界更弱。
  - 非对称签名：对本地双租户切片复杂度过高。

## Decision 4: Session Identity

- **Decision**: 对规范化的 tenant_id、agent_id、binding_id、channel、external_user_id、
  conversation_type 和 external_conversation_id 进行长度无歧义编码后 SHA-256，
  生成带 `sess_` 前缀的平台 session_id。
- **Rationale**: 所有影响会话归属的维度都进入标识；摘要不暴露外部标识，长度编码
  避免简单分隔符拼接碰撞。相同输入稳定复用，不同租户或不同 Agent 必然隔离；绑定
  改绑 Agent 后不会把旧 Agent 上下文交给新 Agent。
- **Alternatives considered**:
  - 仅使用 external_conversation_id：会跨租户和跨用户碰撞。
  - 随机 UUID：无法让后续消息稳定恢复同一会话。
  - 明文拼接：日志和响应更容易暴露外部身份。

## Decision 5: SDK Integration Boundary

- **Decision**: Agent Worker Adapter 独占 LlmAgent、Runner、Event 和 SDK Session
  Service 的调用；Gateway 只依赖 AgentExecutor Protocol。
- **Rationale**: 平台编排不依赖 SDK 内部细节，未来 SDK 升级或模型替换仅影响适配
  层。Worker 使用官方 `Event.is_final_response()` 和 `get_text()` 判定回复。
- **Alternatives considered**:
  - Gateway 直接创建 Runner：框架依赖扩散，生命周期难管理。
  - 模拟 Runner：不能证明真实 SDK 链路。
  - 复制 SDK 会话实现：违反 framework-first。

## Decision 6: Idempotency State Machine

- **Decision**: 原子 claim 的作用域为 `(tenant_id, binding_id,
  external_message_id)`，并保存规范消息指纹、first_claim_trace_id、当前
  owner_trace_id 和实际 execution_trace_id。状态为 PENDING、RUNNING、SUCCEEDED、
  FAILED_PRE_START、FAILED_POST_START、OUTCOME_UNKNOWN。
- **Rationale**: claim 决定唯一执行者，指纹阻止同标识覆盖不同内容。只有
  FAILED_PRE_START 可重新 claim；RUNNING 之后的失败均禁止自动重放。
- **Alternatives considered**:
  - 先执行后记录：并发重复会同时执行。
  - 所有失败自动重试：可能重复不可见副作用。
  - 所有失败永久禁止：会让真正的执行前暂时错误无法恢复。

## Decision 7: Same-Session Concurrency

- **Decision**: 幂等 claim 后按平台 session_id 获取异步锁；同会话不同消息串行，
  不同会话使用独立锁并行。
- **Rationale**: 多轮上下文依赖消息顺序。按平台 session_id 锁定自然包含租户作用
  域，不需要 sticky session；端口语义可在第三阶段映射到共享锁或版本控制。
- **Alternatives considered**:
  - 最后写入覆盖：会造成不可预测上下文。
  - 全局锁：破坏不同会话并发。
  - 本阶段实现乐观版本重试：复杂度超出最小切片。

## Decision 8: Audit Ordering and Failure

- **Decision**: Agent 开始前必须写入可识别的审计开始记录；写入失败则 fail closed。
  Agent 结果产生后，幂等记录保持 RUNNING，先可靠写入最终审计，再条件提交业务
  终态。最终审计失败保存为 FAILED_POST_START/audit_incomplete；幂等终态写入结果
  不确定时保存为 OUTCOME_UNKNOWN。两种情况均禁止相同标识自动重放，终态后不再
  执行可能失败的审计更新。
- **Rationale**: 审计失败不能被吞掉；Agent 开始后可能已有会话或未来工具副作用，
  所以不能通过自动重试“修复”审计失败。
- **Alternatives considered**:
  - 异步尽力审计：可能把无审计执行报告为成功。
  - 审计失败后回滚 Agent：外部副作用通常不可回滚。
  - 审计失败后自动重跑：增加重复动作风险。

## Decision 9: Per-Delivery Trace Semantics

- **Decision**: 每次投递使用自己的 trace_id。幂等记录分别保存首次 claim、当前
  owner attempt 和实际 execution trace；`processing.original_trace_id` 指向当前
  owner attempt，终态缓存结果的 `original_trace_id` 指向实际 execution trace。
- **Rationale**: 每次入口尝试都必须可调查，同时需要证明多个投递只对应一次业务
  执行。复用首次 trace_id 会丢失重复投递自身的时间和来源证据。
- **Alternatives considered**:
  - 重复投递复用首次 trace：不能区分独立入口尝试。
  - 不保存 original_trace：难以关联缓存结果的来源。

## Decision 11: Agent Start, Timeout and Cancellation Boundary

- **Decision**: Worker 先完成 Runner/Session/请求对象准备；准备失败保持 PENDING 并
  进入 FAILED_PRE_START。准备成功后 Gateway 标记 RUNNING，Worker 在首次请求
  Runner Event 前建立 `execution_started=true` 边界。默认本地执行超时为 30 秒；
  RUNNING 后超时或取消进入 OUTCOME_UNKNOWN，且会话 lease 必须释放。
- **Rationale**: 把可能安全重试的准备失败与可能已产生 SDK Session 副作用的执行
  阶段分开，落实 D-002，而不是用“调用了某个函数”作为模糊边界。
- **Alternatives considered**:
  - 进入 Worker 即视为开始：过度保守，会永久拒绝安全的准备失败重试。
  - 收到首个最终 Event 才视为开始：过度乐观，中间 Event 已可能写入 Session。
  - 不设超时：会让幂等记录和同会话锁无限停留。

## Decision 12: Tenant-Scoped Audit and Local Metrics

- **Decision**: 认证后的 AuditRepository 与 MetricsRecorder 操作必须传入
  TenantScope；预认证拒绝只允许受限 PreAuthScope。审计支持 tenant/session/trace
  查询。MetricsRecorder 提供本地 InMemory 快照，记录请求量、错误量、阶段/Agent/
  状态后端延迟、投递量、token 和成本；不适用项明确为零或 not_applicable。指标写入
  失败输出脱敏 `metrics_incomplete` 运维事件，但不改写已提交的业务终态，也不改变
  预认证拒绝响应。
- **Rationale**: 审计记录中存在 tenant_id 不等于查询已被授权；显式 scope 可让未来
  SQL/Redis/Telemetry Adapter 复用安全语义，也补齐宪法要求的最小可观测证据。
- **Alternatives considered**:
  - 仅按 trace 无作用域查询：可能跨租户读到记录。
  - 只依赖日志和审计推导指标：无法提供稳定、可测试的指标合同。
  - 本阶段实现生产 Telemetry：超出本地纵向切片范围。

## Decision 10: Test and Demonstration Boundary

- **Decision**: 测试分为 unit、contract、integration 与既有 sdk_validation。
  HTTP 集成测试通过 ASGI 发起真实请求，Worker 走真实官方 Runner，但继续使用确定
  性离线模型；Repository 故障通过可控 fake 注入。
- **Rationale**: 可同时证明平台业务规则和真实 SDK 边界，又不需要外部账号、网络、
  凭据或费用。相同 Repository 契约测试可在第三阶段复用于 Redis/SQL Adapter。
- **Alternatives considered**:
  - 全部 mock：无法证明完整链路。
  - 真实模型和 IM：不可重复且超出范围。
  - 只做端到端测试：失败定位差，不能覆盖状态转换。

## Resolved Planning Questions

| Topic | Resolution |
|---|---|
| HTTP framework | Starlette ASGI + Uvicorn |
| Tenant trust root | Verified Channel Binding |
| Request authentication | Per-binding HMAC-SHA256, ±300 seconds |
| Session isolation | Tenant-scoped deterministic digest |
| Duplicate concurrency | Atomic claim and content fingerprint |
| Same-session concurrency | Per-session serialization |
| Retry boundary | Pre-start only |
| Audit failure | Pre-start fail closed; final audit failure becomes non-replayable FAILED_POST_START/audit_incomplete |
| Agent rebinding | Agent-scoped session; rebinding creates a new session |
| Agent lifecycle | Prepare before RUNNING; 30-second timeout after start |
| Audit finalization | Final audit while RUNNING, then terminal idempotency CAS |
| Trace relationship | Current trace plus owner/execution source trace |
| Observability | Tenant-scoped audit queries and local MetricsRecorder |
| SDK boundary | Worker Adapter over official public interfaces |

No unresolved research item remains for Phase 1 design.

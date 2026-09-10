# Implementation Plan: 共享状态多节点消息闭环

**Branch**: feature/luwenjie
**Logical Feature**: 003-shared-state-multinode-flow
**Date**: 2026-09-07
**Spec**: [spec.md](./spec.md)
**Decision Record**: [clarification-decisions.md](./clarification-decisions.md)

## Summary

在保持第二阶段 HTTP Channel Adapter、Gateway 编排职责、Agent Worker、官方
tRPC-Agent Runner 和统一回复 v1 契约兼容的前提下，增加 shared runtime profile：
两个独立 Worker 进程连接同一 Redis 与 PostgreSQL。Redis 提供跨节点幂等状态、
处理租约、Session 租约、generation/fencing 和短期 Session/Event；PostgreSQL
持久保存 Tenant、Agent、Channel Binding、Audit Log 与 Recovery Marker。

共享 I/O 端口升级为 async，但保留第二阶段端口名称、领域对象、稳定错误类别和业务
语义。InMemory 与共享实现运行同一组参数化契约测试。官方 Runner 继续通过公开
BaseSessionService 边界访问 Session；平台新增带 fencing 的 Session Service Adapter，
不修改或复制 Runner 内部源码。

## Technical Context

**Language/Version**: Python 3.12（项目约束 >=3.12,<3.13）
**Primary Dependencies**: trpc-agent-py 1.1.19、Starlette 1.6.0、Uvicorn 0.52.4、
Pydantic 2.13.5、redis-py 8.1.0、SQLAlchemy 2.0.52、asyncpg 0.31.0
**Storage**: Redis 7.4.11（AOF、本地 Docker volume）保存短期共享状态；
PostgreSQL 17.11 保存配置、审计和恢复记录
**Testing**: pytest、pytest-asyncio、httpx ASGI/loopback、参数化 Repository/Adapter
契约测试、真实 Redis/PostgreSQL 集成测试、双进程并发与故障注入测试
**Target Platform**: Windows/Linux 本地开发机；Docker Desktop/Compose 提供受控
Redis 与 PostgreSQL；服务仅监听 loopback
**Project Type**: Python ASGI web service with two local Worker processes
**Performance Goals**: 确定性离线模型下，非故障请求本地 p95 < 2 秒；Redis/SQL
单次操作本地 p95 < 100ms；不同 Session 能观察到并行执行
**Constraints**: 无 sticky session；同 Session 最大执行并发 1；Agent 最多执行一次；
租约裁决不依赖 Worker 本机时间；权威配置不可用时 fail closed；无真实模型/IM/生产
Telemetry；不宣称生产 HA 或跨系统 exactly-once
**Scale/Scope**: 2 个 Worker；2 个租户；至少 20 轮跨节点会话、50 组并发重复、
100 次顺序重复、50 组同会话并发、20 组不同会话并行、200 次随机节点路由；本地演示
10 分钟内完成

所有规划未知项已在 [research.md](./research.md) 中解决，目前没有未决规划问题。

## Constitution Check

### Pre-Research Gate

| Principle | Gate Result | Evidence |
|---|---|---|
| I. Framework-First | PASS | 固定 trpc-agent-py 1.1.19；Runner/Event/Session 公共抽象直接复用；不修改 SDK 源码 |
| II. Tenant Isolation | PASS | Redis key、SQL 主外键、Repository scope、查询和测试均显式 tenant scoped |
| III. Stateless Workers | PASS | Worker 只保留单次请求与客户端连接；业务状态全部位于 Redis/PostgreSQL |
| IV. Contract-First | PASS | 002 HTTP v1 不变；InMemory/Redis/PostgreSQL 运行同一核心端口契约 |
| V. Security by Default | PASS | 配置后端不可验证时拒绝；秘密仅以 runtime reference 注入；日志和审计脱敏 |
| VI. Observability | PASS | delivery/owner/execution trace、node_id、generation、恢复状态贯穿；错误不吞没 |
| VII. Vertical Slice | PASS | 双节点、共享状态、故障注入、快速演示均有独立验收证据 |

**Gate result before Phase 0**: PASS，无需宪法例外或 Complexity Tracking justification。

## Framework Reuse and Platform Ownership

### Directly Reused from tRPC-Agent

- LlmAgent、Runner、Event、Content、Part 与确定性离线模型调用路径。
- BaseSessionService、Session 及 Session Service 的公开异步方法语义。
- Runner 的 Event 流、is_final_response() 与 get_text() 最终回复选择方式。
- 现有固定版本和第一阶段 SDK 兼容验证。

### Platform-Owned Additions

- RedisIdempotencyRepository：跨节点 claim、execution-start、终态 CAS 与恢复补齐。
- RedisSessionLeaseManager：同 Session 互斥、续期、generation 与 fencing。
- FencedRedisSessionService：通过官方 Session Service 接口为 Runner 提供共享 Session，
  所有变更写入均校验当前 Session lease generation。
- PostgresConfigurationRepository：Tenant/Agent/Binding 权威配置及 schema version。
- PostgresAuditRepository 与 PostgresRecoveryRepository：持久审计、诊断审计和部分提交恢复。
- SharedPlatformAdapters、节点身份、租约 heartbeat、错误映射、双 Worker 启动与故障测试。

官方 RedisSessionService 不直接作为最终写入实现，因为它的公开调用不接收平台
generation，无法原子拒绝失去 Session 租约的旧 Worker。平台 Adapter 实现公开
BaseSessionService 边界，但不复制或修改 Runner 内部逻辑。

## Architecture

### Runtime Topology

~~~text
Local sender / test router
          |
          +----------------------+----------------------+
          |                                             |
  Worker process A :8001                       Worker process B :8002
  HTTP Adapter -> Gateway -> Agent Worker      HTTP Adapter -> Gateway -> Agent Worker
          |                 |                           |                 |
          |                 +-- official Runner -------+                 |
          |                                             |                 |
          +---------------- Redis 7.4.11 ----------------+-----------------+
          |        idempotency / leases / session / events
          |
          +------------- PostgreSQL 17.11 -------------------------------+
                   tenant config / binding / audit / recovery
~~~

每个 Worker 拥有独立的 Gateway、Runner cache、连接池和 node_id。Runner cache 仅保存
可重建对象，不保存继续会话所需业务状态。请求可显式交替或随机发往任意节点。

### Request Sequence

1. HTTP Adapter 验证 v1 请求结构并创建/继承 delivery trace_id。
2. PostgresConfigurationRepository 获取最小 BindingAuthMaterial；SQL 不可用时返回
   configuration_unavailable，任何正向缓存不得放行。
3. HMAC 成功后，用 VerifiedBindingScope 从 PostgreSQL 解析 Tenant/Agent/Binding。
4. 查询 RecoveryRepository 是否存在阻断性 terminal_pending/outcome_unknown 记录；
   有则先读取/条件修复原终态，绝不重新调用 Agent。
5. RedisIdempotencyRepository 原子 claim 消息，返回 acquired、processing、
   completed 或 conflict，并保存 first/owner/execution trace 关系。
6. acquired 请求在限定等待时间内取得 tenant-scoped Session lease；无法取得则安全
   结束本次 pre-start attempt 并返回可重试 session_busy。
7. 写入 authorized 审计，准备 Agent/Runner 输入；任一步失败均保持执行前可重试语义。
8. Redis 原子校验消息与 Session generation，并将消息阶段条件推进为
   EXECUTION_STARTED；未确认成功时不得调用 Runner。
9. 启动 lease heartbeat。Runner 通过 FencedRedisSessionService 读取/写入共享
   Session；create/update/append_event 必须携带当前 SessionFence 并原子校验。
10. Agent 完成后，在 generation 仍有效时进入 FINALIZING；PostgreSQL 单事务写最终
    Audit 与 terminal_pending Recovery Marker，保存脱敏结果或安全引用。
11. Redis 以消息 generation、Session generation 和 execution_trace_id 条件提交终态；
    成功后将 Recovery Marker 标记 reconciled。Redis 结果不确定则返回 outcome_unknown，
    恢复流程只补齐原结果。
12. 记录 node-scoped metrics，释放租约，按既有统一回复契约返回。

## Clarification Decision Mapping

| Decision | Architecture | Data Model | State Machine | Error Semantics | Required Tests |
|---|---|---|---|---|---|
| D-003-001 租约续期 | Redis 原子脚本以 PTTL/当前 token 与 generation 裁决；默认 lease 10s、每 3s 续期 | Lease 含 generation、token、node、phase；generation counter 不回退 | ACTIVE 仅未过期当前代可 RENEW；过期只能 ACQUIRE 新代 | 续期不确定：开始前 lease_unavailable；开始后 outcome_unknown | 续期/到期/接管三方竞争 50 组，双 owner 为 0 |
| D-003-002 fencing 与审计 | 所有权威写携带 MessageFence/SessionFence；迟到写仅触发独立 diagnostic audit | Audit 区分 business/diagnostic，记录 rejected/current generation | LOST generation 不得写业务状态；诊断 append 不改变终态 | stale_fence 内部错误；开始后映射 outcome_unknown，诊断失败不放行旧写 | 旧代 Session/Event/terminal/business audit 全拒绝，诊断可查 |
| D-003-003 节点接管 | Runner 前先原子写 EXECUTION_STARTED；heartbeat 不是“未开始”证据 | IdempotencyRecord 保存 execution_stage 与 execution_trace_id | 仅 CLAIMED/PREPARED 且旧 lease 确认失效可换代；EXECUTION_STARTED/未知不可重放 | 无法证明未开始：outcome_unknown、retryable=false | 开始标记前/中/后三个 kill point，Agent call <= 1 |
| D-003-004 部分提交 | SQL 事务写 final audit + terminal_pending marker，再 Redis terminal CAS；reconciler 只复制原结果 | RecoveryMarker 保存 digest/安全结果、generation、trace、状态 | TERMINAL_PENDING -> RECONCILED 或 CONFLICT_REVIEW；不回 EXECUTING | Redis terminal 不确定：outcome_unknown；永不对部分成功返回 succeeded | 每个跨后端写点超时；修复不增加 Agent 调用 |
| D-003-005 配置缓存 | PostgreSQL 是授权权威；正向缓存仅在同次可验证版本下加速 | 配置含 config_version/status/ownership/secret_ref；缓存非权威 | BACKEND_UNVERIFIED 只能 DENY，不可 ACTIVE | configuration_unavailable，503、execution_started=false、可稍后重试 | SQL outage + 各类旧缓存时 Agent call = 0 |

## Shared State Design

### Redis Atomicity

- 使用短小、版本化 Lua scripts，通过 SCRIPT LOAD/EVALSHA 执行 claim、renew、release、
  mark_execution_started、fenced event append、terminal CAS 和 reconcile。
- 每个脚本只访问一个业务作用域的少量 key，不执行扫描或外部调用。
- 租约有效性以 Redis key 是否存在及 PTTL 为准，不使用 Worker 本机时间。
- generation counter 独立持久且单调增长；lease key 可过期，counter 不因过期回退。
- Redis timeout 的“请求未到达”和“已执行但响应丢失”不可猜测；调用方先 read-back，
  仍无法证明时进入相应 unavailable/outcome_unknown 语义。

### Key Rules

- 所有 key 使用 trpc:v1 前缀和 tenant/agent/binding/session/message 的规范化摘要。
- 原始 external_user_id、external_conversation_id、正文、秘密和签名不得进入 key。
- 幂等记录终态在本阶段不自动过期；Session/Event 默认保留 24 小时用于本地演示，
  测试使用独立 run_id 前缀并显式清理。
- Redis 开启 AOF everysec 与 named volume；这是本地恢复证据，不宣称生产持久保证。

### Lease Values

- 默认 processing/session lease：10 秒。
- heartbeat：每 3 秒；只有当前 token + generation 且 PTTL > 0 可续期。
- Session acquire 最大等待：2 秒；失败返回 session_busy，原消息仍处于安全执行前状态。
- Agent timeout：沿用 30 秒；heartbeat 覆盖执行与 finalization。
- 测试通过依赖注入使用 100–500ms lease，不以真实等待拖慢测试。

## Persistent State Design

- PostgreSQL 17.11 使用 SQLAlchemy AsyncEngine + asyncpg。
- migrations 采用仓库内版本化 SQL 文件与 schema_migrations 表；启动只校验版本，
  显式 init 命令执行迁移与 demo seed。检测到未知更新版本时 fail closed。
- Tenant、AgentApplication、ChannelBinding 使用外键和复合唯一约束表达所有权。
- AuditRecord append-only；普通业务终态使用唯一
  (tenant_id, idempotency_key_digest, owner_generation, decision) 防止重复。
- RecoveryMarker 与最终业务 Audit 在同一 SQL 事务写入；结果只保存统一回复所需的
  脱敏字段或加密/安全引用，本阶段不保存原始消息正文。
- 密码、Redis URL 与数据库 URL 由运行时环境注入；公开错误不返回连接或 SQL 信息。

## State Machines

完整字段和转换见 [data-model.md](./data-model.md)。

### Message Execution

~~~text
ABSENT
  -> CLAIMED(generation, lease)
  -> PREPARED
  -> EXECUTION_STARTED(execution_trace_id)
  -> FINALIZING
  -> SUCCEEDED | FAILED_POST_START | OUTCOME_UNKNOWN

CLAIMED/PREPARED -- lease expired --> CLAIMED(new generation)
EXECUTION_STARTED/FINALIZING -- owner lost --> OUTCOME_UNKNOWN (no replay)
terminal states --> immutable
~~~

### Session Lease

~~~text
FREE -> ACTIVE(generation)
ACTIVE -- valid current owner renew --> ACTIVE(same generation)
ACTIVE pre-start -- expired --> ACTIVE(new generation)
ACTIVE post-start -- expired --> QUARANTINED
ACTIVE -- valid release after terminal/pre-start abort --> FREE
LOST generation -- any business write --> FENCE_REJECTED + diagnostic audit
~~~

### Recovery

~~~text
NONE
  -> TERMINAL_PENDING
  -> RECONCILED
  -> CONFLICT_REVIEW (conditional target differs)

TERMINAL_PENDING never authorizes Agent execution.
~~~

## Error and Recovery Semantics

| Condition | Public Result | Retryable | Execution Started | Recovery |
|---|---|---:|---:|---|
| Redis unavailable before claim/start | 503 state_backend_unavailable | yes | false | Same ID may retry after backend recovery |
| PostgreSQL unavailable before auth/context | 503 configuration_unavailable | yes | false | No positive cache authorization |
| Session lease busy before start | 503 session_busy | yes | false | Pre-start attempt released/marked safe, retry later |
| Lease/fence lost before EXECUTION_STARTED | 503 lease_lost | yes | false | New generation only after confirmed expiry |
| Lease/fence lost after EXECUTION_STARTED | 503 outcome_unknown | no | true | Quarantine; never auto-replay |
| Final audit transaction fails | 503 audit_incomplete | no | true | Preserve non-replayable execution evidence |
| SQL audit succeeds, Redis terminal uncertain | 503 outcome_unknown | no | true | Reconcile only saved terminal result |
| Stale owner business write | safe failure; normally outcome_unknown | no | depends | Reject write, append diagnostic audit |
| Same key, different fingerprint | 409 idempotency_conflict | no | false | Immutable original record |
| Existing owner still processing | 202 processing | yes | current value | Link owner_trace_id; no Agent call |

Vendor timeout/connection/serialization exceptions are mapped once at Adapter boundaries. Public
messages remain fixed and redacted。A retryable transport result never overrides the same-ID
non-replayable rule after EXECUTION_STARTED。

## Trace and Audit Semantics

- trace_id：当前 HTTP 投递，全链路每次调用均保留。
- first_claim_trace_id：第一次创建幂等记录的投递，只用于证据。
- owner_trace_id：当前 generation 的 owner 投递，接管时更新。
- execution_trace_id：成功写入 EXECUTION_STARTED 的投递，终态与重复回复均指向它。
- node_id：由启动参数提供的非秘密稳定进程标识，进入 Redis owner 和 SQL audit。
- Audit 记录 message_generation、session_generation、audit_kind 与 recovery_status。
- diagnostic late-write audit 只能描述拒绝事实，不能成为成功终态或触发业务回复。

## Contract Evolution

- POST /v1/local/messages 的请求、HMAC canonical string、成功/重复/冲突语义保持不变。
- 增加 state_backend_unavailable、configuration_unavailable、session_busy、lease_lost
  安全错误；均使用既有统一 envelope。
- GET /healthz 继续仅表示进程存活；新增 GET /readyz，仅返回 ready/not_ready，不泄露
  后端地址或凭据。
- Repository 方法升级为 async；InMemory Adapter 同步升级，领域结果与稳定异常不变。
- IdempotencyRepository 和 SessionLockManager 增加 generation/renew/fence 方法；
  002 核心断言抽成参数化套件，InMemory 与共享实现共同执行。
- 详细契约见 [contracts/shared-state-ports.md](./contracts/shared-state-ports.md)、
  [contracts/error-semantics.md](./contracts/error-semantics.md) 与
  [contracts/local-message-http-v1.md](./contracts/local-message-http-v1.md)。

## Testing Strategy

### Test-First Order

1. 先扩展领域模型、状态机和端口契约测试，确认 InMemory 失败。
2. 更新 InMemory 实现使新契约通过，证明兼容基线。
3. 实现 Redis 原子脚本与 Adapter，让同一契约套件对 Redis 通过。
4. 实现 PostgreSQL 配置/Audit/Recovery，让同一 scope/错误契约通过。
5. 实现 FencedRedisSessionService 与 Gateway 编排，运行 SDK 边界测试。
6. 最后增加双进程 E2E、并发、kill point、后端 outage 和恢复测试。

### Required Layers

- Unit：key 编码、指纹、状态转换、错误映射、trace 关系、配置校验。
- Contract：同一测试工厂参数化 InMemory/Redis/PostgreSQL；不得断言 vendor payload。
- Integration：真实 Redis/PostgreSQL，两个 Runtime/Worker 实例，不共享 Python 对象。
- Process E2E：两个 Uvicorn 进程监听 8001/8002，随机路由、停止节点并继续会话。
- Fault injection：Redis/SQL 连接阻断、脚本响应丢失、各状态写点 kill、续期/接管竞争。
- Regression：完整 001 SDK validation 与 002 的 101 项现有测试继续通过。
- Security：跨租户相同外部 ID、旧缓存授权、secret/log/trace/audit 扫描。

## Local Operations and Rollback

- compose 只暴露 loopback 端口，镜像固定 redis:7.4.11-alpine3.21 与
  postgres:17.11-alpine3.24。
- shared profile 使用独立环境变量；默认 local profile 继续走 InMemory，不要求 Docker。
- 数据初始化必须显式执行且幂等；Worker 启动不自动降级或清空后端。
- 回退时停止 shared Workers 并恢复 002 local profile；不得把 shared 数据自动导入
  InMemory。测试数据通过 feature-specific namespace/database 清理。
- Redis/PostgreSQL 任一依赖不可用时 readyz 为 not_ready，healthz 仍只反映进程存活。

## Project Structure

### Documentation

~~~text
specs/003-shared-state-multinode-flow/
├── spec.md
├── clarification-decisions.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── local-message-http-v1.md
│   ├── shared-state-ports.md
│   └── error-semantics.md
└── tasks.md                       # created by speckit-tasks, not this command
~~~

### Source Code

~~~text
trpc_service/
├── _cli.py                        # preserve local commands; add shared init/serve
├── web/app.py                     # composition profiles, node_id, health/readiness
├── gateway/service.py             # async repositories, lease heartbeat, recovery order
├── worker/service.py              # official Runner; bind SessionFence context
├── storage/
│   ├── contracts.py               # async compatible ports and typed failures
│   ├── models.py                  # generation, phase, lease and recovery domain models
│   ├── inmemory.py                # upgraded compatibility implementation
│   ├── locks.py                   # upgraded InMemory lease contract
│   ├── shared.py                  # shared profile composition/lifecycle
│   ├── redis_idempotency.py
│   ├── redis_leases.py
│   ├── redis_session.py           # BaseSessionService-compatible fenced adapter
│   ├── redis_scripts/             # versioned short Lua scripts
│   └── postgres/
│       ├── models.py
│       ├── repositories.py
│       └── migrations/001_shared_state.sql
├── config/settings.py             # profile/URL refs/timeouts; no embedded secrets
└── audit/models.py                # node/generation/kind/recovery fields

deploy/local-shared/
└── compose.yaml

tests/
├── unit/
├── contract/                      # parameterized InMemory/shared port suites
├── integration/shared/
├── e2e/test_two_worker_processes.py
└── sdk_validation/
~~~

**Structure Decision**: 保持现有单项目 Python 包结构，通过 storage 子模块增加共享
Adapters，不建立第二套 Gateway/Worker。部署文件只服务本地共享后端验收。

## Post-Design Constitution Check

| Gate | Result | Design Evidence |
|---|---|---|
| Framework-first | PASS | Runner 保持官方实现；Fenced Session Adapter 实现公开 Session Service 边界 |
| Tenant isolation | PASS | 数据模型、key、SQL FK/index、scope contract 和交叉租户测试齐全 |
| Stateless workers | PASS | 业务状态位于 Redis/PostgreSQL；进程缓存均可重建 |
| Contract-first | PASS | 外部 v1 契约兼容，内部端口有 InMemory/shared 共用测试矩阵 |
| Security/governance | PASS | 配置缓存不授权、secret reference、默认拒绝和迟到写审计 |
| Observability | PASS | 三类 trace、node/generation、Audit/Recovery 与 backend metrics 可关联 |
| Evidence | PASS | quickstart、双节点 E2E、并发/故障测试和范围披露均已规划 |

**Gate result after Phase 1 design**: PASS。没有宪法违规，无需 Complexity Tracking 表。

## Planning Risks

1. SDK Session 公共模型升级导致 Adapter 不兼容：固定 1.1.19，保留 sdk_validation。
2. Runner 内一次执行产生多次 Event 写入：Session lease 覆盖整个执行，event append
   使用 sequence + event_id + generation 原子去重。
3. Redis 响应丢失导致调用方不知道脚本是否执行：read-back 后仍不确定即 fail closed。
4. 两后端无分布式事务：SQL terminal_pending marker + Redis CAS + 幂等 reconciler，
   不回滚、不重新执行。
5. Worker 暂停超过 lease：heartbeat + fenced writes；开始后 Session 进入 quarantine。
6. SQL 配置缓存陈旧：缓存从不在权威后端不可验证时授予权限。
7. Docker 不可用：共享集成/E2E 无法验收，但 002 local profile 仍可回归；不得声称
   第三阶段完成。
8. 本地基础设施性能不稳定：性能阈值单独报告环境，正确性门禁不因延迟放宽。

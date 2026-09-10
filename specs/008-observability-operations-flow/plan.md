# Implementation Plan：生产可观测性与运维收敛

**Git Branch**：`feature/luwenjie` | **Feature**：`008-observability-operations-flow` | **Date**：2026-09-10 | **Spec**：[spec.md](./spec.md)

**Input**：`/specs/008-observability-operations-flow/spec.md`

**Decision Record**：[clarification-decisions.md](./clarification-decisions.md)

## Summary

在前七阶段多租户双 IM、共享状态、治理与数据后端能力之上，建立供应商中立的生产可观测性与运维控制面：用同一可信关联上下文串联 Channel Adapter、Gateway、幂等/Session、Worker、官方 tRPC-Agent Runner、治理、Tool、数据、回复、恢复与 Audit；通过 OpenTelemetry/OTLP 输出脱敏 trace 与低基数 metric；提供角色级健康、告警、租户级灰度/回滚、安全排空、故障演练和可重复容量门禁。

五项人工决策确定了实现路线：关键异常 100% 完整轨迹、普通成功比例采样；普通 telemetry fail-open 而正式 Audit 边界不变；角色级 readiness 和路径级 degraded；安全/一致性硬门槛自动回滚、质量门槛暂停人工决定；容量使用正确性零容忍和同机相对性能不超过 10% 的双门禁。

## Implementation Baseline

- 第二、三阶段已有可信 tenant、tenant-scoped Session、Redis 幂等/租约、PostgreSQL 权威配置与 Audit、generation/fencing、双 Worker 和恢复。
- 第五阶段已有飞书/企业微信正式 Adapter、真实 identity key、长连接接管、统一入站/回复和投递状态。
- 第六阶段已有治理策略、主体授权、危险操作确认、预算状态机与 Audit fail-closed 边界。
- 第七阶段已有 Event/Memory/Summary/Artifact/Knowledge 的统一数据边界、PostgreSQL schema v6、迁移与跨节点恢复。
- 当前 `metrics/`、`log/`、`observability/operational.py` 只提供早期计数和安全日志骨架，尚未形成标准 trace、统一分类、出口故障缓冲、健康、告警或发布控制；现有 raw `trace_id` 日志和可能携带高基数 scope 的 metric 必须按本计划收敛。
- 官方 `trpc-agent-py==1.1.19` 已产生 OpenTelemetry Runner/Agent/LLM/Tool span 和 `gen_ai.*` metric；平台复用其公开行为，不复制执行循环。

## Technical Context

**Language/Version**：Python 3.12（项目约束 `>=3.12,<3.13`）

**Primary Dependencies**：trpc-agent-py 1.1.19、OpenTelemetry API/SDK/OTLP HTTP exporter 1.44.0、Pydantic 2.13.5、Starlette 1.6.0、Uvicorn 0.52.4、SQLAlchemy 2.0.52、asyncpg 0.31.0、redis 8.1.0、pytest/pytest-asyncio

**Storage**：PostgreSQL 保存配置快照、release/route/pin、硬门槛、告警活动状态、转换事件与正式 Audit；Redis 保存 controller lease/fence 和非权威缓存；OTLP/可替换诊断后端保存可丢运行遥测；有界缓冲只在进程内

**Testing**：Unit、Repository/Adapter contract、PostgreSQL/Redis integration、双节点/双 IM E2E、安全扫描、出口/依赖故障注入、固定 1,000 消息容量 A/B、前七阶段全量回归

**Target Platform**：Windows/Linux 本地开发；Docker Compose 最小可观察部署；生产拓扑仅形成供应商中立建议，不交付真实 Kubernetes 集群

**Project Type**：Python 异步服务，保持 Channel Adapter → Gateway → Governance → Worker → 官方 Runner → Delivery 的单一路径

**Performance Goals**：2 tenants、2 Workers、100 concurrent Sessions、1,000 messages 正确性零失败；telemetry on 相对同机 baseline 的 throughput 下降和 p50/p95/p99 增幅均不超过 10%

**Constraints**：可信 tenant scope、低基数 labels、进程内出口前脱敏、关键采样 100%、普通遥测非阻断且有界、Audit 仍 fail closed、Worker 无持久业务状态、配置发布 PG 单权威、Secret/正文不得出现在可观察输出或 Git

**Scale/Scope**：双 IM、双租户、双节点、成功/拒绝/重复/冲突/接管/恢复/回滚；真实模型、商业监控、管理 UI、生产 K8s 与绝对 SLA 不在范围

## Constitution Check

*GATE：Phase 0 前检查，并在 Phase 1 设计完成后复查。*

### Pre-Research Gate

| Principle | Result | Evidence |
|---|---|---|
| I. Framework-First | PASS | 复用官方 Runner 及 `trpc_agent_sdk.telemetry` 的 span/metric 生命周期；平台只补边界、脱敏和出口 |
| II. Tenant Isolation | PASS | correlation、诊断查询、release/route/pin、告警与 Audit 均要求可信 TenantScope；外部 tenant_id 无效 |
| III. Stateless Workers | PASS | release/alert 状态在 PostgreSQL，租约/fence 在 Redis；Worker 只保存请求内 context 与有界可丢 telemetry |
| IV. Contract-First | PASS | Telemetry、Exporter、Health、Alert、Release、Route、Capacity 和 Drain 都先定义可替换端口与稳定错误 |
| V. Security by Default | PASS | span 出口前 allowlist；Secret/正文/身份/URL 禁止进入 trace/metric/log/alert；配置权威不可用 fail closed |
| VI. Observability | PASS | 端到端阶段、重试、接管、恢复、依赖、投递与 release 转换有一致分类、指标、trace 和正式 Audit 边界 |
| VII. Spec-Driven Evidence | PASS | DEC-001～005、FR-001～036、NFR 与 SC 映射到架构、状态机、测试和可重复证据 |

**Gate result**：PASS。无宪法例外；Phase 0 前没有未解决澄清。

## Framework Reuse and Platform Ownership

### 直接复用官方 tRPC-Agent

- 继续使用官方 `Runner.run_async()`、Session/Event/Memory/Knowledge 公共接口及官方 Runner/Agent/LLM/Tool OpenTelemetry instrumentation。
- Gateway/Worker 激活平台 OTel 父上下文后调用 Runner，使官方 `invocation` 等 span 成为同一 trace 的子节点。
- 不复制 Runner 执行循环、模型重试、取消、流式或 Tool instrumentation；不修改 `.venv` 或 SDK 私有实现。

### 平台必须控制的边界

- Channel/Gateway/治理/数据/投递/恢复的 span、可信 tenant/trace 关联和跨进程 W3C context。
- 官方及平台 span 在离开进程前的 allowlist/redaction；原始 `ReadableSpan` 不得直接交给网络 exporter。
- Metric definition/label registry、结果分类、采样分类、优先级缓冲、出口健康与 drop counter。
- 角色 readiness、平台聚合、告警状态、配置快照/灰度/回滚、容量 harness 和排空生命周期。

## Architecture

```text
Feishu / WeCom / Signed HTTP
            │ provider event / trusted identity
            ▼
 Channel Adapter ─► Gateway ─► Governance ─► Worker ─► official tRPC-Agent Runner
      │                │                         │          ├─ Agent / LLM / Tool spans
      │                └─ idempotency/session ──┘          └─ official gen_ai metrics
      │
      └────────────── Delivery / Recovery / Audit
                              │
                 TrustedCorrelationContext
                              │
           ┌──────────────────┼────────────────────┐
           ▼                  ▼                    ▼
 platform stage spans   bounded metrics/logs   formal Audit (fail closed)
           │                  │                    │
           └──── SanitizingSpanProcessor ──────────┘ (Audit is not exported as telemetry fact)
                              │ safe envelopes only
                    PriorityTelemetryBuffer
                              │ OTLP/HTTP
                              ▼
                 OpenTelemetry Collector
                    processors / exporter
                              │
                  replaceable diagnostic backend

 PostgreSQL authority                    Redis coordination
 - config snapshots/releases/routes      - release/alert leases
 - execution pins/hard signals           - fencing/generation
 - alert incidents/transition events     - non-authoritative cache
 - immutable Audit                       - never config authority
```

## DEC-001～DEC-005 Design Mapping

| Decision | Architecture / State | Failure Semantics | Required Evidence |
|---|---|---|---|
| DEC-001 sampling | SDK AlwaysOn 收集；结束时 critical/normal 分类；关键全保留，普通成功稳定 hash，默认 10%、tenant ceiling 25%；Collector tail policy | 正常出口保证关键采样不丢；长期出口故障超有界容量只留 minimal summary，并显式统计 full drop | 关键类别 100% keep、成功桶确定性、tenant override 不越界、全 span 父子完整 |
| DEC-002 telemetry boundary | Safe envelope + 双优先级有界内存队列 + 异步 OTLP；正式 Audit 独立 | 普通 telemetry fail open；最多 3 次有限重试；普通成功先丢；Audit 故障仍按既有边界拒绝 | 出口断连业务终态不变、队列不越界、drop counter、Audit outage 零受保护副作用 |
| DEC-003 health | role dependency matrix + liveness/readiness/status 三层；path aggregation | 关键依赖 missing/unknown 使本角色 unready；单渠道/telemetry 故障使平台 degraded；无安全路径 unready | 全矩阵表驱动、30 秒内状态变化、恢复稳定 60 秒内告警 resolved |
| DEC-004 canary | PG immutable snapshot/release/tenant route/execution pin；Redis lease/fence；hard latch | hard gate 自动 stop/rollback 新请求；quality breach 只暂停；in-flight 固定 snapshot，pending side effect 重新授权 | 双 tenant 范围隔离、双节点 CAS/fence、各崩溃点恢复、Audit 同事务、旧节点写拒绝 |
| DEC-005 capacity | 固定 manifest 的 telemetry off/on A/B；同机同配置同 seed | 正确性任一非零立即失败；throughput 或任一 percentile 增幅 >10% 失败；环境不等价标 invalid | 2 tenant/2 Worker/100 Session/1,000 message 报告，含资源峰值且明确非生产 SLA |

## Correlation and Stage Model

### 可信关联

1. Adapter 接收外部事件时校验/重建业务 `request_trace_id` 和 W3C root context；外部 metadata 不可指定 tenant。
2. Channel Binding 成功后绑定 `TenantScope`；失败前只允许 PreAuth/PlatformScope 安全记录。
3. 幂等 claim 将 first/owner/execution trace 与 generation 链接，不覆盖原 request。
4. 新执行读取 `TenantConfigRoute` 并创建不可变 `ExecutionConfigPin`；Worker/Runner/恢复沿用该 pin。
5. 普通日志和用户可见诊断使用 `trace_digest`；完整 trace id 只在授权诊断/Audit/受控 OTel context 使用，永不进入 metric label。

### 统一阶段枚举

`adapter.receive → binding.resolve → gateway.accept → idempotency.claim → session.lock → governance.evaluate → worker.dispatch → runner.invoke → agent/model/tool → data.access → reply.compose → delivery.queue/attempt/result → recovery.reconcile`

每个阶段记录 start/end、outcome、stable error、retryable、role、父子关系；未进入阶段用 `not_applicable`，不得伪造成功。

## Telemetry Pipeline and Security

1. `TelemetryBootstrap` 在进程启动时幂等创建一次 TracerProvider/MeterProvider，并在所有 Runner 实例之前安装。
2. 平台阶段与官方 Runner 使用同一 current context。
3. `SanitizingSpanProcessor` 从只读 span 构造 `SafeSpanEnvelope`：保留 ID/父子/时间/status/scope 和白名单属性，删除输入输出、state、Tool/LLM payload、用户/消息/tenant 原值、URL 和异常文本。
4. `MetricRegistry` 拒绝动态名称和未注册 label；精确 tenant 聚合用 repository scope，不用 OTel label。
5. `OutcomeAwareSamplingPolicy` 在 root 结束后对整棵 trace 作选择；Collector production topology 必须用 trace-affinity 聚合同一 trace。
6. `PriorityTelemetryBuffer` 非阻塞接受安全 envelope，关键保留区防止被普通成功挤占。完整关键空间耗尽时形成固定大小 minimal summary；不写本地磁盘/Redis/PostgreSQL。
7. `OtlpHttpExporterAdapter` 只接收安全 envelope；有限 retry/backoff/jitter 和 timeout。Collector transform/redaction 只作为第二道防线。
8. 安全兼容 gate：禁止修改 OpenTelemetry 私有属性；锁定版本的安全映射契约未通过时，网络 OTLP 默认关闭并阻断本阶段完成声明，而不是原样导出官方 span。

## Health and Alert Semantics

### Role readiness

| Role | Critical dependencies | Non-critical/degraded |
|---|---|---|
| Gateway | PG config/Binding、Redis idempotency/lease、Worker path | telemetry、一个非唯一渠道 |
| Worker | PG Session/Data/Audit、Redis lease/fence、governance、Runner | telemetry、任一 IM SDK |
| Feishu Adapter | credentials、SDK connection、Gateway path、Binding identity | telemetry、WeCom |
| WeCom Adapter | credentials、SDK connection、Gateway path、Binding identity | telemetry、Feishu |
| Recovery/Operator | PG authority、Redis lease/fence | telemetry、单渠道 |

`/health/live` 仅表达进程能否推进；`/health/ready` 决定当前角色是否接收新工作；授权 `/health/status` 给出路径级汇总。过期关键观察为 unknown→unready，不能使用最后一次绿色结果无限延期。

### Alert state machine

```text
INACTIVE → PENDING → FIRING → RECOVERING → RESOLVED
              ▲          └──── condition returns ────┘
```

指纹只使用 rule/severity/role/component/scope digest/stable reason。PG CAS 去重跨节点事件；`notification_id=fingerprint:state_version` 支持至少一次通知。网络结果未知可能物理重复，不宣称 exactly-once。Alert/telemetry 故障不得冒充正式 Audit。

## Release, Rollback and Recovery

### Request pinning

`ConfigurationSnapshot` 聚合 Agent、治理和数据后端的精确版本/摘要/兼容边界。新执行在 Gateway 完成可信 tenant 解析后读取 PG route 并创建 `ExecutionConfigPin`；Runner cache key 纳入 snapshot/contract version。幂等重放、接管和恢复读取原 pin。

### Release state machine

```text
DRAFT → VALIDATED → CANARY → COMPLETED
  │        │           ├─ quality breach ─► PAUSED_QUALITY ─► CANARY | ROLLING_BACK
  │        │           ├─ insufficient ───► PAUSED_INSUFFICIENT_SAMPLE ─► CANARY | ROLLING_BACK
  │        │           └─ hard signal ────► ROLLING_BACK ─► ROLLED_BACK
  └────────┴─ invalid/incompatible ───────► FAILED
ROLLING_BACK ─ invalid last-good ─────────► FAILED_REQUIRES_REPAIR
```

- Cohort 是不可变、有序的 tenant 集合；范围外 tenant 不可见 candidate。
- 最小样本和观察窗口未满足不能推进。
- hard signal 由安全/一致性 enforcement point 持久写入，不能依赖可丢 telemetry；resolver 看到 latch 即选择 last-good。
- release 命令持有 Redis lease/fence，PG 按 expected revision/fence CAS；状态、route、transition event 和 Audit 同事务。Redis 缓存失败不影响 PG 权威。
- 提交前崩溃无变化；提交后响应丢失按 command id 返回原结果；旧节点因 fence 被拒；新节点从 PG 最后 revision 接管。
- rollback 仅切换新执行。在途执行不换 snapshot，但尚未执行的 Tool/外部副作用必须按当前治理 generation 重新授权。已完成副作用、预算、数据事件和 Audit 不回写。

## Error Semantics

| Stable code | Retry | Business effect |
|---|---|---|
| `telemetry_unavailable` | internal bounded | 业务不变，health degraded |
| `telemetry_dropped` | no | 业务不变，分类计数 |
| `telemetry_adapter_incompatible` | operator | 网络出口关闭，不泄露原 span |
| `diagnostic_access_denied` | no | 无诊断数据返回 |
| `health_state_unknown` | later | 关键角色 unready |
| `release_not_authorized` | no | 无发布变化 |
| `release_conflict` | re-read | 无发布变化 |
| `stale_release_fence` | reacquire | 旧节点无写入 |
| `snapshot_invalid` / `snapshot_digest_mismatch` | no | 无 route 变化 |
| `configuration_incompatible` | no | 节点 unready 或 release failed |
| `quality_gate_paused` | human | 停止扩面，现有 cohort 保持 |
| `hard_gate_triggered` | automatic | candidate 新执行转 last-good |
| `rollback_target_unavailable` | repair | route fail closed |
| `release_state_unavailable` | later | 新执行 fail closed，不用缓存/默认值 |
| `drain_timeout` | recovery | 未知任务标记，不自动重放非幂等副作用 |

## Transaction and Persistence Boundaries

1. **Create snapshot**：规范化/digest 在事务外计算；tenant-scoped immutable insert + Audit 同事务。
2. **Resolve new execution**：事务内读取 route/hard latch，校验 snapshot/compatibility，创建或读取唯一 pin；冲突无执行。
3. **Release transition**：锁 release 与目标 route，校验 actor/revision/fence/gates，更新 projection、append transition/rollback decision、写 Audit 后一次提交。
4. **Hard gate**：durable signal/latch + 正式 Audit 同事务；route resolver 立即避开 candidate，controller 幂等完成批量回滚。
5. **Alert transition**：fingerprint + expected state version CAS；通知在提交后，结果未知允许同 notification id 重试。
6. **Telemetry**：不加入业务事务；只处理已脱敏 envelope，失败不回滚业务。

PostgreSQL schema 从 v6 升到 v7：`configuration_snapshots`、`configuration_releases`、`release_targets`、`tenant_config_routes`、`execution_config_pins`、`release_gate_signals`、`release_transition_events`、`alert_incidents`。所有 tenant 表联合键以 tenant 开头；mutable projection 携带 revision/fence。

## Capacity and Deployment Strategy

### Capacity gate

- 固定 manifest：2 tenant × 50 sessions × 10 ordered messages，100 Session 并发，2 Worker，无 sticky session；固定 seed、消息大小桶、重复/Tool/数据读写比例。
- 同机同拓扑同数据初态：warm-up → telemetry off baseline → telemetry on；至少记录 throughput、p50/p95/p99、CPU/内存及 Redis/PostgreSQL 压力。
- 先判正确性零容忍，再判相对性能 10%。环境不等价则 run invalid，不放宽阈值。

### Minimal observable Compose

新增 `deploy/local-observable/` 叠加层：schema-init、Gateway、Worker-A/B、OTel Collector；复用 `deploy/local-shared/compose.yaml` 的 Redis/PostgreSQL。真实双 IM 为显式 profile，不作为无 Secret 的默认验收前提。Collector 故障只使平台 degraded。

### Production recommendation boundary

文档给出 LB + 多 Gateway、每渠道冗余 Adapter、多 Worker、独立 Recovery/Operator、外部 HA Redis/PostgreSQL、两层 Collector（trace-affinity tail sampling）、外部 Secret Provider、滚动发布/排空/备份恢复/扩缩容信号。只作为推荐蓝图，不提交 Kubernetes 资源、不声明生产 HA/SLA。

## Project Structure

### Documentation

```text
specs/008-observability-operations-flow/
├── spec.md
├── clarification-decisions.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── observability-contracts.md
│   └── release-operations-contracts.md
├── deployment-topology.md       # implementation phase
├── risk-register.md             # implementation phase, >= 8 risks
├── capacity-results.md          # implementation/validation phase
├── validation-results.md        # implementation phase
└── tasks.md                     # later generated by $speckit-tasks
```

### Source Code

```text
trpc_service/
├── observability/
│   ├── models.py
│   ├── contracts.py
│   ├── context.py
│   ├── taxonomy.py
│   ├── sampling.py
│   ├── sanitizing.py
│   ├── buffer.py
│   ├── otel.py
│   ├── health.py
│   ├── alerts.py
│   └── service.py
├── operations/
│   ├── models.py
│   ├── contracts.py
│   ├── release.py
│   ├── capacity.py
│   └── drain.py
├── metrics/                     # adapt existing recorder to central registry
├── log/                         # replace raw trace logging with safe envelope
├── gateway/service.py           # correlation, config pin, outer stages
├── worker/service.py            # current context + snapshot-aware Runner cache
├── channels/                    # receive/delivery spans and role health
├── governance/                  # durable hard signals, side-effect reauthorization
├── recovery/                    # pin/release/drain recovery evidence
├── web/app.py                   # health endpoints
├── config/settings.py           # bounded observability/release settings
└── storage/postgres/
    ├── models.py
    ├── operations_repositories.py
    └── migrations/007_observability_operations.sql

deploy/local-observable/
├── compose.yaml
└── otel-collector.yaml

tests/
├── unit/observability/
├── unit/operations/
├── contract/observability/
├── contract/operations/
├── integration/observability/
├── integration/operations/
├── e2e/observability/
├── e2e/operations/
├── performance/
└── security/
```

**Structure Decision**：延续单一 Python 包与现有入口，不建立独立微服务或管理 UI。`observability` 负责可丢诊断和健康/告警，`operations` 负责持久灰度/排空/容量；SQL 实现只在 `storage/postgres`。现有 metrics/log API 通过兼容适配逐步收敛，避免一次性破坏前七阶段。

## Test Strategy

严格测试先行：每个行为先运行目标测试并记录 RED，再最小实现得到 GREEN，随后运行相邻回归与全量回归；命令和结果持续写入 `validation-results.md`。

- **Unit**：taxonomy/allowlist、safe digest、sampling hash/ceiling、buffer 容量/公平/重试、role matrix、alert/release/drain 状态机、capacity comparison。
- **Contract**：InMemory/PostgreSQL release/alert repository 共用 suite；fake/OTLP exporter 共用 safe-envelope contract；official span sanitization 版本锁定契约。
- **Integration**：PG v6→v7 升级、release transaction+Audit、双 controller CAS/fence、hard latch、execution pin、告警接管、telemetry outage。
- **E2E**：双 IM/双 tenant/双 Worker 全阶段 trace；成功/拒绝/重复/冲突/接管/恢复；Collector 中断；hard rollback/quality pause；Worker drain。
- **Security**：预置 Secret、Token、response URL、手机号、邮箱、正文，扫描 trace/metric/log/alert/diagnostic/提交文件为 0；跨 tenant 查询为 0。
- **Capacity**：固定 1,000 消息 A/B，正确性零容忍、throughput/p50/p95/p99 相对退化不超过 10%。
- **Regression**：前七阶段全量 pytest；每个 skip 必须有外部原因，不以 skip 替代 pass。

## Failure Injection and Recovery Matrix

| Injection | Expected state | Recovery evidence |
|---|---|---|
| OTLP/Collector down | business unchanged; platform degraded; bounded drops | exporter恢复、buffer下降、alert resolved |
| PG config/release down | Gateway/Operator unready; new execution fail closed | authority恢复后 route/pin 可读 |
| Redis lease/fence down | affected role unready; no unsafe release/claim | higher fence reacquire, stale writer rejected |
| Audit down | protected write/release/rollback/read fail closed | zero protected side effect, later authorized retry |
| Feishu down | Feishu path unready; WeCom continues; platform degraded | connection ready + recovery alert |
| delivery timeout | existing unknown/retry semantics, trace linked | no duplicate Agent, delivery record reconciled |
| controller crash before/after commit | no partial state / idempotent replay | command id + revision evidence |
| hard gate during telemetry outage | candidate blocked, new requests last-good | durable signal/latch + Audit, no reliance on OTel |
| incompatible mixed node | node unready, no config fallback | supported contract restored/upgraded |
| Worker termination | readiness off then complete/handoff/unknown | generation/fence and drain snapshot |
| clock skew | monotonic durations; wall time flagged | stable reason and operator evidence |

## Phase 1 Post-Design Constitution Check

| Principle | Result | Post-design evidence |
|---|---|---|
| I. Framework-First | PASS | Safe processor consumes official span output; no Runner fork/copy/monkey patch |
| II. Tenant Isolation | PASS | tenant is trusted partition key across diagnostic query, config snapshot/route/pin and alert; metric labels exclude tenant |
| III. Stateless Workers | PASS | all recoverable operations state is shared; only bounded non-authoritative telemetry remains local |
| IV. Contract-First | PASS | two contract documents define ports, states, external health JSON, errors and adapters before tasks/code |
| V. Security by Default | PASS | in-process allowlist precedes network; incompatible sanitizer closes OTLP; Secret references only; route failure closes business |
| VI. Observability | PASS | trace, metrics, logs, health, alerts, delivery, recovery and release evidence have centralized taxonomy and failure ownership |
| VII. Spec-Driven Evidence | PASS | quickstart defines runnable contract/integration/E2E/security/capacity/regression gates and honest scope labels |

**Post-design gate result**：PASS。没有需要 Complexity Tracking 例外的设计；所有 Phase 0 未知均已消除。

## Complexity Tracking

无宪法违规或需要豁免的额外项目层级。新增 `observability` 与 `operations` 是两个责任不同的领域边界：前者是可丢、非阻断诊断；后者是 PostgreSQL 权威的治理控制状态，合并会混淆故障语义。

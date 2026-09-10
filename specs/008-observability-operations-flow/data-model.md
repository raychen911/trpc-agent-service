# Data Model：生产可观测性与运维收敛

**功能**：`008-observability-operations-flow`

**依据**：[spec.md](./spec.md)、[clarification-decisions.md](./clarification-decisions.md)、[research.md](./research.md)

## 1. 模型边界

- PostgreSQL 是配置发布、租户路由、执行配置固定、告警活动状态及运维转换事件的持久权威。
- Redis 只保存 release/alert controller 租约、generation/fence 与可丢缓存，不是发布或告警事实权威。
- OpenTelemetry/OTLP 保存运行诊断，不是业务状态或正式 Audit 的替代品。
- 所有 tenant-scoped 实体必须从可信 Channel Binding/授权上下文取得 tenant，不接受外部消息中的 `tenant_id`。
- Secret、正文、用户原始身份、response URL、完整对象引用和任意异常文本不属于以下可观察实体的允许字段。

## 2. 可信关联与诊断实体

### 2.1 TrustedCorrelationContext（请求内，不持久化）

| 字段 | 类型 | 约束 |
|---|---|---|
| `request_trace_id` | UUID | 平台生成或校验后接受；外部正文/metadata 不得覆盖 |
| `otel_trace_id` | 128-bit | W3C trace context；跨进程传播 |
| `tenant_scope` | TenantScope / PlatformScope | Channel Binding/授权解析后设置；不可空 |
| `trace_digest` | string | `sha256:<16hex>`，普通日志与诊断查询的安全引用 |
| `first_claim_trace_id` | UUID? | 幂等首次 claim 的业务关联 |
| `owner_trace_id` | UUID? | 当前租约所有者关联 |
| `execution_trace_id` | UUID? | 实际执行或恢复轨迹 |
| `node_id` / `role` | enum | 受控节点身份和运行角色 |
| `configuration_snapshot_id` | UUID? | 新执行开始时固定；恢复时复用 |
| `route_generation` | positive int? | 配置路由 generation |

**规则**：`tenant_scope` 未验证时只允许 `PreAuthScope`，不得写 tenant 诊断；一次已开始执行的 snapshot 不随当前 route 改变。

### 2.2 DiagnosticSpan

出口前安全 span 的领域表示，由 `SanitizingSpanProcessor` 构造。

| 字段 | 类型 | 约束 |
|---|---|---|
| `trace_id` / `span_id` / `parent_span_id` | opaque id | 仅受控诊断存储；不得成为 metric label |
| `trace_digest` | string | 对外安全关联引用 |
| `scope_digest` | string | tenant 或 platform 作用域摘要 |
| `component` / `stage` | bounded enum | 集中注册 |
| `start_ns` / `end_ns` | int | `end >= start` |
| `outcome` | enum | `success/rejected/failed/unknown/recovered/not_applicable` |
| `error_type` | bounded enum? | 仅稳定错误分类，不含原始异常文本 |
| `retryable` | bool | 与稳定错误契约一致 |
| `role` / `node_digest` | bounded / digest | 节点原名不进入外部标签 |
| `configuration_version` | positive int? | 适用时填写 |
| `generation` | positive int? | 租约/接管适用时填写 |
| `attributes` | map | 只允许 registry 中的白名单键和值类型 |

**明确排除属性**：`*.input`、`*.output`、`state.*`、`*request*` 原文、`*response*` 原文、Tool 参数/响应、URL、Secret、用户/消息/Session 原始标识。

### 2.3 TelemetryEnvelope

| 字段 | 类型 | 约束 |
|---|---|---|
| `envelope_id` | UUID | 唯一 |
| `signal_type` | enum | `trace/metric/log/critical_summary/drop_counter` |
| `priority` | enum | `critical/normal` |
| `scope_digest` | string | 不得为空 |
| `trace_digest` | string? | 适用时填写 |
| `payload` | safe typed payload | 已通过 allowlist |
| `attempt_count` | 0..3 | 达上限不再重试 |
| `created_at` / `expires_at` | UTC time | 到期丢弃并计数 |

队列总容量默认 10,000；关键保留区不少于 20%。队列不是持久权威，重启可丢失并必须增加分类 drop counter。

### 2.4 CriticalDiagnosticSummary

固定大小、最小脱敏摘要，仅在完整关键 envelope 因出口/缓冲耗尽无法保留时产生。

字段限定为 `trace_digest`、`scope_digest`、`component`、`stage`、`error_type`、`retryable`、`configuration_version`、`occurred_at`。它不能冒充完整 trace 或正式 Audit。

### 2.5 MetricDefinition / MetricObservation

`MetricDefinition` 集中声明：`name`、`description`、`unit`、`instrument_type`、`allowed_label_keys`、每个 label 的有限枚举域。`MetricObservation` 引用定义并携带数值与 labels。

禁止标签：tenant/user/session/message/trace 的原值或 digest、正文、URL、异常文本、对象引用。精确 tenant 汇总由受信任 repository 分区键处理，不进入 OTel labels。

## 3. 健康与告警实体

### 3.1 DependencyObservation

字段：`role`、`dependency`、`state(up/degraded/down/unknown)`、`stable_reason`、`observed_at`、`expires_at`、`latency_bucket`。只允许预定义依赖和原因。

### 3.2 RoleReadinessSnapshot

字段：`node_digest`、`role`、`liveness`、`readiness(ready/unready)`、`service_state(ready/degraded/unready)`、`dependency_states`、`reason_codes`、`changed_at`、`observed_at`。

**角色关键依赖矩阵**：

| 角色 | readiness 关键依赖 | 只导致 degraded 的依赖 |
|---|---|---|
| Gateway | PostgreSQL 配置/Binding、Redis 幂等/租约、至少一个 Worker 路径 | 普通 telemetry、单个非唯一渠道 |
| Worker | PostgreSQL 共享 Session/Data/Audit、Redis lease/fence、治理、Runner 初始化 | 普通 telemetry、任一 IM SDK |
| Feishu Adapter | 有效认证、飞书连接、Gateway 路径、Binding identity | 普通 telemetry、企业微信 |
| WeCom Adapter | 有效认证、企业微信连接、Gateway 路径、Binding identity | 普通 telemetry、飞书 |
| Recovery/Operator | PostgreSQL 权威状态、Redis lease/fence | 普通 telemetry、任一单渠道 |

`liveness` 仅在进程/事件循环不能推进时失败；不得用外部依赖故障触发 liveness 重启风暴。

### 3.3 PlatformHealthSnapshot

字段：`state(ready/degraded/unready)`、`available_paths`、`unavailable_paths`、`role_counts`、`reason_codes`、`generated_at`。只要至少一条安全端到端路径仍可用且权威共享后端满足该路径要求，即可为 `degraded`；无完整路径或权威状态不可安全访问为 `unready`。

### 3.4 AlertIncident（PostgreSQL）

| 字段 | 类型 | 约束 |
|---|---|---|
| `incident_id` | UUID | 唯一 |
| `fingerprint` | digest | `rule_id + severity + role + component + scope_digest + stable_reason` |
| `rule_id` / `severity` | bounded enum | 规则注册表定义 |
| `scope_digest` | string | tenant/platform 摘要；不含原值 |
| `state` | enum | `pending/firing/recovering/resolved` |
| `first_observed_at` / `last_observed_at` | UTC time | 单调 |
| `state_version` | positive int | CAS 更新 |
| `occurrence_count` | non-negative int | 重复合并 |
| `evidence_digest` | string? | 安全证据引用 |
| `last_notification_id` | string? | `fingerprint:state_version` |
| `resolved_at` | UTC time? | 仅 resolved |

状态机：

```text
INACTIVE → PENDING → FIRING → RECOVERING → RESOLVED
              ▲          │          │
              └──────────┴─ condition returns
```

触发持续窗口未满足时保持 PENDING；恢复稳定窗口未满足时保持 RECOVERING。多节点相同 fingerprint/state version 只生成一个逻辑通知，外部通知是至少一次而非物理 exactly-once。

## 4. 配置发布实体

### 4.1 ConfigurationSnapshot（PostgreSQL，不可变）

| 字段 | 类型 | 约束 |
|---|---|---|
| `snapshot_id` | UUID | 唯一 |
| `tenant_id` | string | 可信作用域；联合索引第一列 |
| `sequence` | positive int | tenant 内递增 |
| `contract_version` / `min_runtime_contract` | string | 节点兼容门禁 |
| `agent_config_ref` | versioned ref | 指向精确 Agent 版本 |
| `governance_policy_ref` | versioned ref | 指向精确治理版本 |
| `data_backend_profile_ref` | versioned ref | 指向精确数据配置 |
| `payload_digest` | digest | 规范化配置摘要 |
| `change_summary` | bounded text | 不含 Secret/正文 |
| `created_by_digest` | digest | 授权主体摘要 |
| `created_at` | UTC time | 创建后不可修改 |

Secret 只能以 `secret_ref` 间接引用，不得成为 snapshot payload 明文。

### 4.2 CanaryRelease（PostgreSQL）

字段：`release_id`、`candidate_snapshot_id`、`rollback_snapshot_id`、不可变 `cohorts`、`observation_window`、`minimum_sample`、版本化 `quality_gates`、`hard_gate_types`、`state`、`revision`、`owner_fence_generation`、`created_by_digest`、时间戳。

状态机：

```text
DRAFT → VALIDATED → CANARY → COMPLETED
  │        │           ├─ quality breach ─► PAUSED_QUALITY ─► CANARY | ROLLING_BACK
  │        │           ├─ insufficient ───► PAUSED_INSUFFICIENT_SAMPLE ─► CANARY | ROLLING_BACK
  │        │           └─ hard gate ──────► ROLLING_BACK ─► ROLLED_BACK
  └────────┴─ invalid/incompatible ───────► FAILED

ROLLING_BACK ─ rollback target invalid ─► FAILED_REQUIRES_REPAIR
```

所有转换要求 `expected_revision` 与不低于已见值的 release fence。重复 command id 返回原结果；非法转换无副作用。

### 4.3 TenantConfigRoute（PostgreSQL，当前新请求权威）

字段：`tenant_id(PK)`、`stable_snapshot_id`、`candidate_snapshot_id?`、`release_id?`、`route_generation`、`owner_fence_generation`、`hard_gate_latched`、`updated_at`。

范围外 tenant 不得出现本 release 的 candidate。Redis 缓存丢失时重读 PostgreSQL；PostgreSQL 不可用时 fail closed，不回退进程默认配置。

### 4.4 ExecutionConfigPin（PostgreSQL，不可变）

字段：`tenant_id`、`idempotency_key_digest`、`content_fingerprint`、`snapshot_id`、`route_generation`、`release_id?`、`created_at`。联合唯一约束确保重复/接管读取同一 snapshot；同 key 不同 fingerprint 为幂等冲突。

### 4.5 ReleaseGateSignal / ReleaseTransitionEvent

- `ReleaseGateSignal`：append-only；字段含 tenant/release、`signal_digest`、`gate_type`、`severity(hard/quality)`、观察窗口/样本、受控值、证据摘要和时间。`signal_digest` 唯一去重。
- `ReleaseTransitionEvent`：append-only；记录 `command_id`、from/to state、revision/fence、actor digest、reason code、evidence digest 和时间，并与正式 Audit 同事务。

### 4.6 RollbackDecision

字段：`decision_id`、`release_id`、`command_id`、`actor_digest(system/authorized-human)`、`reason_code`、`target_snapshot_id`、`affected_tenant_count`、`from_revision`、`to_revision`、`created_at`。不删除已完成业务副作用、预算、Data Event 或 Audit。

## 5. 容量、排空与验收实体

### 5.1 CapacityScenario（不可变文档/fixture）

字段：`scenario_version`、tenant 数、Worker 数、并发 Session 数、每 Session 消息数、消息大小桶、重复率、Tool 比例、数据读写比例、固定 seed、warm-up 和测量轮次。

正式场景固定为 2 tenant、2 Worker、100 Session、每 Session 10 条有序消息，共 1,000 条。

### 5.2 CapacityRun / CapacityComparison

`CapacityRun` 保存非敏感环境指纹、场景版本、telemetry mode、开始结束、成功/拒绝/错误、丢失/跨租户/不可解释重复、throughput、p50/p95/p99、CPU/内存/Redis/PostgreSQL 峰值和瓶颈判断。`CapacityComparison` 关联 baseline/enabled run，计算 throughput 与各分位增幅。

正确性任一计数非零即失败；吞吐下降或任一分位延迟增幅超过 10% 即失败。产物必须标记 `local_evidence`，不得称为生产 SLA。

### 5.3 DrainSnapshot

字段：`node_digest`、`role`、`state(accepting/draining/drained/timed_out)`、`deadline`、`inflight_count`、`completed_count`、`handed_off_count`、`unknown_count`、`started_at`、`completed_at?`。状态只能前进，进入 draining 后不得接受新 claim。

## 6. PostgreSQL schema 007 规划

新增 `007_observability_operations.sql`，至少包含：

- `configuration_snapshots`
- `configuration_releases`
- `release_targets`
- `tenant_config_routes`
- `execution_config_pins`
- `release_gate_signals`
- `release_transition_events`
- `alert_incidents`

所有 tenant 表以 `tenant_id` 为联合键首列并设置 tenant-scoped unique/index；不可变表禁止 update 业务路径；mutable projection 使用 `revision/version + fence_generation` CAS。正式发布转换、route 更新、transition event 与 Audit 在同一 PostgreSQL事务。

容量报告和风险清单是版本化验收文件，不进入业务数据库。Telemetry queue 不建表。

## 7. 稳定错误语义

| Code | Retryable | 业务副作用 | 含义 |
|---|---:|---|---|
| `telemetry_unavailable` | internal retry | 业务不变 | 普通出口故障，进入有界降级 |
| `telemetry_dropped` | no | 业务不变 | envelope 过期/溢出，已计数 |
| `diagnostic_access_denied` | no | none | 查询 scope 未授权 |
| `health_state_unknown` | later | none | 依赖快照过期，不能伪造成 ready |
| `release_not_found` | no | none | release 不存在于可信 scope |
| `release_not_authorized` | no | none | 操作者无权发布/回滚 |
| `release_conflict` | re-read | none | revision/CAS 冲突 |
| `stale_release_fence` | reacquire | none | 旧 controller 被 fencing 拒绝 |
| `snapshot_invalid` | no | none | 快照字段或引用不完整 |
| `snapshot_digest_mismatch` | no | none | 规范化摘要不符 |
| `configuration_incompatible` | no | none | 节点/配置契约不兼容 |
| `quality_gate_paused` | human | none | 质量门槛越线后停止扩面 |
| `hard_gate_triggered` | automatic rollback | no new candidate work | 零容忍门槛已锁存 |
| `rollback_target_unavailable` | operator repair | route fail closed | last-good 无法证明可用 |
| `release_state_unavailable` | later | none | PostgreSQL 权威不可读，禁止回退默认配置 |
| `drain_timeout` | recovery | unknown work marked | 排空截止仍有在途任务 |

外部错误只返回稳定 code、retryable、safe trace reference，不返回 SQL、DSN、Secret、正文、原始 tenant 或供应商异常。

# Observability Contracts

**功能**：`008-observability-operations-flow`

**适用范围**：追踪、指标、日志、健康、告警和诊断查询

## 1. 通用约束

- 所有端口均为异步或显式非阻塞；普通 telemetry 异常不得穿透到业务调用方。
- 所有记录先通过中央枚举注册、字段 allowlist、大小限制和 Secret/原文检测，再进入 exporter。
- 正式 Audit Repository 不实现本契约，仍保持既有 fail-closed 语义。
- tenant scope 只能由可信上下文提供；PreAuth 事件只能使用 platform/channel identity digest。

## 2. CorrelationContextPort

| 操作 | 输入 | 输出 | 失败语义 |
|---|---|---|---|
| `start_root` | 已验证/重建的 request trace、role、node | TrustedCorrelationContext | 非法外部 trace 时生成新值，不接受正文覆盖 |
| `bind_tenant` | PreAuth context、VerifiedTenantContext | tenant-scoped context | scope 不匹配时 `tenant_scope_invalid` |
| `link_attempt` | first/owner/execution trace、generation | 新不可变 context | 只追加可信关联，不改根 request |
| `inject` | context、内部 transport carrier | W3C carrier | 仅内部 transport 可调用 |
| `extract` | 内部 carrier、验证过的 scope | child context | 无效 carrier 建新 root 并记录稳定原因 |

## 3. TelemetryRecorderPort

### 3.1 操作

- `start_stage(context, component, stage, attributes) -> StageHandle`
- `finish_stage(handle, outcome, error_type=None, retryable=False) -> None`
- `record_metric(scope, definition_name, value, labels) -> None`
- `record_operational(scope, event) -> None`
- `flush(deadline) -> FlushResult`
- `shutdown(deadline) -> ShutdownResult`

### 3.2 结果与失败

`finish_stage` 必须闭合阶段并保留父子关系；不适用阶段显式记录 `not_applicable`。任一 exporter/buffer 失败只更新 telemetry health/drop counter，不抛出业务异常。未知 label、越界值或未脱敏 payload 在进程边界拒绝并形成安全 operational error。

## 4. SanitizingSpanProcessor / TelemetryExporterPort

`SanitizingSpanProcessor` 读取官方/平台 span 的公开只读属性，映射为 `DiagnosticSpan`。它必须：

1. 保留 trace/span/parent id、时间、status、instrumentation scope。
2. 只保留中央 registry 明确允许的属性。
3. 丢弃所有 events/links 中非白名单属性及任何原始内容。
4. 不修改 `ReadableSpan` 私有字段，不把原始 span 直接传给网络 exporter。
5. 对不支持的 SDK 形状 fail closed：拒绝网络导出并报告 `telemetry_adapter_incompatible`。

`TelemetryExporterPort.export(batch)` 只接受已验证的 `SafeSpanEnvelope/MetricEnvelope/OperationalEnvelope`。结果为 `success | retryable_failure | permanent_failure`；异常必须折叠为稳定结果。

## 5. SamplingPolicyPort

输入：完成态 trace 分类、平台默认成功率、tenant override、平台 ceiling、sampling config version。输出：`keep_full | keep_minimal | drop_normal` 及 reason。

规则：

- `critical` 分类永远 `keep_full`，不能被 tenant override 降低。
- 普通成功按 trace + scope digest + config version 的稳定 hash 决策。
- tenant override 必须夹在 0 和平台 ceiling 内；非法值拒绝配置而非静默扩大。
- 正常出口下关键 full trace 保留率 100%；长期出口故障超出有界容量时才允许 `keep_minimal`，并增加 `critical_full_dropped_total`。

## 6. TelemetryBufferPort

| 操作 | 行为 |
|---|---|
| `offer(envelope)` | 非阻塞；关键使用保留区，普通满时淘汰最旧普通项 |
| `take_batch(limit)` | 关键优先且防止普通永久饥饿 |
| `ack(ids)` | 删除已成功导出项 |
| `retry(ids, retry_after)` | 最多 3 次并受 envelope TTL 限制 |
| `drop(ids, reason)` | 分类计数；不得输出 payload |
| `snapshot()` | 只返回容量、按类型数量、drop counters 和 oldest age |

实现必须为有界内存；禁止把 envelope 原文写入 PostgreSQL、Redis 或本地文件。

## 7. MetricsRegistryContract

每个定义必须固定 name、unit、instrument type、description、allowed labels 和 label domain。禁止动态 metric name。建议核心名称：

| Name | Type/Unit | Labels |
|---|---|---|
| `trpc.requests` | counter `{request}` | `channel,role,outcome` |
| `trpc.stage.duration` | histogram `ms` | `component,stage,outcome` |
| `trpc.runner.duration` | histogram `ms` | `outcome` |
| `trpc.tool.duration` | histogram `ms` | `tool_class,outcome` |
| `trpc.channel.delivery` | counter `{attempt}` | `channel,outcome` |
| `trpc.state.operation.duration` | histogram `ms` | `backend_type,operation,outcome` |
| `trpc.recovery` | counter `{operation}` | `kind,outcome` |
| `trpc.telemetry.dropped` | counter `{envelope}` | `signal_type,priority,reason` |
| `trpc.release.transition` | counter `{transition}` | `from_state,to_state,reason` |

真实模型 Token/成本在本阶段确定性 Runner 中必须标记 `not_applicable`，不得伪造数值。

## 8. HealthProbePort 与 HTTP 契约

### 8.1 Probe port

- `probe_liveness() -> LiveSnapshot`
- `probe_dependency(role, dependency) -> DependencyObservation`
- `evaluate_role(role, observations) -> RoleReadinessSnapshot`
- `aggregate(role_snapshots) -> PlatformHealthSnapshot`

探针有独立短超时、不得暴露底层异常文本。过期观察视为 `unknown`，关键依赖 unknown 导致角色 unready。

### 8.2 Endpoints

`GET /health/live`

```json
{"status":"live","role":"worker","observed_at":"<UTC>"}
```

仅在进程不可推进时返回非 200。

`GET /health/ready`

```json
{
  "status":"ready|unready",
  "service_state":"ready|degraded|unready",
  "role":"gateway",
  "reasons":["telemetry_unavailable"],
  "observed_at":"<UTC>"
}
```

ready 返回 200，unready 返回 503。不得返回 DSN、host、Secret 或 tenant 原值。

`GET /health/status` 仅供授权运维入口，返回各角色计数、可用路径、不可用路径和稳定 reason code；未授权为 403 并形成最小 Audit。

## 9. AlertRepository / AlertNotifierPort

`AlertRepository.observe(fingerprint, condition, expected_version)` 按状态机 CAS，返回 `no_change | transitioned`。跨节点相同 fingerprint 只有一个逻辑 state version。

`AlertNotifierPort.notify(notification)` 使用稳定 `notification_id=fingerprint:state_version`。返回 `sent | retryable_unknown | permanent_failure`；只保证至少一次尝试，不承诺物理 exactly-once。通知只含影响范围摘要、时间、stable reason、safe evidence 和建议动作。

## 10. DiagnosticQueryPort

输入必须含授权主体、可信 TenantScope/PlatformScope、时间窗、可选 component/outcome/trace digest。查询步骤：先授权并写最小 access Audit，再访问诊断存储。tenant 调用只能返回同 scope 数据；平台 scope 需要显式运维权限。

返回：安全 trace reference、阶段图、结果、稳定错误、节点角色、配置版本、generation 与证据完整性状态。若 telemetry outage 导致仅有摘要，必须标记 `partial_telemetry`，不得伪造成完整 trace。

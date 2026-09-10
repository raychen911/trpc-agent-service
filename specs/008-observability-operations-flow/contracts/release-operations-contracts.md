# Release and Operations Contracts

**功能**：`008-observability-operations-flow`

**适用范围**：配置快照、租户灰度、回滚、跨节点恢复、容量与排空

## 1. ConfigurationSnapshotRepository

- `create(scope, snapshot, audit_context) -> ConfigurationSnapshot`
- `get(scope, snapshot_id) -> ConfigurationSnapshot | None`
- `verify_compatible(scope, snapshot_id, runtime_capabilities) -> CompatibilityResult`

`create` 只允许不可变插入；相同 tenant/id/digest 返回原值，不同 digest 冲突。Secret 只允许 `secret_ref`。scope、digest、兼容性或 Audit 不可验证时无写入。

## 2. TenantConfigRouteRepository

- `resolve_for_new_execution(scope, idempotency_key, content_fingerprint) -> ExecutionConfigPin`
- `get_route(scope) -> TenantConfigRoute`
- `compare_and_route(scope, expected_generation, owner_fence, target_snapshot, release_id, hard_latch) -> TenantConfigRoute`

`resolve_for_new_execution` 必须在同一 tenant scope 内读取权威 route 并创建/读取不可变 pin。重复 key+fingerprint 返回相同 pin；不同 fingerprint 为冲突。PostgreSQL 不可用、route 缺失、禁用、摘要不符或不兼容时 fail closed，绝不回退进程默认配置或 Redis 缓存。

## 3. ReleaseRepository / ReleaseCoordinator

### 3.1 命令

| Command | 必要输入 | 正常结果 |
|---|---|---|
| `create_release` | candidate/rollback snapshot、cohorts、gates、actor | DRAFT |
| `validate` | command id、expected revision、fence | VALIDATED 或 FAILED |
| `start_canary` | 首 cohort、观察窗、最小样本 | CANARY |
| `advance` | 下一 cohort、gate evidence | CANARY/COMPLETED |
| `pause` | quality/insufficient reason | PAUSED_* |
| `resume` | 授权主体、fresh evidence | CANARY |
| `rollback` | system hard gate 或授权主体 | ROLLING_BACK/ROLLED_BACK |
| `repair` | 授权主体、兼容 last-good | 从 FAILED_REQUIRES_REPAIR 恢复 |

所有命令携带 tenant/release scope、`command_id`、`expected_revision`、`owner_fence_generation`、actor digest 和 safe evidence digest。

### 3.2 原子边界

单次转换在 PostgreSQL 事务内锁定 release 和目标 route rows，验证权限、scope、snapshot digest/compatibility、revision/fence，然后一起提交：release state、tenant routes、append-only transition events、RollbackDecision（适用时）和正式 Audit。Audit 失败整体回滚。

提交后才刷新 Redis 缓存和发送告警；Redis/通知失败不反向修改 SQL 权威。

### 3.3 幂等和 fencing

- 相同 `command_id` 重试返回首次提交结果，不新增转换/Audit。
- revision 过期为 `release_conflict`；旧 fence 为 `stale_release_fence`。
- controller 崩溃后，新节点获取更高 fence 并从最后 committed revision 恢复。
- 旧节点即使持有缓存也不能修改 route 或 release。

## 4. GateEvaluationPort

输入：release immutable gate policy、window observations、durable hard signals。输出：

- `HARD_STOP`：任一跨租户、未授权副作用、数据一致性或配置不兼容 signal 首次出现；立即 latch，阻止所有候选新请求并自动回滚。
- `QUALITY_PAUSE`：达到 observation window + minimum sample 后，错误率/延迟等任一版本化阈值越线；只暂停扩面。
- `INSUFFICIENT_SAMPLE`：窗口结束仍未达到样本；禁止静默推进。
- `PASS`：所有门槛满足，允许 CAS 推进。

硬 signal 必须由安全 enforcement point 持久产生，不能只来自可采样/可丢弃 telemetry。

## 5. 在途请求与副作用

- Gateway 在新执行开始时固定 `ExecutionConfigPin`，Worker、Runner、数据访问、恢复与 Audit 全程携带同一 snapshot id。
- 回滚只改变之后的新执行；不得让已开始请求中途混用新旧快照。
- Tool/外部副作用真正执行前必须重新读取当前权威治理/安全 generation。若旧确认失效则重新确认或拒绝；无法证明安全时进入既有拒绝/outcome-unknown/recovery。
- 已完成副作用、预算事实、Session/Data Event 与 Audit 不得因回滚而删除或改写。

## 6. Stable Errors

| Code | HTTP/CLI 类别 | Retry | 说明 |
|---|---|---:|---|
| `release_not_found` | 404/not_found | no | scope 内不存在 |
| `release_not_authorized` | 403/denied | no | 主体无权限 |
| `release_conflict` | 409/conflict | re-read | revision/CAS 冲突 |
| `stale_release_fence` | 409/conflict | reacquire | 旧 controller |
| `snapshot_invalid` | 422/invalid | no | 字段/引用缺失 |
| `snapshot_digest_mismatch` | 409/conflict | no | 摘要不符 |
| `configuration_incompatible` | 409/incompatible | no | runtime 不支持 |
| `quality_gate_paused` | 409/paused | human | 质量门槛暂停 |
| `hard_gate_triggered` | 409/rolled_back | automatic | 零容忍门槛 |
| `rollback_target_unavailable` | 503/repair | operator | last-good 不可证明 |
| `release_state_unavailable` | 503/unavailable | later | SQL 权威不可读 |

错误 envelope 只含 code、retryable、safe trace reference；不包含数据库、Secret、tenant 原值和异常文本。

## 7. CapacityHarnessPort

- `prepare(scenario, environment_fingerprint) -> PreparedRun`
- `run(prepared, telemetry_mode) -> CapacityRun`
- `compare(baseline, enabled) -> CapacityComparison`

正式 scenario 必须固定 2 tenant、2 Worker、100 concurrent Session、10 messages/Session、总计 1,000 条，并固定 seed/重复率/Tool 比例/数据读写比例。baseline 与 enabled 使用同机、同拓扑、同初态和同一 workload manifest。

判定顺序：先检查零丢失、零跨租户、零不可解释重复；再检查 throughput 下降和 p50/p95/p99 增幅均不超过 10%。环境不等价时标记 invalid，不得修改阈值强行通过。

## 8. DrainControllerPort

- `begin(deadline)`：原子撤销 readiness，停止新 claim/接收。
- `snapshot()`：返回在途、完成、移交、未知计数。
- `complete_or_handoff()`：已有幂等执行可完成或借助 lease/fencing 移交。
- `expire()`：deadline 后将不能证明结果的任务标为 outcome unknown，禁止自动重放非幂等副作用。

状态只能 `accepting → draining → drained|timed_out`，重复 SIGTERM/stop 命令幂等。

## 9. 最小部署契约

`deploy/local-observable/` 必须提供 PostgreSQL、Redis、schema-init、Gateway、Worker-A、Worker-B、OpenTelemetry Collector；真实飞书/企业微信 Adapter 放在显式 profile 中。核心服务 healthcheck 使用 `/health/ready`，Collector 故障只使平台 degraded。

从干净、唯一 Compose project name 启动与删除测试卷；任何 `down -v` 前必须显式展示并确认目标 project，不能使用宽泛路径/变量。Secret 仅通过当前进程或受控 provider 注入，不写入 Compose、Git 或 validation 输出。

## 持久化命名约定（T091 一致性收口）

- 本文档与 `data-model.md` 中的状态机使用逻辑大写名；代码与数据库中以
  lower_snake_case 持久化，一一对应：`DRAFT→draft`、`VALIDATED→validated`、
  `CANARY→canary`、`PAUSED_QUALITY→paused_quality`、
  `PAUSED_INSUFFICIENT_SAMPLE→paused_insufficient_sample`、
  `ROLLING_BACK→rolling_back`、`ROLLED_BACK→rolled_back`、
  `COMPLETED→completed`、`FAILED→failed`、
  `FAILED_REQUIRES_REPAIR→failed_requires_repair`。
- 原因码以代码为准：`quality_gate_paused`（本文档逻辑名）对应持久化原因码
  `quality_threshold_breached`；样本不足为 `insufficient_sample`；硬门槛为
  `hard_gate_triggered`；授权恢复为 `authorized_resume`；回滚完成为
  `rollback_completed`；门槛通过推进为 `gates_passed`；操作者发起为
  `operator_requested`。

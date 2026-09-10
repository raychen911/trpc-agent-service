# 生产风险登记（第八阶段，FR-030，SC-010/SC-011）

每项含触发条件、影响范围、检测信号、预防措施、处置步骤、恢复验证和剩余风险。演练证据引用 `validation-results.md` 与 `tests/e2e/operations/test_fault_exercise.py`。

## R1 — PostgreSQL 权威不可用

- 触发：数据库宕机、网络分区、连接池耗尽
- 影响：新执行 fail closed（configuration_unavailable），发布命令不可执行；在途执行按原 pin 继续
- 检测：角色 readiness `postgres_authoritative` unready；`release_state_unavailable` 告警
- 预防：HA PostgreSQL、连接池上限、迁移 forward-only
- 处置：切换主备；恢复后 `ReleaseCoordinator` 从最后 committed revision 接管
- 恢复验证：T055（crash/fence 测试）、`exercise_postgres_authority_down`
- 剩余风险：RPO>0 时未提交命令丢失（command_id 幂等可安全重放）

## R2 — Redis lease/fence 不可用

- 触发：Redis 宕机、网络分区
- 影响：受影响角色 unready；幂等 claim 与锁不可用
- 检测：`redis_lease_fence` readiness unready
- 预防：HA Redis、租约 TTL 有界
- 处置：failover；fence 单调递增保证陈旧写者被拒（StaleReleaseFence）
- 恢复验证：T044 依赖故障矩阵、T055 fence 测试
- 剩余风险：failover 期间短暂 claim 拒绝

## R3 — OTLP/Collector 遥测出口故障

- 触发：Collector 宕机、OTLP 网络故障
- 影响：平台 degraded；缓冲有界、drop counter 可见；业务零影响
- 检测：`telemetry_unavailable` 告警、exporter health degraded
- 预防：两层 Collector、缓冲容量+关键保留区、tail sampling 网络侧第二道防线
- 处置：恢复 Collector；恢复后 flush 有界缓冲
- 恢复验证：T072（exporter_outage）、`exercise_collector_down`
- 剩余风险：缓冲耗尽后普通成功样本丢失（关键遥测有 CriticalDiagnosticSummary 兜底）

## R4 — 配置发布硬门槛误触发/漏触发

- 触发：跨租户泄露、未授权副作用、数据一致性、配置不兼容信号
- 影响：cohort 租户新请求回退 last-good，自动回滚；误触发造成发布中断
- 检测：`hard_gate_triggered`（resolution=automatic_rollback）、RollbackDecision
- 预防：硬信号必须来自持久 enforcement point（evidence_digest 强制），telemetry 硬信号不 latch
- 处置：核对 RollbackDecision 与 journal；修复后新 release
- 恢复验证：T056（hard_gate_rollback）、`exercise_telemetry_outage_hard_gate`
- 剩余风险：enforcement point 本身漏报（需独立审计对账）

## R5 — Worker 终止导致在途执行丢失

- 触发：SIGTERM、崩溃、编排驱逐
- 影响：在途执行完成/高 fence 接管/标记 unknown；禁止自动重放非幂等副作用
- 检测：drain snapshot（inflight/completed/handed_off/unknown）
- 预防：优雅排空 deadline、lease TTL、fence 单调
- 处置：unknown 执行进入人工对账（conflict review）
- 恢复验证：T069/T070、`exercise_worker_termination`
- 剩余风险：unknown 结果需要人工确认

## R6 — 时钟偏移导致时序判断错误

- 触发：NTP 偏移、容器时钟漂移
- 影响：窗口/截止时间计算偏差
- 检测：wall-time anomaly 标记
- 预防：时长一律用单调时钟（monotonic），wall-time 异常显式标记
- 处置：校准 NTP；按标记重算窗口
- 恢复验证：`exercise_clock_skew`
- 剩余风险：跨节点 wall-time 比较仍受偏移影响（设计已避免依赖）

## R7 — 密钥/DSN 泄露进入日志或遥测

- 触发：错误信息带 backend 细节、span 属性带敏感值
- 影响：凭据泄露
- 检测：sentinel 扫描测试（7 类标记零命中）
- 预防：脱敏处理器白名单 20 键、stable error 只渲染 code、secret_ref-only 快照
- 处置：轮换凭据；清除泄露材料
- 恢复验证：T036（no_sensitive_leak）、T087 安全门禁
- 剩余风险：新增属性键需同步白名单

## R8 — 容量回归（遥测开销超阈值）

- 触发：遥测处理路径性能退化
- 影响：吞吐/p50/p95/p99 超 10% 相对增幅
- 检测：双门禁容量报告（correctness 零容忍 + overhead ≤10%）
- 预防：正式场景固定 2/2/100/10=1000、环境指纹等价校验（不等价 invalid）
- 处置：定位热点；恢复后重跑正式门禁
- 恢复验证：T067/T068（capacity-results.md）
- 剩余风险：本地证据不代表生产绝对值（报告已声明边界）

## R9 — 共享后端迁移版本不一致

- 触发：部分节点 v6、权威 v7
- 影响：schema gate 拒绝启动（fail closed）
- 检测：SUPPORTED_SCHEMA_VERSION=7 校验
- 预防：forward-only 迁移（IF NOT EXISTS、无 DROP）、schema-init 先行
- 处置：跑 schema-init 到 v7 后重启
- 恢复验证：T011（schema_v7_upgrade）
- 剩余风险：跨版本滚动发布窗口内需保持向后兼容

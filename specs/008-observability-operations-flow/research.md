# Phase 0 Research：生产可观测性与运维收敛

**功能**：`008-observability-operations-flow`

**日期**：2026-09-10

**输入**：[spec.md](./spec.md)、[clarification-decisions.md](./clarification-decisions.md)、项目宪法与现有实现

## 研究结论概览

本阶段不替换 tRPC-Agent Runner，也不建立第二套追踪协议。平台采用 OpenTelemetry API/SDK 与 OTLP 作为供应商中立出口，直接接入官方 Runner 已产生的 `trpc.python.agent` span 和 `gen_ai.*` metric；平台只负责可信上下文、外层业务阶段、脱敏、采样策略、健康、告警、发布与验收。

## R-001 官方 Runner 遥测复用边界

- **Decision**：保留 `trpc-agent-py==1.1.19` 的官方 Runner/Agent/LLM/Tool OpenTelemetry instrumentation；Gateway 在调用 Runner 前激活平台根上下文，官方 span 自动成为其子 span。平台不得复制或修改官方执行循环。
- **Rationale**：已安装版本通过 `trpc_agent_sdk.telemetry` 使用 OpenTelemetry tracer/meter，并在 Runner、Agent、模型与 Tool 层记录标准 `gen_ai.*` 属性。复用同一 Provider 可以形成一棵完整 trace，同时遵守 framework-first 宪法。
- **Alternatives considered**：复制官方遥测代码会产生版本漂移；只从 Audit 推断 Runner 阶段无法得到准确耗时和父子关系；Monkey patch SDK 会形成脆弱的私有接口依赖。

## R-002 标准遥测栈与依赖声明

- **Decision**：平台直接声明与锁文件兼容的 `opentelemetry-api`、`opentelemetry-sdk` 和 `opentelemetry-exporter-otlp-proto-http` 依赖；生产出口使用 OTLP/HTTP，自动化使用 InMemory exporter/receiver 替身。最小可观察部署增加可选 OpenTelemetry Collector 和 debug exporter，不绑定商业监控产品。
- **Rationale**：服务要直接 import SDK/Exporter，不能依赖 `trpc-agent-py` 的传递依赖。OTLP 保持供应商中立，Collector 提供 receiver→processor→exporter 的标准流水线并能在本地输出可检查证据。
- **Alternatives considered**：只写 JSON 日志不足以表达跨阶段父子关系；直接绑定 Prometheus/Jaeger/商业 SDK 会把实现与后端耦合；依赖传递包没有兼容承诺。

## R-003 完整轨迹与比例采样

- **Decision**：应用侧使用 AlwaysOn 记录完整 span，上报前执行结果感知的 tail policy：关键分类（错误、安全拒绝、恢复、跨租户尝试、结果未知）100% 保留，普通成功按稳定 trace hash 做确定性比例采样。默认成功采样率为 10%，租户可调范围为 0%～25%；自动化可显式设为 100%。Collector 配置使用相同策略语义。
- **Rationale**：关键性只有在请求结束时才能可靠判定，head sampler 不能把起初未采样的请求在失败后升级为完整 trace。稳定 hash 使同一 trace 的所有 span 作出一致决定。平台上限防止单租户扩大成本。
- **Alternatives considered**：入口 head sampling 会丢后发错误；所有请求 100% 保留成本无界；只留错误摘要不满足正常出口下关键异常完整诊断轨迹要求。
- **Operational note**：生产多副本 tail-sampling Collector 必须保证同一 trace 的 span 汇聚到同一有状态采样实例，例如使用前置 load-balancing Collector。长时间出口中断超过有界缓冲容量时，不谎称完整轨迹仍在；按 DEC-002 降级为关键最小摘要并记录丢弃。

## R-004 可信关联与保密字段

- **Decision**：内部 `TrustedCorrelationContext` 同时保存 W3C trace context、平台 `request_trace_id`、首次 claim/owner/execution 关联和可信 tenant scope。对外诊断与普通日志使用 `sha256:<16hex>` 安全关联引用；完整 trace ID 只在受控 OTel 上下文、授权诊断查询和正式 Audit 中使用，不进入指标标签。
- **Rationale**：既要关联原始处理、重复命中、接管和恢复，又不能把用户、消息、tenant、URL 或正文变成高基数/敏感标签。可信上下文只能由已验证 Channel Binding 或本地签名入口建立。
- **Alternatives considered**：接受外部 tenant/trace metadata 会破坏隔离；把所有 UUID 放入 metric label 会造成无界基数；只保留 digest 而丢弃内部 trace context 会切断分布式父子关系。

## R-005 指标与租户视图

- **Decision**：集中定义 `MetricDefinition` 注册表，导出标签仅允许 role、component、stage、outcome、error_type、channel、backend_type、operation 等有界枚举。精确 tenant 维度作为受信任 repository 分区键，不作为 OTel metric label；授权租户视图由 tenant-scoped 聚合查询生成。
- **Rationale**：这同时满足租户级诊断和 FR-006 的标签有界约束。现有 `MetricsRecorder` 可演进为适配层，旧测试继续共享契约。
- **Alternatives considered**：tenant digest 虽脱敏但仍是高基数，不能作为通用 label；取消租户视图无法满足隔离运营；为每租户建立独立 MeterProvider 会造成资源爆炸。

## R-006 遥测出口故障隔离

- **Decision**：在业务事件与 OTLP exporter 之间设置进程内有界双优先级缓冲。默认总容量 10,000 envelope，其中至少 20% 为关键事件保留；单批有限重试 3 次，指数退避带抖动，总导出等待不进入业务请求等待链。普通成功最先丢弃，关键完整轨迹空间耗尽后仅保留固定大小最小脱敏摘要和分类 drop counter。
- **Rationale**：上限使内存可预测，保留区避免正常流量挤掉关键事件；有限重试与熔断避免阻塞事件循环。容量和重试参数集中配置并有硬上限。
- **Alternatives considered**：无限队列会导致 OOM；本地磁盘缓冲可能把敏感字段落盘；同步导出会改变业务时延和终态；完全静默丢弃无法运营。

## R-007 角色就绪、平台汇总与安全停机

- **Decision**：以 `RoleDependencyMatrix` 计算 Gateway、Worker、Feishu Adapter、WeCom Adapter 和 Recovery/Operator 的 readiness；liveness 只代表事件循环存活。实例先将 readiness 置为 false，再停止新 claim/消息接收，在有界 drain deadline 内完成、移交或标记在途任务。平台状态按可安全完成的路径聚合为 `ready | degraded | unready`。
- **Rationale**：权威配置、幂等、Session、fencing、治理、Audit、Runner 或渠道认证的缺失对不同角色含义不同。排空顺序可避免在终止时制造新的未知结果。
- **Alternatives considered**：单一 `/health` 掩盖局部故障；把普通 telemetry 设为关键依赖会因监控故障停止业务；直接杀进程会破坏在途语义。

## R-008 告警状态与去重

- **Decision**：告警规则只消费有界分类事件和健康快照；`AlertIncident` 使用 `rule_id + impact_scope_digest + stable_reason` 作为指纹，在 PostgreSQL 中 CAS 打开/合并/恢复。状态为 `inactive → pending → firing → recovering → resolved`，通知采用稳定 notification id 和至少一次语义。
- **Rationale**：共享状态可让多节点对同一故障只形成一个活动事件，并在恢复稳定窗口后只关闭一次。持久指纹支持节点重启后继续抑制，且诚实承认网络结果未知时可能物理重复通知。
- **Alternatives considered**：每节点内存告警会风暴且重启丢状态；把原始异常文本放入指纹会泄密和导致无限基数；承诺 exactly-once 外部通知无法跨网络事务证明。

## R-009 租户级灰度与回滚权威

- **Decision**：PostgreSQL 保存不可变 `ConfigurationSnapshot`、`CanaryRelease`、`TenantConfigRoute`、`ExecutionConfigPin` 和 `RollbackDecision`；Redis 仅承担 release controller 租约/fence 和缓存。配置快照一次引用 Agent、治理与数据后端的精确版本及兼容边界。请求在 Gateway 解析可信 tenant 后读取并固定 snapshot，接管和恢复复用同一 pin。
- **Rationale**：组合快照避免三类配置分别切换产生撕裂；发布状态必须跨节点可见且能恢复，不能由单节点内存或 Redis 单独决定。固定版本可防止在途请求混用。
- **Alternatives considered**：全局一次切换没有租户隔离；Redis 双写会形成双权威；独立 active pointers 会读出不一致组合；在途请求热切换会产生不可解释副作用。

## R-010 灰度门槛与事务边界

- **Decision**：硬门槛由安全/一致性 enforcement point 写入持久 hard latch，不依赖可丢弃 telemetry；signal、release transition、tenant route 更新和正式 Audit 在 PostgreSQL 事务/CAS 边界完成。质量观测满足观察窗口和最小样本后评估，越线只进入 `PAUSED_QUALITY` 等待授权决定。
- **Rationale**：零容忍事件必须立即停止扩面且留下不可变证据；质量指标可能受噪声影响。正式发布操作属于治理事实，Audit 不可用时不得推进或回滚配置。
- **Alternatives considered**：从采样指标推断硬违规可能漏报；所有质量越线自动回滚容易误触发；先切 route 后写 Audit 会出现无证据配置变更。

## R-011 容量门禁

- **Decision**：使用固定随机种子和确定性负载：2 tenants × 50 sessions/tenant × 10 ordered messages/session = 1,000 messages，100 Session 同时启动、Session 内串行、Session 间并行、两个 Worker。至少一次 warm-up 后分别运行 telemetry off 基线和 telemetry on，对比 throughput、p50、p95、p99，并记录环境与资源峰值。
- **Rationale**：相对门禁能识别本阶段引入的回归，正确性计数能保护租户隔离和幂等。相同机器、拓扑、数据初态和 seed 才能形成有效比较。
- **Alternatives considered**：绝对 QPS 受机器影响，不能代表生产；只测平均延迟会隐藏尾延迟；不同工作负载前后比较无效。

## R-012 最小部署与生产推荐拓扑

- **Decision**：新增独立 `deploy/local-observable/` Compose 叠加层，复用现有 Redis/PostgreSQL，包含 schema-init、Gateway、两个 Worker 和 OpenTelemetry Collector debug 出口；真实飞书/企业微信 Adapter 使用显式 profile。生产文档给出多故障域副本、Collector agent/gateway 两层、共享 PostgreSQL/Redis、外部 Secret Provider 和备份恢复建议，但不提交真实 Kubernetes 集群或生产 SLA。
- **Rationale**：独立叠加层避免破坏已验证的 `local-shared` 环境，Collector 可在无商业平台情况下证明 OTLP 出口。生产拓扑保持设计证据而非虚假部署证明。
- **Alternatives considered**：修改旧 Compose 会扩大前七阶段回归面；真实 IM 凭证作为默认启动前提不可移植；把商业可观测后台或 Kubernetes 作为本阶段交付会超出范围。

## R-013 官方 span 的出口前脱敏

- **Decision**：复用官方 span 的生命周期、状态、层级和耗时，但绝不把原始 `ReadableSpan` 直接交给网络 exporter。新增隔离的 `SanitizingSpanProcessor`，只从公开只读属性构造不可变 `SafeSpanEnvelope`，保留 trace/span/parent id、时间、状态、instrumentation scope 和明确白名单属性；官方版本中可能包含输入输出、state、Tool 参数/响应、LLM 请求/响应的属性全部丢弃。OTLP adapter 只接受 `SafeSpanEnvelope`。OpenTelemetry 依赖锁定版本，并为兼容层设置契约测试；验证失败时网络 OTLP 默认关闭，而不是绕过脱敏。
- **Rationale**：当前官方 1.1.19 确实会把 `runner.input/output`、`agent.input/output`、`state.*`、`tool_call_args/tool_response`、`llm_request/llm_response` 写入 span。原样导出违反 FR-031；完全关闭官方 telemetry 再重建又会丢失官方错误/取消语义。出口前 allowlist 是兼顾复用和安全的最窄边界。
- **Alternatives considered**：只在 Collector 脱敏意味着敏感字段已离开进程，拒绝；修改官方 span 私有字段脆弱且污染其他 processor，拒绝；复制 Runner instrumentation 违反 framework-first，拒绝。
- **Implementation gate**：禁止修改 `ReadableSpan` 私有字段；不得把未经 allowlist 的 span 送入 OTLP。兼容层若无法用锁定版本的公开只读表面完成安全映射，则实现必须停在 RED/blocked 并升级设计，不能以 Collector 二次清洗冒充完成。

## 已消除的未知项

- 技术栈、版本、存储权威、采样位置、默认采样范围、遥测失败边界、角色就绪矩阵、告警去重、发布权威、回滚边界、容量口径与部署范围均已有明确决定。
- Phase 1 设计不得再留下待澄清项；若实施发现必须改变上述决策，应先更新规格与本文件，再生成任务。

## 主要参考资料

- [OpenTelemetry Python](https://opentelemetry.io/docs/languages/python/)：Python trace/metric SDK 状态与依赖边界。
- [OpenTelemetry Python Exporters](https://opentelemetry.io/docs/languages/python/exporters/)：OTLP、Collector 与批量 exporter 方式。
- [OpenTelemetry Collector Architecture](https://opentelemetry.io/docs/collector/architecture/)：receiver、processor、exporter 流水线。
- [OpenTelemetry Collector Processors](https://opentelemetry.io/docs/collector/components/processor/)：tail sampling、redaction、memory limiter 等组件边界。
- [Scaling the OpenTelemetry Collector](https://opentelemetry.io/docs/collector/scaling/)：有状态 tail sampling 的 trace-affinity/扩展约束。
- [OTLP Exporter Specification](https://opentelemetry.io/docs/specs/otel/protocol/exporter/)：endpoint、timeout 与 transient failure retry 语义。
- 本地锁定依赖源码：`.venv/Lib/site-packages/trpc_agent_sdk/telemetry/` 与 `runners.py`，用于确认 tRPC-Agent 1.1.19 的实际 span、metric 和敏感属性行为。

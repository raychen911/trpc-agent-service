# Issue #3：系统架构图与企业微信核心时序图研究

> 本文只整理绘图所需的一手证据、边界和待决问题，不包含最终图源。术语以 [`CONTEXT.md`](../../CONTEXT.md) 为准，架构选择以 [Accepted ADR 索引](../adr/README.md) 为准。

## 1. 研究问题与验收基线

[Issue #3](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/3) 要求交付可版本控制、可在 CI 中渲染的系统架构图和企业微信核心链路时序图。架构图必须覆盖六类部署单元、Adapter、Filter、Telemetry、存储、治理基础设施，以及企业微信和飞书；时序图必须覆盖企业微信入站、验签去重、Outbox/Kafka、Runner、Tool、Session、Summary/Memory 和回复投递，并明确 `trace`、`execution`、`session`、`tool invocation`、`idempotency`、`delivery` 标识的传播。其前置 [Issue #2](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/2) 已关闭，总体设计正文已落在 [`architecture-design.md`](../architecture/architecture-design.md)。

据此，最终交付至少应有两张图，而不是把部署拓扑和逐步交互挤在一张图中：

1. **系统架构图**：说明边界、归属、同步/异步连接和基础设施依赖。
2. **企业微信核心时序图**：说明事务提交点、标识传播、审批/恢复、降级和投递失败恢复。

## 2. 系统架构图的证据清单

### 2.1 六类部署单元与平面

六类部署单元已经由 [ADR-0006](../adr/0006-deployment-unit-boundaries.md) 固定，最终图不得把 Kafka、OPA、LLM Gateway、OpenTelemetry Collector 或 Adapter 再计为第七类业务部署单元。

| 部署单元 | 平面 | 图中应表达的职责 | 主要连接 |
|---|---|---|---|
| Admin API | 控制面 | 版本化管理 API、RBAC、审批、配置和发布 | Web Console、企业 OIDC、控制面 PostgreSQL、治理/密钥基础设施 |
| Web Console | 控制面 | 管理和运营界面；只调用 Admin API | 平台用户、Admin API |
| Agent Gateway | 数据面 | HTTP、SSE、AG-UI、A2A 入口；认证、租户和 Release 解析；提交 Agent 执行 | API/Agent 客户端、Kafka 兼容执行总线、本地 OPA |
| Channel Gateway | 数据面 | IM 入口；Channel Adapter、通道绑定、入站幂等、回复投递状态 | 企业微信、飞书、PostgreSQL、执行总线、本地 OPA |
| Agent Worker | 数据面 | 加载固定 Agent Release，在 Session 并发控制和 Filter 下驱动 Runner | 执行总线、共享存储、OPA、LLM Gateway、Tool/Sandbox、Telemetry |
| Job Worker | 数据面 | 异步处理 Summary、Memory、Knowledge、迁移、删除、审计归档和补偿 | 执行总线、共享存储、本地 OPA、Telemetry |

职责和扩缩容边界来自总体设计的[六类部署单元表](../architecture/architecture-design.md#3-总体架构)、[ADR-0006](../adr/0006-deployment-unit-boundaries.md) 与统一语言中的 [Admin API、Web Console、Gateway 和 Worker 定义](../../CONTEXT.md#统一语言)。生产运行边界是 Kubernetes；图可用集群边框表达这一点，但不要把 Docker Compose 最小拓扑画成生产等价物（[ADR-0005](../adr/0005-kubernetes-production-runtime.md)）。

### 2.2 必须画在所属进程内的能力

- **企业微信 Adapter、飞书 Adapter**：都是 `Channel Gateway` 内的 `Channel Adapter` 插件，负责各自协议的验签、解密、规范化、能力协商和回复转换；不能画成独立微服务。架构图应同时出现两个 Adapter，核心时序图只展开企业微信路径（[`CONTEXT.md`](../../CONTEXT.md#统一语言)、[ADR-0006](../adr/0006-deployment-unit-boundaries.md)）。
- **Filter**：位于 Agent 执行管线内，把 Runner/Agent 上下文提交给 Policy Bundle 决策并落实允许、拒绝、脱敏或需要审批；它不是唯一权限边界，也不是 HTTP 中间件（[`CONTEXT.md`](../../CONTEXT.md#统一语言)、[ADR-0027](../adr/0027-opa-policy-bundles-with-trpc-filters.md)）。
- **Storage Adapter**：是 Worker 使用的进程内适配模块，将 Session、Memory、Knowledge、Artifact 和审计契约映射到租户的存储配置档；不能画成存储微服务（[`CONTEXT.md`](../../CONTEXT.md#统一语言)、[ADR-0006](../adr/0006-deployment-unit-boundaries.md)）。
- **tRPC-Agent Runner**：属于 Agent Worker 内的执行编排能力。平台复用 tRPC-Agent-Python 的 Runner、Model、Tool/MCP、Session、Memory、Knowledge、Filter、Telemetry 和 HITL 接口，同时在其外部补足持久幂等、Session fencing、发布治理、预算、审计与灾备（[总体设计的责任边界](../architecture/architecture-design.md#5-能力责任边界)）。

### 2.3 基础设施和外部依赖

最终架构图宜把以下内容分成“平台部署单元”“共享基础设施”“企业外部系统”三层，以免混淆责任：

| 分组 | 节点 | 必须表达的关系 |
|---|---|---|
| 消息与事务 | Kafka 兼容执行总线 | 连接 Channel Gateway、Agent Worker、Job Worker；按 `(tenant_id, session_id)` 维持会话内正常顺序；执行、投递、Memory、审计、重试和死信使用独立 Topic（[ADR-0010](../adr/0010-kafka-compatible-durable-execution-bus.md)） |
| 权威存储 | PostgreSQL 16 HA | 控制面元数据、Session Event/State、入站幂等、Outbox、审计索引和事务提交点（[ADR-0014](../adr/0014-default-production-storage-profile.md)） |
| 派生与热数据 | Redis 7 Cluster、PostgreSQL/PGVector、S3 兼容对象存储 | Redis 只承载缓存、限流、fencing 租约和热投影；PGVector 承载 Knowledge/语义 Memory；S3 承载 Artifact、知识源、导出与归档（[ADR-0014](../adr/0014-default-production-storage-profile.md)） |
| 治理 | OPA Sidecar / Policy Bundle | Gateway、Agent Worker、Job Worker 就近决策；敏感操作在 Bundle 失效时 fail closed；Tool 执行器和存储层仍做纵深校验（[ADR-0027](../adr/0027-opa-policy-bundles-with-trpc-filters.md)） |
| 密钥 | OpenBao/Vault HA 或兼容服务 | 平台配置仅保存租户范围 `secret_ref`；服务取短期凭证，正文不进入日志、Trace、Kafka 或 Session Event（[ADR-0020](../adr/0020-vault-compatible-secret-management.md)） |
| 模型 | LLM Gateway 与模型 Endpoint | Agent Worker 经 Model 接口调用集群内 LLM Gateway，由其解析别名、注入凭据、限流、计费和受控 fallback；它属于基础设施，不增加业务服务边界（[ADR-0022](../adr/0022-central-llm-gateway.md)） |
| 工具 | Tool/MCP、受限 Tool 执行器、可选 Sandbox Pod | Tool 调用按副作用分级和审批；不可信代码在独立 gVisor Sandbox Pod 执行，禁止退化为普通容器（[ADR-0018](../adr/0018-tool-side-effect-and-approval-policy.md)、[ADR-0019](../adr/0019-kubernetes-gvisor-sandbox-execution.md)） |
| Telemetry | OpenTelemetry Collector、Prometheus、Tempo、Loki、Grafana | 平台单元向 Collector 发遥测；指标、Trace、日志、展示分别由后四者承担，审计数据保持独立权威存储（[ADR-0024](../adr/0024-opentelemetry-observability-stack.md)） |
| 外部身份与通道 | 企业 OIDC、企业微信、飞书 | 平台用户经 OIDC/RBAC；IM 用户走独立通道身份链；企业微信/飞书只进入 Channel Gateway，不进入 Agent Gateway（[ADR-0003](../adr/0003-enterprise-oidc-and-platform-rbac.md)、[ADR-0034](../adr/0034-versioned-admin-and-agent-apis.md)） |

图中还应以注记表达：租户是硬隔离边界，所有租户实体持续携带并校验 `tenant_id`；内部实体由应用侧生成 UUIDv7，租户表使用 `(tenant_id, id)`、复合外键和 RLS。不要把企业微信、飞书或外部存储中的“租户”字段误标成平台 `tenant_id`（[`CONTEXT.md`](../../CONTEXT.md#使用约定)、[ADR-0040](../adr/0040-tenant-composite-keys-and-rls.md)）。

## 3. 企业微信核心时序图的事实顺序

下表是可以直接转成时序图消息的证据化脚本；“同步”是指企业微信回调确认前的关键路径，“异步”表示 PostgreSQL 事务提交后的平台处理。该顺序主要由总体设计的[关键数据流](../architecture/architecture-design.md#4-关键数据流与一致性)及 ADR-0010/0011/0012/0017/0018/0032/0042 固定。

| 阶段 | 交互与提交语义 | 图中标注的标识/控制量 | 来源 |
|---|---|---|---|
| 1. 入站 | IM 用户经企业微信向 Channel Gateway 的企业微信 Adapter 发送事件；Adapter 验签、解密、规范化并解析唯一通道绑定 | 原始外部事件 ID、`channel_binding_id`；在 IM 入口生成 `trace_id` | [统一语言：Channel Adapter/入站消息](../../CONTEXT.md#统一语言)、[ADR-0024](../adr/0024-opentelemetry-observability-stack.md) |
| 2. 身份与 Session | 以 `tenant_id + channel_binding_id + external_user_id` 解析 IM 主体，并按单聊或群聊规则生成不暴露原始标识的内部 `session_id` | `tenant_id`、IM 主体 ID、`session_id` | [ADR-0016](../adr/0016-channel-scoped-identities-and-sessions.md) |
| 3. 去重 | 在 PostgreSQL 以租户、通道绑定和稳定外部事件 ID 建立持久幂等记录；协议无稳定 ID 时用版本化规范字段摘要 | **入站幂等键**；Payload hash | [ADR-0017](../adr/0017-durable-inbound-idempotency-ledger.md) |
| 4. 原子接收 | 同一 PostgreSQL 事务创建入站消息、唯一 Agent 执行和 Outbox；内部实体采用 UUIDv7。只有事务提交后才能确认企业微信回调 | `inbound_message_id`、`execution_id`、`session_id`、`trace_id`、入站幂等键 | [ADR-0017](../adr/0017-durable-inbound-idempotency-ledger.md)、[ADR-0040](../adr/0040-tenant-composite-keys-and-rls.md) |
| 5. 发布与消费（异步起点） | Outbox 可靠发布执行命令到 Kafka 兼容执行总线；按 `(tenant_id, session_id)` 分区；Agent Worker 以幂等状态机消费 | 事件信封携带 tenant、causation、correlation、trace、schema 和数据分级信息 | [ADR-0010](../adr/0010-kafka-compatible-durable-execution-bus.md)、[ADR-0041](../adr/0041-versioned-cloudevents-json-contracts.md) |
| 6. 并发门禁 | Agent Worker 取得带 fencing token 的 Session 租约，检查 `expected_version`，加载执行启动时固定的 Agent Release 和共享 Session 上下文 | `execution_id`、`session_id`、`release_id`、fencing token、`expected_version` | [ADR-0011](../adr/0011-stateless-workers-and-session-concurrency.md)、[ADR-0021](../adr/0021-immutable-agent-releases.md) |
| 7. 治理与 Runner | Filter 将规范 Agent 上下文交给本地 OPA，落实 allow/deny/needs_approval、脱敏、数据范围和预算决策；通过后驱动 tRPC-Agent Runner | `trace_id`、`execution_id`、策略版本；Filter 不是唯一权限边界 | [ADR-0027](../adr/0027-opa-policy-bundles-with-trpc-filters.md)、[总体设计责任边界](../architecture/architecture-design.md#5-能力责任边界) |
| 8. Model | Runner 通过 Model 接口调用 LLM Gateway；网关先做预算预留、模型路由、密钥注入和策略允许范围内的重试/fallback | `trace_id`、`execution_id`、预算预留/结算记录 | [ADR-0022](../adr/0022-central-llm-gateway.md)、[ADR-0023](../adr/0023-hard-budget-reservation-and-settlement.md) |
| 9. Tool | Agent 提议 Tool 后，平台持久创建 Tool Invocation 和独立工具幂等键；按 READ_ONLY、IDEMPOTENT_WRITE、NON_IDEMPOTENT_WRITE、HIGH_RISK 处理 | `tool_invocation_id`（领域上的 Tool Invocation 标识）、**工具幂等键**、工具版本、参数哈希 | [统一语言：工具调用](../../CONTEXT.md#统一语言)、[ADR-0018](../adr/0018-tool-side-effect-and-approval-policy.md) |
| 10. 审批分支 | 需要审批时持久化审批请求，Agent 执行进入 `WAITING_APPROVAL` 且释放 Agent Worker；企业微信卡片或 Web Console 可批准/拒绝；批准后重新取得 Session 租约并从检查点恢复 | 审批绑定 tenant、Release、execution、Tool/参数哈希、请求者、策略版本、有效期 | [ADR-0032](../adr/0032-tiered-tool-approval-and-resume.md) |
| 11. Tool 结果 | Tool 结果先持久化，之后才允许追加 Tool Result Event；非幂等写超时进入 `OUTCOME_UNKNOWN`，不得盲目重试 | 同一 `tool_invocation_id` 和工具幂等键 | [ADR-0018](../adr/0018-tool-side-effect-and-approval-policy.md) |
| 12. Session 提交 | 校验执行幂等键、fencing token、`expected_version` 后，在一个提交点原子追加不可变 Session Event、递增版本、应用 `state_delta` 并写 Outbox；丢租约或版本冲突不得提交 | `execution_id`、`session_id`、Session version、fencing token、Outbox record ID | [ADR-0011](../adr/0011-stateless-workers-and-session-concurrency.md)、[ADR-0012](../adr/0012-session-events-as-source-of-truth.md) |
| 13. Summary/Memory | Job Worker 只消费已提交 Session Event；Summary 按确定事件版本范围生成并带 `source_version`，Memory 按来源 Session/Event 范围和策略版本幂等生成 | `session_id`、Event range、`source_version`；用 Span Link 关联原 `trace_id` | [ADR-0012](../adr/0012-session-events-as-source-of-truth.md)、[ADR-0013](../adr/0013-eventually-consistent-memory.md)、[ADR-0024](../adr/0024-opentelemetry-observability-stack.md) |
| 14. 回复入队 | 执行结果通过 Outbox/独立 IM 投递 Topic 进入 Channel Gateway 的持久回复投递状态机；每个逻辑回复创建稳定 `delivery_id` | `execution_id`、`session_id`、`delivery_id`；Span Link 关联原执行 | [ADR-0010](../adr/0010-kafka-compatible-durable-execution-bus.md)、[ADR-0042](../adr/0042-at-least-once-im-delivery.md) |
| 15. 企业微信投递 | 企业微信 Adapter 按通道能力发送占位、合并增量或最终回复；同一逻辑回复/更新共享 `delivery_id`，每次物理发送使用独立 `attempt_id` | `delivery_id` 稳定；`attempt_id` 每次变化；尽量复用通道幂等键/原消息更新 | [ADR-0031](../adr/0031-adaptive-im-streaming.md)、[ADR-0042](../adr/0042-at-least-once-im-delivery.md) |
| 16. 终态与对账 | 成功后持久化投递终态；超时先进入 `OUTCOME_UNKNOWN` 并查询外部结果，再决定重试；限流按通道指示退避，超期进入死信并以原 `delivery_id` 重放 | 原 `delivery_id`、新 `attempt_id`；尝试和终态进入审计、指标和 Trace | [ADR-0042](../adr/0042-at-least-once-im-delivery.md) |

### 3.1 必须显式画出的异常和降级分支

- **验签/解密/绑定失败**：不得形成规范入站消息或 Agent 执行；这是 Channel Adapter 与 Channel Gateway 的入口边界（[`CONTEXT.md`](../../CONTEXT.md#统一语言)）。
- **同键重复、内容相同**：复用原执行；**同键但 Payload hash 不同**：隔离并告警，不能覆盖原执行（[ADR-0017](../adr/0017-durable-inbound-idempotency-ledger.md)）。
- **接收事务失败**：不确认回调；不能让“已回复企业微信成功”出现在 PostgreSQL 提交之前（[ADR-0017](../adr/0017-durable-inbound-idempotency-ledger.md)）。
- **Kafka 不可用**：已提交 Outbox 保持积压并稍后发布；不能绕过持久化改走进程内队列（[ADR-0030](../adr/0030-fail-closed-critical-paths-and-explicit-degradation.md)、[ADR-0010](../adr/0010-kafka-compatible-durable-execution-bus.md)）。
- **租约丢失或版本冲突**：Worker 不得提交；由重投/重新取得租约恢复（[ADR-0011](../adr/0011-stateless-workers-and-session-concurrency.md)）。
- **安全决策、密钥、租约或副作用结果不确定**：fail closed。Memory 暂时不可用时可用权威 Session 上下文继续；Knowledge、Artifact 或模型 fallback 只有 Release 明确允许时才能降级，且必须向用户和审计披露（[ADR-0030](../adr/0030-fail-closed-critical-paths-and-explicit-degradation.md)）。
- **审批拒绝/过期/撤销**：形成工具结果和审计记录，不继续原工具副作用（[ADR-0032](../adr/0032-tiered-tool-approval-and-resume.md)）。
- **回复发送超时**：先对账后重试，不能直接产生第二个逻辑回复；外部 IM 不承诺 Exactly Once（[ADR-0042](../adr/0042-at-least-once-im-delivery.md)）。

## 4. 标识传播矩阵

Issue #3 所说的“idempotency 标识”并非单一全局 ID。最终时序图应区分入站、执行、Tool 和投递四种幂等/关联范围，避免把一个键错误地贯穿所有层。

| 标识 | 创建位置 | 传播/使用范围 | 不应混同 |
|---|---|---|---|
| `trace_id` | Channel Gateway 的 IM 入口 | 内部 HTTP、Kafka Header、Agent Worker；异步 Summary、Memory 和回复投递以 Span Link 关联 | 审计事件、`execution_id`；Trace 可能采样，审计另有权威存储（[ADR-0024](../adr/0024-opentelemetry-observability-stack.md)） |
| `execution_id` | 入站事务中唯一创建 | Outbox、Kafka 执行命令、Worker、审批、Session 提交、回复投递关联 | HTTP 请求或 Session；一次 Session 可有多次执行（[`CONTEXT.md`](../../CONTEXT.md#统一语言)、[ADR-0017](../adr/0017-durable-inbound-idempotency-ledger.md)） |
| `session_id` | 通道身份解析后确定性生成 | Kafka 分区、Session 租约、版本控制、Session Event/State/Summary、Memory 来源 | 外部 chat ID、Worker 状态（[ADR-0016](../adr/0016-channel-scoped-identities-and-sessions.md)、[ADR-0011](../adr/0011-stateless-workers-and-session-concurrency.md)） |
| `tool_invocation_id` | Agent 提议并持久化确定 Tool 操作时 | 治理决策、审批、执行、Tool Result Event 与审计 | 工具定义 ID、工具幂等键。当前 ADR 固定了“持久 Tool Invocation”语义，但尚未固定字段名/Schema（[ADR-0018](../adr/0018-tool-side-effect-and-approval-policy.md)） |
| 入站幂等键 | Channel Gateway | 去重账本、入站消息、唯一执行创建 | 工具幂等键、API `Idempotency-Key`；其语义是 `tenant + binding + external event ID` 或版本化摘要（[ADR-0017](../adr/0017-durable-inbound-idempotency-ledger.md)） |
| 工具幂等键 | Tool Invocation 创建时 | 允许下游幂等写重试和恢复 | Session 锁、入站幂等键（[ADR-0018](../adr/0018-tool-side-effect-and-approval-policy.md)） |
| `delivery_id` | 逻辑回复进入持久投递状态机时 | 占位、增量更新、最终回复、死信重放 | `attempt_id`、外部平台消息 ID（[ADR-0031](../adr/0031-adaptive-im-streaming.md)、[ADR-0042](../adr/0042-at-least-once-im-delivery.md)） |
| `attempt_id` | 每次物理发送尝试时 | 单次发送、超时对账、审计和指标 | 稳定的 `delivery_id`（[ADR-0042](../adr/0042-at-least-once-im-delivery.md)） |
| `tenant_id` | 平台租户上下文 | 关系键、消息、策略、存储路由、预算、审计全链路携带并校验 | 企业微信/飞书/模型/存储自己的租户字段（[`CONTEXT.md`](../../CONTEXT.md#使用约定)、[ADR-0040](../adr/0040-tenant-composite-keys-and-rls.md)） |
| causation / correlation | 领域事件信封 | Kafka Producer/Consumer 间连接原事件、派生事件和 Trace | `trace_id` 或业务实体 ID（[ADR-0041](../adr/0041-versioned-cloudevents-json-contracts.md)） |

## 5. 图源与 CI 渲染建议

采用两个独立、可审查的 Mermaid 源文件最直接：架构图用 `flowchart`（便于用 `subgraph` 表达平面、进程内插件和基础设施），核心链路用 `sequenceDiagram`。Mermaid 官方文档分别给出 flowchart 和 sequence diagram 语法；官方 `@mermaid-js/mermaid-cli` 可把 `.mmd` 输入渲染为 SVG/PNG/PDF，因此可在 CI 对两份源文件执行确定性语法/渲染检查（[Mermaid flowchart 官方文档](https://mermaid.js.org/syntax/flowchart.html)、[Mermaid sequence diagram 官方文档](https://mermaid.js.org/syntax/sequenceDiagram.html)、[Mermaid CLI 官方仓库](https://github.com/mermaid-js/mermaid-cli)）。

建议后续实现时：

- 将图源放在 `docs/architecture/`，例如 `system-architecture.mmd` 与 `wecom-core-sequence.mmd`；由 `docs/README.md` 和总体设计正文链接。
- 在项目锁文件或 CI Action 中固定 Mermaid CLI 版本，并运行等价于 `mmdc -i <source>.mmd -o <artifact>.svg` 的检查；不要依赖开发者浏览器“能显示”作为验收。
- 架构图的线型图例至少区分同步调用、Kafka/Outbox 异步调用、配置/治理分发、Telemetry；时序图用明确的事务框、异步消息和 `alt`/`opt` 分支表达提交、审批、降级与失败恢复。
- CI 除渲染退出码外，再检查两份源文件均存在且非空；生成 SVG 可以作为 CI artifact，是否提交到仓库由维护者另行决定。
- 组件标签逐字采用 `CONTEXT.md` 的规范术语；英文框架概念保留英文，避免把 Session Event 写成“Kafka 消息”、把 Outbox 写成“重试队列”或把 Storage Adapter 写成“存储服务”。

## 6. 尚未由现有 ADR 固定的歧义

以下内容不能在最终图中凭空具体化；需要在实现图源时采用逻辑名称或另补契约：

1. **Outbox Publisher/Relay 的部署归属**：ADR 只规定写入方通过 Transactional Outbox 发布，没有规定独立进程、sidecar 还是各服务内后台任务。可画成逻辑组件，但不能擅自增加第七类部署单元。
2. **回复投递 Topic 的精确名称和消费者组**：已确定使用独立 Topic、Channel Gateway/Adapter 负责投递，但 Topic 名、Schema 字段和状态枚举尚未固化。
3. **Tool Invocation、入站消息、Outbox、审批、投递的数据库字段名**：领域语义和必须携带的信息已确定，具体 API/Schema 仍未定义；图中可用规范概念名和 `*_id` 占位，不应伪装成已发布契约。
4. **企业微信的稳定外部事件 ID、回调确认正文、发送结果查询和幂等能力映射**：平台 ADR 规定了抽象行为，但仓库尚无企业微信 Adapter 协议契约。最终图可表达“验签/解密/确认/查询外部结果”，不能声称某个具体字段或 API 已确定。
5. **审批恢复的具体消息路径**：已确定企业微信/飞书卡片或 Web Console 可审批，批准后重新取租约从检查点恢复；回调经哪个 API、Topic 和 Outbox 尚未固定。
6. **Runner 内部每一次 Model/Tool 循环**：ADR 固定的是治理、预算、副作用和提交边界，而非循环次数；核心图应使用 `loop` 概括，避免把一次 Model→Tool→Model 写成唯一流程。
7. **Summary/Memory 的触发批次和 Topic 名称**：已确定只消费已提交 Session Event、异步且幂等；触发阈值和批次策略未固定。
8. **回复状态机的精确状态枚举**：`OUTCOME_UNKNOWN`、死信、重放和 attempt 语义已确定，但完整状态集合尚未形成正式 Schema。
9. **企业微信与飞书 Adapter 的流式能力细节**：目前只决定企业微信复用流式回复、飞书优先流式卡片以及不支持更新时退化为占位加最终消息，具体限流值、卡片协议和能力协商字段仍待 Adapter 契约固定（[ADR-0031](../adr/0031-adaptive-im-streaming.md)）。

这些歧义不阻塞 Issue #3 的高层图，但应以注记或抽象参与者呈现，不能越过后续 Schema/API/Adapter 交付提前发明接口。

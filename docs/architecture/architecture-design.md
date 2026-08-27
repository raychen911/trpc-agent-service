# 总体架构设计与实施规划

> 本文是生产平台的总体方案正文，给出系统边界、职责分工和实施基线。术语以 [`CONTEXT.md`](../../CONTEXT.md) 为准；关键选择以 [Accepted ADR](../adr/README.md) 为准。详细图表、Schema、接口和运行手册由后续交付物展开，不在本文重复固化。

系统边界、部署关系和企业微信端到端执行链路见[系统架构图与企业微信核心时序图](system-diagrams.md)。

## 1. 目标与设计思路

平台把基于 tRPC-Agent-Python 的单点 Agent 提升为可部署、可治理、可审计和可灾备的**生产平台**。首版采用一企业一实例的**私有化部署**；企业内的部门、业务线或子公司是扁平的**租户**，**Tenant Group**只汇总成本、容量和 SLO，不继承成员租户的数据访问权（[ADR-0002](../adr/0002-private-enterprise-deployment.md)、[ADR-0039](../adr/0039-flat-tenants-with-non-inheriting-groups.md)）。平台不把“增加 `tenant_id`”当作隔离完成，而是在身份、关系约束、缓存键、消息、对象路径、向量过滤、密钥、预算和审计上持续携带并校验租户上下文。

总体设计遵循四项原则：以不可变版本和事件保证可追溯；以持久幂等和显式并发控制应对至少一次投递；让安全、密钥、租约和副作用结果不确定时 fail closed；让 Memory 等增强能力异步化并在降级时明确披露（[ADR-0012](../adr/0012-session-events-as-source-of-truth.md)、[ADR-0030](../adr/0030-fail-closed-critical-paths-and-explicit-degradation.md)）。交付边界是完整生产能力，而非演示代码或空壳 Adapter（[ADR-0001](../adr/0001-deliver-a-complete-production-platform.md)）。

## 2. 业务、租户与环境边界

**平台用户**经企业 OIDC 登录并由平台 RBAC 授权；**IM 用户**通过通道绑定解析为租户内 **IM 主体**，两条身份链路不相互继承（[ADR-0003](../adr/0003-enterprise-oidc-and-platform-rbac.md)）。跨通道 Memory 仅能在同一租户内通过已验证的**主体关联**共享，Session、Knowledge、密钥和运行时数据禁止跨租户共享。

控制面元数据位于共享 PostgreSQL，租户表使用 `(tenant_id, id)` 唯一约束、复合外键、事务级 Tenant Context 和非特权连接的 RLS；运行时数据由**存储配置档**路由，共享后端强制逻辑隔离，高敏租户可绑定专属 SQL、Redis、向量库、对象存储和 Agent Worker Pool（[ADR-0004](../adr/0004-hybrid-tenant-data-isolation.md)、[ADR-0040](../adr/0040-tenant-composite-keys-and-rls.md)）。Development、Staging、Production 是发布、凭据和信任域边界，不等同于租户或 Kubernetes namespace。

## 3. 总体架构

**控制面**由 Admin API 和 Web Console 构成，管理租户、权限、Agent Draft、不可变 Agent Release、环境 Deployment、配置档、策略、审批和运营数据。Web Console 只调用版本化 Admin API。控制面短暂不可用时，数据面可继续使用已发布且已验证的版本，不允许绕过控制面创建新配置。

**数据面**由 Agent Gateway、Channel Gateway、Agent Worker、Job Worker 和执行总线构成。六类独立部署单元按扩缩容与故障域拆分如下（[ADR-0006](../adr/0006-deployment-unit-boundaries.md)）：

| 部署单元 | 平面 | 核心职责与扩缩容信号 |
|---|---|---|
| Admin API | 控制面 | 版本化管理接口、RBAC、审批和配置发布；按 API 延迟与管理流量扩容 |
| Web Console | 控制面 | 简体中文管理与运营界面；静态资源独立发布 |
| Agent Gateway | 数据面 | HTTP、SSE、AG-UI、A2A 认证和 Agent 执行提交；按连接数与请求率扩容 |
| Channel Gateway | 数据面 | Channel Adapter、验签解密、通道绑定、入站幂等和回复投递；按通道流量扩容 |
| Agent Worker | 数据面 | 加载固定 Agent Release，以 Filter 治理并驱动 Runner；按并发执行和模型等待扩容 |
| Job Worker | 数据面 | Summary、Memory、Knowledge、迁移、删除、审计归档和补偿；按 Topic 积压独立扩容 |

Channel Adapter 是 Channel Gateway 内插件，Storage Adapter 是 Worker 共享的进程内模块，Filter 位于 Agent 执行管线；它们不被拆成额外网络服务。生产仅支持 Kubernetes，地域内至少三个故障域高可用，跨地域采用禁用业务入口和消费者的温备（[ADR-0005](../adr/0005-kubernetes-production-runtime.md)、[ADR-0038](../adr/0038-multi-az-active-with-cross-region-standby.md)）。本地最小拓扑可用 Docker Compose 单实例运行 PostgreSQL、Redis、Kafka、MinIO、OPA 和 Fake 外部服务，但不能据此宣称具备生产 HA、gVisor、零信任或灾备能力。

## 4. 关键数据流与一致性

企业微信或飞书事件先由 Channel Gateway 验签、解密并解析唯一通道绑定；随后在一个 PostgreSQL 事务内写入**入站消息**、唯一**Agent 执行**和 Outbox，提交后才确认回调。重复事件复用原执行，键相同但 Payload 摘要不同则隔离告警（[ADR-0017](../adr/0017-durable-inbound-idempotency-ledger.md)）。

Outbox 将执行命令投递到 Kafka 兼容**执行总线**，按 `(tenant_id, session_id)` 分区。Agent Worker 取得带 fencing token 的租约并检查 `expected_version`，加载启动时固定的 Agent Release，通过 Filter/OPA 执行治理，再驱动 tRPC-Agent Runner。提交点原子追加 Session Event、推进 Session 版本、更新可重建 Session State 并写 Outbox；失去租约或版本冲突的 Worker 不得提交（[ADR-0010](../adr/0010-kafka-compatible-durable-execution-bus.md)、[ADR-0011](../adr/0011-stateless-workers-and-session-concurrency.md)）。

模型调用经 LLM Gateway 完成模型别名解析、凭据注入、限流、预算预留与结算；Tool 按副作用等级决定重试、审批或进入 `OUTCOME_UNKNOWN`。Job Worker 只消费已提交 Session Event 异步生成 Summary 和 Memory。回复以稳定 delivery ID 进入可对账的至少一次投递状态机，超时先查询外部结果再决定重试（[ADR-0013](../adr/0013-eventually-consistent-memory.md)、[ADR-0018](../adr/0018-tool-side-effect-and-approval-policy.md)、[ADR-0042](../adr/0042-at-least-once-im-delivery.md)）。

## 5. 能力责任边界

| 责任方 | 直接承担的能力 | 明确不承担 |
|---|---|---|
| tRPC-Agent-Python | Agent/Runner 编排，Model、Tool/MCP、Session、Memory、Knowledge 扩展接口，Filter、Telemetry、Tool Safety/HITL、Evaluation、AG-UI 与 A2A 能力 | 租户控制面、跨节点一致性、持久幂等、发布治理、预算、审计和灾备 |
| 平台新增 | 租户与身份边界，Draft/Release/Deployment，Channel Gateway 与正式 Adapter，执行状态机，Session fencing 与事件模型，Storage Adapter/迁移，Policy Bundle 集成，Tool 审批恢复，预算和成本账本，数据生命周期、Admin API 与 Web Console | 自研替代 Runner 或把基础设施包装成无业务价值的微服务 |
| 基础设施 | Kubernetes 调度，Kafka 持久传递，PostgreSQL 权威事务，Redis 缓存/限流/租约，PGVector 检索，S3 Artifact/归档，OPA 决策执行，Vault 密钥托管，Istio mTLS，OpenTelemetry 栈，Helm/Argo 发布 | 自动理解租户语义、替代平台的复合外键、幂等状态机、授权和数据分级校验 |

项目精确锁定并持续验证 tRPC-Agent-Python 稳定版；上游内部对象不直接成为数据库或 Kafka 契约，领域事件使用版本化 CloudEvents JSON Schema，以控制升级影响（[ADR-0041](../adr/0041-versioned-cloudevents-json-contracts.md)、[ADR-0043](../adr/0043-verified-upstream-update-policy.md)）。

## 6. 重点技术与量化预期

重点技术包括：不可变 Agent Release 和独立 Knowledge Revision；Outbox、幂等账本、Session Event、fencing token 与乐观版本组成的一致性链；OIDC/RBAC、RLS、OPA/Filter、四级数据分级、Vault、mTLS 和四眼审批组成的纵深防御；版本化存储迁移；OpenTelemetry 端到端关联；Helm、Argo 和 Expand-Migrate-Contract 发布（[ADR-0015](../adr/0015-versioned-online-storage-migration.md)、[ADR-0021](../adr/0021-immutable-agent-releases.md)、[ADR-0027](../adr/0027-opa-policy-bundles-with-trpc-filters.md)、[ADR-0037](../adr/0037-gitops-with-helm-and-argo.md)）。

预期效果以可验证目标表达：单集群持续 1,000 条入站消息/秒、3,000 条/秒突发 60 秒、至少 10,000 个并发 Agent 执行；数据面月可用性 99.95%，控制面 99.9%；Memory 正常跨节点可见 P99 不超过 5 秒；整集群灾难 RPO 不超过 5 分钟、RTO 不超过 60 分钟（[ADR-0007](../adr/0007-initial-capacity-envelope.md)、[ADR-0008](../adr/0008-availability-slos.md)、[ADR-0009](../adr/0009-disaster-recovery-objectives.md)）。这些是容量、故障和灾备演练的退出标准，不是未经测试的性能承诺。

## 7. 时间规划、关键路径与退出标准

规划以 **4 个日历周**为固定交付窗口。估算假设：从当前空工程骨架起步；技术负责人、后端/Agent、前端、SRE/平台和 QA/安全工程师全职投入，产品负责人、安全负责人和 DBA 随时参与门禁。45 项 ADR 在开工前冻结，接口评审在 24 小时内闭环；三故障域 Kubernetes、温备地域、OIDC、企业微信/飞书测试租户、Kafka、PostgreSQL、Redis、对象存储、OPA/Vault 和模型端点均在第 1 天可用。项目依靠六条工作流并行而非削减生产范围；任一前置条件不成立时，四周目标不再有效，必须补足资源、恢复时间或以新 ADR 调整范围。

| 里程碑 | 时间 | 主要依赖 | 阶段门禁与退出标准 |
|---|---:|---|---|
| W1 契约、底座与贯通骨架 | 第 1 周 | ADR、基础设施和外部测试账号第 1 天就绪 | API/事件/数据契约冻结；CI、GitOps 和可观测性底座运行；Fake IM→Gateway→Kafka→Worker→Runner→回复纵向链路可重复演示 |
| W2 控制面与关键数据面 | 第 2 周 | W1 契约；OIDC、PostgreSQL、Kafka 稳定 | 租户/RBAC、Draft→Release→Deployment 可审计；入站幂等、Session Event、fencing 和至少一次回复通过重复、乱序及 Worker 故障测试 |
| W3 治理、数据与生产拓扑 | 第 3 周 | W2 执行与 Session 模型；OPA/Vault、真实 IM 沙箱 | Tool 审批恢复、预算、数据分级、Memory/Knowledge、Storage Adapter/迁移完成；三故障域、mTLS、审计删除和备份恢复就绪 |
| W4 硬化、灾备与试点验收 | 第 4 周 | 前三周功能冻结；温备地域与评审人随时可用 | 全量回归、安全供应链、容量、Chaos 和独立温备演练通过；RPO/RTO、回滚、运行手册和试点退出证据完整 |

关键路径按日控制：领域与租户契约（D1）→ Release 契约（D3）→ 入站幂等/Outbox（D6）→ Session 并发与事件权威（D10）→ Tool/预算/策略治理（D15）→ 生产发布门禁（D19）→ 多可用区和温备验证（D25–D28）。核心数据面、控制面与 Web、通道与模型、治理与数据、平台与可观测性、质量与交付资产组成六条并行工作流，每日合并、每周冻结一个里程碑；任何工作流不能绕过周门禁。最终退出要求是代码、契约、测试、图表、部署资产和运行手册一致，且不存在任何越权访问、任何重复副作用、秘密泄漏、迁移不一致或恢复目标失败等发布阻断项（[ADR-0036](../adr/0036-production-release-quality-gates.md)、[ADR-0044](../adr/0044-mandatory-agent-release-evaluation-gates.md)）。

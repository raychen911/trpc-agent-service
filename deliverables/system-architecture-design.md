# 多租户节点化 Agent 平台架构设计

> 本架构设计文档基于当前项目代码、Docker Compose/Kubernetes 部署清单及已经完成的企业微信真实链路验证编写。

![系统架构图](./system-architecture.png)

## 1. 建设目标与设计原则

本项目面向企业内多个部门、业务线和 IM 账号共用 Agent 基础设施的场景，将单体 Agent 扩展为多租户、可水平伸缩、可治理和可恢复的平台。租户可以创建 Agent App，配置模型、工具权限、IM Channel Binding、数据后端与审计策略，再以版本形式发布或回滚。平台需要保证：消息能够找到正确的租户、应用和 Session；任一 Worker 故障后任务可以由其他节点接管；Redis、SQL、向量库和对象存储之间不存在不可恢复的双写窗口；外部消息重复投递不会产生重复业务效果。

架构遵循五项原则：第一，Worker 无状态化，运行状态进入共享后端，不依赖负载均衡器 sticky session；第二，PostgreSQL 保存权威事实，Redis 负责短期协调，Qdrant 和 MinIO 承担专业化数据；第三，外部系统按“至少一次投递 + 幂等消费”处理；第四，控制面配置版本化，运行面只读已发布版本；第五，所有请求携带 `trace_id/request_id`，治理、审计和故障恢复均能回到同一条业务链路。

## 2. 总体架构与组件对应关系

系统架构图从左至右表示一条消息的主数据流，从下至上虚线表示可观测数据流。各图形与代码职责对应如下：

| 架构图组件 | 项目职责与实现 |
|---|---|
| IM 平台 | 企业微信 AIBot、自建应用及 Telegram 等外部消息源 |
| Channel Adapter | Webhook 验签/解密、`NormalizedMessage`、身份映射、回复适配 |
| Agent Gateway | Channel Binding 解析、节点目录、Rendezvous Hash 和跨节点转发 |
| Agent Worker | Durable Inbox、治理 Filter、`AgentFactory`、Runner、Tool/MCP、Turn Coordinator |
| Storage Adapter | Session、Event、Memory、Summary、Artifact、Audit、Inbox/Outbox 统一访问抽象 |
| 多后端数据层 | Redis、PostgreSQL、Qdrant、MinIO；InMemory/Local 仅用于开发测试 |
| Telemetry | OpenTelemetry Collector 汇聚 Trace，Jaeger 展示，Prometheus 采集指标 |

图中主链路聚焦运行面。控制面 Admin API 由 FastAPI 的 `/admin/v1` 提供，负责维护 Tenant、Agent App、配置草稿、发布版本、回滚和 Channel Binding。其数据写入 PostgreSQL，图中 `Tenant / App / Channel Binding` 节点代表 Gateway 对这些控制面结果的运行时读取。生产环境由 OIDC/RBAC 保护 Admin API，节点间转发采用共享认证并预留 mTLS。

## 3. 多租户控制面与隔离

Tenant 是隔离根，包含 `tenant_id`、租户状态、审计策略和密钥命名空间；一个 Tenant 可拥有多个 Agent App。每个 App 通过 Revision 保存模型配置、工具权限、通道配置和后端配置，`active_version` 指向当前生效版本。更新草稿使用 `lock_version` 乐观锁，发布后 Revision 不可变，因此正在处理的消息不会读取到半更新配置，回滚只需把活动版本切回历史 Revision。

隔离并非只依赖一个字段。配置查询始终限定 tenant 和 app；Session、Event、Memory、Summary、Artifact、Audit、Inbox、Outbox 均携带 `tenant_id`；PostgreSQL 使用复合约束和 RLS，生产连接账号要求 `NOBYPASSRLS`；Redis 键和 Qdrant 命名空间包含 tenant/app/user；MinIO 对象键以 `tenant_id/agent_app_id/session_id` 开头。工具在执行前由租户白名单过滤，预算按租户累计。IM token、模型 API Key、数据库密码只保存 `env://`、`vault://` 或 KMS 引用，由 `SecretResolver` 在运行时解析，禁止进入配置正文、日志和 Trace。

## 4. Channel Adapter 与 IM 接入

Channel Adapter 隔离不同 IM 协议。回调首先依据 URL 中的 account/binding key 查找唯一的 Channel Binding，由此确定 tenant、Agent App、验签参数和身份映射策略；随后完成验签、解密与格式归一化，输出统一的 `NormalizedMessage`，其中至少包含 tenant、app、channel、external message id、内部 user id、session id、文本/媒体引用和 trace context。

企业微信与 Telegram 的差异如下：

| 项目 | 企业微信 | Telegram |
|---|---|---|
| 安全校验 | token 参与 SHA-1 签名；加密模式还需 AES 解密与 EncodingAESKey | `X-Telegram-Bot-Api-Secret-Token` 比对 |
| 消息标识 | 使用企业微信消息 ID，缺失时由稳定字段生成摘要 | 使用全局递增的 `update_id` |
| 回复方式 | AIBot 采用回调/主动回复能力，自建应用支持加密被动回复 | 调用 Bot API `sendMessage`，也可编辑消息模拟流式更新 |
| 会话规则 | 单聊按账号+用户，群聊按账号+群；tenant/app 始终参与隔离 | 私聊按 bot+chat，群聊按 bot+chat，必要时加入 topic |
| 平台约束 | 回调超时严格、加解密复杂、企业身份明确 | 公网依赖明显，限频和长消息上限需拆分处理 |

Webhook 不同步等待模型完成，而是将消息写入 Durable Inbox 后快速返回。回复适配器负责文本、卡片、图片/文件、长消息拆分、限频、重试与投递死信；媒体正文进入 MinIO，消息和 Artifact 元数据进入 SQL。

## 5. Gateway、节点路由与无状态 Worker

每个实例启动后把 `node_id`、可直连 `NODE_BASE_URL`、能力和心跳写入 Redis Node Directory，并通过 TTL 自动剔除失联节点。Gateway 将 `tenant_id + agent_app_id + session_id` 组成 route key，使用 Rendezvous Hash 在健康节点间稳定选路。同一 Session 通常落到同一节点以提高缓存命中，但这只是路由优化，不是正确性条件。目标是本节点时直接处理，目标是其他节点时通过内部接口转发；目标节点消失后重新计算即可转移到幸存节点。

Worker 从 PostgreSQL Inbox 领取任务，PostgreSQL 环境使用 `FOR UPDATE SKIP LOCKED`，领取后设置租约。崩溃节点未完成的记录在租约过期后可被其他 Worker 领取。因此系统不需要 sticky session：Agent 执行所需的 Session、Memory、Summary 和配置都来自共享后端；同 Session 的并发写由 Redis 分布式锁降低竞争，SQL 的版本 CAS 负责最终正确性。真实双节点测试覆盖节点注册、稳定路由和故障转移。

## 6. Agent Worker 与治理执行

Inbox 消息进入 Worker 后依次经过图中的 `Filter → AgentFactory/Runner → Tool/MCP → Turn Coordinator`。治理 Filter 在模型调用前执行 IM 用户 ACL、输入 PII 脱敏和租户预算检查；在工具调用前根据已发布白名单判断是否允许，高风险工具必须取得二次确认。拒绝、脱敏、预算超限和确认结果写入 Audit Log。

`AgentFactory` 将已发布配置转换为 tRPC-Agent-Python Runner：解析模型服务和密钥引用，加载允许的 Tool/MCP，装配 Session、Memory、Knowledge 与 Filter。Runner 负责模型推理、Agent 编排、Tool 调用和事件产出；平台的 Execution Ledger 记录 `pending → runner_started → runner_completed → platform_committed → delivery_enqueued`。这样 Runner 已成功而平台提交失败时可复用已保存结果；若外部 Tool 是否成功无法判断，则进入 uncertain 状态等待人工决策，避免盲目重放产生重复副作用。

## 7. tRPC-Agent-Python 复用与平台新增能力

可直接复用的能力是 Agent/Runner 编排、模型调用、Tool/MCP、Session/Memory/Knowledge 概念、Filter 扩展点和 OpenTelemetry 接入能力。平台不重复实现推理引擎，而是由 `AgentFactory` 把租户的已发布配置装配到这些框架能力上。

新增的平台层模块包括 Tenant/Admin API 与版本发布、Channel Binding 和 IM 协议适配、NormalizedMessage 与身份映射、Durable Inbox、Node Directory 与跨节点路由、租户后端解析、Redis 锁/幂等/限流、Turn Coordinator、Execution Ledger、Transactional Outbox/Dead Letter、RLS/审计/预算、SecretResolver、迁移工具及 Compose/Kubernetes 运维能力。二者边界清晰：tRPC-Agent-Python 解决“Agent 如何执行”，平台层解决“谁可以执行、在哪个节点执行、状态如何可靠保存、如何回复及如何运营”。

## 8. Storage Adapter 与多后端分工

Storage Adapter 向上提供稳定接口，隐藏不同后端的客户端和一致性差异。租户可按数据类型选择后端，小型租户可以主要使用 SQL，检索型租户增加 Qdrant，包含大量媒体时增加 MinIO；但生产环境禁止使用进程内 Session。

| 后端 | 主要数据 | 一致性与作用 |
|---|---|---|
| Redis | Session 锁、幂等 claim、限流、短期确认状态、Node Directory；可选热点 Session | 原子操作、低延迟、TTL；不是长期审计事实源 |
| PostgreSQL | Tenant/App/Binding、Session/Event/Summary、Memory 原文、Audit、Inbox/Outbox/Execution Ledger | 事务强一致、RLS、唯一约束，是平台权威事实源 |
| Qdrant | Memory 与 Knowledge 向量及过滤 metadata | 语义检索索引，Outbox 异步 upsert，允许最终一致且可由 SQL 重建 |
| MinIO | Artifact、图片、文件、知识原件 | 保存大对象；SQL 保存 object key、MIME、大小和 checksum |

本地开发可以使用 InMemory Coordination/Vector、SQLite 和 Local Artifact，便于零依赖单元测试；Compose 联调使用 PostgreSQL、Redis、Qdrant、MinIO 和 OTel Collector；生产 Kubernetes 使用共享高可用服务，并通过 Pod IP 暴露可直连的节点地址。

## 9. 数据同步、并发与幂等

外部消息的规范幂等键为 `tenant_id:channel:external_message_id`。平台形成四层防线：SQL Inbox 唯一约束阻止重复入队；Redis `SET NX EX` 阻止多个节点同时执行；Session Event 的外部消息唯一约束阻止重复事实；Execution Ledger 与 Outbox 的唯一 dedupe key 阻止重复执行记录、重复向量任务和重复回复。系统不承诺物理上的端到端 exactly-once，而以至少一次投递和幂等副作用实现业务效果接近恰好一次。

同一 Session 先取得带 token 和自动续租的 Redis 锁，再读取当前版本。`TurnCoordinator` 在一个 SQL 事务内固定执行：

```text
Event → Session State CAS → Summary → Memory 事实 → Transactional Outbox
```

任一步失败则整体回滚。SQL 无法与 Qdrant、MinIO、IM 平台组成分布式事务，因此提交事实的同时写 Outbox；后台 Worker 通过租约和 `SKIP LOCKED` 领取任务，失败指数退避，超过阈值进入 Dead Letter。Memory 使用稳定业务 ID 和版本 upsert Qdrant，SQL 提交后立即跨节点可见，向量检索在 Outbox 完成后最终可见。Redis→SQL、SQL Memory→Qdrant、Local→MinIO 的迁移均采用全量回填、增量校验、切换读取和保留回滚窗口的步骤。

## 10. 完整消息链路与 Trace

以已经验证的企业微信 AIBot 文本消息为例：

1. 企业微信向 `/webhooks/wecom/{account}` 发起回调，HTTP 中间件创建或继承 `trace_id/request_id`；Adapter 校验签名、解密并解析 Binding。
2. Adapter 建立内部用户与 Session ID，将 trace context 写入 `NormalizedMessage` 和 SQL Inbox，随后快速返回 200。
3. Inbox Consumer 恢复 trace context，产生 `im.consume` span；Gateway 产生 `gateway.route` span并选择本地或远端 Worker。
4. Filter 完成 ACL、PII、预算与工具授权；`agent.execute` span 下由 tRPC Runner 调用模型，Tool/MCP 产生子 span。
5. Storage Adapter 读取 Session/Memory，`storage.commit_turn` 在同一事务提交 Event、State、Summary、Memory 和回复 Outbox。
6. Outbox Worker 恢复同一 trace context，`memory.upsert` 将索引写入 Qdrant，`outbox.deliver` 通过回复适配器把结果发送至企业微信。
7. OpenTelemetry Collector 接收上述 span，Jaeger 中可看到从 `HTTP POST /webhooks/wecom/...` 到 `im.consume → gateway.route → agent.execute → storage.commit_turn → outbox.deliver` 的完整父子关系；日志和 Audit Log 保存同一 trace id，Prometheus 汇总请求量、延迟、错误、Token 与投递指标。

## 11. 可观测性、安全与审计

Telemetry 覆盖 Channel Adapter、Gateway、Worker 和 Storage Adapter。核心指标包括各通道回调量和验签失败率、模型与工具耗时、Token/租户成本、Redis 锁等待、Session CAS 冲突、SQL 事务延迟、Inbox/Outbox backlog、向量同步延迟及 IM 投递成功率。告警以队列最老年龄和错误率为主，避免只观察实例存活。

Audit Log 至少记录 tenant、channel、user、session、agent、tool、decision、latency、error type、cost 和 trace id。结构化日志采用字段白名单，SensitiveDataFilter 对 token、Authorization、API Key、手机号等内容脱敏，第三方 SDK 日志经过相同过滤器。管理面使用 OIDC/RBAC；服务间使用 mTLS；SecretResolver 对不同租户限制命名空间，确保密钥既不明文落库，也不进入异常栈、指标标签或 Trace attribute。

## 12. 故障恢复、发布与部署

节点故障由 Node Directory TTL、重新选路和 Inbox/Outbox 租约接管；IM 重试由四层幂等吸收；数据库短暂不可用时不宣称消息已持久接收；模型超时进行受限重试并返回降级文案；只读型 Tool 可重试，含外部副作用的 Tool 必须使用业务幂等键，结果不确定时转人工；Qdrant 不可用不阻塞 SQL turn，待 Outbox 恢复后补齐索引；IM 投递失败退避重试并进入死信。

灰度发布采用 Kubernetes stable/canary Deployment，按租户白名单或小比例流量导向 canary；配置灰度通过 Agent App Revision 控制，不直接修改活动配置。发现问题时先回滚 active version，再回退工作负载镜像。备份覆盖 PostgreSQL 全量/增量、MinIO 版本对象、Qdrant 可重建数据和配置清单，并定期演练恢复。容量评估以 IM 峰值 QPS、单 turn 平均耗时和 Token、每节点并发 Session、Redis 锁/限流 QPS、SQL 事务 QPS、Inbox/Outbox 积压增长速度为输入，通过压测确定 Worker 副本、连接池和后端容量。

最小可运行环境采用单个应用实例加 PostgreSQL、Redis、Qdrant、MinIO、OTel Collector/Jaeger；生产推荐 Kubernetes 多副本 Gateway/Worker、托管 PostgreSQL 与 Redis、高可用对象/向量服务、独立 Telemetry Collector，并配置 readiness/liveness、PodDisruptionBudget、HPA、NetworkPolicy、备份和告警。

生产风险不在本设计中重复展开，完整风险等级、触发信号、缓解措施和验证方法见 `production-risk-register.md`。

## 13. 总结

该架构使一个 Agent 服务能够安全承载多个租户和多类 IM 账号；Worker 可无状态水平扩展，单节点退出不会丢失已入队消息；Session 更新具备锁和 CAS 双保险；SQL 与向量库、对象存储、IM 回复之间可以通过 Outbox 恢复；租户权限、预算、密钥和数据边界可审计；一条消息可以在 Jaeger、日志、指标和审计记录中凭 trace id 完整定位。由此，项目从可运行机器人提升为能够继续工程化扩容、灰度、回滚和故障演练的平台基线。

相关详细设计见：`data-model-design.md`、`data-sync-idempotency-strategy.md`、`multi-backend-adaptation-plan.md` 和 `production-risk-register.md`。

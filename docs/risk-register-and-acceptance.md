# 生产风险登记与交付验收矩阵

> 本文是 [Issue #5](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/5) 的交付基线，将[生产平台规格](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/1)、[Accepted ADR](adr/README.md)、后续工单和最终证据串联起来。术语以 [`CONTEXT.md`](../CONTEXT.md) 为准。设计文档评审门禁通过前，不得开始 [Issue #6](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/6) 及后续代码实现。

## 1. 使用规则

- 风险状态只有 `OPEN`、`MITIGATED`、`ACCEPTED`、`CLOSED`。预防代码合入不等于关闭；只有“验收证据”全部存在且复核通过才可标为 `CLOSED`。
- `ACCEPTED` 必须记录期限、业务理由、剩余影响和批准人；任何越权访问、任何重复副作用、秘密泄露、迁移不一致、灾备双主和恢复目标失败不得接受为发布例外。
- 可能性：`L1` 低、`L2` 中、`L3` 高。严重度：`S1` 轻微、`S2` 中等、`S3` 严重、`S4` 灾难。任一 `S4` 风险在最终验收前必须有生产等价环境证据。
- “责任角色”对风险闭环负责，可以委派执行但不能转移签字责任。所有证据必须可复现、带版本/环境/时间，并能追溯到提交、测试运行或演练报告。
- 本登记包含规格固定的 16 项初始风险；新增风险使用连续 ID。删除风险必须由新 ADR 或规格变更说明依据，不能因暂未实现而删除。

## 2. 风险登记

| ID | 风险 | 触发条件 | 影响 | 可能性 / 严重度 | 预防 | 检测 | 恢复 | 责任角色 | 验收证据、关联工单与 ADR | 状态 |
|---|---|---|---|---|---|---|---|---|---|---|
| R-01 | 跨租户数据泄露 | Repository 缺失租户上下文；复合外键/RLS/缓存键/向量过滤/对象前缀任一失效 | 合规违规、客户及业务数据暴露，必须阻断发布 | L2 / S4 | 事务作用域内签名租户上下文逐层传递并交叉校验；`(tenant_id,id)` 复合约束、强制 RLS、非特权连接、Storage Adapter 强制过滤、独立密钥 | 跨租户攻击用例、RLS CI、缓存/向量/对象扫描、审计告警 | 立即隔离租户与工作负载，吊销凭据，停止相关出口，按事件响应取证、通知并清除派生副本 | 安全负责人 | 必需：所有数据路径双重阻断测试和负向攻击报告；[#8](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/8)、#9、#16、#28、#38；[ADR-0004](adr/0004-hybrid-tenant-data-isolation.md)、[ADR-0040](adr/0040-tenant-composite-keys-and-rls.md) | OPEN |
| R-02 | IM 重复或乱序投递 | IM 重试、Webhook 超时、Kafka 重投或分区顺序配置错误 | 重复回复、重复 Tool 副作用、Session 顺序损坏 | L3 / S3 | 持久入站幂等 ledger、Payload 摘要冲突隔离、Session 分区、稳定 execution/delivery ID | 重复率、冲突隔离数、分区顺序异常、回放对比 | 复用原执行；隔离摘要冲突；从 ledger/Outbox 重放且不生成新逻辑 ID | Channel 负责人 | 必需：重复、乱序、相同键不同 Payload 和崩溃回放测试；[#13](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/13)、#21–#24、#38；[ADR-0017](adr/0017-durable-inbound-idempotency-ledger.md)、[ADR-0042](adr/0042-at-least-once-im-delivery.md) | OPEN |
| R-03 | Session 并发写冲突 | Consumer 重平衡、租约过期、Worker 迟到提交或同 Session 并发执行 | Session Event、Session State、Summary 与对话上下文不一致 | L2 / S4 | `(tenant,app,session)` 分区、可续期租约、单调 fencing token、`expected_version` CAS | fence/version 冲突指标、过期 Worker 提交拒绝审计、Session State 重建摘要比对 | 拒绝迟到提交，重新取得租约并从已提交 Session Event 重跑；损坏投影从 Session Event 重建 | 分布式执行负责人 | 必需：并发、租约过期、重平衡和 Worker 崩溃故障测试，证明冲突事务零事件落库；[#13](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/13)、#35、#38；[ADR-0011](adr/0011-stateless-workers-and-session-concurrency.md)、[ADR-0012](adr/0012-session-events-as-source-of-truth.md) | OPEN |
| R-04 | 非幂等 Tool 结果不确定 | NON_IDEMPOTENT_WRITE 调用在外部已执行但响应超时/断连 | 重复且不可逆的外部操作、财务或业务损失 | L2 / S4 | 持久 Tool Invocation、稳定参数摘要/幂等键、副作用分级、审批前冻结操作意图 | `OUTCOME_UNKNOWN` 指标、下游查询差异、人工待办超时 | 禁止盲目重试；优先下游对账，无法确认时人工处置并保留审计 | Tool 治理负责人 | 必需：超时后零自动重试、下游已成功/未成功/不可查询三类演练；[#17](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/17)、#18、#38；[ADR-0018](adr/0018-tool-side-effect-and-approval-policy.md)、[ADR-0032](adr/0032-tiered-tool-approval-and-resume.md) | OPEN |
| R-05 | 模型或 Tool 泄露敏感数据 | 分类错误、DLP 漏检、OPA/Filter 绕过、外发 Endpoint 不在允许范围 | 密钥、个人信息或企业机密外发 | L2 / S4 | 四级数据分类、等级只升不降、DLP、OPA/Filter、Endpoint allowlist、外发前清理秘密/Baggage | DLP/策略拒绝指标、出口审计、模型与 Tool 泄露 canary、秘密扫描 | 立即阻断出口、轮换秘密、隔离数据与 Release，调查受影响主体并按合规流程通知 | 数据保护负责人 | 必需：各分级出站矩阵、Filter 绕过、秘密 canary 和外部模型/Tool 负向测试；[#16](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/16)、#17、#33、#38；[ADR-0027](adr/0027-opa-policy-bundles-with-trpc-filters.md)、[ADR-0028](adr/0028-data-classification-enforcement.md) | OPEN |
| R-06 | Kafka 或 Outbox 长时间积压 | Broker/Publisher 故障、消费者容量不足、下游限流或毒消息 | 回复、Memory、审计、删除延迟并突破 SLO/合规期限 | L3 / S3 | 容量水位、Topic 隔离、背压、HPA、分级重试、死信和 Outbox 保留 | Consumer lag、最老 Outbox 年龄、Topic 吞吐、死信数和删除期限告警 | 扩容/限流非关键生产者，隔离毒消息，从已提交 Outbox/位点恢复并对账 | SRE 负责人 | 必需：Broker 故障、积压到恢复、毒消息隔离、死信重放和无丢失对账报告；[#13](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/13)、#33–#35、#38；[ADR-0010](adr/0010-kafka-compatible-durable-execution-bus.md)、[ADR-0030](adr/0030-fail-closed-critical-paths-and-explicit-degradation.md) | OPEN |
| R-07 | PostgreSQL 故障或 RLS 误配置 | 主库/可用区故障、错误迁移、业务角色获得 Owner/BYPASSRLS 或新表漏启 RLS | 权威状态不可用、跨租户隔离失效 | L2 / S4 | 多可用区 HA、迁移门禁、非特权角色、`FORCE RLS`、新表策略检查、备份/PITR | 数据库 SLO、复制延迟、角色权限漂移、新表 RLS CI、跨租户探针 | 故障转移或 PITR；冻结写入；回滚 Expand 阶段迁移；安全问题按 R-01 响应 | DBA 负责人 | 必需：主库故障转移、PITR、错误迁移回滚、角色漂移和新表漏 RLS 阻断测试；[#9](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/9)、#35、#36–#38；[ADR-0040](adr/0040-tenant-composite-keys-and-rls.md) | OPEN |
| R-08 | Redis 租约故障 | Redis 分区/主从切换/时钟异常导致租约丢失、续租失败或重复持有 | 并发执行、迟到提交，或关键链路被错误降级 | L2 / S4 | Redis Cluster、Owner compare-and-renew、数据库单调 fence、租约不可绕过 | 租约获取/续租错误、fence 冲突、同 Session 并发持有探针 | 关键提交 fail closed；等待租约恢复，重新取 fence 并从权威 Session Event 继续 | 执行平台负责人 | 必需：Redis 主从切换、网络分区、TTL 到期和迟到 Worker 故障注入；[#13](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/13)、#28、#35、#38；[ADR-0011](adr/0011-stateless-workers-and-session-concurrency.md)、[ADR-0030](adr/0030-fail-closed-critical-paths-and-explicit-degradation.md) | OPEN |
| R-09 | 存储在线迁移数据缺失 | 回填漏项、CDC/Outbox 游标错误、切换时仍有活跃写、Embedding 空间混用 | Session 历史、Memory/Knowledge 向量或 Artifact 不完整 | L2 / S4 | 版本化迁移状态机、源端权威、快照回填、增量追赶、摘要/抽样校验、观察期、禁止盲目双写 | 行数/对象数/摘要差异、增量 lag、影子检索差异、孤儿对象扫描 | 切换前失败保持源端；观察期内验证源端追平后原子回滚；投影从 Session Event 重建 | 数据平台负责人 | 必需：断点续跑、摘要不一致阻断、活跃 Session 排空、向量重建和观察期回滚演练；[#28](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/28)、#29、#30、#35、#38；[ADR-0015](adr/0015-versioned-online-storage-migration.md) | OPEN |
| R-10 | IM 投递 `OUTCOME_UNKNOWN` | 通道已收消息但响应超时、查询能力缺失或限流窗口跨越重试 | 用户看不到回复或收到重复消息 | L3 / S3 | 稳定 delivery ID、attempt ledger、外部会话串行、通道幂等/更新能力协商 | 未知状态年龄、重复外部消息摘要、投递成功率、限流和死信告警 | 先查询对账；确认未送达才按策略重试；不能确认则人工重放且沿用 delivery ID | Channel 负责人 | 必需：已送达超时、未送达超时、无查询能力、限流和人工重放测试；[#21](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/21)–#25、#35、#38；[ADR-0042](adr/0042-at-least-once-im-delivery.md) | OPEN |
| R-11 | Sandbox 逃逸或资源滥用 | 不可信代码获得宿主/网络/凭据访问或耗尽 CPU、内存、磁盘 | 集群、数据、下游系统或供应链受损 | L1 / S4 | gVisor、默认禁网、非 root、只读根文件系统、独立身份、资源/时间限制、固定镜像 | Runtime 告警、异常系统调用/网络、资源配额、镜像和逃逸 Smoke Test | 终止 Sandbox、隔离节点/身份、吊销凭据、重建受影响节点并取证 | 安全工程负责人 | 必需：逃逸 Smoke、默认禁网、提权/凭据访问阻断和资源耗尽测试；[#19](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/19)、#37、#38；[ADR-0019](adr/0019-kubernetes-gvisor-sandbox-execution.md) | OPEN |
| R-12 | 密钥服务或 OIDC 不可用 | Vault/IdP 故障、凭据过期、网络隔离或轮换错误 | 管理登录、模型、通道或存储访问中断 | L2 / S3 | 仅保存密钥引用、短期缓存有明确边界、自动轮换、受保护应急管理员、关键路径 fail closed | 登录/解析错误、租约到期、轮换失败、缓存剩余 TTL 和 Break-glass 告警 | 启用限时应急管理员；恢复 Vault/IdP；重新签发短期凭据并审计撤销应急权限 | IAM 负责人 | 必需：IdP/Vault 中断、缓存到期、密钥轮换/吊销和 Break-glass 全流程演练；[#9](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/9)、#11、#35、#38；[ADR-0003](adr/0003-enterprise-oidc-and-platform-rbac.md)、[ADR-0020](adr/0020-vault-compatible-secret-management.md) | OPEN |
| R-13 | 预算并发超扣 | 多 Worker 同时预留、重试重复结算、价格表漂移或最坏 Token 估算不足 | 租户产生不可接受费用，成本账本不可复算 | L2 / S3 | 调用前原子预留、稳定账本键、版本化价格表、最坏用量上限、结算/释放状态机 | 预留与结算差额、供应商账单对账、预算拒绝率、重复账本键告警 | 停止新预留，释放过期额度，以不可变账本重算并用审计调整项纠正 | FinOps 负责人 | 必需：高并发预留、重复回调、超时释放、价格变更和供应商账单对账测试；[#15](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/15)、#34、#38；[ADR-0023](adr/0023-hard-budget-reservation-and-settlement.md) | OPEN |
| R-14 | 上游 SDK 升级回归 | `trpc-agent-py` 新版本改变 Runner、Session、Tool、协议或内部对象 | 运行行为、契约或治理边界漂移 | L2 / S3 | 精确锁定版本、禁止直接序列化内部对象、自动升级 PR、历史契约和全量回归 | 版本漂移检查、Schema/API 合约、Eval 回归、Staging/灰度指标 | 停止推广；回退锁文件中的固定依赖，重建已签名服务镜像并回滚 Helm/Argo 服务版本；不得移动 Agent Deployment，数据迁移和外部副作用不随代码自动回滚 | 平台维护负责人 | 必需：锁文件/运行时版本一致、完整回归、Staging、灰度中止和服务版本回滚证据；[#6](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/6)、#14、#31、#37、#38；[ADR-0043](adr/0043-verified-upstream-update-policy.md) | OPEN |
| R-15 | 灾备双主 | 主地域未 fencing 即启用备用入口/消费者，或 Failover Lease 失效 | 跨地域双写、重复消费和重复外部副作用 | L1 / S4 | Active/Standby、全局 Failover Lease、主集群 fencing、备用默认禁用、切换顺序自动化 | 双写探针、Lease Owner、两地消费者/入口状态、数据库时间线与消息位点告警 | 立即停止备用或未确认一侧，重新取得全局 Lease，对账权威存储/位点/外部副作用后单边恢复 | 灾备负责人 | 必需：独立温备季度演练，证明主集群 fencing、单一入口/消费者、RPO≤5 分钟、RTO≤60 分钟；[#36](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/36)、#38；[ADR-0038](adr/0038-multi-az-active-with-cross-region-standby.md) | OPEN |
| R-16 | 可观测性采集敏感正文 | 自动埋点、异常、Kafka Header、日志参数或 Baggage 捕获正文/秘密/原始身份 | 日志和 Trace 形成新的敏感副本及外泄面 | L2 / S4 | Attribute allowlist、正文默认关闭、身份哈希、外发前清除 Baggage、错误快照脱敏 | 遥测秘密/PII 扫描、未授权 Attribute 指标、采样导出审计 | 停止相关 Collector/Exporter，删除允许删除的副本、轮换秘密、修正规则并重放安全遥测 | 可观测性负责人 | 必需：日志/Trace/Kafka/错误快照扫描及秘密 canary 测试，结果为零泄露；[#33](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/33)、#37、#38；[ADR-0024](adr/0024-opentelemetry-observability-stack.md) | OPEN |

## 3. README 八项交付验收矩阵

状态含义：`REVIEW_PENDING` 表示设计基线尚待评审合入；`BASELINE_READY` 只表示设计基线已合入；`EVIDENCE_PENDING` 表示实现或生产等价验证尚未完成；只有 [Issue #38](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/38) 完成公开边界复核后才可记为 `ACCEPTED`。

| ID | README 必需交付物 | 规格 / ADR 基线 | 主责工单 | 当前受控产物 | 最终验收证据 | 当前状态 |
|---|---|---|---|---|---|---|
| D-01 | 架构设计文档 | [规格 #1 Required Deliverable 1](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/1)、[ADR 索引](adr/README.md) | [#2](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/2)，最终 #38 | [总体架构设计](architecture/architecture-design.md) | 术语/ADR/实现追溯检查；最小与生产拓扑可按运行手册复现；评审记录无未解决阻断项 | BASELINE_READY / EVIDENCE_PENDING |
| D-02 | 系统架构图 | 规格 Deliverable 2、[ADR-0006](adr/0006-deployment-unit-boundaries.md) | [#3](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/3)，最终 #38 | [图表说明](architecture/system-diagrams.md)、[`system-architecture.mmd`](architecture/diagrams/system-architecture.mmd)、[`system-architecture.svg`](architecture/diagrams/system-architecture.svg) | CI 渲染源与 SVG 无漂移；图中组件、部署、同步/异步/遥测/密钥/存储连线均能追溯到实现 | BASELINE_READY / EVIDENCE_PENDING |
| D-03 | 核心时序图 | 规格 Deliverable 3、ADR-0010–0018、0042 | [#3](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/3)，实现 #13、#17–#18、#21–#27，最终 #38 | [`wecom-core-sequence.mmd`](architecture/diagrams/wecom-core-sequence.mmd)、[`wecom-failure-recovery-sequence.mmd`](architecture/diagrams/wecom-failure-recovery-sequence.mmd) 及 SVG | 企业微信回放/E2E Trace 与图中 ID、事务、审批、失败、Memory 和投递顺序一致；源与 SVG 无漂移 | BASELINE_READY / EVIDENCE_PENDING |
| D-04 | 数据模型设计 | 规格 Deliverable 4、[ADR-0040](adr/0040-tenant-composite-keys-and-rls.md)、[ADR-0041](adr/0041-versioned-cloudevents-json-contracts.md) | [#4](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/4)，实现 #9、#12–#31，最终 #38 | [数据模型与同步设计](architecture/data-model-and-sync.md)、[领域事件 Schema](contracts/domain-events.schema.json) | 数据库迁移、复合外键/RLS/分区测试、历史 Schema 兼容测试与核心实体实现逐项对应 | BASELINE_READY / EVIDENCE_PENDING |
| D-05 | 数据同步和幂等策略 | 规格 Deliverable 5、ADR-0010–0018、0042 | [#4](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/4)，实现 #13、#17–#18、#21、#27、#29，最终 #38 | [同步、并发、幂等和迁移章节](architecture/data-model-and-sync.md#7-同步并发和幂等) | 重复/乱序/并发/fence/Outbox/Memory/Tool/回复/迁移的公开边界与故障恢复测试 | BASELINE_READY / EVIDENCE_PENDING |
| D-06 | 多后端适配方案 | 规格 Deliverable 6、[ADR-0014](adr/0014-default-production-storage-profile.md)、[ADR-0015](adr/0015-versioned-online-storage-migration.md) | [#4](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/4)，实现 #28–#30，最终 #38 | [多后端职责与 Adapter 契约](architecture/data-model-and-sync.md#9-多后端职责与取舍) | 所有正式 Adapter 通过同一隔离、幂等、错误、删除、迁移、健康和可见性合约；成本/延迟基准 | BASELINE_READY / EVIDENCE_PENDING |
| D-07 | 生产风险清单 | 规格 Deliverable 7、45 项 Accepted ADR | [#5](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/5)，风险关联工单，最终 #38 | 本文第 2 节 | 16 项初始风险均有版本化测试/演练证据；不可接受风险零例外；剩余风险有期限和批准人 | REVIEW_PENDING / EVIDENCE_PENDING |
| D-08 | GitHub 实现代码 | 规格 Deliverable 8、[ADR-0001](adr/0001-deliver-a-complete-production-platform.md)、[ADR-0036](adr/0036-production-release-quality-gates.md) | [#6](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/6)–[#37](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/37)，最终 #38 | 当前仅有工程骨架与已合入设计资产；不得宣称实现完成 | 干净环境构建；完整后端/Web/迁移/Adapter/策略/测试/容器/Helm/GitOps/观测/备份；CI、安全供应链、部署和恢复全部通过 | EVIDENCE_PENDING |

## 4. 文档评审门禁

### 4.1 Gate 状态

Gate 状态由 GitHub 事实派生，不在本文手工伪造，并按 `FAIL` > `PASS` > `PENDING` 的顺序判定：

- `FAIL`：存在任一阻断项，不论 PR 是否开放。修复后重新走评审，不能口头豁免。
- `PASS`：#2–#5 均关闭；#5 PR 精确提交上的 `docs-gate` 与 `render-mermaid` 检查均成功；至少两个非作者评审人批准且已合入 `main`；没有未解决阻断项。即使仓库未把检查配置为分支保护必需项，也不得省略这两个检查。
- `PENDING`：不满足 `FAIL` 或 `PASS`，包括 Issue #5/PR 仍开放、检查未运行，或批准/合入尚未完成。

[Issue #6](https://github.com/douzhenyu/trpc-agent-service-douzhenyu/issues/6) 明确 blocked by #5；只有 Gate 为 `PASS` 才允许开始代码实现。文档合入之后发现的新事实仍须先更新设计或新增 ADR，再修改实现。

### 4.2 必检项与批准角色

| Gate ID | 必检项 | 通过条件 | 批准角色 |
|---|---|---|---|
| G-01 | 交付物完整性 | D-01–D-07 均有受控产物、主责工单和最终证据定义；D-08 有完整实现范围 | 架构负责人、QA 负责人 |
| G-02 | 规格与 ADR 一致性 | 关键决定均链接 Accepted ADR；无术语漂移、静默替代或新增部署边界 | 架构负责人 |
| G-03 | 数据与安全 | 租户边界、RLS/复合外键、身份、秘密、分级、审计、保留和删除不存在设计缺口 | 安全负责人、DBA/数据负责人 |
| G-04 | 分布式正确性 | 入站、Session、Tool、Memory、Outbox、回复和迁移的权威、幂等、失败及恢复语义闭合 | 后端负责人、SRE 负责人 |
| G-05 | 风险可验收性 | R-01–R-16 字段齐全、责任唯一、证据可执行；S4 风险没有无条件接受路径 | 安全负责人、QA 负责人 |
| G-06 | 文档机器检查 | `docs-gate` 验证本地链接及 JSON Schema 元校验/正反例；`render-mermaid` 成功生成对照产物，架构评审人比较上传产物与受控 SVG 后批准 | QA 负责人、架构负责人 |
| G-07 | 假设管理 | 每个未决项有默认处理、解决工单和 ADR 触发条件，不把未知项写成已确认能力 | 架构负责人及对应责任角色 |

同一人可覆盖多个角色，但作者不能批准自己的变更，且至少两个不同的非作者评审人批准。以下任一情况阻断 Gate：未解决 P0/P1 评审发现；规格/ADR 冲突；缺失风险责任人或验证方式；把待产出的证据写成已通过；新增第七类部署单元或改变一致性/安全边界却没有 ADR；文档链接、Schema 或图表检查失败。

## 5. 未解决假设与 ADR 触发条件

未决项不等于缺陷；默认处理必须保持现有 ADR，不得在实现中偷偷选择新架构。

| ID | 未解决项 | 当前安全默认 | 解决工单 / 责任角色 | 必须新增或替代 ADR 的触发条件 |
|---|---|---|---|---|
| A-01 | Outbox Publisher/Relay 归属写入服务后台任务、sidecar 还是共享 Relay 尚未冻结 | 只视为逻辑组件，不增加第七类业务部署单元 | #13 / 分布式执行负责人 | 需要独立部署、独立扩缩容或新网络信任边界，导致改变 [ADR-0006](adr/0006-deployment-unit-boundaries.md) |
| A-02 | Topic、重试/DLQ 名称、Consumer Group 和保留参数未冻结 | 保持独立领域 Topic、至少一次传递、原事件 ID 和 Session 分区语义；名称属于配置 | #13、#20–#21、#27 / 消息平台负责人 | 改变权威来源、分区键、至少一次语义或把 Redis 引入核心总线，需替代 ADR-0010/0041 |
| A-03 | 企业微信稳定事件 ID、回调确认正文、发送结果查询和客户端幂等能力需真实沙箱验证 | 入站无稳定 ID 时用版本化摘要；投递结果不确定时保守对账，不能宣称 Exactly Once | #22 / Channel 负责人 | 通道能力迫使取消持久 ledger、自动重试非幂等结果或改变 [ADR-0017](adr/0017-durable-inbound-idempotency-ledger.md)/0042 的默认取舍 |
| A-04 | Tool 审批恢复经哪个 API、Topic 和 Outbox 路径尚未冻结 | 使用持久审批/检查点，批准后重新取租约恢复；不占用 Worker | #18 / Tool 治理负责人 | 新增公开协议、部署单元、信任边界，或改变四眼审批/持久恢复语义 |
| A-05 | Summary/Memory 的触发阈值、批次和 Topic 名称未冻结 | 只消费已提交 Session Event，异步幂等；Memory 正常可见 P99≤5 秒且不阻塞当前回复 | #27 / Memory 负责人 | 需要同步阻塞回复、改变 Event 权威或放宽 [ADR-0013](adr/0013-eventually-consistent-memory.md) 可见性目标 |
| A-06 | 企业微信/飞书流式限流值、卡片字段和能力协商细节待沙箱测量 | 单聊合并更新，群聊处理中加最终回复；不支持更新则占位加最终消息，不逐 Token 调 IM API | #22、#23、#25 / Channel 负责人 | 改变默认交互策略、隐私边界、稳定 delivery ID 或至少一次投递语义 |
| A-07 | SQL/向量/对象分片阈值、迁移观察期和校验采样率需容量数据 | 先使用 ADR 默认拓扑；源端切换前权威、全量范围摘要加风险抽样，阈值保守配置 | #29、#34 / 数据平台与 SRE 负责人 | 改变存储权威、租户隔离层级、RPO/RTO 或取消可回滚观察期 |
| A-08 | `trpc-agent-py==1.1.19` 在实现开始时是否仍为可接受稳定版 | 先精确锁定已验证版本；只通过自动升级 PR、完整回归、Staging 和灰度升级 | #6、#37 / 平台维护负责人 | 新版本不兼容导致替换 Runner、改变框架/平台责任边界或公开契约，需新 ADR |
| A-09 | 四周计划假设技术、后端/Agent、前端、SRE/平台、QA/安全全职投入，接口评审 24 小时内闭环；实际可用性尚未确认 | 任一角色或评审 SLA 不满足即标记里程碑阻断并重排计划，不以降质或跳过门禁维持四周承诺 | #6、#34、#38 / 项目负责人及各门禁责任角色 | 资源缺口迫使削减生产范围、合并职责边界或改变质量门禁，需替代相关 ADR 并重估计划 |
| A-10 | 三故障域与温备环境、OIDC、真实 IM 测试租户及 Kafka/PostgreSQL/Redis/S3/OPA/Vault/模型端点第 1 天可用性尚未确认 | Fake 依赖只允许贯通骨架；缺失的真实基础设施或账号阻断对应里程碑及生产能力声明 | #6、#9、#22–#23、#34–#38 / SRE 负责人、Channel 负责人 | 环境缺口迫使改变生产拓扑、通道范围、隔离/恢复目标或以替代基础设施交付，需新增或替代 ADR |

当前 [ADR 索引](adr/README.md) 中 45 项 ADR 均为 `Accepted`，满足开工前冻结假设；G-02 在 Gate 判定时重新校验。后续不得直接改写已接受决定，出现冲突或边界变化时新增替代 ADR。Runner 内部 Model/Tool 循环次数属于框架实现细节，不作为未决架构选择；图和测试只固定治理、预算、副作用与提交边界。参数调优、Topic 名称和供应商字段在不触发上表条件时通过配置/契约/运行手册冻结，不为每个可逆值新增 ADR。

## 6. 最终验收记录规则

Issue #38 汇总验收时，每个 D/R 条目必须记录：证据 URI、提交或镜像 digest、环境、执行时间、执行人、批准人、结论和未关闭偏差。最终结论遵守以下规则：

1. 任一 D-01–D-08 未达 `ACCEPTED`，项目不得宣称完成。
2. 任何越权访问、任何重复副作用、秘密泄露、迁移不一致、灾备双主或恢复目标失败的风险仍为 `OPEN`/`ACCEPTED`，或任一 S4 风险缺少生产等价证据，发布阻断。
3. 文档、API、Schema、迁移、配置、图表、测试或实际部署不一致时，以失败处理；先修正文档或通过新 ADR 改变决策。
4. 证据只能证明其绑定的版本和环境；代码、配置、依赖或基础设施发生实质变化后，受影响证据必须重跑。

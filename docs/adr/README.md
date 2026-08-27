# 架构决策索引

本目录记录多租户 Agent 部署平台已经确认的架构决策。除非后续 ADR 明确替代，以下决策状态均为 **Accepted**；术语含义以仓库根目录的 [`CONTEXT.md`](../../CONTEXT.md) 为准。ADR 只记录难以逆转、容易产生歧义或存在真实取舍的决定，实施细节不在此目录提前固化。

## 产品与租户边界

- [0001：交付完整生产平台](0001-deliver-a-complete-production-platform.md)
- [0002：以企业私有化部署为首要运营模式](0002-private-enterprise-deployment.md)
- [0003：使用企业 OIDC 与平台 RBAC](0003-enterprise-oidc-and-platform-rbac.md)
- [0004：使用分层混合租户隔离](0004-hybrid-tenant-data-isolation.md)
- [0039：租户保持扁平并以 Tenant Group 聚合](0039-flat-tenants-with-non-inheriting-groups.md)
- [0040：使用 UUIDv7、租户复合外键与 PostgreSQL RLS](0040-tenant-composite-keys-and-rls.md)

## 运行拓扑、容量与连续性

- [0005：使用 Kubernetes 作为生产部署基座](0005-kubernetes-production-runtime.md)
- [0006：按扩缩容与故障域拆分部署单元](0006-deployment-unit-boundaries.md)
- [0007：采用可量化的首版容量基线](0007-initial-capacity-envelope.md)
- [0008：区分数据面与控制面可用性目标](0008-availability-slos.md)
- [0009：采用五分钟 RPO 与一小时 RTO](0009-disaster-recovery-objectives.md)
- [0030：关键链路关闭并显式降级增强能力](0030-fail-closed-critical-paths-and-explicit-degradation.md)
- [0037：使用 Helm 与 Argo 实施 GitOps 发布](0037-gitops-with-helm-and-argo.md)
- [0038：地域内高可用并采用跨地域温备](0038-multi-az-active-with-cross-region-standby.md)
- [0045：强制零信任服务通信](0045-zero-trust-service-communication.md)

## 执行、状态与数据后端

- [0010：使用 Kafka 兼容持久执行总线](0010-kafka-compatible-durable-execution-bus.md)
- [0011：使用无状态 Worker 与显式 Session 并发控制](0011-stateless-workers-and-session-concurrency.md)
- [0012：以不可变 Session Event 作为权威记录](0012-session-events-as-source-of-truth.md)
- [0013：异步生成最终一致的 Memory](0013-eventually-consistent-memory.md)
- [0014：采用 PostgreSQL、Redis、PGVector 与 S3 默认存储栈](0014-default-production-storage-profile.md)
- [0015：使用版本化配置档执行在线存储迁移](0015-versioned-online-storage-migration.md)
- [0041：使用版本化 CloudEvents JSON 契约](0041-versioned-cloudevents-json-contracts.md)

## IM 通道与投递

- [0016：使用通道范围身份与确定性 Session 隔离](0016-channel-scoped-identities-and-sessions.md)
- [0017：使用持久入站幂等账本](0017-durable-inbound-idempotency-ledger.md)
- [0031：使用通道能力自适应流式回复](0031-adaptive-im-streaming.md)
- [0042：IM 回复使用可对账的至少一次投递](0042-at-least-once-im-delivery.md)

## Agent、Knowledge、模型与评测

- [0021：使用不可变 Agent Release 与环境 Deployment](0021-immutable-agent-releases.md)
- [0022：生产模型调用统一经过 LLM Gateway](0022-central-llm-gateway.md)
- [0033：Knowledge 使用独立不可变 Revision 发布](0033-independent-knowledge-revisions.md)
- [0034：分离版本化 Admin API 与 Agent 协议入口](0034-versioned-admin-and-agent-apis.md)
- [0035：使用 Python 后端与 React 管理前端](0035-python-backend-and-react-console.md)
- [0036：生产发布必须通过分层质量门禁](0036-production-release-quality-gates.md)
- [0043：精确锁定并持续验证上游稳定版](0043-verified-upstream-update-policy.md)
- [0044：强制 Agent Release 评测门禁](0044-mandatory-agent-release-evaluation-gates.md)

## 治理、安全与合规

- [0018：显式分类工具副作用并持久化审批](0018-tool-side-effect-and-approval-policy.md)
- [0019：使用 Kubernetes gVisor Sandbox 执行不可信代码](0019-kubernetes-gvisor-sandbox-execution.md)
- [0020：使用 Vault 兼容密钥管理服务](0020-vault-compatible-secret-management.md)
- [0023：使用预算预留与结算硬门禁](0023-hard-budget-reservation-and-settlement.md)
- [0024：使用 OpenTelemetry 统一可观测性](0024-opentelemetry-observability-stack.md)
- [0025：使用不可变审计链路并默认保留一年](0025-immutable-audit-chain-and-retention.md)
- [0026：采用分类内容生命周期与可证明删除](0026-default-content-lifecycle-and-erasure.md)
- [0027：使用 OPA Policy Bundle 统一执行治理策略](0027-opa-policy-bundles-with-trpc-filters.md)
- [0028：使用四级数据分级强制出站治理](0028-data-classification-enforcement.md)
- [0029：高风险生产变更强制职责分离](0029-four-eyes-production-changes.md)
- [0032：使用分级 Tool 审批与持久恢复](0032-tiered-tool-approval-and-resume.md)

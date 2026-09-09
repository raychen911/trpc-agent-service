# 验收映射表

本文档按题目要求逐项映射代码、测试和证据文件。

## 多租户与节点部署

### 1.1 租户模型

**要求**: 设计租户模型,至少包含 tenant_id、应用配置、模型配置、工具权限、IM 通道配置、数据后端配置、审计策略。

**实现**:
- 代码: `trpc_service/tenant/models.py`
  - `TenantConfig`: 包含 `tenant_id`、`config_version`、`apps`、`channel_bindings`、`storage_profile`、`audit_policy`、`quota_policy`、`gray_release_policy`；模型和工具策略按 `AgentApp` 配置
- 测试: `tests/test_platform.py::PlatformTests::test_session_is_tenant_and_conversation_scoped`
- 证据: `docs/DATA_MODEL.md:3-80`、`docs/ARCHITECTURE.md:45-48`

### 1.2 节点部署拓扑

**要求**: 设计节点部署拓扑,说明 Agent Gateway、Agent Worker、Channel Adapter、Storage Adapter、Admin API、Telemetry Collector 等组件如何协作。

**实现**:
- 代码: `trpc_service/gateway/router.py` (Gateway/Worker)、`trpc_service/channels/` (Channel Adapter)、`trpc_service/storage/` (Storage Adapter)、`trpc_service/web/app.py` (Admin API)、`trpc_service/telemetry/` (Telemetry)
- 文档: `docs/ARCHITECTURE.md:5-36`
- 部署: `deployment/docker-compose.yml`、`deployment/kubernetes/platform.yaml`

### 1.3 多节点水平扩展

**要求**: 支持多节点水平扩展,说明用户消息如何路由到正确租户和正确 session。

**实现**:
- 代码: `trpc_service/gateway/router.py:110-182` (`dispatch`)
  - 路径: channel + account_id -> tenant/app -> session_id = hash(tenant_id + channel + account_id + user_id + agent_app_id)
- 测试: `tests/test_platform.py::PlatformTests::test_concurrent_session_updates_preserve_tenant_scope`
- 证据: `docs/ARCHITECTURE.md:51-53`

### 1.4 会话粘性（Sticky session）

**要求**: 说明是否需要 sticky session;如果不需要,说明如何依赖共享 Session / Memory 后端实现无状态 Worker。

**实现**:
- 文档: `docs/ARCHITECTURE.md:53-55`
  - 明确不需要 sticky session,Worker 依赖共享 Session/Memory/Inbox/Outbox 后端,InMemory 仅限单进程演示
- 测试: `tests/test_session_mailbox_v2.py::test_session_mailbox_expired_takeover_fences_old_worker`

### 1.5 租户隔离

**要求**: 设计租户隔离机制,包括配置隔离、数据隔离、工具权限隔离、日志脱敏和密钥管理。

**实现**:
- 配置版本隔离: `trpc_service/tenant/models.py:158-224` (`publish/rollback`)
- Redis key 前缀: `trpc_service/storage/redis_store.py` `_key`
- SQL 复合主键: `trpc_service/storage/sql_store.py`；PostgreSQL RLS 验证见 `tests/test_postgres_rls.py`
- 工具 Filter: `trpc_service/policy/tenant_filter.py`
- 日志脱敏: `trpc_service/security/secrets.py`、`trpc_service/telemetry/tracing.py`
- 密钥管理: `trpc_service/security/secrets.py`
- 测试: `tests/test_postgres_rls.py::PostgresRLSIntegrationTests::test_app_role_isolation_and_admin_visibility`
- 证据: `docs/ARCHITECTURE.md:47-50`、`tests/test_postgres_rls.py`

## 数据同步与多后端支持

### 2.1 多后端选择

**要求**: 支持不同租户选择不同数据后端,例如 InMemory、Redis、SQL、向量库、对象存储或外部 Memory 服务。

**实现**:
- 代码: `trpc_service/storage/` (base.py, in_memory.py, redis_store.py, sql_store.py, postgres_store.py, vector_store.py, object_store.py)
- 配置: `trpc_service/tenant/models.py:116-131` (`StorageProfile`)
- 证据: `docs/DATA_MODEL.md:281-290`

### 2.2 统一数据访问抽象

**要求**: 设计统一的数据访问抽象,说明 Session、Memory、Summary、Artifact、Knowledge、Audit Log 分别如何存储。

**实现**:
- 代码: `trpc_service/storage/base.py` (SessionStore, MemoryStore, SummaryStore, ArtifactStore, KnowledgeStore, AuditStore, IdempotencyStore, CompensationStore)
- 证据: `docs/DATA_MODEL.md:281-395`

### 2.3 数据同步策略

**要求**: 设计数据同步策略,至少覆盖: 多节点并发写入同一 session 的一致性、Session event/state/summary 的更新顺序、Memory 写入后的跨节点可见性、后端迁移方案、IM 消息重复投递幂等处理。

**实现**:
- 并发写: `trpc_service/storage/session_mailbox.py:230-426` (Inbox/Outbox、lease)
- 幂等: `trpc_service/storage/base.py:397-422` (IdempotencyStore)、`tests/test_platform.py::PlatformTests::test_webhook_dispatch_and_idempotency`
- 补偿: `trpc_service/storage/base.py:424-462` (CompensationStore)、`tests/test_platform.py::PlatformTests::test_sqlite_compensation_round_trip_does_not_override_idempotency`
- 后端迁移: `trpc_service/storage/migration_control.py`、`tests/test_platform.py::PlatformTests::test_migration_round_trip_with_sqlite_storage`
- Schema 迁移: `trpc_service/database/`、`tests/test_database_migrations.py`、`scripts/database_migration_gate.py`
- 证据: `docs/MIGRATION.md`、`scripts/database_migration_gate.py`、`tests/test_database_migrations.py`

### 2.4 一致性取舍

**要求**: 说明不同后端的一致性取舍,例如强一致、最终一致、读写延迟、成本和运维复杂度。

**实现**: `docs/MIGRATION.md:3-50`、`docs/ARCHITECTURE.md:61-70`

### 2.5 数据模型

**要求**: 给出一个最小数据模型或表结构示例,至少包含 tenant、agent app、session、message/event、memory、summary、channel binding、audit log。

**实现**: `docs/DATA_MODEL.md:81-240`

## IM 软件接入

### 3.1 IM Channel Adapter

**要求**: 设计 IM Channel Adapter,支持企业微信、微信客服、微信公众号、Telegram 或其他 IM 通道中的至少两类。

**实现**:
- 代码: `trpc_service/channels/` (base.py, web.py, wecom_ai_bot.py, feishu.py, telegram.py, wecom.py legacy)
- 运行时注册: `trpc_service/channels/registry.py` (默认 web、wecom_ai_bot、feishu、telegram)
- 测试: `tests/test_platform.py::PlatformTests::test_channel_registry`、`tests/test_platform.py::PlatformTests::test_legacy_wecom_registry_requires_explicit_opt_in`
- 证据: `docs/IM_INTEGRATION.md`、`trpc_service/channels/registry.py`

**企业微信主验收入口**: wecom_ai_bot (智能机器人 API 模式、BotID + BotSecret 长连接)。wecom (传统回调) 默认禁用,需 `ENABLE_LEGACY_WECOM=1` 才注册。

### 3.2 群聊/单聊 session_id 生成规则

**要求**: 说明群聊和单聊的 session_id 生成规则,以及用户跨群、跨租户时的隔离策略。

**实现**:
- 代码: `trpc_service/gateway/router.py:152-156`
  - session_id = hash(tenant_id + channel + account_id + user_id|group_id + agent_app_id)
- 证据: `docs/ARCHITECTURE.md:51-53`

### 3.3 IM 平台限制

**要求**: 考虑 IM 平台限制,例如消息长度、频率限制、异步回复、图片/文件消息、撤回或失败重试。

**实现**:
- 代码: `trpc_service/channels/reliable.py` (split_text、ChannelRateLimiter、重试/DLQ)、`trpc_service/channels/outbound_queue.py`、`trpc_service/policy/quota.py`
- 测试: `tests/test_platform.py::PlatformTests::test_reliable_delivery_retries_and_dead_letters`、`tests/test_platform.py::PlatformTests::test_quota_enforcer_rejects_qps_and_daily_usage`
- 证据: `docs/ARCHITECTURE.md:83-88`、`docs/IM_INTEGRATION.md`

## 治理、监控和安全

### 4.1 Filter 治理策略

**要求**: 使用 Filter 设计租户级治理策略,例如工具白名单、敏感信息脱敏、预算限制、危险工具二次确认、IM 用户权限校验。

**实现**:
- 代码: `trpc_service/policy/tenant_filter.py`
- 测试: `tests/test_platform.py::PlatformTests::test_tool_approval_requires_persistent_confirmation`、`tests/test_hardening.py::HardeningTests::test_tool_risk_and_budget_are_enforced`
- 证据: `docs/ARCHITECTURE.md:71-75`

### 4.2 监控指标

**要求**: 设计监控指标,例如请求量、模型调用耗时、工具调用耗时、IM 投递成功率、错误率、token 消耗、每租户成本、Session 后端延迟。

**实现**:
- 代码: `trpc_service/telemetry/metrics.py`
- 测试: `tests/test_platform.py::PlatformTests::test_gateway_preserves_inbound_traceparent_without_otel_exporter`
- 证据: `docs/CAPACITY.md:28-36`

### 4.3 OpenTelemetry trace

**要求**: 说明如何接入 OpenTelemetry 或等价 tracing,要求 trace 能串起 IM callback、Runner 执行、Tool 调用、Session / Memory 读写和 IM 回复。

**实现**:
- 代码: `trpc_service/telemetry/tracing.py`
- 测试: `tests/test_platform.py::PlatformTests::test_gateway_preserves_inbound_traceparent_without_otel_exporter`
- 证据: `docs/ARCHITECTURE.md:76-82`、`docs/SEQUENCE.md`

### 4.4 审计日志字段

**要求**: 设计审计日志字段,至少包含 tenant_id、channel、user_id、session_id、agent_name、tool_name、decision、latency、error_type、cost、trace_id。

**实现**:
- 代码: `trpc_service/storage/base.py:337-363` (AuditStore)
- 模型: `docs/DATA_MODEL.md:240-260`
- 测试: `tests/test_platform.py::PlatformTests::test_sdk_tool_adapter_executes_platform_tool_with_audit`

### 4.5 密钥管理和脱敏

**要求**: 说明密钥管理和脱敏策略,IM token、模型 API key、数据库密码不能明文出现在日志、trace 或错误报告中。

**实现**:
- 代码: `trpc_service/security/secrets.py`、`trpc_service/telemetry/tracing.py`
- 测试: `tests/test_platform.py::PlatformTests::test_secret_redaction_covers_text_and_nested_provider_data`、`tests/test_platform.py::PlatformTests::test_wecom_robot_webhook_url_can_be_secret_reference`
- 证据: `docs/ARCHITECTURE.md:47-50`

### 4.6 生产治理、可观测性与运维验收

**要求**: 提供存活探针、依赖检查就绪探针、受保护的 Prometheus/Trace 数据、租户隔离的审计查询，以及可自动执行的发布验收门禁。

**实现**:
- 接口: `trpc_service/web/app.py` (`/livez`、`/readyz`、`/metrics`、`/admin/v1/tenants/{tenant_id}/audit`)
- 脱敏与 Trace 保护: `trpc_service/storage/base.py`、`trpc_service/telemetry/tracing.py`
- 告警规则: `deployment/prometheus-alerts.yml`
- 部署探针: `deployment/kubernetes/platform.yaml`、`deployment/docker-compose.yml`
- 门禁: `scripts/observability_gate.py`、`scripts/release_gate.py`
- 测试: `tests/test_observability.py`
- 运维手册: `docs/OPERATIONS.md`

### 4.7 性能验收

**要求**: 对固定工作负载记录吞吐、P50/P95/P99、错误率和资源相关指标，并支持性能回归门禁。

**实现**:
- 门禁: `scripts/performance_gate.py`
- 负载驱动: `scripts/load_test_web_ui.py`
- 统一验收: `scripts/production_acceptance.py` 的 `12_performance_acceptance`
- 测试: `tests/test_performance_gate.py`
- 复现证据: 执行 `scripts/performance_gate.py` 和 `scripts/load_test_web_ui.py` 后，由脚本在本地生成吞吐、延迟和错误率 JSON；不依赖仓库内预置的日期化结果。

本地 fallback 工作负载默认使用 100 请求、10 并发，并支持传入上次报告进行回归比较；每次验收应在当前环境重新生成报告。真实模型性能仍需在固定模型、输入 token、资源拓扑和限流条件下单独验收。

## 故障恢复与运维

### 5.1 降级策略

**要求**: 设计节点故障、IM 重试、数据库短暂不可用、模型超时、工具执行失败时的降级策略。

**实现**:
- 代码: `trpc_service/gateway/router.py` (timeout/retry)、`trpc_service/storage/session_mailbox.py:230-426` (Inbox/Outbox、lease fencing)
- 测试: `tests/test_session_mailbox_v2.py::test_session_mailbox_expired_takeover_fences_old_worker`、`tests/test_hardening.py::HardeningTests::test_toxiproxy_cycle_requires_outage_and_recovery`
- 证据: `docs/ARCHITECTURE.md`、`docs/OPERATIONS.md`

### 5.2 灰度发布和回滚

**要求**: 说明如何做灰度发布和租户级配置回滚。

**实现**:
- 代码: `trpc_service/tenant/models.py:158-224` (publish/rollback/gray-release)
- 测试: `tests/test_platform.py::PlatformTests::test_tenant_versions_survive_repository_restart`、`tests/test_platform.py::PlatformTests::test_tenant_gray_release_resolves_candidate_by_session`
- 证据: `docs/ARCHITECTURE.md:45-48`

### 5.3 容量评估

**要求**: 说明如何做容量评估,例如每节点并发 session 数、平均 token 消耗、Redis / SQL QPS、IM 回调峰值。

**实现**: `docs/CAPACITY.md:11-27`

### 5.4 部署方案

**要求**: 设计最小可运行部署方案和生产推荐部署方案,可以使用 Docker Compose、Kubernetes 或等价部署方式描述。

**实现**:
- Docker Compose: `deployment/docker-compose.yml`
- Kubernetes: `deployment/kubernetes/platform.yaml`
- 测试: `tests/test_hardening.py::HardeningTests::test_production_kubernetes_manifest_passes_static_gate`
- 证据: `docs/ARCHITECTURE.md:113-149`

## 交付物

### 6.1 架构设计文档

**实现**: `docs/ARCHITECTURE.md` (2400+ 字)

### 6.2 系统架构图

**实现**: `docs/ARCHITECTURE.md:5-36` (Mermaid 图)

### 6.3 核心时序图

**实现**: `docs/SEQUENCE.md:5-87` (企业微信智能机器人消息 -> Agent 执行 -> Tool 调用 -> Session / Memory 写入 -> IM 回复)

### 6.4 数据模型

**实现**: `docs/DATA_MODEL.md:81-395`

### 6.5 数据同步和幂等策略

**实现**: `docs/MIGRATION.md`、`docs/OPERATIONS.md`

### 6.6 多后端适配方案

**实现**: `docs/MIGRATION.md:101-126`、`docs/DATA_MODEL.md:281-290`

### 6.7 风险清单

**实现**: `docs/RISKS.md` (15 项生产风险及缓解措施)

### 6.8 GitHub 代码实现

**实现**: 本仓库（已基于 tRPC-Agent-Python 实现平台层）。将仓库推送到目标 GitHub 地址后，审阅者可按 README 和本文件复现。

## tRPC-Agent-Python 复用边界

**要求**: 方案需要明确哪些能力可直接复用 tRPC-Agent-Python,哪些需要新增平台层模块。

**实现**: `docs/ARCHITECTURE.md:37-43`、`docs/IMPLEMENTATION.md:121-145`
- 复用: Runner、Session、Memory、Summary、Knowledge、Tool/MCP、Filter、模型 Provider、FastAPI/Web、Telemetry
- 新增平台层: tenant、gateway、channels、storage (Redis/SQL/PostgreSQL/vector/object)、policy、security、admin、deployment

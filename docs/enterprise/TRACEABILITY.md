# 需求追溯矩阵（Traceability Matrix）

> 将项目申请书的需求条目（2.1–2.5 + 交付物）逐条映射到实现文件与验证用例，
> 作为评审与答辩的依据。所有路径均相对于 `trpc_service/`（实现）
> 与 `tests/service/`（验证）。

## 2.1 多租户与节点部署

| 需求 | 实现 | 验证 |
|---|---|---|
| 租户模型（tenant_id/应用/模型/工具/IM/后端/审计） | `tenant/_models.py` | `test_tenants.py` |
| 租户 CRUD + MySQL 版本 + 回滚 + Redis 热加载 | `tenant/_manager.py`、`_persistence.py`、`_redis_cache.py` | `test_tenant_persistence.py` |
| 配置加载（YAML/JSON + env 展开） | `tenant/_loader.py` | `test_tenants.py::test_load_tenants_from_yaml_with_env_expansion` |
| 组件拓扑（Gateway/Worker/Channel/Storage/Admin/Telemetry） | `web/gateway/`、`agent/`、`channels/`、`workspace/`、`DESIGN.md` | `test_gateway.py`、`test_worker.py`、`test_e2e.py` |
| 消息路由到正确租户/session | `web/gateway/_app.py` + `agent/_worker.py` + `generate_session_id` | `test_e2e.py`、`test_worker.py` |
| 无 sticky session（共享后端 + 无状态 Worker） | `agent/_worker.py` + `workspace/_tenant_session_service.py` | `test_deployment.py::test_multiturn_continuity_uses_shared_backend` |
| 租户隔离（配置/数据/工具/日志脱敏/密钥） | `workspace/`、`tool/_filters.py`、`log/_masker.py`、`SecretStr` | `test_tenant_storage.py`、`test_adversarial.py::test_concurrent_cross_tenant_isolation_same_ids`、`test_tenants.py::test_channel_config_secret_not_leaked_in_repr` |

## 2.2 数据同步与多后端

| 需求 | 实现 | 验证 |
|---|---|---|
| 不同租户为 Session/Memory 选择 Redis/MySQL，Vector 选择 Memory/Qdrant，Object 选择 Local/S3/MinIO/COS | `tenant/_models.py::StorageBackendConfig` + `workspace/_router.py` | `test_tenant_storage.py`、`test_data_backends.py` |
| 统一数据访问抽象 | 复用 `SessionServiceABC`/`MemoryServiceABC`；新增 `VectorStoreABC`/`ObjectStoreABC` 及租户包装器 | `test_tenant_storage.py`、`test_data_backends.py` |
| Session/Summary 存储 | 复用 `sessions/`（summary 作为 summary event） | core `tests/sessions/` |
| Memory 存储 | 复用 `memory/` + `TenantMemoryService` | `test_tenant_storage.py::test_memory_search_key_is_tenant_scoped` |
| Summary/Artifact/Knowledge/Audit | MySQL 元数据表；Qdrant 向量；S3 兼容对象存储；Audit `log/_sql_sink.py` | `test_data_model.py`、`test_data_backends.py`、`test_audit.py::test_sql_audit_sink_persists_and_queries` |
| 多节点并发一致性 | Session lock + 配置 `config_version` CAS/outbox | `test_adversarial.py`、`test_tenant_persistence.py::test_two_managers_reject_stale_concurrent_update` |
| IM 幂等 | `web/gateway/_idempotency.py` | `test_gateway.py::test_gateway_dedups_redelivered_message`、`test_adversarial.py::test_idempotency_store_dedups_concurrent_duplicates` |
| 最小数据模型 | `data/schema.mysql.sql` + `DATA_MODEL.md` + `log/_sql_sink.py::AuditLogRecord` | `test_data_model.py`、`test_audit.py`、`test_tenant_persistence.py` |
| Session/Memory Redis→MySQL 真实迁移 | `workspace/_backend_migration.py` | `test_backend_migration.py` + Docker 单向实测 |
| 异构数据同步/幂等 | `storage_outbox`、版本/CAS、内容 checksum、确定性向量 point id | `SYNC_AND_IDEMPOTENCY.md`、`test_data_backends.py`、`test_data_model.py` |

## 2.3 IM 软件接入

| 需求 | 实现 | 验证 |
|---|---|---|
| 四类 Channel Adapter | `_wecom.py`、`_wechat_kf.py`、`_dingtalk.py`、`_feishu.py` | `test_channels.py`、`test_channel_send.py` |
| QQ Bot 扩展 Adapter | `_qq.py` + `web/gateway/_app.py` | `test_qq_channel.py`（签名、事件、发送与端到端回调） |
| 消息转换（IM↔Agent） | 四类 Adapter + `agent/_worker.py` | `test_channels.py`、`test_e2e.py` |
| 验签 | `_crypto.py` + 平台 token/secret 校验边界 | `test_channels.py::test_wecom_signature_verification`、四平台签名测试 |
| 账号绑定 + 身份映射 | 四类 Adapter + `DESIGN.md` §5.3 | `test_channels.py` |
| session_id 生成（单聊/群聊/跨租户） | `channels/_models.py::generate_session_id` | `test_channels.py::test_generate_session_id_*` |
| 平台限制（分段/流式/重试） | `channels/_models.py::split_text/split_text_bytes`、各 Adapter 的 send/send_stream | `test_channels.py::test_split_text_*`、`test_channel_send.py` |

## 2.4 治理、监控与安全

| 需求 | 实现 | 验证 |
|---|---|---|
| 工具白名单/黑名单 | `tool/_filters.py::ToolAllowlistFilter` | `test_governance.py` |
| 危险工具二次确认 | `tool/_hitl.py` + `agent/_worker.py`（确认回显→放行） | `test_worker.py::test_worker_hitl_confirmation_flow`、`test_budget_hitl_stream.py` |
| 敏感信息脱敏 | `tool/_redactor.py` + `ToolOutputRedactionFilter` | `test_governance.py::test_redaction_filter_masks_sensitive_output` |
| 预算限制 | `tool/_budget.py`（`BudgetTracker`/`ModelBudgetFilter`/原子 `reserve`） | `test_budget_hitl_stream.py`、`test_adversarial.py::test_budget_reserve_is_thread_safe` |
| 监控指标 | `metrics/_metrics.py` + Gateway/Queue/Worker/Storage/IM 埋点 + Collector/Prometheus | `test_observability.py`、`test_gateway.py`、`test_queue.py`、`test_worker.py`、`test_tenant_storage.py` |
| OTel 全链路 | `metrics/_observability.py` 的 Trace/Meter Provider、跨队列 carrier、业务 span | `test_observability.py::test_trace_context_survives_queue_carrier`、`test_queue.py` |
| 审计日志字段 | `log/_models.py::AuditLogEntry` | `test_audit.py::test_audit_entry_fields` |
| 密钥管理 + 脱敏 | `SecretStr` + `log/_masker.py::SecretMasker/RedactingLogFilter` | `test_audit.py::test_secret_masker_*`、`test_tenants.py` |

## 2.5 故障恢复与运维

| 需求 | 实现 | 验证 |
|---|---|---|
| ≥8 项生产风险：节点、重复/乱序、DB、模型、工具、隔离、成本、密钥、异构一致性等 | `DESIGN.md` §7（12 项风险表） + `tenant/_models.py::fallback_model` | 文档审查 + 对应故障/安全测试 |
| 灰度发布 + 配置回滚 | `tenant/_manager.py::rollback` + `DESIGN.md` §7 | `test_tenants.py::test_manager_versioning_and_rollback` |
| 容量评估 | `DESIGN.md` §7（参考值） | 文档级 |
| 最小/生产部署 | `deploy/docker-compose.minimal.yml` + `deploy/kubernetes/` | `test_deployment.py`（会话连续性）+ `test_deployment_compose.py`（静态部署契约） |
| SDK 复用与平台新增边界 | `DESIGN.md` §8 + `pyproject.toml` 外部依赖 | `test_deployment_compose.py::test_service_uses_the_sdk_as_an_external_dependency` |

## 交付物清单

| 编号 | 交付物 | 位置 |
|---|---|---|
| 1 | 模块源码 | `trpc_service/` |
| 2 | 测试工程 | `tests/service/`（服务模块行覆盖率门禁 95%，增量覆盖率门禁 85%） |
| 3 | 演示工程 | `examples/multi_tenant_saas/` |
| 4 | 部署配置 | `deploy/docker-compose.minimal.yml` + `deploy/docker-compose.observability.yml` + `deploy/kubernetes/` |
| 5 | 设计/数据/同步/后端/接入/监控/追溯文档 | `DESIGN.md`、`DATA_MODEL.md`、`SYNC_AND_IDEMPOTENCY.md`、`BACKEND_ADAPTERS.md`、`ONBOARDING.md`、`METRICS.md`、`TRACEABILITY.md` |
| 6 | 项目申请书 | 已提供 |

## 验收基线（2026-09-07）

- `pytest tests/service`：241 passed；
- `--cov=trpc_service --cov-fail-under=95`：95.78%；
- `diff-cover coverage.xml --fail-under=85`：95%；
- Redis pending reclaim/DLQ、跨节点锁、结果缓存、fallback model、Admin API、迁移双写、OTel Trace/Metrics 和 Prometheus 部署契约已纳入自动测试；
- 完整的环境、故障、迁移、容量和逐项验收方法见 `ACCEPTANCE_TEST_PLAN.md`。

# README 需求与可执行证据矩阵

| 验收 | 已实现代码/配置 | 聚焦证据 | 最终命令 |
|---|---|---|---|
| 1 | Gateway/Worker、tenant config、Redis/SQL/S3、Filter、OTel、Compose/Kubernetes | `tests/test_r3_kubernetes.py`、架构图 | `bash scripts/acceptance_final.sh` |
| 2 | `storage/schema.py`、迁移 `0001`–`0014` | `tests/integration/test_sql_migrations.py` | `pytest -q tests/integration` |
| 3 | `channels/wecom`、`channels/feishu`、`channels/webhook`、`ChannelBinding` | `tests/test_wecom_service.py`、`tests/test_feishu_service.py`、`tests/test_webhook_routes.py`、`tests/integration/test_webhook_ingress.py` | `bash scripts/acceptance_webhook.sh` |
| 4 | Redis state、PostgreSQL repositories、`S3ArtifactService`、租户 `knowledge_search` | `tests/integration/test_r1_multibackend_state.py`、`tests/integration/test_r1_artifact_knowledge.py`、`tests/test_agent_components.py` | `bash scripts/acceptance_final.sh` |
| 5 | `telemetry/`、Gateway/Worker tracing、IM service | `tests/test_telemetry.py`、`tests/test_trace_flow.py`、`tests/test_wecom_service.py` | `bash scripts/acceptance_final.sh` |
| 6 | `docs/production-risks.md` 与 R3 fault matrix | `bash scripts/acceptance_r3_operations.sh` | `bash scripts/acceptance_final.sh` |
| 7 | `agent/`、`governance/`、`channels/` 平台边界 | `docs/architecture.md` 责任矩阵 | `pytest -q tests/test_agent_components.py tests/test_governance_tool_filter.py` |

README 具体交付物的详细事实源依次为：架构和时序见 `docs/architecture.md`；表结构见 `docs/data-model.md`；同步幂等见 `docs/data-sync-idempotency.md`；后端选择见 `docs/backend-strategy.md`；风险见 `docs/production-risks.md`。R4B 的 `scripts/acceptance_final.sh` 是唯一完整验收入口；它将明确区分本地必需失败和未配置真实外部 IM。

## 具体要求映射

| README 具体要求 | 可运行实现 | 交付事实源 |
|---|---|---|
| tenant、应用/模型/工具/IM/后端/审计策略 | `config/tenant.py`、`channel_bindings`、Admin | `tests/test_tenant_governance_config.py` |
| Gateway/Worker/Adapter/Storage/Admin/Collector 拓扑 | `gateway/`、`worker/`、`channels/`、`compose.yaml` | `docs/architecture.md` |
| 多节点路由与无 sticky session | Rendezvous routing、Redis lease、共享 state | `tests/test_gateway_routing.py`、`tests/test_execution_coordinator.py` |
| 配置、数据、工具、日志和密钥隔离 | versioned config、namespace、Filter、安全日志、secret_ref | `tests/test_governance_tool_filter.py`、`tests/test_safe_logging.py` |
| Session/Memory/Summary/Artifact/Knowledge/Audit 的统一访问 | backend resolver、SDK service、S3/SQL repositories | `docs/backend-strategy.md`、R1 integration tests |
| 并发、写入顺序、可见性、后端迁移、重复 IM | Redis lease、receipt、水位、`state-backend-migrate` | `docs/data-sync-idempotency.md` |
| 至少两种 IM、身份、群聊、限长、媒体、重试 | WeCom/Feishu facade、binding、identity、delivery policy | `tests/test_wecom_service.py`、`tests/test_feishu_service.py` |
| Filter、预算、限流、审批、用户准入 | `governance/`、`worker/governance.py`、approval repository | `tests/test_worker_content_governance.py` |
| 指标、Trace 与审计字段 | `telemetry/`、execution audit、request usage | `tests/test_metrics.py`、`tests/test_audit_query.py` |
| 节点/IM/数据库/模型/工具故障 | 安全映射、receipt、delivery policy、operations matrix | `scripts/acceptance_r3_operations.sh` |
| 灰度、回滚、容量、最小和生产部署 | rollout repository、capacity probe、Compose、Kubernetes | `tests/test_rollout.py`、`tests/test_r3_kubernetes.py` |

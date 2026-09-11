# 生产风险与缓解

| 风险 | 检测 | 缓解 | 可执行证据 |
|---|---|---|---|
| R1 Worker 死亡 | health 与 receipt processing | 健康路由；重复消息不重跑 | `tests/test_execution_coordinator.py` |
| R2 Redis 租约故障 | readiness/固定错误码 | Session/乱序/限流失败关闭 | `tests/test_state_backend.py` |
| R3 SQL 短暂不可用或旧连接 | repository readiness | 503 固定映射，不写半个终态 | `tests/test_sql_tenant_repository.py` |
| R4 模型超时/错误 | Worker error code | 有界错误，不跨节点自动重试 | `tests/test_worker_service.py` |
| R5 Tool 副作用重复 | receipt/approval 状态 | 业务键幂等、审批单赢家 | `tests/test_worker_approval_service.py` |
| R6 IM 重复或乱序 | receipt 与 Redis watermark | 重放或前置拒绝 | `tests/test_channel_order_gate.py` |
| R7 IM 部分投递 | SDK terminal 状态 | 仅未发送重试，终态审计 | `tests/test_channel_delivery.py` |
| R8 secret/正文泄漏 | 安全日志与 Span 契约 | secret_ref、白名单日志、脱敏错误 | `tests/test_safe_logging.py` |
| R9 binding 串租户 | binding 唯一约束 | account+binding 身份投影、禁用即停止服务 | `tests/test_channel_identity.py` |
| R10 Collector 不可达 | exporter 结果 | 吞 exporter 故障，不影响业务 | `tests/test_telemetry.py` |
| R11 Artifact 半写 | SQL version state | 上传后发布，pending 不可读 | `tests/test_s3_artifact_service.py` |
| R12 迁移/切换错误 | snapshot digest | 离线校验后版本化切换，可前向回滚 | `tests/test_state_migration.py` |
| R13 灰度配置错误 | rollout 状态/审计 | 稳定分桶、abort/promote、配置历史 | `tests/test_rollout.py` |
| R14 容量耗尽 | 聚合 p50/p95/错误计数 | 有界 capacity probe、HPA、租户限流/预算 | `scripts/capacity_probe.py` |

`scripts/acceptance_r3_operations.sh` 将 Worker、Redis、PostgreSQL、模型、Tool、IM、Collector 七类故障的聚焦契约合并为可运行矩阵。真实外部 IM 可用性由操作者凭据决定，不可用时应记录 external unavailable，而不是把 Console 当作外部通过。

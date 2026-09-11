# 故障恢复、容量与部署

## 故障策略

| 故障 | 策略 |
|---|---|
| Gateway/Worker 宕机 | 健康检查摘除；Redis pending reclaim；SQL lease 过期后接管 |
| IM 重复/乱序 | inbox 唯一键；按获得 Session lease 的顺序提交，非平台原始发送顺序；幂等 outbound |
| Redis/SQL 短暂不可用 | SQL Inbox/Outbox 成功提交后才 ACK；Redis 故障由 Outbox 补偿；SQL 不可用返回失败让 IM 重试，不可先 ACK 丢消息 |
| 模型超时/限流 | 租户 fallback model、超时、预算门禁，失败话术可配置 |
| 工具失败 | 幂等工具有限重试；副作用工具未知结果进入人工处理 |
| 部分流式失败 | 关闭当前流，补发一次状态，不重复整段回答 |
| 配置发布错误 | immutable revision、tenant 级灰度、ETag 回滚 |

## 容量估算

先用压测测得单 Worker 稳态吞吐 `r_worker`、活跃 Runner 峰值内存 `m_runner`、模型并发 `c_model` 和 Redis/SQL QPS。`worker_count = ceil(peak_rate / r_worker × 1.3)`；并发上限取 `min(memory/m_runner, c_model, tool_pool)`。Gateway 以 callback P95 < 500 ms 为门禁，生产目标 IM 投递成功率 ≥99.9%。

## 部署分层

- 最小可运行：一个 FastAPI Gateway + 一个 Worker + InMemory（本仓库 `python -m trpc_service._cli demo`），用于离线演示。
- 联调 Compose：Gateway/Worker/Admin、Redis、PostgreSQL、Prometheus、OTel Collector，真实模型和 IM 通过环境变量注入。Collector 的默认 `debug` exporter 只用于联调，生产应替换为 Tempo/Jaeger 等持久 Trace 后端。
- 生产 Kubernetes：Gateway/Worker/Admin 独立 Deployment/HPA，Redis/PostgreSQL HA，Vector/Object Storage、PDB、NetworkPolicy、Secret/KMS、Prometheus/OTel；队列恢复和投影任务独立扩容。

当前具备租户 revision/CAS/回滚 API，但 HMAC bucket 选择尚未连接生产灰度控制器，自动阈值回滚未实现。可执行方案是先挑选测试租户发布新 revision，观察错误率、队列 lag、成本和投递指标，再按租户批次扩大；越阈值由值班人员停止并调用回滚 API。消息入队固定 config_version，重试仍使用原 revision；不保证整个 Session 永久固定 revision。自动化可由部署流水线/渐进发布控制器调用同一 API。

## 容量记录模板

每个目标环境记录：回调峰值 λ、平均 turn 时间 T、输入/输出 token、模型并发额度、工具耗时、每 turn 的 SQL/Redis 命令数、每个并发 Runner 的内存增量。并发需求约为 `λ × T`；token/min 需求为 `60 × λ × (input_tokens + output_tokens)`；后端 QPS 约为 `λ × 每 turn 命令数 + relay/heartbeat/投影 QPS`。

例如假设 20 消息/秒、平均 8 秒、每轮 1000 token，则约需 160 个并发 Runner 和 120 万 token/min 模型额度；若每轮 12 次 SQL/8 次 Redis 操作，则基础负载为 240/160 QPS，另加后台任务和安全余量。以上仅为计算示例，不是实测容量或 SLO 达标证据。按 1、2、4 Worker 测试扩容收益，并记录 P50/P95/P99、错误率、队列 lag、RSS、数据库连接数与恢复时间。

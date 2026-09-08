# 实现审计与生产差距

本文按题目验收项核对“设计文档、参考实现、自动化证据”是否闭环。结论是：仓库已经具备可运行的
多租户平台骨架和关键可靠性机制，但真实 IM 账号、托管后端与跨区域容灾仍需要在目标环境验收，
不能仅凭 Mock 单测宣称生产就绪。

## 已形成闭环的能力

| 验收域 | 已实现 | 自动化证据 |
|---|---|---|
| 多租户与节点化 | 租户配置/版本、Gateway/Worker/Outbox 分离、共享 Session/Memory、Redis Stream 接管 | `test_e2e.py`、`test_queue.py`、`test_adversarial.py` |
| 隔离与治理 | tenant namespace、Tool 白/黑名单、HITL、用户授权、预算、脱敏、SecretRef | `test_tenant_storage.py`、`test_governance.py`、`test_secrets.py` |
| 可靠消息 | Gateway 去重、receipt lease/fencing、事务 Outbox、分片 checkpoint、DLQ/重放 | `test_reliable_messaging.py` |
| 多后端 | Redis/MySQL Session/Memory、Qdrant、S3 兼容对象存储、迁移校验 | `test_data_backends.py`、`test_backend_migration.py` |
| IM 接入 | 企业微信、微信客服、钉钉、飞书、QQ 的验签/规范化/分段/发送边界 | `test_channels.py`、`test_qq_channel.py`、`test_channel_send.py` |
| 可观测与审计 | callback→queue→Worker→Runner→Tool→Storage→reply trace，租户成本/延迟/投递指标 | `test_observability.py`、`test_governance.py`、`test_audit.py` |
| 部署运维 | Compose、Kubernetes、HPA、Collector/Prometheus、migration Job、灰度/故障模板 | `test_deployment*.py`、`test_runtime_entrypoints.py` |

## 本轮审计修复

1. session 哈希改为结构化四元组，消除分隔符和单聊/群聊 ID 碰撞；这是有意的破坏性变更，旧
   `session_id` 不继续兼容。
2. 平台不提供 `message_id` 时使用原始 callback body 的 SHA-256，避免所有无 ID 消息共用空幂等键。
3. 增加租户/通道级入口限流；生产通过 Redis Lua 跨 Gateway 原子执行，故障时 fail-closed。
4. 增加 Tool 调用次数、耗时和 `tool.call` span，补齐要求中的工具可观测性。
5. MySQL 审计索引迁移改为条件创建，允许 DDL 已自动提交但 ledger 未写入后的安全重跑。
6. 引入统一运行时资源所有权；Gateway 关闭后台任务并释放幂等键，Registry、Worker 和 Outbox
   关闭其 HTTP/Redis/SQL 资源，配置失效的旧 Channel Adapter 延迟到安全关闭。
7. 修正文档中的节点路由描述：当前数据面由 Redis consumer group 分配，rendezvous router 不在
   消息正确性关键路径；补充可计算的容量公式和示例。

## 上生产前仍需完成

| 优先级 | 差距/风险 | 完成口径 |
|---|---|---|
| P0 | 真实 IM 协议仍主要由本地 fixture/Mock 覆盖 | 至少用企业微信和另一类 IM 沙箱完成验签、超时 ACK、附件、限流、撤回、Token 刷新和失败重试契约测试 |
| P0 | 外部副作用 Tool 无法由平台单方面保证 exactly-once | 每个写操作 Tool 接受 `turn_id/operation_id`，下游建唯一键并实现“超时先查询、再补偿” |
| P0 | 备份恢复与跨可用区 RTO/RPO 尚无真实演练证据 | 对 Redis AOF、MySQL PITR、对象版本和向量重建做恢复演练并记录 RTO/RPO |
| P1 | Audit `retention_days` 尚未连接归档/清理 Job；SQL 长故障没有持久本地 spool | 增加不可篡改归档、保留期 Job、磁盘有界 spool 与丢弃告警，验证恢复后补传 |
| P1 | 平台只去重，不按各 IM 的 sequence/timestamp 主动丢弃过期乱序事件 | 为每种通道定义 sequence cursor、允许窗口和乱序审计策略，并做重排/过期测试 |
| P1 | Redis→MySQL 可运行迁移会检测漂移并拒绝切换，但零停机 dual-write 尚未接入在线路径 | 接入 durable migration outbox/dual-write，完成 shadow read、单租户切换和回滚演练 |
| P1 | SecretResolver 内置 env/file，KMS/Vault 只是扩展接口 | 接入目标云 KMS/Vault，验证租户授权、轮换、吊销和审计 |
| P1 | Worker HPA 示例按 CPU，未直接按 queue lag/pending age 扩容 | 将 Collector 指标接入 Prometheus Adapter/KEDA，以 lag、oldest age 和模型配额共同伸缩 |
| P2 | Milvus/pgvector 是已声明的工厂扩展点，不是内置完整 Adapter | 若选用对应后端，补实现、租户过滤、schema 迁移和真实集群契约测试 |
| P2 | `max_concurrent_sessions` 当前是容量目标，不是分布式并发硬门禁 | 若业务需要硬配额，增加带 lease 的租户并发 semaphore，并验证 Worker 崩溃后配额回收 |

## 验证基线

2026-09-08 本轮执行结果：`326 passed`，总行覆盖率 `95.78%`，增量覆盖率 `95%`；Flake8、
wheel/sdist 构建、最小/可观测 Compose 配置解析和 migration `--check` 均通过。Kubernetes 清单有
静态契约测试；本机没有 `kubectl/kustomize`，因此仍应由 CI 渲染 production/performance/canary
overlay 后再发布。

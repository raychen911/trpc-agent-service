# 容量评估与部署建议

以下数值是起始规划值，不替代真实压测。容量规划应先按租户峰值、模型延迟、IM 回调峰值和存储 QPS 预估，再通过压测逐步校准。

## 容量估算

| 维度 | 估算方式 | 初始建议 |
| --- | --- | --- |
| Worker 并发 | 受 CPU、模型 I/O 和工具调用影响 | 单 Worker 同时处理 16 - 32 个 in-flight 请求 |
| Gateway 吞吐 | 受 IM 回调峰值和验签开销影响 | 按观测峰值的 2 - 3 倍预留实例 |
| Redis QPS | 每次消息约 8 - 15 次 Key 操作，加上队列读写 | 500 msg/s 需要约 5k - 10k Redis ops/s |
| PostgreSQL QPS | 事件、状态、Memory、Summary、审计写入 | 100 msg/s 需要约 500 - 900 次写操作/s |
| 向量库 QPS | 知识检索和索引写入 | 按租户知识库规模和 top-k 次数评估 |
| IM 回调峰值 | 业务高峰时的消息突发 | 网关与队列应能吸收 2 - 5 分钟抖动 |
| 补偿队列吞吐 | 派生写失败后的重放任务数 | 单补偿进程每租户每轮 100 条起步，按积压和失败率扩容 |
| 队列保留时间 | 处理延迟、故障恢复时间 | 至少覆盖一个最大故障恢复窗口 |

## 资源拆分建议

1. `Agent Gateway` 侧重网络 I/O、验签、幂等和路由，优先扩容副本数。
2. `Agent Worker` 侧重模型调用、工具执行和存储写入，优先按队列深度和 p95 耗时扩容。
3. `Redis` 需要关注内存、热 Key、慢查询和持久化策略。
4. `PostgreSQL` 需要关注连接池、锁等待、索引、分区和归档。
5. `Vector Store` 需要关注 embedding 写入速率、索引刷新时间和召回质量。
6. `Object Store` 主要关注带宽、签名 URL 生命周期和大对象生命周期策略。

## 监控基线

建议至少监控以下指标：

| 指标 | 说明 |
| --- | --- |
| 请求量 | 按 tenant、channel、agent_app 维度统计。 |
| 模型调用耗时 | 平均值、p95、p99、重试次数、超时率。 |
| 工具调用耗时 | 按工具名和审批结果拆分。 |
| IM 投递成功率 | 区分首次成功、重试成功和死信失败。 |
| 错误率 | 验签失败、幂等冲突、配额拒绝、模型失败、存储失败。 |
| Token 消耗 | 按租户和 Agent App 统计输入、输出和总量。 |
| 每租户成本 | 结合模型、存储、IM 和向量库成本汇总。 |
| Session 后端延迟 | Redis/SQL 读写延迟、CAS 冲突率和重试率。 |

当前实现已经暴露的核心指标包括：`trpc_agent_requests_total`、`trpc_agent_model_latency_seconds`、`trpc_agent_tool_calls_total`、`trpc_agent_tool_latency_seconds`、`trpc_agent_im_deliveries_total`、`trpc_agent_errors_total`、`trpc_agent_tokens_total`、`trpc_agent_cost_total` 和 `trpc_agent_session_backend_latency_seconds`。这些指标均带有 tenant、channel、tool、operation 或 status 等必要维度，可支撑租户级容量评估、故障定位和成本核算。

## 部署方式

### 最小可运行部署

适合本地开发、联调和演示：

- 1 个 Gateway
- 1 - 2 个 Worker
- 1 个 Redis
- 1 个 PostgreSQL
- 1 个 Compensation Worker
- 1 个对象存储或本地文件目录
- 1 个可选的 OpenTelemetry Collector

### 生产推荐部署

适合真实多租户环境：

- Gateway 使用 Kubernetes Deployment，按回调峰值自动扩容
- Worker 使用独立 Deployment，按队列长度和执行耗时自动扩容
- Compensation Worker 独立 Deployment，按 pending 补偿任务数、重试失败率和最老任务年龄扩容
- Redis 使用 Sentinel/Cluster 或云托管高可用实例
- PostgreSQL 使用云托管高可用实例和只读副本
- 向量库、对象存储、Telemetry Collector 使用独立托管组件
- Admin API 与 Gateway 分开暴露，管理面必须做鉴权和 RBAC

## 压测建议

压测时应至少覆盖：

1. 单租户高并发会话。
2. 多租户混合流量。
3. Redis 或 PostgreSQL 短暂不可用。
4. 模型 429、5xx 和超时。
5. IM 重复投递和失败重试。
6. Worker 崩溃后重放和幂等恢复。
7. Memory、Summary、Audit 后端短暂不可用后，补偿队列能在恢复后 drain。

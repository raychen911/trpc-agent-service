# 企业监控链路与指标调试

本文说明服务从 IM 回调到最终回复的指标和 Trace 如何贯通，以及如何在本地和生产环境验证。实现入口主要位于
`trpc_service/metrics/`，业务埋点分布在 Gateway、Redis Streams、Worker、Storage Adapter 和 IM 发送路径。

## 1. 两类指标出口

服务同时保留两个出口，但用途不同：

- `/admin/metrics` 在未配置 `PROMETHEUS_URL` 时返回当前进程内诊断快照，`scope=process`；配置后则查询
  Prometheus 并返回 Gateway 与 Worker 的跨进程聚合，`scope=prometheus`。Prometheus 暂时不可用时会自动回退到
  本地快照，并通过 `source_error` 标明原因。
- OpenTelemetry Metrics 是跨进程生产出口。Gateway 和 Worker 分别把指标通过 OTLP HTTP 推送到 Collector，
  Collector 的 Prometheus exporter 在 `:8889/metrics` 暴露聚合抓取端点，Prometheus 再负责时序存储、查询和告警。

因此，仅安装 OTel Metrics 包不够。完整链路必须同时具备应用 `MeterProvider`、OTLP Metric exporter、Collector
的 metrics pipeline，以及 Prometheus 或其他指标后端。本项目已经把这四段接通。

## 2. 完整消息链路

```text
IM callback
  │ agent_callback_total / agent_callback_duration_ms
  ▼
Gateway 验签、解析、幂等
  │ agent_callback_enqueue_total
  ▼
Redis Streams enqueue/read/claim/ack
  │ agent_queue_operation_total / duration_ms
  ▼
Worker task
  │ agent_worker_task_total / duration_ms / retry_total / dlq_total
  ├─ result cache       agent_result_cache_total
  ├─ session lock       agent_session_lock_duration_ms
  ├─ Session/Memory     agent_storage_operation_total / duration_ms
  ├─ Runner             agent_requests_total / agent_runner_latency_ms
  └─ IM reply           agent_im_delivery_total / duration_ms / parts
```

Gateway 的 `im_callback` span 是入口父 span。入队时，W3C Trace Context 被写入 `TaskMessage.trace_headers`；Worker
消费后恢复上下文，再创建 `worker.process_task`、结果缓存、存储和回复 span。这样 Gateway 与 Worker 即使位于不同
Pod，也共享同一个 `trace_id`。`trace_id`、`message_id`、`session_id` 等高基数字段只进入 Trace、日志或审计，
不会成为 Prometheus label。

## 3. 指标目录

| 指标 | 类型 | 主要维度 | 含义 |
|---|---|---|---|
| `agent_callback_total` | Counter | tenant、channel、outcome | Gateway 收到并处理的回调数 |
| `agent_callback_duration_ms` | Histogram | tenant、channel、outcome | 从 HTTP 路由进入到 ACK 的耗时 |
| `agent_callback_enqueue_total` | Counter | tenant、channel、outcome | 回调写入任务队列的结果 |
| `agent_queue_operation_total` | Counter | operation、outcome、error_type | Redis Streams 后端操作数 |
| `agent_queue_operation_duration_ms` | Histogram | operation、outcome | enqueue/read/claim/ack/DLQ 等耗时 |
| `agent_worker_task_total` | Counter | tenant、channel、outcome | Worker 每次任务投递尝试 |
| `agent_worker_task_duration_ms` | Histogram | tenant、channel、outcome | 单次消费处理耗时 |
| `agent_worker_retry_total` | Counter | tenant、channel、error_type | 未 ACK、等待重投的任务数 |
| `agent_queue_dlq_total` | Counter | tenant、channel | 超过最大尝试次数后进入 DLQ 的任务数 |
| `agent_result_cache_total` | Counter | tenant、channel、outcome | 结果缓存 hit/miss，用于避免回复失败时重跑 Agent |
| `agent_session_lock_duration_ms` | Histogram | tenant、phase、outcome | 同一 Session 锁的等待和持有耗时 |
| `agent_storage_operation_total` | Counter | tenant、backend、data_type、operation、outcome | Session/Memory/Vector/Object 操作数 |
| `agent_storage_operation_duration_ms` | Histogram | 同上 | Storage Adapter 操作耗时 |
| `agent_session_backend_latency_ms` | Histogram | tenant、backend、outcome | Session get-or-create 整体耗时 |
| `agent_requests_total` | Counter | tenant、channel、outcome | Agent turn 数，包含 HITL 和前置失败 |
| `agent_runner_latency_ms` | Histogram | tenant、channel、outcome | SDK Runner 执行耗时 |
| `agent_llm_input_tokens_total` | Counter | tenant、model | 模型输入 token 数 |
| `agent_llm_output_tokens_total` | Counter | tenant、model | 模型输出 token 数 |
| `agent_llm_cost_total` | Counter | tenant、model | 根据模型价格估算的累计成本 |
| `agent_budget_rejection_total` | Counter | tenant、model | 因租户预算不足被拒绝的模型调用数 |
| `agent_budget_daily_token_limit` | Gauge | tenant | 当前租户每日 token 上限 |
| `agent_budget_tokens_used` | Gauge | tenant | 当日已使用 token |
| `agent_budget_tokens_reserved` | Gauge | tenant | 执行中模型调用预留的 token |
| `agent_budget_daily_cost_limit` | Gauge | tenant | 当前租户每日成本上限 |
| `agent_budget_cost_used` | Gauge | tenant | 当日已使用成本 |
| `agent_im_delivery_total` | Counter | tenant、channel、outcome | 最终 IM 回复尝试和结果 |
| `agent_im_delivery_duration_ms` | Histogram | tenant、channel、outcome | Adapter 回复耗时 |
| `agent_im_delivery_parts` | Histogram | tenant、channel、outcome | 一次逻辑回复的分段数 |

`outcome` 使用有限枚举，例如 `success`、`error`、`duplicate`、`retry`、`dead_letter`；`error_type` 只记录异常类名。
Admin 概览中的“消息回调”只汇总 `agent_callback_total{outcome="success"}`，“验证回调”单独汇总
`outcome="challenge"`，因此平台 URL 验证不会再被误认为真实用户消息。
代码会拒绝 `user_id`、`session_id`、`message_id`、`trace_id`、`request_id` 和 URL 等高基数指标属性。
本地快照使用 `tenant_id`，导出到 OTel 时会规范化为 `tenant.id`。Collector 已开启 resource-to-telemetry
转换，因此 Prometheus 中还能用 `service_name` 和 `service_instance_id` 区分 Gateway、Worker 和具体实例；点号属性
在 Prometheus 中会规范化为下划线名称。上游 SDK 的 `gen_ai.*` 模型/工具指标也会进入相同 MeterProvider；Collector
在写入 Prometheus 前删除其中的 `gen_ai.user.id`，防止用户规模直接放大时序基数。

## 4. 本地快速验证

不启动 Collector 时，可发一条测试消息后读取当前 Gateway 进程：

```bash
curl -sS \
  -H "X-Admin-API-Key: ${ADMIN_API_KEY}" \
  "http://127.0.0.1:8080/admin/metrics?tenant_id=tenant_a"
```

重点检查 `scope=process`、`service_name`、对应 counter 是否增加，以及 histogram 的 `count/sum/max`。如果使用独立
Worker，这个接口只能检查 Gateway 指标；Worker 指标应通过 OTel/Prometheus 检查。

启动完整 Compose 监控链路：

```bash
docker compose \
  -f deploy/docker-compose.minimal.yml \
  -f deploy/docker-compose.observability.yml \
  up --build -d

curl -sS http://127.0.0.1:8889/metrics | grep '^agent_'
```

Prometheus 页面位于 `http://127.0.0.1:9090`。先用 `{__name__=~"agent_.*"}` 确认序列存在，再查询例如：

```promql
sum by (service_name, outcome) (rate(agent_callback_total[5m]))
sum by (service_name, outcome) (rate(agent_worker_task_total[5m]))
increase(agent_queue_dlq_total[10m])
sum by (service_name, channel) (rate(agent_im_delivery_total{outcome="error"}[5m]))
sum by (tenant_id) (agent_llm_input_tokens_total + agent_llm_output_tokens_total)
max by (tenant_id) (agent_budget_tokens_used) / max by (tenant_id) (agent_budget_daily_token_limit)
```

诊断顺序建议从链路两端向中间收敛：先看 Gateway callback，再看 enqueue 和 queue operation，然后看 Worker task，
最后看 Runner、Storage 与 IM delivery。若 Gateway 成功但 Worker 无数据，检查 Redis Streams 和 Worker；若进程内
快照增加但 Prometheus 无数据，检查应用 OTLP 环境变量、Collector 日志、`:8889/metrics` 和 Prometheus target。

## 5. 配置与生产注意事项

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 未设置 | 同时启用 Trace 与 Metrics 的 OTLP HTTP 基地址 |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | 未设置 | 只启用 Trace 时的完整信号地址 |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | 未设置 | 只启用 Metrics 时的完整信号地址 |
| `OTEL_SERVICE_NAME` | 调用方默认名 | Gateway/Worker 必须设置为不同服务名 |
| `OTEL_SERVICE_INSTANCE_ID` | HOSTNAME/主机名 | 区分进程或 Pod 实例 |
| `OTEL_METRIC_EXPORT_INTERVAL` | `60000` | 周期导出毫秒数，最小 1000；非法值回退默认值 |
| `PROMETHEUS_URL` | 未设置 | Admin API 查询 Prometheus 的内部地址；未设置时使用进程内快照 |

进程退出时会关闭 MeterProvider 和 TracerProvider，触发剩余数据 flush。Kubernetes 示例通过 headless Collector
Service 和 Prometheus DNS 服务发现抓取每个 Collector 副本，避免只抓到负载均衡后的随机实例。仓库内 Prometheus
使用 `emptyDir` 只是可运行参考；正式环境应改为 PVC、Prometheus Operator 或托管监控，并配置远端长期存储、
Alertmanager、保留周期和告警接收人。

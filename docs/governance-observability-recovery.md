# 治理、可观测性与故障恢复

## 1. 租户治理 Filter

租户的 `audit_policy.governance` 是统一策略入口。消息进入 Runner 前依次执行 IM ACL、输入
PII 脱敏和预算预留；Runner 返回后结算 token/成本并执行输出脱敏。拒绝和放行决策均写入
Audit Log。示例：

```json
{
  "governance": {
    "im_acl": {
      "allow_channels": ["telegram", "wecom"],
      "allow_users": ["user-1"],
      "deny_users": ["blocked-user"]
    },
    "pii": {"redact_input": true, "redact_output": true},
    "budget": {
      "daily_requests": 1000,
      "daily_tokens": 500000,
      "daily_cost": 100,
      "reserved_output_tokens": 2048,
      "reserved_cost": 0.1
    }
  }
}
```

PII Filter 当前识别邮箱、中国大陆手机号、身份证号和银行卡号。工具权限配置中的
`requires_confirmation=true` 会在 tRPC-Agent `before_tool_callback` 中检查
`RunConfig.custom_data.confirmed_tools`；没有显式确认时阻断工具调用。预算按租户和自然日存于
SQL，调用前原子预留，调用完成后按实际 usage 结算。

## 2. Trace 与指标

HTTP 中间件接受 W3C `traceparent`，响应返回 `x-trace-id`。webhook 入队时把传播载体写入消息
metadata，Inbound Worker 和 Transactional Outbox 消费时恢复父上下文，因此异步边界不会切断
trace。主要 span 包括 `im.consume`、`gateway.route`、`agent.execute`、工具治理、
`storage.commit_turn` 和 `outbox.deliver`。

Prometheus 指标位于 `GET /metrics/`，包括 HTTP 请求/耗时、Gateway 消息、持久化入站队列结果、
Agent 耗时、工具治理决策、存储耗时、Outbox 投递结果及健康节点数。启用 OTLP：

```powershell
$env:TRPC_SERVICE_OTEL_ENABLED="true"
$env:TRPC_SERVICE_OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4318/v1/traces"
```

## 3. IM 持久化队列与异步回复

Telegram/企业微信 webhook 只完成验签、归一化和 SQL 入队，随即返回 `accepted`/`success`，不再
等待模型。唯一约束 `(tenant_id, channel, external_message_id)` 消除 IM 重投。Worker 使用带租约
的 claim；进程退出后，过期的 `processing` 消息可被其他节点接管。执行完成后，Inbox 状态、
回复 Outbox 和执行账本在同一个 SQL 事务内提交。Outbox 再通过 Telegram `sendMessage` 或企业微信
主动消息 API 异步回复，失败自动重试。

查询入站状态：

```http
GET /gateway/v1/messages/{message_id}
X-Gateway-Token: ...
```

## 4. Runner/平台 SQL 双事务恢复

外部 Runner 与平台 SQL 无法组成真正的分布式事务，平台采用执行账本和状态机实现可恢复的
Saga：

```text
pending -> runner_started -> runner_completed -> platform_committed
                                                -> delivery_enqueued
                   \-> uncertain
```

- Runner 成功后先持久化完整回复；若平台 Event/State/Summary 事务失败，重试直接复用该回复，
  不再次调用模型或工具。
- Event、State、Summary、Memory Outbox 和 `platform_committed` 在同一个 SQL 事务中写入。
- 进程在 Runner 调用中断时无法判断工具是否产生副作用，状态标为 `uncertain`，自动重放停止，
  避免危险工具重复执行。
- 运维人员核对外部副作用后，可明确选择重试或终止：

```http
POST /gateway/v1/executions/{execution_id}/resolve
X-Gateway-Token: ...
Content-Type: application/json

{"action":"retry"}
```

`retry` 是显式的人工授权，会将对应 Inbox 重新变为可领取；`fail` 永久终止本次执行。

## 本地验证

```powershell
$env:PYTHONUTF8="1"
.\.venv\Scripts\python.exe -m pytest -q
.\start.ps1
```

随后访问 `http://127.0.0.1:8000/docs`，调用 `/health/live` 后访问
`http://127.0.0.1:8000/metrics/`。在 Admin API 创建租户时设置上述治理策略，发布包含 IM binding
的应用，再向 webhook 投递消息；webhook 返回的 `message_id` 可用于轮询状态。

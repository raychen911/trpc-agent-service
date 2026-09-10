# Contract: Local Message HTTP v1 Compatibility

**Feature**: 003-shared-state-multinode-flow
**Normative Baseline**: [002 local-message-http.md](../../002-multitenant-local-message-flow/contracts/local-message-http.md)

## Compatibility Rule

POST /v1/local/messages 的 headers、HMAC canonical string、request body、统一 response
envelope、duplicate suppression、trace_id/original_trace_id 和认证非披露语义全部保持
002 v1 兼容。第三阶段不得：

- 在请求中增加能够授予 tenant 权限的字段。
- 改变签名输入、时间窗口或 Binding 信任根。
- 改变 succeeded、duplicate、processing、conflict 的既有含义。
- 将 owner_trace_id 或 first_claim_trace_id 冒充 execution_trace_id。
- 因请求落到不同 Worker 而改变同一业务状态的回复语义。

## Shared Runtime Node Selection

本地验证可以把相同 v1 请求发送到：

- http://127.0.0.1:8001/v1/local/messages（worker-a）
- http://127.0.0.1:8002/v1/local/messages（worker-b）

端口只用于测试选择节点，不进入签名和业务身份。同一个签名请求可以发送到任一节点；
正确性不得依赖端口或 sticky session。

## Additional Safe Errors

第三阶段只增加 [error-semantics.md](./error-semantics.md) 中定义的
configuration_unavailable、state_backend_unavailable、session_busy 和 lease_lost。
这些错误使用既有 envelope：

~~~json
{
  "status": "failed",
  "trace_id": "<current-delivery-uuid>",
  "original_trace_id": null,
  "data": {
    "tenant_id": "<only-after-authentication>",
    "platform_session_id": "<only-after-resolution>",
    "external_message_id": "<validated-id>",
    "delivery_action": "none"
  },
  "error": {
    "code": "state_backend_unavailable",
    "message": "Shared state is unavailable.",
    "retryable": true,
    "execution_started": false
  }
}
~~~

认证前发生 configuration_unavailable 时，data 不包含 tenant/session，响应不得证明
binding 是否存在。

## Trace Rules

- trace_id 是本次 delivery，在两个节点上都按 v1 创建/继承。
- processing.original_trace_id 指向当前 owner_trace_id。
- cached terminal original_trace_id 指向 execution_trace_id。
- owner 接管会更新 owner_trace_id，但不修改 first_claim_trace_id。
- Audit/Recovery/Redis records 必须能从 execution_trace_id 关联实际执行节点。

## Health and Readiness

### GET /healthz

- 继续返回 200 与 status=ok，仅表示进程事件循环和 HTTP app 存活。
- 不查询 Redis/PostgreSQL，不泄露 node/backend details。

### GET /readyz

- shared profile 在 Redis、PostgreSQL schema/config 均可验证时返回 200：

~~~json
{"status":"ready"}
~~~

- 任一依赖不可验证时返回 503：

~~~json
{"status":"not_ready"}
~~~

- 响应不得包含连接 URL、凭据、tenant、表名或 vendor exception。

## Compatibility Tests

- 002 的 HTTP contract tests 对 local InMemory profile 保持原样通过。
- 同一请求/响应测试对 worker-a 和 worker-b 参数化运行。
- 首次成功在 A、duplicate 在 B：结果文本一致，B suppress，original_trace 指向 A 的执行。
- 同会话第一轮在 A、第二轮在 B：Session ID 相同且上下文连续。
- 同签名发送到不同端口：认证结果一致。
- 新错误只增加结果类别，不改变已有字段的类型或含义。

# HTTP API 与预期效果

接口类型以 [schemas.py](../trpc_service/web/schemas.py) 和运行时生成的 `/docs` 为准。开发模式可以免 Token；其他环境中，普通 API 使用 `X-Tenant-Token`，Admin API 使用 `X-Admin-Token`。企业环境可以在这一基础上由接入网关扩展 OIDC、RBAC 和用户级权限。

`im-demo` 会额外注册 `/im` 和 `/api/v1/dev/im/*`，这些接口只用于本地验证：

- `bootstrap` 返回可选通道；
- `messages` 按真实协议结构生成 IM 消息并送入异步链路；
- `messages/{request_id}` 查询 Request 与 Outbox；
- `faults` 设置下一次模拟故障。

这些开发接口只由 `im-demo` 命令注册，普通 `serve` 与生产角色仅暴露正式业务接口。详见 [IM 接入与本地可视化验证](im.md)。

## 1. Chat、异步任务与 SSE

三个入口共用请求体：

```json
{"tenant_id":"demo","app_id":"assistant","user_id":"student","session_id":"lesson-1","message":"你好","idempotency_key":"lesson-1-turn-1"}
```

| 路由 | 行为 | 如何判断成功 |
|---|---|---|
| POST /api/v1/chat | 直接执行并聚合 | 200，text/events/usage |
| POST /api/v1/chat/async | 持久请求、入队 | 202，request_id/state/status_url |
| POST /api/v1/chat/stream | 直接执行并输出 SSE | 以 `completed` 事件表示完整执行成功 |
| GET /api/v1/tenants/{tenant_id}/requests/{request_id} | 查租户范围内任务 | succeeded 携带 result；失败携带 error_code |

如果 Header 和请求体都提供幂等键，以 `X-Idempotency-Key` 为准。

- 同一个键、同一业务内容：异步接口返回原 `request_id`；同步接口在任务完成后返回原结果，处理中返回 409。
- 同一个键、不同业务内容：返回 409 和 `idempotency_payload_conflict`。

SSE 用于当前连接的实时输出。连接中断后，客户端可以用同一幂等键调用同步 Chat 获取已经聚合的结果。

任务状态中的计数含义如下：

- `attempts`：Worker 处理任务的次数；
- `model_attempts`：实际发起模型调用的次数，包括供应商侧重试；
- `successful_model_calls`：完整返回且没有模型错误的调用次数；
- `recovery_count`：通过 retry、Pending reclaim 或 reserved 补入队恢复的次数；
- `result.usage`：已经取得的 token 用量和估算成本。

普通请求通常对应 `1、1、1、0`。若第一次模型调用被取消，随后由另一 Worker 重试成功，通常对应 `2、2、1、1`。

事件聚合器用最终完整文本覆盖同一 Event 的 partial 片段，usage 按唯一的非 partial Event 统计。SDK 返回 error Event 时，请求进入失败状态。SSE 异常会输出经过脱敏的 error 事件，客户端以 `completed` 事件确认完整执行成功。

## 2. IM

`POST /api/v1/channels/{binding_id}/webhook` 用于 Telegram，并校验官方 secret header。服务先验证 Binding 与 ACL，再把消息写入队列并返回 202；重复消息复用原 `request_id`。

企微没有可供公网提交 decoded Frame 的 HTTP 入口：向企微绑定的该路由提交会返回 405。只有已认证 WSClient 回调可以归一化企微消息。

微信客服绑定复用上述路径，协议不同：

| 方法 | 输入 | 成功结果 | 失败结果 |
|---|---|---|---|
| GET | query: msg_signature/timestamp/nonce/echostr | 200，返回解密后的纯文本 echostr | Binding 缺失返回404，验签或接收者错误返回403 |
| POST | query: msg_signature/timestamp/nonce；body: Encrypt XML | 通知保存后返回200和纯文本success，Agent由后台处理 | 验签错误403，消息类型错误422，保存失败503 |

微信客服入口接收官方加密通知，真实消息由 Worker 调用 `sync_msg` 拉取。详见 [微信客服](customer-service.md)。Request `succeeded` 表示 Agent 已完成；客服发送 `UNKNOWN` 表示远端收件结果仍待核查，两种状态分别描述执行和投递阶段。

## 3. Artifact 与 Knowledge

上传：
```json
{"tenant_id":"demo","app_id":"assistant","name":"note.txt","mime_type":"text/plain","content_base64":"aGVsbG8="}
```

- POST /api/v1/artifacts：201，返回平台生成的 artifact_id、checksum、MIME、大小。
- GET /api/v1/tenants/{tenant_id}/artifacts/{artifact_id}：返回 metadata 和 base64。资源查询限定在当前租户范围，未找到时返回 404。
- POST /api/v1/knowledge：字段 tenant_id/app_id/title/text；返回 document_id。
- GET /api/v1/tenants/{tenant_id}/apps/{app_id}/knowledge/search?q=agent&limit=5：只返回该租户/应用结果。

内置对象存储支持 10 MiB 以内的文件，HTTP 示例入口使用 JSON base64。大文件可以由外部对象存储 Provider 提供分片上传能力。内置 Knowledge Provider 使用确定性关键词匹配；接入向量库时替换 Provider，Agent 继续使用相同的 `knowledge_search` Tool 契约。

## 4. 管理 API

| 路由 | 输入/含义 |
|---|---|
| GET /api/v1/admin/tenants | 当前活动快照 |
| PUT /api/v1/admin/tenants/{tenant_id}/config | 完整 TenantConfig，版本递增 |
| POST /api/v1/admin/tenants/{tenant_id}/rollback | {"version":1} |
| POST /api/v1/admin/approvals | tenant_id/user_id/session_id/tool_name/arguments_json |
| POST /api/v1/admin/approvals/{id}/decision | {"approve":true,"actor":"admin"} |
| POST /api/v1/admin/migrations | 创建迁移；可设置 batch_size、shadow_sample_rate、rollback_window_seconds |
| GET /api/v1/admin/migrations/{id} | 查看阶段、checkpoint、计数和错误 |
| POST /api/v1/admin/migrations/{id}/advance | 执行一个阶段或一个有限批次 |
| GET /api/v1/admin/migrations/{id}/items | 查看逐Session/Memory资源的hash、状态和重试次数 |
| POST /api/v1/admin/migrations/{id}/rollback | 调用 Provider 的回源逻辑 |

审批使用内部 user/session ID，可以从 Chat 结果或任务记录中取得。批准后返回一次性 Token。

Chat 请求同时提交 `approval_id`、`approval_token`、`approval_tool_name` 和 `approval_arguments_json`。服务端消费 Token 后，Tool Filter 会比较实际参数 Hash，使一次授权只覆盖指定工具及指定参数。

生产容器注册 Redis→PostgreSQL 和 PostgreSQL→Redis 两种 `session_memory` Provider。后端 URL 来自租户已发布配置，源端与当前活动的 Session/Memory 后端对应。接口校验资源类型和迁移方向，受支持的组合才会创建迁移任务。

## 5. 错误与探针

公共业务错误形如 `{"code":"session_busy","message":"...","request_id":"...","retryable":true}`。FastAPI 输入校验保留框架原生 `detail`，平台业务错误使用统一错误码、`request_id` 和 `retryable` 字段，调用方可据此区分格式错误与业务失败。

401 表示认证失败；403 表示 ACL 或审批校验失败；404 表示资源或角色路由缺失；409 表示幂等冲突或处理中；422 表示格式或消息类型错误；429 表示预算或通道限流；501 表示缺少对应 Provider；503 表示外部依赖或会话锁暂时异常。其他异常统一返回内部错误，并通过日志和 Trace 保留异常类型。

如果 Redis 中存在旧幂等记录，而 PostgreSQL 中缺少对应 Request，接口返回 503 `admission_in_doubt` 并设置 `retryable=false`。运维人员核对原任务和副作用状态后再决定后续处理。

Summary 或 Memory 失败时，请求进入 `retryable_failed`。SSE 的 delta 表示生成过程，`completed` 表示后处理与结果提交均已完成。HTTP/SSE 使用统一执行错误，具体失败阶段由内部恢复记录判断。

`healthz` 表示进程能够响应；`readyz` 检查该角色依赖，依赖异常时返回 503；`metrics` 输出 Prometheus 文本。测试见 [HTTP/SDK 回归](../tests/test_v3_resumption.py)。

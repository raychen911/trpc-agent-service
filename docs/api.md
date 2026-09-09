# HTTP API 与预期效果

接口类型以 [schemas.py](../trpc_service/web/schemas.py) 和运行时生成的 `/docs` 为准。开发模式可以免 Token；其他环境中，普通 API 使用 `X-Tenant-Token`，Admin API 使用 `X-Admin-Token`。这是项目提供的基础鉴权方式，不等同于完整的 OIDC 或 RBAC 系统。

`im-demo` 会额外注册 `/im` 和 `/api/v1/dev/im/*`，这些接口只用于本地验证：

- `bootstrap` 返回可选通道；
- `messages` 按真实协议结构生成 IM 消息并送入异步链路；
- `messages/{request_id}` 查询 Request 与 Outbox；
- `faults` 设置下一次模拟故障。

普通 `serve` 和生产进程不会开放这些接口，详见 [IM 接入与本地可视化验证](im.md)。

## 1. Chat、异步任务与 SSE

三个入口共用请求体：

```json
{"tenant_id":"demo","app_id":"assistant","user_id":"student","session_id":"lesson-1","message":"你好","idempotency_key":"lesson-1-turn-1"}
```

| 路由 | 行为 | 如何判断成功 |
|---|---|---|
| POST /api/v1/chat | 直接执行并聚合 | 200，text/events/usage |
| POST /api/v1/chat/async | 持久请求、入队 | 202，request_id/state/status_url |
| POST /api/v1/chat/stream | 直接执行并输出 SSE | 收到 completed；未收到不应视为完成 |
| GET /api/v1/tenants/{tenant_id}/requests/{request_id} | 查租户范围内任务 | succeeded 携带 result；失败携带 error_code |

如果 Header 和请求体都提供幂等键，以 `X-Idempotency-Key` 为准。

- 同一个键、同一业务内容：异步接口返回原 `request_id`；同步接口在任务完成后返回原结果，处理中返回 409。
- 同一个键、不同业务内容：返回 409 和 `idempotency_payload_conflict`。

SSE 不提供 `Last-Event-ID` 断点续传。任务完成后，可以用同一幂等键调用同步 Chat 获取聚合结果。

任务状态中的计数含义如下：

- `attempts`：Worker 处理任务的次数；
- `model_attempts`：实际发起模型调用的次数，包括供应商侧重试；
- `successful_model_calls`：完整返回且没有模型错误的调用次数；
- `recovery_count`：通过 retry、Pending reclaim 或 reserved 补入队恢复的次数；
- `result.usage`：已经取得的 token 用量和估算成本。

普通请求通常对应 `1、1、1、0`。若第一次模型调用被取消，随后由另一 Worker 重试成功，通常对应 `2、2、1、1`。

SDK 的 partial 文本不会与最终完整文本重复拼接，usage 只统计非 partial 的唯一 Event。SDK 返回 error Event 时，请求不会被标记为成功。SSE 发生普通异常时会输出不含内部细节的 error 事件；取消或网络中断也可能直接终止连接，因此客户端必须以 `completed` 事件作为成功依据。

## 2. IM

`POST /api/v1/channels/{binding_id}/webhook` 用于 Telegram，要求官方 secret header；正常或重复消息返回 202，重复保留原 request_id。先验证绑定与 ACL，再入队，不等待模型。

企微没有可供公网提交 decoded Frame 的 HTTP 入口：向企微绑定的该路由提交会返回 405。只有已认证 WSClient 回调可以归一化企微消息。

微信客服绑定复用上述路径，协议不同：

| 方法 | 输入 | 成功结果 | 失败结果 |
|---|---|---|---|
| GET | query: msg_signature/timestamp/nonce/echostr | 200，纯文本解密 echostr | 未配置404，验签/接收者错误403 |
| POST | query: msg_signature/timestamp/nonce；body: Encrypt XML | 通知保存后200纯文本success，不等待Agent | 验签403、不支持通知422、保存失败503 |

不接受普通 JSON 客户消息替代加密通知；真实消息由 Worker 调用 sync_msg 拉取。详见 [微信客服](customer-service.md)。客服发送 UNKNOWN 表示远端结果不确定，与 Request succeeded 不矛盾：Agent完成不代表客户已收件。

## 3. Artifact 与 Knowledge

上传：
```json
{"tenant_id":"demo","app_id":"assistant","name":"note.txt","mime_type":"text/plain","content_base64":"aGVsbG8="}
```

- POST /api/v1/artifacts：201，返回平台生成的 artifact_id、checksum、MIME、大小。
- GET /api/v1/tenants/{tenant_id}/artifacts/{artifact_id}：返回 metadata 和 base64。跨租户与不存在统一 404。
- POST /api/v1/knowledge：字段 tenant_id/app_id/title/text；返回 document_id。
- GET /api/v1/tenants/{tenant_id}/apps/{app_id}/knowledge/search?q=agent&limit=5：只返回该租户/应用结果。

对象存储限制 10 MiB，HTTP 教学入口使用 JSON base64，并由部署层设置相同或更严格的请求体上限；大文件由外部对象存储的分片上传接口处理。内置 Knowledge Provider 使用确定性关键词匹配；接入向量库时替换 Provider，不改变 Agent 的 `knowledge_search` Tool 契约。

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

Chat 请求同时提交 `approval_id`、`approval_token`、`approval_tool_name` 和 `approval_arguments_json`。服务端消费 Token 后，Tool Filter 还会比较实际参数 Hash。只批准工具名，不能授权任意参数。

生产容器已注册 Redis→PostgreSQL 和 PostgreSQL→Redis 的 `session_memory` Provider。后端 URL 只从租户已发布配置读取，不能由 API 请求临时指定。源端必须与当前活动配置的 Session/Memory 后端一致；其他资源类型或后端组合会明确拒绝，不会空跑后标记 `completed`。

## 5. 错误与探针

公共业务错误形如 `{"code":"session_busy","message":"...","request_id":"...","retryable":true}`。FastAPI 输入校验保留框架原生 `detail`，平台业务错误使用统一错误码、`request_id` 和 `retryable` 字段，调用方可据此区分格式错误与业务失败。

401 表示认证失败；403 表示 ACL 或审批拒绝；404 表示资源或角色路由不存在；409 表示幂等冲突或处理中；422 表示格式或消息类型不支持；429 表示预算或通道限流；501 表示 Provider 不可用；503 表示外部依赖或会话锁暂时不可用。未识别异常统一返回内部错误，并通过日志和 Trace 保留异常类型。

如果 Redis 中存在旧幂等记录，但 PostgreSQL 找不到原 Request，接口返回 503 `admission_in_doubt`，并设置 `retryable=false`。这类请求需要核查，不能换键重跑。

Summary 或 Memory 失败时，请求进入 `retryable_failed`，SSE 不发送 `completed`。已经输出的 delta 只是临时内容，不表示最终提交成功。HTTP/SSE 使用统一执行错误，具体失败阶段由内部恢复记录判断。

`healthz` 只表示进程能够响应；`readyz` 检查该角色依赖，未就绪时返回 503；`metrics` 输出 Prometheus 文本。测试见 [HTTP/SDK 回归](../tests/test_v3_resumption.py)。

# 数据模型与关系

真实表定义位于 `trpc_service/storage/schema.py`，迁移为 `0001`–`0014`。下表列出核心关系；向量库作为 Knowledge 文档的派生检索索引，不承担配置或审计事实存储。

| 资源 | 主键/约束 | 关系与用途 |
|---|---|---|
| `tenant_configs` | `tenant_id`，当前 `version` | tenant 的 agent app、模型、工具、governance、backend_profile、audit_policy |
| `tenant_config_versions` | `(tenant_id, version)` | tenant 当前配置的不可变历史；配置乐观并发和前向回滚 |
| `tenant_config_rollouts` | `rollout_id` | 关联 tenant，保存 active/candidate version、比例和状态 |
| `channel_bindings` | `binding_id`；`(channel, external_account_id)` 唯一 | IM 账号到 tenant/app 的唯一授权；只存 `secret_ref` 及可选 `webhook_token_ref`/`webhook_aes_key_ref` |
| `channel_binding_versions` | `(binding_id, version)` | binding 的历史版本，关联 tenant/binding |
| SDK Session/Event/State | SDK namespace `(app,user,session)` | Redis 或 SQL state backend；租户/绑定投影进入 user/session namespace |
| SDK Memory/Summary | 同 tenant app namespace | Redis 或 SQL；Summary 由 Event/State 后更新 |
| `message_receipts` | `receipt_id`；业务键唯一 | 每条 IM/API message 的 claim、配置版本、请求和终态 |
| `message_audit_events` | `audit_id` | receipt 的 accepted/completed/failed 摘要审计 |
| `execution_audit_events` | `audit_id`；receipt 四元复合 FK | 内容、agent、tool、delivery 决策；delivery 可无 receipt |
| `request_usage_records` | `(tenant_id, request_id)` | 单请求 token/cost；receipt 四元身份 FK |
| `tenant_usage_daily` | `(usage_date, tenant_id, model_profile)` | 原子每日聚合；`NULL` 表示 unknown，不伪造零 |
| `tool_approval_requests` | `approval_id`；`(receipt_id,function_call_id)` 唯一 | review 工具暂停、决定、受控一次执行 |
| `tool_approval_audit_events` | `audit_id` | 审批 created/decided/terminal 追加审计 |
| `artifact_metadata` / `artifact_versions` | `(tenant_id, artifact_path[,version])` | PostgreSQL 发布目录；对象字节在 S3/MinIO；版本状态 pending/available/deleted |
| `knowledge_documents` | `(tenant_id, document_id)` | SQL tenant-scoped knowledge 文本和 JSON metadata |

逻辑关系为：tenant 配置决定 agent runtime 和后端；binding 决定 channel 输入属于哪个 tenant/app；一个 receipt 关联配置版本、request 和审计；审批、执行审计、用量记录都以 receipt 的 tenant/request/version 身份约束。Session、Event、State、Memory、Summary 是 SDK 的共享后端对象，且 namespace 已含 tenant 投影，避免跨租户可见。

JSON 配置要求 `backend_profile={state_backend: redis|sql, artifact_backend: s3, knowledge_backend: sql, audit_backend: sql}`；`audit_policy` 要求 retention 天数和 delivery 事件保留方式。两者进入配置版本历史，因此回滚仍能复现该版本的运行边界。

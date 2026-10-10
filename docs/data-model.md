# 数据模型

PostgreSQL 16 + pgvector 是默认权威存储。所有租户表以 `tenant_id` 开头建立复合主键/索引；运行账号
不是表 owner，事务先执行 `set_config('app.tenant_id', tenant, true)`，RLS 同时约束读取与写入。
跨租户 binding 解析只允许调用最小权限 `SECURITY DEFINER` 函数，函数固定 `search_path` 且只返回
路由所需字段。

| 领域 | 表 | 关键约束 |
|---|---|---|
| 控制面 | `tenants`, `agent_apps`, `config_revisions`, `storage_profiles`, `tenant_policies` | revision 不可变；tenant control_version 用 ETag CAS |
| 管理幂等 | `admin_idempotency` | `(tenant_id,idempotency_key)`；请求 hash 不同则 409 |
| 通道 | `channel_bindings`, `channel_identities` | 外部账号只能通过 binding 进入租户；secret 仅存 SecretRef |
| 接入/投递 | `inbound_messages`, `outbound_messages`, `delivery_attempts` | 外部消息唯一键；outbound 保留 pending/sent/failed/ambiguous |
| Session | `sessions`, `session_turns`, `turn_intents`, `session_events`, `session_summaries` | lease_epoch/fencing；event sequence 连续；summary CAS 到 up_to_sequence |
| 长期数据 | `memories`, `artifacts`, `knowledge_items`, `knowledge_embeddings` | PG 元数据权威；向量和对象投影可重建 |
| 运维治理 | `outbox_events`, `dead_letters`, `tool_executions`, `confirmation_challenges`, `audit_logs`, `migration_checkpoints` | 至少一次、人工重放、一次确认和可恢复迁移 |

消息幂等键为 `(tenant_id, channel, account_id, external_message_id)`。Session 保存 `version`、
`next_sequence`、`lease_epoch/owner/expires_at`；turn 保存 pinned `config_version`、attempt、fencing token
和状态。事件使用 `(tenant_id,session_id,sequence)` 主键并以 `event_id` 再去重。

Memory 权威 JSON 先写 PostgreSQL，`projection_status` 驱动 projector 写 pgvector 或外部 Memory。
Knowledge item 与 embedding 分离，固定 profile 的 embedding dimension；不同维度使用独立 profile/DSN。
Artifact 元数据只在临时对象 checksum 验证后提交，object key 使用 tenant hash 命名空间，后台清理无
元数据引用的 staging 对象。

配置 revision 包含模型/fallback、工具策略、预算、通道能力、storage profile、审计策略和 policy
version。消息进入 Inbox 时固定 revision，灰度桶由 Session HMAC 决定，因此同一 Session 的重试不会
漂移。100% 激活和历史 revision 回滚都只更新 `agent_apps` 指针，不修改旧 JSON。

初始 schema 位于 `migrations/versions/0001_production_runtime.py`。后续迁移必须遵循 expand-contract：
先加可空列/新表并双写，滚动升级全部完成后再收紧约束或删除旧结构。

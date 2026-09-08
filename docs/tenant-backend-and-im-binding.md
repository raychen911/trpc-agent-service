# 租户后端选择、最小数据模型与 IM 账号绑定

本文对应三个可验收能力：租户级后端选择、数据同步的最小关系模型，以及 IM 账号到租户/用户的安全绑定。

## 1. 租户级后端选择

后端配置隶属于 `tenant -> agent_app -> agent_app_revision`。租户管理员在草稿的 `backends` 中为每个 `backend_kind` 最多选择一个实现，发布后才生效；历史发布版本不可修改，回滚时后端选择也随版本一起回滚。

当前运行时已真正支持 `session` 的 `sql` 与 `inmemory` 按租户/Agent App 并存。未配置时回退 `TRPC_SERVICE_CONVERSATION_BACKEND`；填写未知类型时不会中断旧业务，而是回退平台默认值，并在有效配置接口返回 `runtime_supported=false`。Memory、Knowledge、Artifact 等配置也会随版本保存并展示，但当前分别由平台级向量和对象存储适配器执行，后续可沿相同 Router 接口扩展。

草稿示例：

```json
{
  "expected_lock_version": 1,
  "model": {"provider": "openai", "model_name": "gpt-4.1-mini"},
  "backends": [
    {"backend_kind": "session", "backend_type": "sql", "options": {}},
    {"backend_kind": "memory", "backend_type": "qdrant", "secret_ref": "env://QDRANT_API_KEY", "options": {"collection": "tenant-a"}}
  ],
  "channels": [],
  "tools": []
}
```

查看最终生效值：

```http
GET /admin/v1/tenants/{tenant_id}/apps/{app_id}/backends/effective
```

返回中的 `configured_type` 是发布版本声明，`effective_type` 是当前进程实际使用值，`source` 表明来自 Agent App 还是平台默认配置。密钥只返回 Secret Reference，不返回明文。

消息执行时以 `(tenant_id, agent_app_id)` 查询 active revision，然后选择 SQL 或 InMemory `TurnCoordinator`。两者共用同一个分布式锁/幂等层，且都保证 `Event -> State(CAS) -> Summary -> Outbox` 的提交顺序。SQL 适合生产持久化；InMemory 仅适合本地开发、单元测试或允许进程重启丢失状态的租户。

## 2. 最小数据模型

下面是逻辑最小表结构；项目 ORM 还包含配置版本、工具权限、Inbox/Outbox、Artifact 和执行恢复等生产字段。

```sql
CREATE TABLE tenant (
  id UUID PRIMARY KEY, slug VARCHAR(63) UNIQUE NOT NULL,
  name VARCHAR(128) NOT NULL, status VARCHAR(16) NOT NULL
);

CREATE TABLE agent_app (
  id UUID PRIMARY KEY, tenant_id UUID NOT NULL REFERENCES tenant(id),
  slug VARCHAR(63) NOT NULL, active_version INT,
  UNIQUE (tenant_id, slug), UNIQUE (tenant_id, id)
);

CREATE TABLE channel_binding (
  id UUID PRIMARY KEY, tenant_id UUID NOT NULL, agent_app_id UUID NOT NULL,
  config_version INT NOT NULL, channel_type VARCHAR(32) NOT NULL,
  account_id VARCHAR(255) NOT NULL, webhook_path VARCHAR(255) NOT NULL,
  token_secret_ref VARCHAR(512), secret_ref VARCHAR(512),
  enabled BOOLEAN NOT NULL, options JSON NOT NULL,
  UNIQUE (agent_app_id, config_version, channel_type, account_id)
);

CREATE TABLE session (
  id UUID PRIMARY KEY, tenant_id UUID NOT NULL, agent_app_id UUID NOT NULL,
  user_id VARCHAR(255) NOT NULL, state JSON NOT NULL, version INT NOT NULL,
  UNIQUE (tenant_id, id)
);

CREATE TABLE event (
  id UUID PRIMARY KEY, tenant_id UUID NOT NULL, session_id UUID NOT NULL,
  sequence_no INT NOT NULL, event_type VARCHAR(64) NOT NULL,
  payload JSON NOT NULL, channel VARCHAR(32), external_message_id VARCHAR(255),
  trace_id VARCHAR(64) NOT NULL,
  UNIQUE (session_id, sequence_no),
  UNIQUE (tenant_id, channel, external_message_id)
);

CREATE TABLE memory (
  id UUID PRIMARY KEY, tenant_id UUID NOT NULL, agent_app_id UUID NOT NULL,
  user_id VARCHAR(255) NOT NULL, memory_key VARCHAR(255) NOT NULL,
  content TEXT NOT NULL, version INT NOT NULL,
  UNIQUE (tenant_id, agent_app_id, user_id, memory_key)
);

CREATE TABLE summary (
  id UUID PRIMARY KEY, tenant_id UUID NOT NULL, session_id UUID NOT NULL,
  version INT NOT NULL, through_sequence INT NOT NULL, content TEXT NOT NULL,
  UNIQUE (session_id, version)
);

CREATE TABLE audit_log (
  id UUID PRIMARY KEY, tenant_id UUID NOT NULL, agent_app_id UUID NOT NULL,
  user_id VARCHAR(255), session_id UUID, decision VARCHAR(64) NOT NULL,
  request_id VARCHAR(64) NOT NULL, trace_id VARCHAR(64) NOT NULL,
  details JSON NOT NULL, created_at TIMESTAMP NOT NULL
);

CREATE TABLE im_user_identity (
  id UUID PRIMARY KEY, tenant_id UUID NOT NULL REFERENCES tenant(id),
  channel_type VARCHAR(32) NOT NULL, account_id VARCHAR(255) NOT NULL,
  external_user_id VARCHAR(255) NOT NULL, internal_user_id VARCHAR(255) NOT NULL,
  status VARCHAR(16) NOT NULL, attributes JSON NOT NULL,
  UNIQUE (tenant_id, channel_type, account_id, external_user_id)
);
```

关系为：Tenant 1:N Agent App；Agent App 1:N Channel Binding/Session/Memory/Audit Log；Session 1:N Event/Summary；同一个 `internal_user_id` 可通过多条 IM Identity 关联企业微信、Telegram 等多个外部身份。

## 3. 数据写入与同步

一次 turn 在 SQL 或 InMemory 原子边界内固定执行：

1. 插入 Event，并由唯一键拒绝重复外部消息；
2. 对 Session `version` 执行 CAS 更新；
3. 写入 Summary 和 Memory 事实表；
4. 同事务写入 Transactional Outbox；
5. 事务提交后，Outbox Worker 异步、可重试地同步向量库或投递 IM 回复。

Outbox 的 `dedupe_key` 包含 Memory 版本，向量写采用 upsert。消费者失败采用指数退避；超过阈值进入死信（支持该能力的持久化后端）。因此 SQL 是事实源，向量库/IM 平台是可重建的派生侧。InMemory 模式遵循相同顺序，但进程退出后不能恢复，不能作为生产事实源。

## 4. IM 账号绑定

### 4.1 绑定对象和 Webhook URL

一个 Channel Binding 将 `(channel_type, account_id)` 绑定到一个租户的一个已发布 Agent App。发布时会检查：同一 IM 账号不能同时属于两个 active Agent App/租户，消除 webhook 只携带 `account_id` 时的路由歧义。

完整 URL 由 `TRPC_SERVICE_PUBLIC_BASE_URL + webhook_path` 生成，可通过以下接口查询：

```http
GET /admin/v1/tenants/{tenant_id}/apps/{app_id}/channel-bindings
```

推荐路径：

- Telegram：`/webhooks/telegram/{account_id}`
- 企业微信：`/webhooks/wecom/{account_id}`

`token_secret_ref` 与 `secret_ref` 只能填写 `env://`、`vault://`、`kms://` 等引用。Telegram 的 token 对应 `X-Telegram-Bot-Api-Secret-Token`；企业微信的 token 与 EncodingAESKey 用于 SHA-1 签名校验、AES-CBC 解密及 `receive_id` 校验。平台日志和 Admin API 不返回密钥明文。

### 4.2 回调处理顺序

```text
IM -> webhook(account_id)
   -> 查询 active Channel Binding/租户
   -> 校验 webhook_path
   -> SecretResolver 取 token/secret
   -> 常量时间验签；企业微信再解密
   -> 标准化消息
   -> 外部用户 ID 映射为 tenant-local internal_user_id
   -> Inbox 入队（唯一键去重）
   -> Gateway/Runner -> Event/State/Summary/Outbox
   -> 异步回复 IM
```

幂等键是 `tenant_id:channel:external_message_id`。Telegram 使用 `update_id`；企业微信使用 `MsgId`，若协议事件没有 `MsgId`，适配器生成稳定摘要键。Inbox 和 Event 两层唯一约束分别防止重复入队与重复提交。

### 4.3 用户身份映射

Channel Binding 的 `options.identity_mode` 有三种模式：

- `strict`：必须预先配置映射，缺失时 webhook 返回 403；生产环境推荐。
- `auto`：首次出现时，用租户、通道、账号和外部 ID 生成稳定 `internal_user_id` 并落库。
- `passthrough`：直接使用外部 ID，默认用于兼容已有本地配置；跨 IM 通道不能自动合并身份。

管理员维护映射：

```http
PUT /admin/v1/tenants/{tenant_id}/im-identities
GET /admin/v1/tenants/{tenant_id}/im-identities?channel_type=wecom&account_id=corp-a
```

请求示例：

```json
{
  "channel_type": "wecom",
  "account_id": "corp-a",
  "external_user_id": "zhangsan",
  "internal_user_id": "employee-10086",
  "display_name": "张三",
  "attributes": {"department": "研发部"}
}
```

写入时会确认该账号确实是本租户的 active binding，防止管理员把其他租户的外部账号误映射进来。标准化消息会保留 `metadata.external_sender_user_id` 与 `metadata.identity_mapped`，便于审计和问题排查。

## 5. 验收建议

创建两个租户及 Agent App，分别将 `session` 发布为 `sql` 与 `inmemory`，调用 effective API 确认结果。随后为企业微信绑定设置 `identity_mode=strict`：未建映射时发送回调应返回 403；调用身份映射 API 后重发应返回 success，且 Inbox 中的 `sender_user_id` 应为内部 ID。最后重复发送同一 `MsgId/update_id`，确认只存在一条 Inbox/Event，且异步回复不会重复投递。

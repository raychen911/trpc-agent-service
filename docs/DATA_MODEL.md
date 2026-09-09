# 数据模型设计

本文档定义多租户 Agent 平台的最小数据模型。模型按租户强隔离设计，所有业务表和缓存 Key 都必须包含 `tenant_id`，避免跨租户读取、写入和审计串扰。

## 租户配置 JSON 示例

```json
{
  "tenant_id": "tenant_demo",
  "status": "active",
  "active_config_version": 3,
  "audit_policy": {
    "retention_days": 180,
    "redact_rules": ["authorization", "token", "secret", "api_key", "phone", "email"]
  },
  "quota_policy": {
    "qps_limit": 20,
    "daily_token_limit": 1000000,
    "daily_cost_limit": 200.0,
    "max_input_chars": 8000
  },
  "apps": [
    {
      "agent_app_id": "app_support",
      "agent_name": "support-agent",
      "status": "active",
      "prompt_ref": "secret://tenant_demo/prompts/support_system",
      "model_config": {
        "provider": "openai-compatible",
        "model": "demo-model",
        "temperature": 0.2,
        "timeout_ms": 60000,
        "api_key_ref": "secret://tenant_demo/model/api_key"
      },
      "tool_policy": {
        "allowlist": ["search_knowledge", "create_ticket"],
        "denylist": ["shell_exec"],
        "approval_rules": ["send_external_message", "refund_order"],
        "risk_levels": {"create_ticket": "medium", "refund_order": "critical"},
        "require_confirmation_for_risk": ["high", "critical"],
        "max_calls_per_request": 16,
        "max_side_effect_calls_per_request": 4
      }
    }
  ],
  "channel_bindings": [
    {
      "binding_id": "bind_wecom_ai_bot_support",
      "channel": "wecom_ai_bot",
      "account_id": "corp_bot_1",
      "agent_app_id": "app_support",
      "webhook_path": null,
      "token_ref": null,
      "secret_ref": "secret://tenant_demo/wecom_ai_bot/bot_secret",
      "config": {
        "bot_id_ref": "secret://tenant_demo/wecom_ai_bot/bot_id",
        "identity_mapping": {
          "external_to_internal": {
            "wecom-bot-user-id": "user-1001"
          }
        }
      },
      "status": "active"
    }
  ],
  "storage_profile": {
    "session_backend": "redis",
    "memory_backend": "postgresql",
    "summary_backend": "postgresql",
    "knowledge_backend": "vector",
    "artifact_backend": "object",
    "audit_backend": "postgresql"
  }
}
```

## 核心表结构

以下结构是平台层的逻辑数据模型，生产环境可在 PostgreSQL 中使用同名表或按租户/时间分区。SQLite 适合本地开发，Redis 适合热状态和幂等，PostgreSQL 适合持久配置、事件和审计。

当前代码的配置仓库采用 `tenant_config(tenant_id, version, config_json, created_at, updated_at)` 和 `tenant_active(tenant_id, active_version)` 保存租户配置版本；`tenant`、`agent_app`、`channel_binding` 是从配置 JSON 投影出的逻辑实体。Session、Memory、Summary、Audit Log 和 Idempotency 则由各自的 Storage Adapter 建立物理表或 Redis key。也就是说，下面的规范化表结构是跨后端契约，不代表 SQLite 必须创建同名的全部表。

### tenant

```sql
CREATE TABLE tenant (
  tenant_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  active_config_version INTEGER NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
```

### tenant_config_version

```sql
CREATE TABLE tenant_config_version (
  tenant_id TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  config_json TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  publish_status TEXT NOT NULL,
  checksum TEXT NOT NULL,
  PRIMARY KEY (tenant_id, config_version)
);
```

### agent_app

```sql
CREATE TABLE agent_app (
  tenant_id TEXT NOT NULL,
  agent_app_id TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  agent_name TEXT NOT NULL,
  status TEXT NOT NULL,
  model_config_json TEXT NOT NULL,
  tool_policy_json TEXT NOT NULL,
  PRIMARY KEY (tenant_id, agent_app_id, config_version)
);
```

### channel_binding

```sql
CREATE TABLE channel_binding (
  tenant_id TEXT NOT NULL,
  binding_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  account_id TEXT NOT NULL,
  agent_app_id TEXT NOT NULL,
  webhook_path TEXT,
  token_ref TEXT,
  secret_ref TEXT,
  status TEXT NOT NULL,
  config_version INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, binding_id, config_version),
  UNIQUE (channel, account_id, config_version)
);
```

实际路由依赖 `channel + account_id` 找到唯一启用的绑定，因此物理实现还需要保证 active 配置中这两个字段全局唯一，避免 Gateway 在跨租户查找时出现歧义。

### session_state

```sql
CREATE TABLE session_state (
  tenant_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  state_version INTEGER NOT NULL,
  latest_event_seq INTEGER NOT NULL,
  state_json TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (tenant_id, session_id)
);
```

### message_event

```sql
CREATE TABLE message_event (
  tenant_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  idempotency_key TEXT,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (tenant_id, event_id),
  UNIQUE (tenant_id, session_id, seq)
);
```

### memory

```sql
CREATE TABLE memory (
  tenant_id TEXT NOT NULL,
  memory_id TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  content TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  embedding_version TEXT,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (tenant_id, memory_id)
);
```

### summary

```sql
CREATE TABLE summary (
  tenant_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  summary_version INTEGER NOT NULL,
  source_event_seq INTEGER NOT NULL,
  content TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (tenant_id, session_id, summary_version)
);
```

### artifact

```sql
CREATE TABLE artifact (
  tenant_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  session_id TEXT,
  object_uri TEXT NOT NULL,
  content_type TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  checksum TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (tenant_id, artifact_id)
);
```

### knowledge_chunk

```sql
CREATE TABLE knowledge_chunk (
  tenant_id TEXT NOT NULL,
  collection_id TEXT NOT NULL,
  chunk_id TEXT NOT NULL,
  text TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  embedding_version TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (tenant_id, collection_id, chunk_id)
);
```

### audit_log

```sql
CREATE TABLE audit_log (
  audit_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  user_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  agent_name TEXT NOT NULL,
  tool_name TEXT,
  decision TEXT NOT NULL,
  latency_ms INTEGER NOT NULL,
  error_type TEXT,
  token_usage INTEGER NOT NULL,
  cost NUMERIC(12, 6) NOT NULL,
  trace_id TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL
);
```

### idempotency

```sql
CREATE TABLE idempotency (
  tenant_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL,
  response_ref TEXT,
  result_json TEXT,
  trace_id TEXT NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  PRIMARY KEY (tenant_id, idempotency_key)
);
```

## 统一数据访问抽象

| 数据类型 | 抽象接口 | 推荐后端 | 说明 |
| --- | --- | --- | --- |
| Session event/state | `SessionStore` | Redis 或 PostgreSQL | event 追加写入，state 使用版本 CAS。 |
| Memory | `MemoryStore` | PostgreSQL、Redis 或外部 Memory 服务 | 写入后按租户和 scope 读取，长期记忆推荐 SQL 或外部服务。 |
| Summary | `SummaryStore` | PostgreSQL 或 Redis | 按 `source_event_seq` 防止旧摘要覆盖新摘要。 |
| Artifact | `ArtifactStore` | 文件系统、Redis 或对象存储 | 本地开发可用文件系统，生产推荐对象存储。 |
| Knowledge | `KnowledgeStore` | 本地向量库、Redis、PostgreSQL 或远端向量库 | 检索结果必须带租户和 collection 过滤。 |
| Audit Log | `AuditStore` | PostgreSQL | 需要可查询、可保留、可导出，生产不建议只放 Redis。 |
| Idempotency | `IdempotencyStore` | Redis 或 PostgreSQL | 必须支持原子创建 processing 状态和完成态复用。 |
| Compensation | `CompensationStore` | Redis 或 PostgreSQL | 保存派生写入失败后的重试任务，按 `available_at` 延迟重放。 |

### compensation_task

```sql
CREATE TABLE compensation_task (
  task_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  operation TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMP NOT NULL,
  last_error TEXT,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
CREATE INDEX idx_compensation_pending
  ON compensation_task (status, available_at, tenant_id);
```

`operation` 当前包含 `memory.put`、`summary.put`、`audit.append`。`payload_json` 使用对应数据对象的 JSON 结构，错误信息写入前必须脱敏。审计 `decision` 允许 `allow`、`deny`、`error`、`approval_required`、`revoked`，用于区分正常执行、策略拒绝、系统错误、危险工具待审批和 IM 撤回事件。

## Redis Key 示例

```text
tenant:{tenant_id}:session:{session_id}:state
tenant:{tenant_id}:session:{session_id}:events
tenant:{tenant_id}:summary:{session_id}
tenant:{tenant_id}:memory:{scope_key}
tenant:{tenant_id}:idem:{idempotency_key}
tenant:{tenant_id}:quota:{yyyyMMdd}
tenant:{tenant_id}:compensation:{task_id}
queue:agent-run
queue:agent-run:processing
queue:compensation
queue:im-outbound:retry
queue:im-outbound:dlq
```

Redis Key 必须设置合理 TTL。Session 热状态和幂等记录可按租户策略过期，审计、配置版本和长期 Memory 需要落到持久后端。

# 数据模型设计

## 1. 设计原则

- 运行期 Session/Memory 由框架 `SqlSessionService / SqlMemoryService / RedisSessionService / RedisMemoryService` 自管理（框架内建 schema）；平台侧维护**租户元数据、通道绑定、审计、幂等**四类表。
- 框架检索 key 固定为 `{app_name}/{user_id}`，租户隔离通过 app_name 前缀表达。
- 全部表 MySQL 8 / InnoDB / utf8mb4；Redis 侧以同名 key 结构做热数据。

## 2. 表结构（SQL DDL）

### tenant — 租户（已实现，迁移 b3d7f2a91c4e + d5e9b0c3a7f1）
```sql
CREATE TABLE tenant (
  tenant_id   VARCHAR(36) PRIMARY KEY,
  name        VARCHAR(128) NOT NULL,
  status      VARCHAR(16) NOT NULL DEFAULT 'active',
  config      JSON NOT NULL,           -- TenantConfig 完整快照
  revision    INT NOT NULL DEFAULT 1,  -- 当前配置版本号
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### tenant_revision — 配置历史版本（灰度/回滚，只增不改）
```sql
CREATE TABLE tenant_revision (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenant_id  VARCHAR(36) NOT NULL,
  revision   INT NOT NULL,
  config     JSON NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tenant_revision (tenant_id, revision)
);
```

### agent_app — 租户下的 Agent 应用（tenant 配置的应用级投影）
```sql
CREATE TABLE agent_app (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  app_name    VARCHAR(64) NOT NULL UNIQUE,  -- 框架 Session key 前缀，全局唯一
  tenant_id   VARCHAR(36) NOT NULL,
  instruction TEXT NOT NULL,
  model_name  VARCHAR(64) NOT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ix_agent_app_tenant_id ON agent_app (tenant_id);
```

### session — 会话（逻辑视图；运行期由框架服务承载）
```sql
CREATE TABLE session (
  session_id    VARCHAR(64) PRIMARY KEY,   -- sha1(tenant:channel:user[:chat])[:32]
  tenant_id     VARCHAR(36) NOT NULL,
  app_name      VARCHAR(64) NOT NULL,
  user_id       VARCHAR(64) NOT NULL,
  state_json    JSON,
  event_count   INT NOT NULL DEFAULT 0,
  last_update_time DATETIME,
  INDEX idx_session_user (tenant_id, app_name, user_id),
  INDEX idx_session_time (last_update_time)
);
```

### message — 消息/事件
```sql
CREATE TABLE message (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  session_id  VARCHAR(64) NOT NULL,
  seq         INT NOT NULL,
  author      VARCHAR(64) NOT NULL,        -- user / agent 名
  content_json JSON NOT NULL,
  trace_id    CHAR(32),
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_session_seq (session_id, seq),
  INDEX idx_msg_trace (trace_id)
);
```

### memory — 长期记忆（key = "{app_name}/{user_id}"）
```sql
CREATE TABLE memory (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenant_id  VARCHAR(36) NOT NULL,
  mem_key    VARCHAR(160) NOT NULL,        -- {app_name}/{user_id}
  content    TEXT NOT NULL,
  embedding  JSON NULL,                    -- 向量库迁移预留
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_memory_key (mem_key)
);
```

### summary — 会话摘要
```sql
CREATE TABLE summary (
  session_id   VARCHAR(64) PRIMARY KEY,
  summary_text TEXT NOT NULL,
  model        VARCHAR(64),
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### channel_binding — IM 账号与租户绑定
```sql
CREATE TABLE channel_binding (
  id               BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenant_id        VARCHAR(36) NOT NULL,
  channel_type     VARCHAR(16) NOT NULL,   -- web / feishu / wecom / wecom_smartbot
  external_user_id VARCHAR(128) NOT NULL,  -- 飞书 open_id
  chat_id          VARCHAR(128) NOT NULL DEFAULT '',  -- 群聊 id
  session_id       VARCHAR(64) NOT NULL,
  status           VARCHAR(16) NOT NULL DEFAULT 'active',
  UNIQUE KEY uk_binding (tenant_id, channel_type, external_user_id, chat_id),
  INDEX idx_binding_session (session_id)
);
```

### audit_log — 审计日志
```sql
CREATE TABLE audit_log (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  tenant_id   VARCHAR(36) NOT NULL,
  channel     VARCHAR(16) NOT NULL,
  user_id     VARCHAR(64),
  session_id  VARCHAR(64),
  agent_name  VARCHAR(64),
  tool_name   VARCHAR(128),
  decision    VARCHAR(16) NOT NULL,        -- allow / block / error
  latency_ms  INT NOT NULL DEFAULT 0,
  error_type  VARCHAR(64) NOT NULL DEFAULT '',
  cost        DECIMAL(10,4) NOT NULL DEFAULT 0,
  trace_id    CHAR(32),
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_audit_tenant_time (tenant_id, created_at),
  INDEX idx_audit_trace (trace_id)
);
```

### idempotency — 幂等去重
```sql
CREATE TABLE idempotency (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  idempotency_key VARCHAR(128) NOT NULL,   -- {channel}:{external_msg_id}
  dedupe_status   VARCHAR(16) NOT NULL DEFAULT 'processed',
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_idem (idempotency_key)
);
```

## 3. Redis 键结构（热数据）

| 键 | 值 | 说明 |
|----|----|------|
| `session:{app}:{user}:{session_id}` | hash | 框架 RedisSessionService 管理 |
| `memory:{app}:{user}` | list/zset | 框架 RedisMemoryService 管理 |
| `dedupe:{channel}:{msg_id}` | string, SETNX + EX 300s | 多节点消息去重（Deduper） |
| `budget:{tenant}:{date}:calls` / `:tokens` | string, INCRBY + EX 48h | 多节点预算计数（BudgetManager） |
| `ratelimit:{tenant}:{user}:{window}` | string, INCRBY + EX 120s | 每用户每分钟限流（RateLimiter） |
| `trpc:chat:tasks` / `:processing` / `trpc:chat:result:{id}` | list / list / string(TTL 60s) | 队列模式任务与结果（worker.py） |

## 4. JSON Schema 层面对照

TenantConfig（tenants.yaml ↔ config_json）字段：`tenant_id / name / status / app{app_name,description,instruction} / model{provider,model_name,api_key,base_url,max_tokens,temperature} / storage{session_backend,memory_backend,redis_url,sql_url} / channels{feishu{enabled,app_id,app_secret,token,encrypt_key},wecom{enabled,bot_id,token,corp_id,encoding_aes_key},wecom_smartbot{enabled,bot_id,secret}} / tools{allowed_tools,blocked_tools} / audit{enabled,log_level,mask_pii,retention_days} / workspace{mode,image} / skills{enabled} / daily_api_calls / daily_token_budget / rate_limit_per_minute / release_stage`。

## 5. 实体关系

```
tenant 1─n agent_app 1─n session 1─n message
tenant 1─n channel_binding（external_user_id → session_id）
session 1─1 summary
tenant 1─n memory（mem_key = {app}/{user}）
tenant 1─n audit_log；message/audit_log 均携带 trace_id
tenant 1─n tenant_revision（配置版本历史，回滚依据）
```

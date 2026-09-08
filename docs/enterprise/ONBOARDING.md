# 租户接入指南

> 本文档说明如何把一个新租户接入 `trpc_service` 多租户体系：编写租户配置、
> 注册到运行时、配置模型/工具/IM/后端/审计预算，并验证接入。配套文档见 `DESIGN.md`。

---

## 1. 接入总览

接入一个租户 = 编写一份租户配置 + 加载到 `TenantConfigManager`。所有能力（模型、工具、
IM 通道、数据后端、审计、预算）都由这份配置驱动，**无需改代码**。

```
tenants.yaml ──► load_tenants() ──► TenantConfigManager ──► Worker/治理 Filter/IM Adapter
```

## 2. 步骤一：编写租户配置

最小可用的租户配置只需 `tenant_id` + `model`，其余字段均有默认值：

```yaml
tenants:
  - tenant_id: tenant_demo          # 全局唯一，同时作为 Agent 名（须为合法 Python 标识符）
    name: 演示租户
    status: active                   # active | disabled（disabled 立即拒绝所有请求）

    model:                           # 模型配置（字段名是 model，非 model_config）
      model_name: gpt-4o
      api_endpoint: https://api.openai.com/v1
      timeout: 30
      retry: 2
      fallback_model: gpt-4o-mini    # 主模型超时降级
```

完整配置示例见 `examples/multi_tenant_saas/tenants.yaml`。

### 各配置域字段速查

| 配置域 | 字段 | 说明 |
|---|---|---|
| 应用 | `app_config.app_list` / `default_app_id` / `default_instruction` / `max_concurrent_sessions` | 多 Agent App、默认路由、提示词、容量目标 |
| 模型 | `model.provider` / `model_name` / `api_endpoint` / `timeout` / `retry` / `fallback_model` | 主模型与降级 |
| 工具 | `tool_permissions.tool_whitelist` / `tool_denylist` / `dangerous_tools` | 白名单（空=全放行）/黑名单/需二次确认 |
| IM | `channel_configs.<channel>` | 每通道密钥（`SecretStr`） |
| IM 入口治理 | `im_access_policy.callback_requests_per_minute` | 租户/通道级回调限流（`None`=不限流） |
| 后端 | `session_backend` / `memory_backend` / `vector` / `object` | Session/Memory 选 Redis/MySQL；Knowledge 选向量库；Artifact 选对象存储 |
| 审计 | `audit_policy.enabled` / `retention_days` / `desensitize_rules` | 审计开关与脱敏规则 |
| 预算 | `budget.daily_token_budget` / `daily_cost_limit` | 日 token / 成本上限（`None`=不限额） |

## 3. 步骤二：注册到运行时

```python
from trpc_service import TenantConfigManager, load_tenants

manager = TenantConfigManager()
for tenant in load_tenants("tenants.yaml"):
    manager.register(tenant)
```

`TenantConfigManager` 提供 `get / list / update / delete / rollback / history / subscribe`，
支持版本历史与一键回滚（见 `DESIGN.md` 第 7 节）。

## 4. 步骤三：配置模型

模型 API Key **不写入配置**，从环境变量注入：

```bash
export TRPC_SERVICE_MODEL_API_KEY=sk-xxx
```

`create_agent(tenant)` 用 `tenant.model` 构建 `OpenAIModel`，Key 读取环境变量。多租户共用
不同 Key 时，可在 `AgentFactory` 里按租户查密钥服务（Vault/K8s Secret）。

## 5. 步骤四：配置工具权限

```yaml
tool_permissions:
  tool_whitelist: [query_order, query_logistics]   # 仅允许这两个工具
  tool_denylist: [delete_order]                     # 永远拒绝
  dangerous_tools: [cancel_order]                   # 触发二次确认
```

- **白名单**：非空时，不在列表内的工具调用被 `ToolAllowlistFilter` 拒绝。
- **危险工具**：触发 HITL，`ToolAllowlistFilter` 生成一次性确认 token（`ToolConfirmationRequired`），
  用户在 IM 回显 token 后由 `ConfirmationManager.resolve()` 放行。

## 6. 步骤五：配置 IM 通道

建议同时配置入口限流。生产多 Gateway 节点共用 Redis 原子计数；未配置 Redis 的单节点演示
使用进程内计数。Redis 限流后端不可用时 Gateway 默认 fail-closed 返回 `503`，避免治理旁路：

```yaml
im_access_policy:
  callback_requests_per_minute: 600
```

### 企业微信

```yaml
channel_configs:
  wecom:
    channel_type: wecom
    token: env://TRPC_SERVICE_WECOM_TOKEN          # 回调 token
    aes_key: env://TRPC_SERVICE_WECOM_AES_KEY      # EncodingAESKey（43 位 base64）
    corp_id: wx123456
    agent_id: "1000001"
```

回调地址指向 `POST /webhook/tenant_demo/wecom`，服务端自动完成 AES 解密 + SHA1/HMAC-SHA256
验签（`trpc_service/channels/_crypto.py`）。

### 微信客服、钉钉与飞书

```yaml
channel_configs:
  wechat_kf:
    channel_type: wechat_kf
    corp_id: ${TRPC_SERVICE_WECHAT_KF_CORP_ID}
    open_kfid: ${TRPC_SERVICE_WECHAT_KF_OPEN_KFID}
    token: env://TRPC_SERVICE_WECHAT_KF_TOKEN
    aes_key: env://TRPC_SERVICE_WECHAT_KF_AES_KEY
  dingtalk:
    channel_type: dingtalk
    app_id: ${TRPC_SERVICE_DINGTALK_CLIENT_ID}
    robot_code: ${TRPC_SERVICE_DINGTALK_ROBOT_CODE}
    secret: env://TRPC_SERVICE_DINGTALK_CLIENT_SECRET
  feishu:
    channel_type: feishu
    app_id: ${TRPC_SERVICE_FEISHU_APP_ID}
    verification_token: env://TRPC_SERVICE_FEISHU_VERIFICATION_TOKEN
    encrypt_key: env://TRPC_SERVICE_FEISHU_ENCRYPT_KEY
```

回调地址分别使用 `/wechat_kf`、`/dingtalk`、`/feishu`。没有真实凭据时，可通过
`tests/service/test_channels.py` 中的平台 fixture 验证标准化、session 路由和回复转换。

### QQ 机器人

```yaml
channel_configs:
  qq:
    channel_type: qq
    app_id: ${TRPC_SERVICE_QQ_APP_ID}
    secret: env://TRPC_SERVICE_QQ_APP_SECRET
```

在 QQ 机器人开放平台将公网 HTTPS 回调配置为
`POST /webhook/tenant_demo/qq`。网关会自动处理 `op=13` 地址验证、Ed25519 回调验签与
`op=12` 确认，并用 AppID/AppSecret 获取和缓存 OpenAPI AccessToken。无需手工配置
AccessToken；`access_token` 字段只用于本地联调或已有令牌的特殊场景。

当前支持 C2C 单聊、群聊、频道消息和频道私信的文本接收与回复。除了填写 AppID 和
AppSecret，还必须在 QQ 机器人开放平台启用对应事件订阅/权限，并确保回调地址可被
QQ 公网访问。

### 身份映射与 session 隔离

- 单聊：`session_id = sha256(JSON[tenant, channel, private, user_id])`
- 群聊：`session_id = sha256(JSON[tenant, channel, group, chat_id])`

结构化 JSON 编码保留字段边界，即使平台 ID 含 `:` 也不会与另一组字段发生拼接碰撞；把
`chat_type` 纳入哈希可避免同名用户和群 ID 得到相同 `session_id`。

session_id 已内嵌租户与通道，用户跨群/跨租户自动落到不同会话，无需额外映射表。

## 7. 步骤六：配置数据后端

```yaml
storage_config:
  session_backend: redis     # redis | mysql
  memory_backend: mysql      # redis | mysql
  summary_backend: redis     # 兼容字段，自动跟随 session_backend
  audit_backend: mysql       # 固定 mysql
  redis_url: env://TRPC_SERVICE_REDIS_URL
  mysql_url: env://TRPC_SERVICE_MYSQL_URL
  vector:
    backend: qdrant
    url: env://TRPC_SERVICE_VECTOR_URL
    api_key: env://TRPC_SERVICE_QDRANT_API_KEY
    collection: tenant_demo_knowledge
    dimensions: 1536
  object:
    backend: s3
    endpoint_url: ${TRPC_SERVICE_OBJECT_STORE_ENDPOINT}
    bucket: tenant-demo-artifacts
    access_key: env://TRPC_SERVICE_OBJECT_STORE_ACCESS_KEY
    secret_key: env://TRPC_SERVICE_OBJECT_STORE_SECRET_KEY
```

- 多节点共享会话使用 `redis` 或 `mysql`，Worker 无状态，无需 sticky session。
- 不同租户可指向不同后端，`TenantSessionService`/`TenantMemoryService` 按租户做 key 前缀隔离。
- `TenantVectorStore`/`TenantObjectStore` 分别为 namespace 和 object key 注入租户前缀；本地开发
  可使用 `memory` 向量后端和 `local` 对象后端，生产使用 Qdrant 与 S3/COS/MinIO。
- Redis→MySQL 迁移只覆盖 Session/Memory；全量复制及校验通过后按租户切换路由，旧 Redis
  数据保留一个回滚窗口。Audit 始终写 MySQL，不参与迁移；内存仅保留最近 500 条用于本地查看。

## 8. 步骤七：配置审计与预算

```yaml
model:
  model_name: gpt-4o
  pricing:                           # 示例；按供应商合同价维护
    gpt-4o:
      input_per_mtok: 2.5
      output_per_mtok: 10.0

audit_policy:
  enabled: true
  retention_days: 90
  desensitize_rules:                 # 追加脱敏规则（叠加在默认规则之上）
    - pattern: "1[3-9]\\d{9}"        # 手机号
      replace: "1**********"

budget:
  daily_token_budget: 2000000
  daily_cost_limit: 50.0
```

预算由 `ModelBudgetFilter` 在每次模型调用前后检查/累计；调用前会同时原子预留 token 和保守估算成本，
超限抛出 `BudgetExceededError`。配置 `daily_cost_limit` 时若当前模型没有价格，系统会 fail closed 拒绝调用，
防止成本控制静默失效。模型供应商调价时应通过租户配置灰度更新 `pricing`。

## 9. 步骤八：验证接入

### 离线验证（无需 IM/LLM 凭证）

```bash
cd examples/multi_tenant_saas
python simulate.py   # 驱动 worker，验证租户隔离 + session 路由 + 指令隔离
```

### HTTP 验证（mock 模式）

```bash
cd examples/multi_tenant_saas
python run_gateway.py     # 或 uvicorn run_gateway:app --port 8080
curl -X POST http://localhost:8080/webhook/tenant_demo/dingtalk
```

### 本地管理后台

Gateway 启动后访问 `http://127.0.0.1:8080/admin`，页面会跳转到只读管理台。输入启动时配置的
`TRPC_SERVICE_ADMIN_API_KEY`，可以查看租户、渠道、最近审计记录和运行指标。完整聊天正文不通过 Admin API 暴露。

### 隔离自检清单

- [ ] 同一 `user_id` 在两个租户下 `session_id` 不同（`generate_session_id` 幂等且跨租户隔离）
- [ ] 未配置通道的租户，`/webhook/{tenant}/{channel}` 返回 404
- [ ] `status: disabled` 的租户，请求返回 404
- [ ] 白名单外的工具调用被 `ToolAllowlistFilter` 拒绝
- [ ] 重复回调（同 `message_id`）只处理一次（幂等）

## 10. 密钥管理最佳实践

- 密钥字段均为 `pydantic.SecretStr`，`repr`/日志不泄露明文。
- 配置里用 `${VAR}` 引用环境变量；生产环境映射 K8s Secret / Vault。
- 全局日志脱敏：`install_redacting_log_filter()` 遮蔽 `sk-*`/`Bearer`/`password=`/连接串。

```python
from trpc_service import install_redacting_log_filter
install_redacting_log_filter()
```

## 11. 常见问题（FAQ）

| 问题 | 处理 |
|---|---|
| 企业微信验签失败 | 核对 `token`/`aes_key`（43 位）/`corp_id`；确认走 HMAC-SHA256 或 SHA1 均被兼容 |
| 消息重复处理 | Gateway 已按 `tenant:channel:msg_id` 做 `SETNX` 幂等；TTL 300s |
| 工具调用被拒 | 检查 `tool_whitelist`/`tool_denylist`；危险工具需回显确认 token |
| 预算超限 | 查看 `budget` 配置；`BudgetTracker.reset()` 可手动清零（生产接定时重置） |
| session 串数据 | 确认 `TenantSessionService` 已按 `tenant_id` 包装，且后端为共享 Redis/MySQL |
| 变更不生效 | `TenantConfigManager.update()` 后订阅变更监听或重建 worker；配置缓存需失效 |

## 12. 相关文件

- 部署指南：`docs/enterprise/DEPLOYMENT.md`
- 设计文档：`docs/enterprise/DESIGN.md`
- 演示工程：`examples/multi_tenant_saas/`
- 租户模型：`trpc_service/tenant/_models.py`
- 配置加载：`trpc_service/tenant/_loader.py`
- 多后端方案：`docs/enterprise/BACKEND_ADAPTERS.md`

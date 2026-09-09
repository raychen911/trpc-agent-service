# 配置与模型替换

## 1. 先配置哪一层

配置分为两层。进程配置决定服务角色和控制面连接；`TenantConfig` 决定租户使用的模型、工具以及 Session/Memory 后端。模型 Secret 只在创建 Runtime 时解析。生产环境启动时会校验活动配置，缺少必需的 Secret 会直接报错。

本地已进入 Anaconda 环境后：

```bat
python -m pip install -e ".[dev,postgres,wecom]"
REM 首次初始化 .env；已有文件时直接编辑
if not exist ".env" copy ".env.example" ".env" >nul
notepad .env
python -m trpc_service._cli check-config examples/config/tenants.yaml
```

本地更换模型时，修改 `.env` 中的 provider、model name、base URL 和 API key，然后重启开发进程。生产配置以版本快照保存在数据库中：配置内容变化时发布新版本；轮换密钥时滚动重启使用该密钥的角色。

## 2. 环境变量

| 变量 | 默认/含义 |
|---|---|
| TRPC_SERVICE_ENV | development；其他值走生产装配 |
| TRPC_SERVICE_ROLES | loader 默认 gateway,worker,delivery,admin；需要企微长连接时加入 wecom |
| TRPC_SERVICE_CONFIG | examples/config/tenants.yaml |
| TRPC_SERVICE_HOST / PORT | 默认 127.0.0.1 / 8080；`serve` 命令可通过 `--host` 和 `--port` 覆盖 |
| TRPC_SERVICE_LOG_LEVEL | INFO；CLI --log-level 优先 |
| TRPC_SERVICE_WORKER_ID | Worker 身份，默认主机名-PID；每个生产进程使用独立值 |
| TRPC_SERVICE_REDIS_URL | redis://127.0.0.1:6379/15 |
| TRPC_SERVICE_POSTGRES_URL | PostgreSQL 控制面 DSN；生产环境填写实际数据库凭据 |
| TRPC_SERVICE_ADMIN_TOKEN | Admin Header 凭据 |
| TRPC_TENANT_DEMO_TOKEN | Demo 租户 API Token，通过 api_token_ref 引用 |
| TRPC_SERVICE_OTLP_ENDPOINT | OTLP 接收地址；填写后启用 Exporter |
| TRPC_SESSION_BACKEND / TRPC_MEMORY_BACKEND | 示例 YAML 默认 memory；Compose 明确设置 redis |
| TRPC_AGENT_MODEL_PROVIDER | openai-compatible / anthropic / litellm |
| TRPC_AGENT_MODEL_NAME / BASE_URL / API_KEY | 所选供应商模型、接口和凭据 |
| WECOM_BOT_ID / WECOM_BOT_SECRET | 企业微信真实长连接认证 |
| WECOM_KF_CORP_ID / WECOM_KF_OPEN_KFID / WECOM_KF_SECRET | 微信客服 API 与客服账号 |
| WECOM_KF_CALLBACK_TOKEN / WECOM_KF_ENCODING_AES_KEY | 微信客服回调验签与解密 |
| WECOM_KF_TEST_EXTERNAL_USER_ID | 统一 Live 入口发送测试消息的客户 ID |
| TELEGRAM_BOT_TOKEN / TELEGRAM_TEST_CHAT_ID | Telegram 真实发送测试 |
| TRPC_TEST_REDIS_URL / TRPC_TEST_POSTGRES_URL | 真实依赖测试使用的专用数据库地址 |

已在终端导出的同名环境变量优先于 `.env`。`demo all` 使用本地模型完成验证。`.env` 保存密钥和连接信息，由 `.gitignore` 排除。

## 3. TenantConfig 的关键字段

代码中的租户模型采用分层配置，下面展示最外层字段。`apps` 中的每个成员都是独立的 `AgentAppConfig`，分别保存对应 Agent 的模型、工具和运行参数。

```python
class TenantConfig(BaseModel):
    tenant_id: str
    name: str
    version: int
    status: TenantStatus
    api_token_ref: str
    apps: dict[str, AgentAppConfig]
    channels: list[ChannelBindingConfig]
    storage: StoragePolicy
    audit: AuditPolicy
    budget: BudgetPolicy
```

- `tenant_id`、`version`、`status`：标识租户配置快照。每次发布生成新版本，回滚只切换活动版本。
- `apps`：保存租户下的 Agent App。字典键与 `app_id` 对应，每个 App 独立配置 instruction、model、tools 和 runtime。
- `model`：指定模型供应商、模型名、服务地址、密钥引用、超时、重试次数和 token 单价。
- `tools`：`allowed` 是白名单，`denied` 是禁用名单，`confirmation_required` 标记需要审批的工具。发布服务会检查规则冲突和工具注册状态。
- `runtime`：控制并发数、执行时限、群聊会话方式、摘要策略和预估输出 token。必要的 post-turn 处理采用同步完成方式，使下一次请求可以读取最新 Summary 和 Memory。
- `channels`：保存 Binding ID、通道类型、目标 App、外部账号、Secret 引用、ACL 和消息长度限制。
- `storage`：指定 Session/Memory 后端、连接地址、TTL 和外部 Provider 名称。
- `audit`、`budget`：分别控制审计策略、数据保留天数和租户每日请求/token/金额额度。

## 4. 后端选择

```yaml
storage:
  session: redis
  memory: sql
  redis_url: redis://redis:6379/0
  sql_url: postgresql://trpc_agent:LOCAL_TEST_PASSWORD@postgres/trpc_agent_service
  session_ttl_seconds: 86400
  memory_ttl_seconds: 604800
```

InMemory 只用于开发和单元测试。Redis/SQL Session 与 Memory 直接复用 SDK。SQL 适配使用同步驱动；`postgres` extra 同时安装控制面所需的 `asyncpg` 和 SDK SQL 驱动。

外部存储通过 Provider 接入。一个 Provider 包含三部分：

1. 通过 `StorageProviderFactory.register(name, provider)` 注册。
2. 实现 `create_session` 和 `create_memory`。
3. 提供原生写保护，声明 `supports_fenced_writes=True` 并暴露 `write_guards`。

配置发布时会检查这三部分是否齐全。Artifact 和 Knowledge 使用各自的 Protocol；内置实现负责本地存储和 PostgreSQL 元数据，云对象存储或向量数据库可按同一接口扩展。

## 5. Channel 示例

```yaml
channels:
  - binding_id: demo-telegram
    app_id: assistant
    channel: telegram
    external_account_id: my-bot
    secret_ref: env://TELEGRAM_BOT_TOKEN
    webhook_secret_ref: env://TELEGRAM_WEBHOOK_SECRET
    options:
      allowed_user_ids: ["123456"]
  - binding_id: demo-wecom
    app_id: assistant
    channel: wecom
    external_account_id: YOUR_BOT_ID
    secret_ref: env://WECOM_BOT_SECRET
```

服务启动时根据 Channel Binding 创建 Adapter：

- 微信客服使用 HTTP Client；
- 企业微信由 `wecom` 角色建立长连接；
- Telegram Webhook 由 Gateway 接收。

`im-demo` 使用 `examples\config\im-demo.yaml` 中带 `simulator_mode` 的开发 Binding，并注册本地页面接口。生产服务使用正式 Binding。Channel 配置变更通过滚动重启对应角色生效，在途请求继续使用固定的配置版本。

## 6. Secret 与验证

环境变量 Provider 会校验引用值。`SecretProviderRegistry` 支持注册 Vault/KMS 等 Scheme，对应客户端由部署环境注入。解析结果只保存在运行期，配置快照仍保存引用。Admin 和 `show-config` 在受控管理环境中使用，因为数据库 URL 可能包含凭据。

`check-config` 负责检查字段格式和 YAML 变量替换。模型、数据库等外部依赖由 Live 或 Integration 命令验证，详见 [testing](testing.md)。

## 7. 摘要、群聊与存储兼容性

摘要默认启用，触发条件是**尚未纳入摘要的 Event 超过 30 个**。这个阈值按 Event 数量计算；生成摘要后保留最近 10 个 Event，SDK 会结合对话边界调整实际范围。

`summary_keep_recent` 小于 `summary_event_threshold`，用于控制摘要后保留的近期上下文。`summary_enabled` 控制摘要开关，Memory 和结果提交拥有独立处理阶段。摘要使用该 App 配置的模型，因此达到阈值时会增加一次模型调用。Usage Ledger 的核算范围以 Agent 主执行链路返回的用量为准。

摘要功能与存储类型相互独立。使用 InMemory 时，Session 和 summary event 保存在当前进程；本地 Demo 使用确定性的 OfflineModel。切换到 Redis 或 SQL 后，摘要内容随 Session 写入共享后端。

`group_session_mode` 有两种模式：

- `per_user`：默认模式，Binding、群和成员共同确定会话。
- `shared`：整个群共用历史，真实成员身份仍保存在 `metadata.actor_user_id`。

SDK `user_id` 表示存储归属，真实发言人另存于可信元数据并用于 ACL 和审批。群聊与私聊的 Memory 分开保存。IM 身份规则变化后，旧历史继续留在原命名空间，新会话按新规则写入。

微信客服字段、环境变量和示例见 [customer-service.md](customer-service.md)。生产部署按编号依次运行数据库迁移；双向 Session/Memory 迁移使用 `006_real_storage_migration.sql` 和 `007_reverse_storage_migration.sql`。

已有数据库通过 `migrations/` 中的增量 SQL 更新 DDL。

生产 SQL 后端使用 PostgreSQL，Redis 写保护按相关键位于同一 slot 设计，部署 Redis Cluster 时需要保持这一约束。

服务关闭 SDK SQL 后台清理任务，统一由带写入保护的管理作业按保留策略清理历史数据。配置 TTL 用于判断数据有效期，物理删除由清理作业执行。

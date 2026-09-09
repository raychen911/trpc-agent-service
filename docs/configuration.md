# 配置与模型替换

## 1. 先配置哪一层

配置分为两层。进程配置决定服务角色和控制面连接；`TenantConfig` 决定租户使用的模型、工具以及 Session/Memory 后端。模型 Secret 只在创建 Runtime 时解析。生产环境启动时会校验活动配置，缺少必需的 Secret 会直接报错。

本地已进入 Anaconda 环境后：

```bat
python -m pip install -e ".[dev,postgres,wecom]"
REM 仅首次且 .env 不存在时复制，保留已有模型信息
if not exist ".env" copy ".env.example" ".env" >nul
notepad .env
python -m trpc_service._cli check-config examples/config/tenants.yaml
```

本地更换模型时，修改 `.env` 中的 provider、model name、base URL 和 API key，然后重启开发进程即可。生产配置以不可变版本保存在数据库中，修改本地 YAML 或 `.env` 不会覆盖历史快照。配置内容变化时发布新版本；只轮换密钥时滚动重启使用该密钥的角色。

## 2. 环境变量

| 变量 | 默认/含义 |
|---|---|
| TRPC_SERVICE_ENV | development；其他值走生产装配 |
| TRPC_SERVICE_ROLES | loader 默认 gateway,worker,delivery,admin；wecom 必须显式启用 |
| TRPC_SERVICE_CONFIG | examples/config/tenants.yaml |
| TRPC_SERVICE_HOST / PORT | 默认 127.0.0.1 / 8080；`serve` 命令可通过 `--host` 和 `--port` 覆盖 |
| TRPC_SERVICE_LOG_LEVEL | INFO；CLI --log-level 优先 |
| TRPC_SERVICE_WORKER_ID | 主机名-PID；生产每进程必须不同 |
| TRPC_SERVICE_REDIS_URL | redis://127.0.0.1:6379/15 |
| TRPC_SERVICE_POSTGRES_URL | PostgreSQL 控制面 DSN，生产必须覆盖示例凭据 |
| TRPC_SERVICE_ADMIN_TOKEN | Admin Header 凭据 |
| TRPC_TENANT_DEMO_TOKEN | Demo 租户 API Token，通过 api_token_ref 引用 |
| TRPC_SERVICE_OTLP_ENDPOINT | 空时不装 OTLP exporter |
| TRPC_SESSION_BACKEND / TRPC_MEMORY_BACKEND | 示例 YAML 默认 memory；Compose 明确设置 redis |
| TRPC_AGENT_MODEL_PROVIDER | openai-compatible / anthropic / litellm |
| TRPC_AGENT_MODEL_NAME / BASE_URL / API_KEY | 所选供应商模型、接口和凭据 |
| WECOM_BOT_ID / WECOM_BOT_SECRET | 企业微信真实长连接认证 |
| WECOM_KF_CORP_ID / WECOM_KF_OPEN_KFID / WECOM_KF_SECRET | 微信客服 API 与客服账号 |
| WECOM_KF_CALLBACK_TOKEN / WECOM_KF_ENCODING_AES_KEY | 微信客服回调验签与解密 |
| WECOM_KF_TEST_EXTERNAL_USER_ID | 统一 Live 入口发送测试消息的客户 ID |
| TELEGRAM_BOT_TOKEN / TELEGRAM_TEST_CHAT_ID | Telegram 真实发送测试 |
| TRPC_TEST_REDIS_URL / TRPC_TEST_POSTGRES_URL | 真实依赖测试专用，不指向业务数据库 |

.env 加载不覆盖已经导出的同名环境变量。测试与 demo all 不读取/调用真实模型。不要将 .env 提交 Git。

## 3. TenantConfig 的关键字段

代码中的租户模型采用分层配置，下面只保留最外层字段。每个 `apps` 成员都是独立的 `AgentAppConfig`，不会把所有 Agent 的模型和工具混在同一个字典中。

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
- `apps`：保存租户下的 Agent App。字典键必须与 `app_id` 一致，每个 App 独立配置 instruction、model、tools 和 runtime。
- `model`：指定模型供应商、模型名、服务地址、密钥引用、超时、重试次数和 token 单价。
- `tools`：`allowed` 是白名单，`denied` 是禁用名单，`confirmation_required` 标记需要审批的工具。规则冲突或工具未注册时拒绝发布。
- `runtime`：控制并发数、执行时限、群聊会话方式、摘要策略和预估输出 token。平台要求必要的 post-turn 处理同步完成，因此会拒绝关闭或延迟该流程的配置。
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

外部存储不是填写一个 URL 就能自动接入。Provider 需要先完成三项工作：

1. 通过 `StorageProviderFactory.register(name, provider)` 注册。
2. 实现 `create_session` 和 `create_memory`。
3. 提供原生写保护，声明 `supports_fenced_writes=True` 并暴露 `write_guards`。

缺少其中任何一项，生产配置都会拒绝启用。Artifact 和 Knowledge 使用各自的 Protocol；内置实现负责本地存储和 PostgreSQL 元数据，云对象存储或向量数据库可按同一接口扩展。

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

`im-demo` 使用 `examples\config\im-demo.yaml` 中带 `simulator_mode` 的专用 Binding，只用于本地页面，不能发布到生产。Channel 配置变更通过滚动重启对应角色生效，在途请求继续使用固定的配置版本。

## 6. Secret 与验证

环境变量 Provider 不接受空值。`SecretProviderRegistry` 支持注册 Vault/KMS 等 Scheme，对应客户端由部署环境注入。解析后的密钥不会写回配置快照。数据库 URL 可能含有密码，因此 Admin 和 `show-config` 只应在受控环境使用。

`check-config` 负责检查字段格式和 YAML 变量替换，不测试模型或数据库连通性。外部依赖通过 Live 或 Integration 命令验证，详见 [testing](testing.md)。

## 7. 摘要、群聊与存储兼容性

摘要默认启用，触发条件是**未被摘要覆盖的 Event 超过 30 个**，并非进行了 30 轮聊天。生成摘要后保留最近 10 个 Event，SDK 会结合对话边界调整实际范围。

`summary_keep_recent` 必须小于 `summary_event_threshold`。设置 `summary_enabled=false` 只关闭摘要，不影响 Memory 和结果提交。摘要使用该 App 配置的模型，因此达到阈值时会增加一次模型调用。Usage Ledger 的核算范围以 Agent 主执行链路返回的用量为准。

没有 Redis/SQL 也能生成摘要：Session 和 summary event 一起保存在 InMemory；离线 Demo 使用专门的确定性 OfflineModel。SQL/Redis 只是存储方式，不决定能否调用摘要管理器。

`group_session_mode` 有两种模式：

- `per_user`：默认模式，Binding、群和成员共同确定会话。
- `shared`：整个群共用历史，真实成员身份仍保存在 `metadata.actor_user_id`。

SDK `user_id` 表示存储归属，不能代替真实发言人进行 ACL 或审批。群聊与私聊的 Memory 分开保存。Web 身份规则不受影响；IM 身份规则变化后，旧历史继续保留，但不会自动合并到新会话。

微信客服字段、环境变量和示例见 [customer-service.md](customer-service.md)。生产部署必须依次运行全部数据库迁移。双向 Session/Memory 迁移还需要 `006_real_storage_migration.sql` 和 `007_reverse_storage_migration.sql`。

服务代码更新不会自动修改旧数据库的 DDL。

生产 SQL 后端使用 PostgreSQL，SQLite 只用于开发演示，不能验证多节点 fencing。Redis 写保护按相关键位于同一 slot 设计，部署 Redis Cluster 时需要保持这一约束。

SDK SQL 后台清理可能绕过请求租约，因此服务关闭了该任务。SQL 历史数据按照运维保留策略通过受控管理作业清理；配置 TTL 本身不代表记录会立即从数据库物理删除。

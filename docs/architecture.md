# 系统架构设计：多租户节点化 Agent 部署平台

> 基于 tRPC-Agent-Python（trpc_agent_sdk 1.1.19）的多租户、可节点化部署、多后端数据同步、可接入飞书/企业微信的生产级 Agent 平台。

## 1. 总体架构

设计总览：平台把"一次对话"收敛为一条固定管线——**接入（验签/去重）→ 路由（确定性 session_id）→ 治理（预算/限流前置校验）→ 执行（Runner + Agent + 工具治理过滤链）→ 记账（预算/指标/审计）→ 触达（IM 回复/卡片）**。管线上的每个环节都无状态或依赖共享后端，因此可以按环节独立扩容：gateway 多副本承载回调洪峰，worker 多副本承载模型推理。租户是所有配置、数据与治理策略的唯一划分维度：每个租户一份 TenantConfig、一个 Runner、一套独立的数据键空间与审计轨迹。

选型原则：**能复用框架的不自研**（Session/Memory 三后端、Filter 机制、OTel 埋点均来自 tRPC-Agent-Python），平台只新增框架没有的"多租户层"（租户模型、路由、预算限流、审计、IM 双通道协议、Admin API）。数据侧按特性分后端：热会话走 Redis，合规审计走 MySQL，消息幂等以 SQL 唯一索引兜底，沙箱产物落本地/容器工作区。

```
                 ┌────────────────────────────────────────────────┐
   Web UI ─────► │                Agent Gateway                    │
   飞书/企微 ───► │  路由 / 预算前置校验 / 用户权限 / 验签去重        │
                 └───────────────┬────────────────────────────────┘
                                 │ SessionRouter（无状态路由）
                 ┌───────────────▼────────────────────────────────┐
                 │                Agent Worker                     │
                 │  Runner + LlmAgent + Tool Filter 治理链         │
                 │  （白名单 / 脱敏 / 预算 / 危险确认 / 计时）       │
                 └──────┬─────────────────┬───────────────────────┘
                        │                 │
              ┌─────────▼───────┐ ┌───────▼────────────┐
              │ Storage Adapter │ │  Telemetry (OTel)  │
              │ InMemory/Redis/ │ │  trace + gen_ai 指标│
              │ SQL Session/    │ └───────┬────────────┘
              │ Memory 服务     │         │
              └─────────┬───────┘         │
                        ▼                 ▼
                 Redis / SQL（MySQL）   OTLP Collector / Jaeger
                        ▲
              ┌─────────┴─────────┐
              │ Audit 审计服务     │（JSONL 兜底 / SQL 表）
              └───────────────────┘
```

### 组件职责

| 组件 | 职责 | 代码位置 |
|------|------|----------|
| Agent Gateway | 统一入口：会话路由、预算前置校验、IM 验签解密去重、用户权限 | `web/` + `agent/routing.py` + `channels/` |
| Agent Worker | 无状态执行体：Runner + Agent + 治理 Filter，可水平扩展 | `agent/` + `worker.py` + `tenant/governance/` |
| Channel Adapter | IM 协议适配（Web UI / 飞书 / 企微HTTP回调 / 企微智能机器人长连接） | `channels/` |
| Storage Adapter | Session/Memory 三后端装配（InMemory/Redis/SQL）+ 后端延迟采集 | `tenant/storage/factory.py` |
| Admin API | 租户 CRUD、热加载、版本回滚、灰度发布、审计与指标查询 | `web/app.py` |
| Telemetry Collector | trace/指标收集上报（trace + metrics 双通道） | `metrics/` + `deploy/otel-config.yaml` |
| Audit 审计 | 每请求审计落盘（脱敏后，SQL 批量 + JSONL 兜底） | `tenant/audit/` |
| 沙箱与技能 | Skill 脚本执行隔离（本地目录/Docker 两级） | `workspace/` + `skill/` |

## 2. 多租户与节点部署

### 2.1 租户模型

租户配置覆盖：应用配置（app_name/提示词）、模型配置（provider/model_name/api_key/base_url）、工具权限（allowed/blocked）、IM 通道配置（feishu 的 app_id/app_secret/token/encrypt_key，wecom 的 token/corp_id/encoding_aes_key/bot_id）、数据后端配置（session_backend/redis_url/sql_url）、审计策略（mask_pii/retention）。密钥**只经环境变量注入**，不写入配置文件与日志。

### 2.2 无状态路由（无 sticky session）

`SessionRouter` 用 `sha1(tenant:channel:user:chat)[:32]` 生成稳定 session_id。任意节点收到同一用户消息都能推导出同一 session_id，配合共享 Session 后端（Redis/SQL）实现**无 sticky 的无状态 Worker**：会话状态全部在存储后端，节点本地只缓存。

### 2.3 租户隔离

- **配置隔离**：每租户独立 TenantConfig，ConfigManager 按租户装配 Runner。
- **数据隔离**：框架 Session/Memory 的 key 为 `{app_name}/{user_id}`，app_name 绑定租户，天然按租户切分。
- **工具权限隔离**：TOOL 层白名单 Filter 按 AgentContext 中的 tenant_id 校验。
- **日志脱敏与密钥管理**：审计/日志入库前经 `audit/redact`（键名匹配整值遮蔽 + PII 打码）；api_key/token 不出现在日志、trace 与错误报告。

### 2.4 IM 账号与租户绑定（webhook / token / 验签 / 去重 / 身份映射）

| 要求 | 实现 |
|------|------|
| webhook URL | 每租户独立回调地址（URL 携带 tenant_id）：`/api/v1/channels/wecom/webhook/{tenant_id}`、`/api/v1/channels/feishu/webhook/{tenant_id}`；通道未启用返回 404 |
| token / secret | 每租户独立 ChannelConfig：企微 `token/encoding_aes_key/corp_id/bot_id`，飞书 `app_id/app_secret/token/encrypt_key`；密钥不进代码与日志（redact 兜底） |
| 回调验签 | 企微：SHA1(sort(token,timestamp,nonce,encrypt)) + AES-256-CBC 解密；飞书：SHA256(timestamp+nonce+encrypt_key+body) + verification token；不符一律 403 |
| 消息去重 | 三层幂等：① Deduper（Redis SETNX 多节点共享，`DEDUPE_REDIS_URL`，故障永久降级内存）→ ② SQL `idempotency` 表唯一索引兜底（进程重启仍拦截）→ 幂等键 `{channel}:{msg_id}` |
| 用户身份映射 | IM 侧 `open_id/userid` + `chat_id` → `SessionRouter` 生成 `session_id` → `channel_binding` 表落库（tenant_id, channel_type, external_user_id, chat_id, session_id，唯一约束）；治理/审计按此身份贯穿 |

### 2.5 Web 端租户管理（Admin API）

- **CRUD**：`GET/POST/DELETE /api/v1/tenants`（需 `ADMIN_API_KEY`）。创建 → `ConfigManager.register`（app_name 全局唯一校验）→ 即时装配 Runner → 落 `tenant`/`agent_app` 表（节点重启自动恢复）；删除 → 下线 Runner + 清配置 + 清库。
- **版本与回滚**：每次保存自动版本化（`tenant.revision` + `tenant_revision` 历史表）；`GET /api/v1/tenants/{id}/revisions` 查看，`POST .../rollback` 按 revision 回滚并热重建 Runner，回滚本身产生新版本可再回滚。
- **配置面**：`config/tenants.yaml` 为引导源，SQL 为持久层；`POST /api/v1/tenants/reload`（Admin）触发热加载并重建配置有变化的租户 Runner（含 SQL 持久层租户重新合入、YAML 移除租户下线）。
- **运营面**：`/api/v1/audit`（审计查询）、`/api/v1/metrics`（每租户请求量/错误率/IM 投递成功率/token）。

### 2.6 多节点设计

多节点 = 同一服务多进程/多机实例（如 2 网关 + N worker），目标是水平扩容 + 高可用。

1. **Worker 无状态（无 sticky session 的论证）**：`session_id` 为确定性哈希（2.2），任意节点对同一请求推导出同一 session，状态全在共享后端 → 任意节点可处理任意请求，扩容 = 加副本。
2. **跨节点共享状态**：预算（Redis INCRBY）/去重（Redis SETNX）/限流（Redis 窗口）多节点原子共享且故障降级单机；Session/Memory 按租户后端天然共享；trace_id 随 AgentContext 贯穿并落审计，任意节点可查。
3. **Gateway/Worker 分离**：`QUEUE_MODE=redis` 时 gateway 只做校验/入队/等结果，`python -m trpc_service.worker` 无状态消费（BRPOPLPUSH，崩溃任务留存 processing 可重放）；默认 inline 单进程零依赖。
4. **明确取舍**：熔断状态为进程内（快速失败保护，节点本地视角足够，不引入跨节点依赖降低可用性）。

## 3. 复用 tRPC-Agent-Python vs 平台新增

| 能力 | 来源 |
|------|------|
| Runner/RunConfig/Event 流、LlmAgent、OpenAIModel | 框架直接复用 |
| Session/Memory 三后端（InMemory/Redis/SQL） | 框架直接复用，平台封装为 StorageAdapter |
| Filter 机制（BaseFilter + register_*_filter + filters_name） | 框架机制复用，白名单/脱敏/预算/二次确认为平台实现 |
| OTel 埋点（invocation/call_llm/execute_tool span + gen_ai 指标） | 框架自动埋点，平台补 OTLP exporter 初始化 |
| FunctionTool / MCPToolset / LoadMemoryTool | 框架直接复用 |
| 租户模型/配置热加载、Session 路由、预算、审计、飞书/企微协议、Admin API | 平台新增 |

## 4. 治理、监控和安全

治理过滤链（顺序执行）：`tool_whitelist` → `pii_mask` → `budget_limit` → `dangerous_confirm`，均以 TOOL Filter 实现，租户差异从 AgentContext 元数据解析；IM 用户权限校验在网关层（channel_binding）执行。

监控：框架自动产出 invocation/call_llm/execute_tool span 与 gen_ai 指标；平台补齐 OTLP exporter（trace + metrics 双通道，`OTEL_ENABLED=1` 开关）与业务指标采集器（`metrics/collector.py`）：按租户聚合请求量/错误率/平均耗时/工具调用/IM 投递成功率/token 消耗，`GET /api/v1/metrics` 查询。trace_id 由入口生成，注入 AgentContext 元数据贯穿全链路（见 `docs/mermaid/sequence.mermaid`），并写回审计记录。

安全：Admin API 与 chat 端点 API-Key 鉴权（`ADMIN_API_KEY` / `CHAT_API_KEY`，未配置为开发模式放行），比较用 `secrets.compare_digest`；CORS 白名单（`CORS_ORIGINS`）；飞书/企微回调不走 API-Key（各自自带验签，信任模型不同）；密钥只经环境变量注入，`redact()` 在审计进缓冲前脱敏。

审计字段（验收标准）：`tenant_id / channel / user_id / session_id / agent_name / tool_name / decision / latency / error_type / cost / trace_id`。

## 5. IM 接入

- **Web UI**：`POST /api/v1/chat` 同步返回聚合回复，`session_id` 维持多轮上下文。
- **飞书（协议已实现，模拟验证）**：`channels/feishu.py` 实现事件订阅 v2.0：URL 验证（challenge 回传）、SHA256 验签（encrypt_key）、verification token 校验、message_id 去重、`open_id`→session 身份映射、群聊拼 chat_id 隔离、立即 ACK + 异步主动回复（tenant_access_token 鉴权，超长分片）。单聊 session=`sha1(tenant:feishu:open_id)`；群聊追加 chat_id。
- **企业微信（两种形态，双轨可选）**：
  - **形态一·HTTP 回调被动回复（已实现，模拟验证）**：`channels/wecom.py`——URL 验证（echostr 解密）、SHA1 验签、AES-256-CBC 解密、MsgId 三层幂等、`FromUserName`→session 身份映射、群聊拼 ChatId 隔离、加密被动回复与超长分片。单聊 session=`sha1(tenant:wecom:userid)`；群聊追加 ChatId。前置条件：回调 URL 域名备案主体须与企业一致（企微平台准入政策）。
  - **形态二·智能机器人长连接（已实现，真机验证通过）**：`channels/wecom_smartbot.py`——基于 `wecom-aibot-sdk-python` 的 WSClient（BotID/Secret 登录 WebSocket，SDK 内置心跳/重连），消息桥接进统一管线（幂等→路由→限流→execute_chat→reply_stream 回复）。**免公网地址与备案域名**，无 5 秒回复限制；Secret 经 `WECOM_BOT_SECRET` 环境变量注入，同租户同 Bot 单连接（多节点经 Redis 抢占锁接管，演进项）。
- **IM 平台限制应对（已实现）**：长度限制（企微被动回复 1800 字分片）、重复投递（三层幂等）、频率限制（rate_limit_per_minute 每用户每分钟窗口计数，Redis 共享 + 内存降级）、图片/语音/视频/文件（识别类型友好回复引导文本对话，多媒体解析为预留）、失败重试（飞书/企微事件重投 + 幂等拦截）。

## 6. 故障恢复与运维

- **韧性层（agent/resilience.py）**：超时/连接类错误指数退避重试（`LLM_RETRY_MAX=2`）；租户级熔断（连续失败 ≥ `CIRCUIT_FAILURE_THRESHOLD=5` 转 open 快速失败，冷却 `CIRCUIT_COOLDOWN=30s` 后 half-open 试探恢复）；BudgetExceeded 属业务拒绝不触发熔断。
- **降级**：模型超时/错误 → 统一兜底话术 + 审计 error_type；数据库短暂不可用 → 文件审计兜底 + 幂等放行；工具失败 → function_response 错误回传由模型自解释。
- **队列化执行（两模式）**：默认 inline（单进程直连，零外部依赖）；`QUEUE_MODE=redis` 时 gateway 入队（LPUSH）、无状态 worker 消费（BRPOPLPUSH，崩溃任务留存 processing 可重放）、结果键 TTL 60s 回传。两种模式共用 `chat.py` 执行管线（预算→韧性→指标→审计→脱敏）。
- **灰度与回滚**（release_stage 机制）：租户配置版本化（tenant.revision + tenant_revision 历史表）。灰度单位 = 租户：`POST /api/v1/tenants/{id}/release` 把指定租户切到目标 revision 并标记 `canary` → 观察 `/api/v1/metrics`（错误率/延迟）→ 正常则以同一 revision 推 `stable`，异常则 `POST .../rollback` 秒回；灰度动作本身产生新版本，全程可审计可回滚。tenants.yaml 变更经 `POST /api/v1/tenants/reload` 热加载。
- **容量评估**：每节点并发 session 数受事件循环与模型并发限制；Redis QPS ≈ 请求 QPS ×（session 读写 + memory 读写 + 预算/限流/去重计数）；IM 回调峰值按事件订阅重投策略反推；worker 扩容 = 加副本（无状态）。
- **部署**：最小方案 = 单进程 `python -m trpc_service.web.app`（InMemory）；生产方案 = `deploy/docker-compose.yml`（gateway + worker 真分离、Redis、MySQL、OTel Collector），K8s 部署 gateway/worker Deployment + HPA，存储用云 Redis/MySQL。

## 7. 数据同步与多后端

详见 `docs/sync-and-idempotency.md`、`docs/backend-adapter.md`、`docs/data-model.md`。要点：append 语义 + state_delta 键值合并解决并发写（并发实测结论见 sync-and-idempotency.md）；event→state→summary 顺序固定；Memory 写后读依赖后端 flush；Redis→SQL 迁移 = 停写→scan 全量→schema 转换导入→切读。

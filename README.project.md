# trpc-agent-service：多租户节点化 Agent 部署平台

基于 [tRPC-Agent-Python](https://example.com/trpc-agent-py)（`trpc-agent-py`）构建的多租户 Agent 平台：多租户隔离、节点化部署、Redis/SQL 双后端、Web UI 与飞书/企业微信接入（含智能机器人长连接形态）、治理过滤、指标审计、故障恢复。

## 架构摘要

```
Web UI / 飞书 / 企业微信 ──► Gateway(FastAPI) ──► Worker(队列模式) ──► AgentRunner
                        │                    │                  ├─ 治理 Filter（白名单/脱敏/预算/二次确认）
                        ├─ SessionRouter（确定性路由，无 sticky session）        ├─ SkillToolSet（技能，经沙箱执行）
                        ├─ BudgetManager / RateLimiter（Redis 共享计数）         └─ Session/Memory（租户可选 Redis/SQL）
                        └─ Channel Adapter（飞书验签/幂等/异步回复）
                                                   │
                        Audit(SQL+JSONL 兜底) · Metrics(OTel) · 平台表(tenant/audit_log/binding/idempotency)
```

- 完整设计：[docs/architecture.md](docs/architecture.md)；架构图与时序图：[docs/mermaid/](docs/mermaid/)
- 数据模型：[docs/data-model.md](docs/data-model.md)；同步与幂等：[docs/sync-and-idempotency.md](docs/sync-and-idempotency.md)；风险清单：[docs/risk-list.md](docs/risk-list.md)

## 快速开始

```bash
# 1. 构建（创建 .venv 并安装依赖）
bash build.sh

# 2. 配置环境变量（模型 Key / 平台库 / 管理密钥）
cp .env.example .env    # 编辑 KEY / URL / MODEL / SQL_URL / ADMIN_API_KEY

# 3. 数据库迁移（MySQL；不配 SQL_URL 则全内存运行，可跳过）
.venv/Scripts/python.exe -m trpc_service._cli migrate

# 4. 启动 / 停止
bash start.sh           # http://localhost:8000 （Web UI 聊天页）
bash stop.sh
```

默认 inline 模式单进程即可运行（Session/InMemory、预算/内存计数）。

## 部署模式

| 模式 | 开关 | 说明 |
|------|------|------|
| inline（默认） | 无 | 单进程，零外部依赖，开发/演示 |
| 队列分离 | `QUEUE_MODE=redis` + `REDIS_URL` | gateway 入队，`python -m trpc_service.worker` 无状态消费，加副本即扩容 |
| 企微长连接 | `wecom_smartbot` 通道 | 智能机器人 BotID/Secret，WebSocket 长连接免公网（Secret 走 WECOM_BOT_SECRET 环境变量） |`n| 多节点共享状态 | `BUDGET_REDIS_URL` / `DEDUPE_REDIS_URL` / `RATE_LIMIT_REDIS_URL` | 预算/去重/限流跨节点共享，故障自动降级单机 |

生产推荐：`docker compose -f deploy/docker-compose.yml up -d`（gateway + worker + Redis + MySQL + OTel Collector）。

## API 一览

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| POST | `/api/v1/chat` | Web 聊天（reply/reasoning/session_id/trace_id） | CHAT_API_KEY（可选） |
| GET | `/api/v1/chat/history` | 会话历史（刷新/重开续聊） | CHAT_API_KEY（可选） |
| GET/POST/DELETE | `/api/v1/tenants[...]` | 租户管理（Admin API） | ADMIN_API_KEY |
| POST | `/api/v1/tenants/reload` | YAML 热加载 + 变更租户 Runner 重建 | ADMIN_API_KEY |
| GET | `/api/v1/tenants/{id}/revisions` | 配置版本列表 | ADMIN_API_KEY |
| POST | `/api/v1/tenants/{id}/rollback` | 按 revision 回滚并热重建 | ADMIN_API_KEY |
| POST | `/api/v1/tenants/{id}/release` | 灰度发布（指定 revision + canary/stable） | ADMIN_API_KEY |
| GET | `/api/v1/audit` / `/api/v1/metrics` | 审计查询 / 每租户指标（含工具耗时/后端延迟） | ADMIN_API_KEY |
| POST | `/api/v1/channels/feishu/webhook/{tenant_id}` | 飞书事件订阅（challenge/验签/去重/卡片回复） | 飞书签名 |
| GET/POST | `/api/v1/channels/wecom/webhook/{tenant_id}` | 企微回调（验签+AES 被动回复） | 企微签名 |

## 租户配置（config/tenants.yaml）

每个租户独立配置：应用（app_name/instruction）、模型、Session/Memory 后端（in_memory/redis/sql）、工具白名单、飞书通道（app_id/app_secret/token/encrypt_key）/ 企微通道（token/aes_key/corp_id/bot_id）、审计策略、预算（daily_api_calls/daily_token_budget）、限流（rate_limit_per_minute）、沙箱（workspace.mode）、技能（skills.enabled）。Admin API 创建的租户自动落库（tenant 表），重启自动恢复。

## 治理与可观测

- **Filter 链**：tool_whitelist → pii_mask → budget_limit → dangerous_confirm → tool_latency（TOOL 层，按租户配置）
- **韧性**：超时指数退避重试（`LLM_RETRY_MAX`）、租户级熔断（`CIRCUIT_FAILURE_THRESHOLD`/`CIRCUIT_COOLDOWN`）
- **灰度**：`release_stage`（canary/stable）+ release/rollback API（详见 docs/architecture.md §6）
- **思考分离**：模型思考内容不混入正文，Web 折叠展示 / 飞书卡片折叠面板 / 企微纯文本剥离
- **审计**：全字段落 SQL（失败降级 JSONL），入库前 redact 脱敏；trace_id 贯穿 IM 回调→Agent→工具→存储→回复
- **指标**：请求量/错误率/耗时/IM 投递成功率/token（进程内聚合 + OTLP 上报）

## 开发

```bash
bash coverage.sh          # 单测覆盖率（pytest）
bash lint_flake8.sh       # 代码检查
bash format.sh            # yapf 格式化
```

源码结构见 `trpc_service/`：agent（执行+韧性）、channels（IM 接入）、config（租户配置）、log、metrics（指标+OTel）、skill（技能）、tenant（治理/审计/预算/限流/存储）、tool、web（API+鉴权+执行管线）、workspace（沙箱）、worker.py（队列消费）。

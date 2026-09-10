# 基于 tRPC-Agent 设计多租户节点化 Agent 部署平台

## 当前实现状态

已完成实施计划第 1～7 步代码、自动化验证和真实 IM 收发验收：跨平台 `uv` 环境引导、
项目/FastAPI 骨架、分环境安全配置、SQLite 数据模型与 Repository，以及真实 tRPC
`LlmAgent + Runner` 执行路径、calculator Tool、多租户 Session 路由、基础 Admin API、
`InlineExecutionBus` / `WorkerService` 分层和 `/v1/chat`；并已实现企业微信、Telegram
真实协议适配、Binding 反查、Webhook 验签、SQLite 幂等、输入治理和 `/metrics`。租户现在还可
分别选择 SQLite 或 Redis 作为 tRPC Session 与 Memory 后端；成功对话会写入 Memory 和带覆盖
序号的 Summary。项目同时提供 SQLite Knowledge 检索、本地租户隔离 Artifact Store、
OpenTelemetry SDK span、模型超时边界和单服务 Dockerfile/Compose。另有可选的多副本路径：
PostgreSQL 事实库、SQL Execution Outbox、Redis Stream 请求/响应队列、owner-token 会话锁、
独立 Gateway/Worker 进程，以及 2 Gateway + 2 Worker 的 Kubernetes 清单。

自动化测试通过依赖注入使用 TestModel 和 TestSender；正式运行不会回退到测试实现。真实模型
问答和 calculator Tool 已通过 OpenAI 兼容服务联调。企业微信、Telegram 支持不需要公网 URL 的
主动连接模式，也保留 webhook 模式，所需配置见[第 6 步 IM 接入指南](docs/step6-channel-setup.md)。
Telegram 已验证连续多轮回复，企业微信 AIBot 已验证真实回复；对应记录包含 delivered 状态、
allowed 审计和贯穿 Tool 调用的 trace_id。验收进程已停止，后续启动仍需运行 `run-channels`。

```sh
sh bootstrap.sh
sh test.sh
sh start.sh
# 另一个终端中启动无需公网 URL 的 IM 长连接
sh channels.sh
```

这些 POSIX `sh` 脚本可直接运行于 Linux/macOS；Windows 使用 Git Bash 或 WSL 执行同一命令。
项目环境统一由 `uv` 创建在 `.venv`，不再要求固定名称的 Conda 环境。

启动后可访问 `/health`、`/ready` 和 `/docs`。换电脑安装请先看
[新电脑环境配置](docs/environment-setup.md)，基础代码说明见
[第 1～4 步开发指南](docs/steps-1-4.md)，整体方案见
[架构设计](docs/multi-tenant-agent-platform-design.md)。

### 第 5 步真实模型配置

本地开发只需复制一次模板并填写 `.env`；启动脚本会自动加载，不需要再执行 `export`。
`.env` 已被 Git 忽略，绝不能提交到仓库：

```sh
cp .env.example .env
```

在 `.env` 中填写 Provider 配置和真实 Secret：

```dotenv
TRPC_SERVICE_APP_ENV=development
TRPC_SERVICE_MODEL_PROVIDER=openai
TRPC_SERVICE_MODEL_NAME=你的模型名称
TRPC_SERVICE_MODEL_BASE_URL=https://你的模型服务地址/v1
TRPC_SERVICE_MODEL_API_KEY_REF=env://TRPC_AGENT_API_KEY
TRPC_SERVICE_ADMIN_API_KEY_REF=env://TRPC_SERVICE_ADMIN_API_KEY
TRPC_SERVICE_SESSION_HMAC_KEY_REF=env://TRPC_SERVICE_SESSION_HMAC_KEY
TRPC_AGENT_API_KEY=模型APIKey
TRPC_SERVICE_ADMIN_API_KEY=自行设置的管理APIKey
TRPC_SERVICE_SESSION_HMAC_KEY=至少16字符的随机HMACKey
TRPC_TELEGRAM_BOT_TOKEN=TelegramBotToken
TRPC_WECOM_BOT_SECRET=企业微信BotSecret
```

填写后执行 `sh start.sh`；需要 IM 时在另一个终端执行 `sh channels.sh`。生产环境仍建议由
容器编排或 Secret 管理系统注入密钥，外部环境变量的优先级高于 `.env`。

`MODEL_BASE_URL` 必须是 HTTPS。服务在 development/production 环境启动时会解析三个
Secret 引用；缺失、为空、使用 test/mock/fake Provider 或直接提交 `api_key` 都会失败关闭。
当前正式模型工厂支持 OpenAI 兼容接口。

### 第 5 步 API

- `POST/GET /admin/tenants`：创建、列出租户。
- `POST/GET /admin/tenants/{tenant_id}/apps`：创建、列出 Agent App。
- `POST/GET /admin/tenants/{tenant_id}/bindings`：创建、列出通道绑定。
- `POST /v1/chat`：通过真实 tRPC Runner 完成非流式对话，返回 `session_id`、`trace_id`
  和 Tool 事件。

Admin API 使用 `X-Admin-API-Key`。test/development 的 HTTP chat 使用 `X-Tenant-ID`；
production 还必须发送 `X-Tenant-API-Key`，其 Secret 引用配置在租户
`audit_policy.http_api_key_ref` 中。两个租户即使使用相同用户和外部会话标识，也会得到不同的
HMAC Session ID。

### 第 6 步 IM 与监控

- `run-channels`：发现 `connection_mode=pull` 的 Binding，启动 Telegram long polling 和
  企业微信 AIBot WebSocket；无需公网 URL 或端口转发。
- `POST /webhooks/telegram/{account_id}`：验证 Telegram webhook secret、去重、执行并回复。
- `GET/POST /webhooks/wecom/{corp_id}`：企业微信 URL 验证、SHA1 验签、AES 解密、执行并回复。
- `GET /metrics`：Prometheus 请求量、Agent 执行结果与延迟、Tool 事件、Memory/Summary
  存储结果与延迟、IM 发送结果。

通道绑定现在包含 `app_id` 和 `connection_mode`。Pull 模式下，Telegram 使用 `getUpdates`，
企业微信使用官方 AIBot SDK 的 WebSocket 长连接；Webhook 模式仍按 URL 中的账号反查租户与
Agent。Telegram 文本按 4000 字符安全拆分，HTTP 429 最多按 `retry_after` 重试一次。当前只
处理文本消息；文件、图片和生产 Reply Outbox 属于后续演进能力。

### 租户级多后端

`tenant.storage_config` 控制 Session 与 Memory 的运行后端，当前可执行选项是 `sqlite` 和
`redis`。SQL 始终保存租户配置、Inbound 幂等、Session Event 和 Audit 等关键事实；后端选择
只改变 tRPC Session 历史与 Memory 的读写位置，避免把审计和去重事实放进可丢失缓存。

创建租户时可以直接指定，或通过 `PUT /admin/tenants/{tenant_id}/storage` 更新：

```json
{
  "storage_config": {
    "session_backend": "redis",
    "memory_backend": "redis",
    "redis_url_ref": "env://TRPC_REDIS_URL"
  }
}
```

Redis 地址只能以 `env://`、`file://` 或 `vault://` Secret 引用保存，不能把连接串直接写进
租户记录。配置变更后，下一次创建对应 Agent Runner 时会按租户选择官方
`RedisSessionService`/`SqlSessionService`；Memory 则通过统一 `MemoryStore` 接口路由。
代码还定义了 `SummaryStore`、`KnowledgeStore`、`ArtifactStore`、`AuditStore`，当前可执行实现
包括 SQLite Knowledge 和 `data/artifacts/<tenant_id>/...` 本地 Artifact（校验大小、路径和
SHA-256）。向量检索、对象存储和外部 Memory 属于生产扩展。

每轮顺序为 Session Event → Session 状态 → Memory → Summary；Memory 用来源 Event 唯一键，
Summary 保存 `source_end_sequence` 和递增版本。默认单进程路径使用本机会话锁；多副本路径使用
PostgreSQL Outbox、Redis Stream Consumer Group 和带 owner token 的 Redis 会话锁。Worker
失败会有限重试并进入死信状态。完整事务 Inbox/Reply Outbox、锁续租和自动人工重放仍是演进项。

### 治理和追踪边界

租户 `audit_policy` 可配置 `allowed_users`、`denied_users`、`max_message_chars` 和
`requests_per_minute`，用于 IM 入口的权限、长度和单进程限流。Agent App 的 `tool_policy.allow`
控制可用 Tool；模型执行受 `TRPC_SERVICE_AGENT_TIMEOUT_SECONDS` 限制。OpenTelemetry SDK 为
Channel、Runner、Memory、Summary 建立 span，并用 `trace_id` 关联平台事件和审计；开发时可设置
`TRPC_SERVICE_OTEL_CONSOLE_EXPORTER=true` 输出 span。OTLP Collector、token/租户成本统计、
危险 Tool 审批和跨节点全局限流尚未实现。

本机 Redis 集成测试示例：

```sh
docker run -d --name trpc-agent-redis-test -p 127.0.0.1:16379:6379 redis:7-alpine
export TRPC_TEST_REDIS_URL="redis://127.0.0.1:16379/15"
uv run --frozen python -m pytest -q tests/test_storage_backends.py
```

### Docker Compose 最小部署

Compose 只使用当前 `trpc-agent-service` 仓库作为构建上下文，tRPC-Agent SDK 从 PyPI 安装。
克隆这一个仓库后即可在服务目录运行：

```sh
docker compose up --build -d
docker compose ps
curl -fsS http://127.0.0.1:8000/ready
```

默认启动单个 Agent Service、持久化 SQLite 数据卷和持久化 Redis。开发/演示环境使用真实模型
时，只需按上面的方式填写 `.env`；Compose 会读取同一个文件，且不会把值构建进镜像。停止
服务使用 `docker compose down`；不要加 `-v`，否则会
删除 SQLite/Redis 数据卷。

### 多 Worker 与 Kubernetes 部署

不用 Kubernetes 时，可先用 Compose 验证 PostgreSQL + Redis + 独立 Worker（1 个 Gateway、
2 个 Worker）：

```sh
docker compose -f compose.multi.yaml up --build -d --scale worker=2
curl -fsS http://127.0.0.1:18000/ready
```

Kubernetes 清单默认部署 2 个 Gateway、2 个 Worker、1 个 PostgreSQL 和 1 个 Redis：

```sh
cp deploy/k8s/secret.example.yaml deploy/k8s/secret.local.yaml
# 编辑 secret.local.yaml 中的 replace-me
kubectl apply -f deploy/k8s/base/namespace.yaml
kubectl apply -f deploy/k8s/secret.local.yaml
kubectl apply -k deploy/k8s/base
```

本地已安装 kind 时，也可执行 `sh deploy/k8s/kind-up.sh` 完成镜像构建、加载和部署，再运行
`sh deploy/k8s/smoke.sh`。详情及扩缩容命令见
[K8s 部署说明](docs/kubernetes-deployment.md)。示例 Secret 只有占位值；健康检查不会调用模型。

## 背景和价值
企业在落地 Agent 应用时，通常不会只部署一个单体机器人，而是希望面向多个部门、多个业务线、多个 IM 入口和多个数据后端
，构建一套可统一管理的 Agent 平台。例如：客服团队希望把 Agent 接入企业微信，研发团队希望接入内部群机器人，运营团队>希望接入微信公众号或微信客服，不同租户又需要隔离会话、记忆、知识库、工具权限和审计日志。
tRPC-Agent-Python 已经具备 Agent 编排、Tool / MCP、Session、Memory、Knowledge、Filter、Telemetry、FastAPI 服务化、OpenClaw / IM 通道、A2A / AG-UI 等能力。该题要求基于这些能力设计一个“多租户、可节点化部署、支持多后端数据同步、可接>入微信 / 企业微信等 IM 软件”的生产级方案。
这个题目解决的业务痛点是：企业希望把 Agent 能力从单点 demo 扩展成平台化服务，同时满足租户隔离、弹性部署、数据一致性
、IM 触达、审计合规和后端可替换等要求。它的价值在于把框架能力真正映射到企业级 Agent 平台架构，而不是只停留在单个 Agent 脚本。 
根据需要可以选择Python或者Go语言框架进行实现
任务描述
请设计一个基于 tRPC-Agent-Python 的多租户节点化 Agent 部署平台。平台需要支持多个租户创建和部署自己的 Agent，每个租>户可以绑定不同 IM 通道、选择不同数据后端、配置不同工具权限和知识库，并允许多个 Agent 节点水平扩展。系统需要考虑跨节
点会话路由、数据同步、后端适配、IM 消息接入、监控审计和故障恢复。
本题以架构设计为主，可以包含少量关键伪代码、接口定义或数据模型示例。不要求实现完整系统，但方案必须足够具体，能指导>后续工程落地。

## 具体要求
### 多租户与节点部署
- 设计租户模型，至少包含 tenant_id、应用配置、模型配置、工具权限、IM 通道配置、数据后端配置、审计策略。
- 设计节点部署拓扑，说明 Agent Gateway、Agent Worker、Channel Adapter、Storage Adapter、Admin API、Telemetry Collector 等组件如何协作。
- 支持多节点水平扩展，说明用户消息如何路由到正确租户和正确 session。
- 说明是否需要 sticky session；如果不需要，说明如何依赖共享 Session / Memory 后端实现无状态 Worker。
- 设计租户隔离机制，包括配置隔离、数据隔离、工具权限隔离、日志脱敏和密钥管理。

### 数据同步与多后端支持
- 支持不同租户选择不同数据后端，例如 InMemory、Redis、SQL、向量库、对象存储或外部 Memory 服务。
- 设计统一的数据访问抽象，说明 Session、Memory、Summary、Artifact、Knowledge、Audit Log 分别如何存储。
- 设计数据同步策略，至少覆盖：  
- 多节点并发写入同一 session 的一致性。
- Session event、state、summary 的更新顺序。
- Memory 写入后的跨节点可见性。
- 后端从 Redis 迁移到 SQL 或从本地向量库迁移到远端向量库时的数据迁移方案。
- IM 消息重复投递时的幂等处理。
- 说明不同后端的一致性取舍，例如强一致、最终一致、读写延迟、成本和运维复杂度。
- 给出一个最小数据模型或表结构示例，至少包含 tenant、agent app、session、message/event、memory、summary、channel binding、audit log。

### IM 软件接入
- 设计 IM Channel Adapter，支持企业微信、微信客服、微信公众号、Telegram 或其他 IM 通道中的至少两类。
- 说明外部 IM 消息如何转换为 tRPC-Agent-Python 的用户输入，Agent Event 如何转换为 IM 回复、流式消息或卡片消息。
- 设计 IM 账号和租户绑定方式，包括 webhook URL、token、secret、回调验签、消息去重、用户身份映射。
- 说明群聊和单聊的 session_id 生成规则，以及用户跨群、跨租户时的隔离策略。
- 考虑 IM 平台限制，例如消息长度、频率限制、异步回复、图片 / 文件消息、撤回或失败重试。

### 治理、监控和安全
- 使用 Filter 设计租户级治理策略，例如工具白名单、敏感信息脱敏、预算限制、危险工具二次确认、IM 用户权限校验。
- 设计监控指标，例如请求量、模型调用耗时、工具调用耗时、IM 投递成功率、错误率、token 消耗、每租户成本、Session 后端延迟。
- 说明如何接入 OpenTelemetry 或等价 tracing，要求 trace 能串起 IM callback、Runner 执行、Tool 调用、Session / Memory 读写和 IM 回复。
- 设计审计日志字段，至少包含 tenant_id、channel、user_id、session_id、agent_name、tool_name、decision、latency、error_type、cost、trace_id。
- 说明密钥管理和脱敏策略，IM token、模型 API key、数据库密码不能明文出现在日志、trace 或错误报告中。

### 故障恢复与运维
- 设计节点故障、IM 重试、数据库短暂不可用、模型超时、工具执行失败时的降级策略。
- 说明如何做灰度发布和租户级配置回滚。
- 说明如何做容量评估，例如每节点并发 session 数、平均 token 消耗、Redis / SQL QPS、IM 回调峰值。
- 设计最小可运行部署方案和生产推荐部署方案，可以使用 Docker Compose、Kubernetes 或等价部署方式描述。
交付物
- 一份架构设计文档，建议 2000 – 4000 字。
- 一张系统架构图，展示 Gateway、Worker、Channel Adapter、Storage Adapter、Filter、Telemetry、数据库和 IM 平台之间的
关系。
- 一张核心时序图，展示“企业微信用户发消息 → Agent 执行 → Tool 调用 → Session / Memory 写入 → IM 回复”的完整链路。
- 一份数据模型设计，包含核心表结构或 JSON schema。
- 一份数据同步和幂等策略说明。
- 一份多后端适配方案，说明 Redis / SQL / 向量库 / 对象存储分别适合存什么。
- 一份风险清单，列出至少 8 个生产风险及对应缓解措施。 
- 一份基于该设计的github实现的代码 

## 题目难点
- 多租户隔离不是只加一个 tenant_id 字段，还涉及配置、权限、密钥、数据、日志、工具和成本隔离。
- 节点化部署要求 Agent Worker 尽量无状态，但 Agent 又天然依赖 Session、Memory、Summary 和工具上下文，需要设计可靠的
共享状态层。
- IM 通道存在消息乱序、重复投递、响应超时、长度限制和身份映射问题，不能简单等同于 HTTP chat API。
- 不同后端的数据一致性能力不同，Redis、SQL、向量库、对象存储无法用同一种同步策略处理。
- Agent 执行链路包含模型、工具、MCP、知识库、沙箱和外部系统，监控和审计必须跨组件串联。 
- 企业级平台必须考虑灰度、回滚、租户级限流、成本控制和合规审计。 

## 验收标准
1.架构方案必须覆盖多租户、节点化部署、数据同步、多后端支持、IM 接入、治理监控和故障恢复。
2.数据模型必须能表达 tenant、agent、channel binding、session、event、memory、summary、audit log 的关系。
3.必须说明至少两种 IM 通道的接入差异，其中至少包含微信或企业微信。
4.必须说明至少三类后端的数据存储和同步策略，例如 Redis、SQL、向量库或对象存储。
5.必须给出一条完整消息链路的时序说明，包含 trace_id 或 request_id 如何贯穿链路。 
6.必须列出至少 8 个生产风险和缓解措施。 
7.方案需要明确哪些能力可直接复用 tRPC-Agent-Python，哪些需要新增平台层模块。

## 代码目录

```txt
|-- README.md  # 说明文档,包含设计, 安装,使用
|-- build.sh   # 开发的时候,用于构建项目
|-- clean.sh   # 清理当前项目的中间产物 
|-- coverage.sh # 运行单测覆盖率 
|-- data     # 存储服务需要的数据文件夹
|-- docs    # 各模块的说明文档目录 
|-- format.sh # 格式化项目代码风格
|-- lint_flake8.sh # 格式化项目代码风格
|-- start.sh  # 运行脚本可以启动服务 
|-- stop.sh  # 运行脚本可以停止服务 
`-- trpc_service # 源码项目
    |-- _cli.py # cli 可以直接命令行运行
    |-- agent   # agent 的源码
    |-- channels # 对接im 的channel
    |-- config   # 需要的配置 
    |-- log   # 日志代码,可以设置日志文件级别的操作
    |-- metrics # 监控
    |-- skill # 可以运行的skill文件
    |-- tenant # 多租户的代码 
    |-- tool # 需要使用的tool
    |-- version.py # 版本
    |-- web # 提供网页版本页面可以访问服务
    `-- workspace # 工作目录,包含本地,容器等沙箱环境
```

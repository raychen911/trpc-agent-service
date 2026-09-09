# 🗺️ 本系统概貌

> **定位**：本文是整个系统的**理解向导读**——讲清楚设计思路、代码版图、功能全貌与一条消息的完整旅程。
> 想看安装操作读 [`README.md`](../README.md)，想看架构细稿读 [`PRD.md`](PRD.md)，
> 文中所有结论均以代码与实测为准（实测数据采集于 2026-09-07/08 生产模式联调，并于 2026-09-09 完成两轮「归零 → 重建 → 部署 → 全板块实测」验收，证据见 [VERIFICATION.md](VERIFICATION.md)）。

---

## 一、一句话定位

> **本平台是一个「AI 助手的开店平台」**：企业里每个部门来开一家店（租户），把 AI 助手接进
> 自己的企微/飞书，配自己的知识库、规矩和预算；平台负责让成百上千家店在多台机器上
> 稳定、隔离、可审计地跑。

关键词对应题目三大主干：

| 关键词 | 含义 |
| ------ | ---- |
| **多租户** | 店与店隔离：配置、记忆、知识、权限、预算、审计全链路按租户隔离 |
| **节点化** | 多台机器无状态水平扩展，加机器不失忆 |
| **多后端** | 状态放共享的 Redis/SQL/向量检索，按数据性质选一致性等级 |

---

## 二、设计哲学：三个「反直觉但正确」的决策

系统的骨架是这三条原则，后面所有功能都能从这里推出来。

### 原则 1：一切状态外置，Worker 必须无状态

Agent 天然依赖记忆（对话历史、知识库），直觉做法是存本机——但加机器就失忆。
铁律：**Gateway 进程内存里不存任何业务状态**。对话在 Redis、配置与审计在 SQL、
连「这条消息处理过没有」（幂等键）和「分布式锁」都在 Redis。

**推论**：不需要 sticky session——任何节点都能服务任何请求。session_id 用
`sha256(tenant + 通道 + 群/用户)` 确定性生成（`tenant/resolver.py`），
同一用户永远算出同一个 session，「路由到正确 session」这个难题就被消解了。

### 原则 2：治理前置成「链」，与业务解耦

限流、预算、权限、脱敏、审计——不写进业务代码，而是 9 个 Filter 组成洋葱圈
（`filters/`），每条消息进 Agent 前必须过闸。好处：加治理规则 = 加一个 Filter，
业务零改动；且 **被拦截的流量同样留审计**（AuditFilter 刻意放在洋葱最外层，
无论成败都执行——这是实测发现「阻断流量不留痕」后修出来的顺序）。

### 原则 3：一切尽在可观测

每条消息进入系统就带上 trace_id，贯穿 Filter → Runner → 工具 → 存储读写 → 审计 → 回复
（`metrics/` + `log/`）。出了问题顺着指标和审计行定位。**失败路径也是设计出来的路径**：
投递失败 502 触发 IM 平台重试、后端写失败自动重试一次、Redis 挂了 `/readyz` 503 摘流量——
降级不是「没做好」，是「设计好的另一条路」。

---

## 三、代码地图：目录、脚本与配置

### 3.1 源码目录（Problem.md 骨架 + 平台层新增）

```txt
trpc_service/
├── _cli.py            # CLI 总入口：gateway / admin / knowledge-add / migrate-summaries 等子命令
├── agent/             # Agent 构建与大脑
│   ├── model_factory.py     # 按租户模型配置构建 LLM 实例（DeepSeek 走 OpenAI 兼容协议）
│   └── summarizer.py        # LLM 异步会话摘要（失败回落确定性摘要）
├── channels/          # 【IM 接入】一个抽象类 + 四个真实通道
│   ├── base.py              # IMAdapter 抽象类 + PlatformLimits（平台限制）+ RecallEvent（撤回）
│   ├── wechat_work.py       # 企微自建应用：SHA1 验签 + AES-256-CBC（32 字节分组）+ 应用消息推送
│   ├── wecom_bot.py         # 企微智能机器人：官方 SDK WSS 长连接，支持群聊 @
│   ├── feishu.py            # 飞书 webhook：HMAC 验签 + 加密事件 + 撤回事件
│   ├── feishu_sdk.py        # 飞书官方 SDK WSS 长连接（免公网回调域名）
│   ├── web.py               # Web UI 自测通道（不计入 IM 实现）
│   └── factory.py           # 通道工厂：按租户配置缓存/创建适配器，支持热更新失效
├── config/            # 配置加载（.env + yaml + TENEURIS_* 环境变量覆盖）+ PII 脱敏规则
├── filters/           # 【治理】9 个 Filter + 令牌桶/Redis 固定窗口限流器
│   ├── impl.py              # Trace/Audit/TenantResolve/Signature/UserAuth/RateLimit/Budget/ToolWhitelist/PII
│   └── rate_limiter.py      # 进程内令牌桶（单机）+ Redis 固定窗口（多节点共享额度）
├── runtime/           # 【编排核心】
│   ├── pipeline.py          # process_event 统一管道：幂等 → Filter 链 → Runtime（所有入口共用）
│   ├── runtime.py           # 总调度：灰度 → 存储解析 → 会话读写（锁内重读）→ 收尾结算
│   ├── runner.py            # FrameworkAgentRunner（真实 LLM）/ MockAgentRunner（回声自测）
│   └── events.py            # 框架事件流 → 平台 RunnerEvent 归一化（含 token 用量提取）
├── storage/           # 【存储适配】六大域抽象 + 多后端实现
│   ├── base.py              # Storage 抽象 + 分布式锁（SET NX + Lua 防误删）+ KnowledgeStore/ArtifactStore ABC
│   ├── redis_store.py       # Redis：session(hash)/幂等/锁/记忆
│   ├── sql_store.py         # SQL：tenant/audit_log/summary/tenant_config_history 四表 + 预算原子累加
│   ├── knowledge_redis.py   # 知识库 Redis 实现（多节点共享）
│   ├── knowledge_vector.py  # 知识库向量检索（哈希 embedding + cosine top-k）
│   ├── migration.py         # Summary 跨后端迁移（copy + verify 两步）
│   └── manager.py           # StorageManager：按租户 backends 配置懒建并缓存 Storage
├── tenant/            # 【多租户核心】
│   ├── models.py            # TenantConfig 租户模型全字段 + 灰度/ACL/预算配置
│   ├── registry.py          # 租户注册表（LRU 缓存 + 回源加载）
│   ├── resolver.py          # 确定性 session_id 生成（群聊/单聊规则）
│   ├── gray.py              # 按用户比例金丝雀分流（sha256 稳定哈希）
│   ├── budget.py            # 预算追踪器：SQL 原子累加 + 跨节点广播失效
│   └── broadcaster.py       # Redis pub/sub 配置失效广播（热更新/回滚的跨节点基础）
├── tool/              # 工具注册表 + 动态构建（白名单过滤 + 危险工具运行时门控）
├── log/               # 结构化 JSON 日志 + 全量脱敏（含异常堆栈）
├── metrics/           # Prometheus 指标注册表（9 类，全带 tenant_id）
├── web/               # 【服务入口】
│   ├── app.py               # Agent Gateway：webhook 接入 + /chat + 投递重试 + 撤回处理 + /metrics
│   └── admin.py             # Admin API + 可视化控制台：租户 CRUD/灰度/回滚/审计
├── skill/             # 技能目录（复用框架，占位）
└── workspace/         # 沙箱运行时（复用框架，占位）
```

### 3.2 运维与联调脚本（scripts/）

| 脚本 | 用途 |
| ---- | ---- |
| `verify-wecom-send.py` | 企微真实发送自检：驱动适配器走 gettoken + message/send 真实链路 |
| `verify-feishu-send.py` | 飞书真实发送自检：im/v1/messages 1-on-1 投递 |
| `verify-multinode.sh` | 多节点验证：docker compose 双 Gateway 轮换，断言同 session 跨节点历史连续 |
| `restore-wecom.sh` | 环境重启后一键恢复企微联调：redis/gateway/隧道/出口 IP 探测 + 自检清单 |

### 3.3 根目录关键文件

| 文件 | 职责 |
| ---- | ---- |
| `start.sh` / `stop.sh` | 一键启停（默认 redis + framework，自动拉起 redis-server，pid 管理） |
| `gate-check.sh` | 本地质量门禁：flake8 真判定 + 全量测试 + 覆盖率，任一失败即拦截 |
| `build.sh` / `clean.sh` / `format.sh` / `coverage.sh` / `lint_flake8.sh` | 构建 / 清理 / yapf 格式化 / 覆盖率 / 静态检查 |
| `Dockerfile` | 生产镜像（Python 3.12 + venv + 全量依赖 + redis-server 内置） |
| `docker-compose.yml` | 多节点最小部署：2×Gateway + Redis（appendonly + healthcheck） |
| `config/teneuris.yaml` | 平台配置：端口 / Redis / SQL DSN / PII 规则（密钥经环境变量注入） |
| `.github/workflows/ci.yml` | CI 质量门禁：push / PR 强制 lint + 测试 + 覆盖率 |
| `pyproject.toml` / `requirements.txt` | 依赖清单（框架 / 平台层 / IM SDK / dev 工具链，Python ≥ 3.12） |

### 3.4 进程模型与启动序列

生产模式执行 `./start.sh` 后，系统里跑着 **3 个进程**：

| 进程 | 端口 | 职责 | 生命周期 |
| ---- | ---- | ---- | -------- |
| `redis-server` | 6379 | 共享状态（session/memory/幂等/锁），持久化文件落 `data/` | 未运行则自动拉起（daemon） |
| Gateway | 8000 | webhook 接入 + Web UI + `/chat` + `/metrics` + **IM 长连接**（`--wecom-bot` / `--feishu-sdk` 可选开启） | `data/logs/gateway.pid` |
| Admin API | 8002 | 租户管理 / 可视化控制台 / 审计查询（`X-Admin-Key` 鉴权） | `data/logs/admin.pid` |

**启动序列**（`start.sh` 依次完成）：

```txt
1. 解析参数/环境变量（--storage redis|inmemory、--runner framework|mock，默认 redis + framework）
2. redis 未运行 → daemon 拉起（--dir data，持久化文件不污染仓库根）
3. nohup 启动 Gateway（含可选 IM 长连接）→ 记录 pid
4. nohup 启动 Admin API → 记录 pid
5. 就绪验证：curl /readyz（真实探活 redis ping + SQL SELECT 1）
```

**停止**（`stop.sh`）：读 pid 文件 → `kill -0` 探活 → 优雅停止 → 清理 pid 文件。

**就绪口径**：`/readyz` 返回 `{"status":"ready","checks":{"redis":"ok","sql":"ok"}}` 才算
就绪；任一依赖异常返回 503 + 明细（实测注入：停 redis → 503 → 恢复自愈）。

**多节点**：本机加第二个 Gateway 用 `TENEURIS_GATEWAY_PORT=8011` 起独立进程（联调口径）；
生产用 `docker compose up`（gw1:8001 / gw2:8004 + 共享 Redis）或 K8s 多副本。

---

## 四、一条消息的完整旅程（每步对应代码落点）

以「企微用户发：报销怎么走」为例：

```
① 用户在企微发消息
      ↓ 企微把加密报文 POST 到回调 URL
② Channel Adapter（入口翻译官）        → channels/wechat_work.py
   验签 → AES 解密 → 解析「谁、在哪个群、说了什么」（群聊按 ChatId 判定）
   → 生成 trace_id → msg_id 幂等检查    → storage/redis_store.py（SET NX EX 24h）
      ↓ 统一成 AgentEvent，进入统一管道   → runtime/pipeline.py::process_event
③ Filter 链（9 道闸门）                → filters/impl.py
   任一不过即拦截并留审计；限流器        → filters/rate_limiter.py
      ↓ 放行
④ Runtime（总调度）                    → runtime/runtime.py
   a. 灰度判定                          → tenant/gray.py
   b. 按租户配置解析存储                 → storage/manager.py
   c. 读 session 历史 + 检索 memory      → storage/redis_store.py / redis_memory.py
   d. 交给 Runner 执行                  → runtime/runner.py
      ↓
⑤ Runner（大脑）                       → agent/model_factory.py + tool/builder.py
   按租户配置动态组装 Agent：system_prompt + DeepSeek 模型 + 白名单工具
   → LLM 自主调工具（get_time / calculator / knowledge_search）
     工具实现内做危险工具门控与租户上下文注入 → tool/registry.py
   → 流式产出回答与 token 用量           → runtime/events.py（归一化）
      ↓
⑥ Runtime 收尾（顺序有讲究）
   - 分布式锁 + 锁内重读最新历史 → 写回 session（防并发丢更新）→ storage/base.py
   - 后台异步生成会话摘要（LLM 失败回落确定性）→ agent/summarizer.py
   - 结算：token × 单价 = 成本 → 执行审计 → SQL 预算原子累加
     → tenant/budget.py + storage/sql_store.py
      ↓
⑦ 回投                                 → web/app.py::_send_reply_with_retry
   超长分段（字节级）→ 平台限频错峰 → 失败 502 触发 IM 平台重试 → im_delivery 指标
```

**一句话总结**：入口翻译 → 九闸门 → 大脑思考 → 记账收尾 → 回话。每个箭头均已实测，
且括号里的模块就是它的代码落点。

---

## 五、功能模块盘点（对照题目四大板块）

### 板块 1 · 多租户与节点部署

**代码落点**：`tenant/`（models / registry / resolver / gray / budget / broadcaster）+ `web/admin.py`。

- **租户模型**（`tenant/models.py`）：一个 `TenantConfig` 装下全部——应用配置（提示词）、
  模型配置（provider / 单价）、工具权限（白名单 + 危险工具）、IM 通道绑定（webhook 路径 /
  密钥引用 / 用户 ACL）、数据后端选择、审计策略、灰度、预算、限流。
  **配置即数据**：Admin 改一行 SQL + Redis 广播失效（`tenant/broadcaster.py`），
  各节点下一请求回源加载，全程不重启。
- **Admin API**（`web/admin.py`）：租户 CRUD、热更新、灰度下发、回滚（快照持久化于
  `tenant_config_history` 表）、审计查询；可视化控制台 + `X-Admin-Key` 鉴权 + operator 留痕。
- **多节点**：无 sticky。双节点轮换实测——mock 节点的回声引用了 LLM 节点写入的记忆
  （「已参考记忆：记住代号：北斗七星」），无状态架构最直观的证据。

### 板块 2 · 数据同步与多后端

**代码落点**：`storage/`（base / redis_store / sql_store / knowledge_redis / knowledge_vector / migration / manager）。

- **统一抽象**（`storage/base.py`）：六个数据域各自一个抽象类
  （Session / Memory / Summary / Audit / Knowledge / Artifact），每域独立选后端。
- **三后端 + 一接口**：Redis（热状态）、SQL（强一致合规数据）、向量检索
  （`knowledge_vector.py` 哈希 embedding + cosine）；对象存储留接口（生产换 S3/MinIO）。
- **StorageManager**（`storage/manager.py`）：按租户 `backends` 配置懒建并缓存，
  不同租户可真的用不同后端组合；配置热更新时失效重建。
- **五条同步纪律**（详见 [README §五](../README.md)）：并发写锁内重读、更新顺序、
  跨节点可见、迁移 copy+verify（`migration.py` + `migrate-summaries` CLI）、
  IM 幂等（含失败释放幂等键）——每条带实测数据。

### 板块 3 · IM 接入

**代码落点**：`channels/`（base + 四通道实现 + factory）+ `web/app.py` webhook 接入。

一个抽象类（`channels/base.py::IMAdapter`），四个真实实现 + 一个自测页：

| 通道 | 实现 | 形态与特色 |
| ---- | ---- | ---------- |
| 企微自建应用 | `channels/wechat_work.py` | HTTP webhook，手写官方协议：SHA1 验签 + AES-256-CBC（32 字节分组）+ 应用消息推送 |
| 企微智能机器人 | `channels/wecom_bot.py` | 官方 SDK WSS 长连接，**群聊 @**，免公网回调域名 |
| 飞书 webhook | `channels/feishu.py` | HMAC 验签 + 加密事件 + 撤回事件识别 |
| 飞书 SDK 长连接 | `channels/feishu_sdk.py` | 官方 SDK WSS，免公网回调域名 |
| Web UI 自测 | `channels/web.py` | **仅自测手段，不计入 IM 实现** |

平台限制全覆盖（`channels/base.py::PlatformLimits`）：字节级消息分段（企微 2048 字节 vs
飞书 2000 字）、出站频率错峰、撤回、投递失败重试。群聊/单聊 session 隔离：
单聊按 `tenant+channel+user`，群聊按 `tenant+channel+群 id+user`（`tenant/resolver.py`）。

### 板块 4 · 治理、监控与安全

**代码落点**：`filters/`（impl / rate_limiter）+ `metrics/` + `log/` + `config/redaction.py`。

- **审计 11 字段全覆盖 + 双行语义**：治理行（allow/block）+ 执行行（executed/execution_error），
  同 trace_id 串联（`filters/impl.py::AuditFilter` + `runtime/runtime.py::_settle_usage`）；
  Admin 操作单独留痕含 operator。
- **指标 9 类全带 tenant_id**（`metrics/metrics.py`）：请求量、LLM 延迟、token、工具调用与
  延迟、IM 投递三态、每租户成本、预算水位、活跃 session、Session 后端延迟。
- **密钥三不**（`config/redaction.py` + `log/logger.py`）：不落库（SecretRef 引用，Admin 输出
  排除）、不入日志（结构化 JSON + 异常堆栈全量脱敏）、不回显。
- **危险工具运行时门控**：LLM 动态调工具不经 Filter 链，门控下沉到工具实现层
  （`tool/builder.py`），未确认的危险工具返回确认提示而非执行。

---

## 六、状态与存储：什么数据放在哪、为什么

| 数据 | 放哪 | 一句话理由 |
| ---- | ---- | ---------- |
| 会话 / 记忆 / 幂等 / 锁 | Redis | 高频读写、多节点共享、原生 TTL 与原子操作 |
| 租户配置 / 审计 / 摘要 / 回滚历史 | SQL | 强一致 + 事务 + 合规可查 |
| 知识向量 | Redis hash（哈希 embedding） | 共享热存；生产换 pgvector，接口不变 |
| Artifact | 接口留白（InMemory 占位） | 生产换对象存储 |
| InMemory | 仅单测与本地自测 | 交付标准不建议用于多节点 |

**取舍要会讲**：预算是最终一致（SQL 原子累加 + 广播失效，容忍瞬时越界但不阻塞回复）；
向量检索是最终一致（近似换毫秒级延迟）；审计必须强一致（合规）。
**没有银弹，按数据性质选一致性等级**。

---

## 七、深入理解：六个关键设计问答

**Q1：为什么不用框架自带的 Session/Memory 服务？**
实测发现直接用会让平台存储层空转，「三类后端」验收悬空。平台实现框架的 Service 抽象（ABC），
内部委托给平台自己的 Storage（`storage/framework_adapter.py`）——框架管编排契约，
平台管存储实现。

**Q2：为什么历史持久化在 `append_event` 而不是 `update_session`？**
实测框架 v1.1.x 的 Runner 正常路径只调 `append_event`，基类 `update_session` 是 no-op。
要看调用方怎么用，而不是看基类声明了什么。

**Q3：并发写同一 session 怎么不丢？**
读-改-写横跨整个 LLM 周期，只锁「写」防不住（实测 12 并发丢 2 轮）。方案：锁内**重读最新
state** 为基线再合并，版本号锁内自增。终稿联调 8 并发 16/16 零丢失。

**Q4：为什么自研 trace_id 不上 OpenTelemetry？**
题目原文是「OpenTelemetry **或等价 tracing**」。自研 trace_id 已实测贯穿全链路（审计行可查）；
删净 OTel 死依赖比留个没接线的摆设诚实，生产演进随时可接。

**Q5：拿锁失败为什么不报错？**
可用性优先：锁等待超时后尽力写 + 告警（锁内重读后竞态窗口已极小）；返回错误会让用户看到
失败，IM 重发反而放大写冲突。

**Q6：mock 回声和真实 LLM 怎么切换、怎么保证不是假的？**
`--runner` 显式选择；生产默认 framework，缺 API key **直接拒绝启动**不静默降级
（`_cli.py` 启动预检）。所有演示数据均来自真实 DeepSeek + 真实 IM 通道。

---

## 八、09-09 终稿前加固四件套（补遗）

文档重组时新增的四块能力，完整设计见对应详设：

| 能力 | 一句话 | 详设 |
| ---- | ------ | ---- |
| 首启自动播种 | 空库首启自动落 demo 租户，消除 Admin/Gateway 配置源分裂 | [design/02 §1.5](DESIGN-MULTI-TENANT.md) |
| 生产安全 fail-closed | `env=prod` 六项校验，危险配置启动即拒 | [design/05 §4.6](DESIGN-GOVERNANCE.md) |
| 依赖锁定 | uv.lock 精确锁定 115 包（框架 1.1.20），frozen 构建防漂移 | [design/06 §5.5](DESIGN-OPERATIONS.md) |
| K8s 生产部署 | deploy/kustomize：HPA+PDB+探针+Secret 注入，部署即触发 fail-closed | [design/06 §5.6](DESIGN-OPERATIONS.md) |

另：开发镜像（`.ide/Dockerfile`）与生产镜像（根 `Dockerfile`）职责分离——见 [design/01 §0.9](DESIGN-ARCHITECTURE.md)。

---

## 九、文档地图

| 文档 | 回答什么问题 |
| ---- | ------------ |
| [`../README.md`](../README.md) | 这是什么、怎么装怎么用、实测战报（**入口文档**） |
| [`Problem.md`](Problem.md) | 题目要求与验收标准（最高锚点，不可变） |
| [`PRD.md`](PRD.md) | **主设计文档（spec）**：按题目板块的决策、架构图、风险清单、验收映射 |
| [`DESIGN-*.md`](DESIGN-ARCHITECTURE.md) | 详设六篇：各板块的实现细节、DDL、演进记录（架构/多租户/数据同步/IM/治理/运维） |
| [`VERIFICATION.md`](VERIFICATION.md) | 验证记录：两轮全链路实测证据与复现命令 |
| 本文档 | 系统概貌导读：代码地图 + 思路 + 全貌 + 旅程 + 设计问答 |

# 详设 2 · 数据同步与多后端支持

> 主文档：[PRD.md §2](PRD.md)　|　验证证据：[VERIFICATION.md](VERIFICATION.md)
> 本文为该章节的完整详设（spec 深度层）；与代码/实测不一致时，以后者为准。
> 小节编号沿用原 PRD 章号（如本篇 §N.x）；跨篇 § 引用指向对应编号的详设文件。

### 2.1 Storage Adapter 统一抽象

上层业务（Session/Memory/Summary 等）不感知具体后端：

```python
from abc import ABC, abstractmethod
from typing import Optional

class BackendType:
    INMEMORY = "inmemory"      # 仅单机开发与单元测试，多节点/验收环境禁用（硬约束）
    REDIS    = "redis"
    SQL      = "sql"
    VECTOR   = "vector"
    S3       = "s3"
    # 外部 Memory 服务（Mem0 / Zep 等第三方记忆 API），经 HTTP 委托，见下方说明
    EXTERNAL_MEMORY = "external_memory"

class SessionStore(ABC):
    @abstractmethod
    async def get_session(self, tenant_id: str, session_id: str) -> Optional[dict]: ...
    @abstractmethod
    async def save_session(self, tenant_id: str, session: dict) -> None: ...
    @abstractmethod
    async def update_state(self, tenant_id: str, session_id: str, state: dict) -> None: ...

class MemoryStore(ABC):
    @abstractmethod
    async def add_memory(self, tenant_id: str, user_id: str, memory: dict) -> None: ...
    @abstractmethod
    async def search_memory(self, tenant_id: str, user_id: str, query: str, top_k: int) -> list[dict]: ...

class SummaryStore(ABC):
    @abstractmethod
    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[dict]: ...
    @abstractmethod
    async def save_summary(self, tenant_id: str, session_id: str, content: str) -> None: ...

class ArtifactStore(ABC):
    @abstractmethod
    async def put_artifact(self, tenant_id: str, session_id: str, name: str, data: bytes) -> str: ...
    @abstractmethod
    async def get_artifact(self, tenant_id: str, session_id: str, artifact_id: str) -> bytes: ...

class KnowledgeStore(ABC):
    @abstractmethod
    async def add_document(self, tenant_id: str, doc_id: str, chunks: list[dict]) -> None: ...
    @abstractmethod
    async def search(self, tenant_id: str, query: str, top_k: int) -> list[dict]: ...

class AuditStore(ABC):
    @abstractmethod
    async def write_log(self, tenant_id: str, log: dict) -> None: ...
    @abstractmethod
    async def query_logs(self, tenant_id: str, filters: dict) -> list[dict]: ...
```

> **外部 Memory 服务说明**：`EXTERNAL_MEMORY` 类型的 `MemoryStore` 实现不做本地存储，
> 而是把 `add_memory` / `search_memory` 委托给第三方记忆服务 API（如 Mem0 / Zep），
> 内部仍按 `tenant_id` 注入命名空间/用户隔离参数；凭证经 `SecretStr` 注入，请求失败
> 按 §5.1 降级为本地 Redis 兜底。Summary / Artifact / Knowledge 与 Session / Memory / Audit
> 一样走统一抽象，**上层业务不感知后端**——各数据域可独立换后端（如 Summary 从 SQL 换 Redis）
> 而不改调用方。

> ⚠️ **实现状态（2026-09-02 校准）**：`EXTERNAL_MEMORY` 为**生产演进设计**，当前未落地
> （无第三方记忆服务客户端）；现有 Memory 走 Redis / InMemory 实现。

> 说明：tRPC-Agent-Python 已内置 `SessionService` / `MemoryService` 及其 Redis/SQL 后端。平台层 Storage Adapter 的职责是**在其上做租户级封装**（key 前缀隔离、分布式锁、版本号），而非重写存储引擎。
>
> ✅ **后端配置语义收紧（09-06；09-04 补 session=sql 同口径；09-07 更新 vector）**：`StorageFactory` 对租户配置
> `session=sql` / `artifact=s3`（未实现的生产演进后端）**显式抛
> `NotImplementedError`**，不再静默降级 InMemory——此前租户以为配置已生效、实际用占位实现，
> 属「文档声称 ≠ 代码行为」（session=sql 静默降级还会使多节点会话一致性静默失效）；
> session 请改用 redis 或 inmemory。
>
> 🆕 **knowledge=vector 已落地（09-07）**：本地哈希 embedding（纯 Python，零新依赖）+ cosine
> top-k 检索（`storage/knowledge_vector.py`），向量随 chunk 存 Redis hash 多节点共享；
> 未配置 Redis 时显式抛 `RuntimeError`。语义泛化弱于真实 embedding 模型，接 pgvector /
> embedding API 属生产演进（接口不变）。

### 2.2 各数据域的存储策略

| 数据域                  | 推荐后端                   | 存储结构                                                           | 一致性   |
| ----------------------- | -------------------------- | ------------------------------------------------------------------ | -------- |
| **Session**       | Redis（热）+ SQL（持久化） | `session:{tenant_id}:{session_id}` 存 state/events，异步刷盘 SQL | 强一致   |
| **Memory**        | 向量库 / Redis             | 内容 + 向量索引，按租户分 collection                               | 最终一致 |
| **Summary**       | SQL / Redis                | 每 session 一条，低频更新                                          | 强一致   |
| **Artifact**      | MinIO / S3                 | `artifacts/{tenant_id}/{session_id}/{artifact_id}`               | 最终一致 |
| **Knowledge**     | 向量库                     | 按租户分 collection 或 tenant_id tag                               | 最终一致 |
| **Audit Log**     | PostgreSQL                 | 独立审计表，按 tenant_id 分区                                      | 强一致   |
| **Message/Event** | Redis Stream / SQL         | 有序事件流，支持 replay                                            | 强一致   |

#### 知识库向量选型（生产推荐，阶段三设计并入）

| 选型 | 优势 | 代价 | 结论 |
|---|---|---|---|
| **pgvector** | 复用平台 SQL 层，事务/行级隔离，不新增中间件 | 亿级以下规模 | **生产推荐** |
| Redis（RediSearch vector） | 复用已有 Redis 依赖 | 需 Redis Stack，内存成本高 | 备选 |
| Milvus | 十亿级、多副本 | 独立集群运维重 | 大规模专用 |
| Chroma | 原型极简 | 不适合生产 | 本地原型 |
| 腾讯云 VectorDB | 免运维 | 依赖云 | 云上备选 |

知识切块（chunk）落库结构：`tenant_id / doc_id / chunk_id / content / embedding / metadata`，
行级隔离（PRD 1.4）；写入「先写内容表、后更新索引」，检索 query embedding → ANN top-k 注入 prompt。

> ⚠️ **实现状态（2026-09-02 校准，09-06 更新）**：当前提供 **InMemory（单机）与 Redis（多节点
> 共享）两个关键词打分实现**（`storage/knowledge_inmemory.py` / `storage/knowledge_redis.py`，
> `KnowledgeStore` 抽象接口不变），检索均为关键词重叠打分占位，真实 embedding + ANN 检索
> 列为**生产替换**（pgvector 推荐）。Redis 实现动机：InMemory 各进程独立实例，多节点部署下
> 知识库互不可见且重启即丢（09-06 联调实测「搜索功能暂时不可用」的根因）；Redis 与
> Session/Memory 同为共享热存，`knowledge-add` CLI 录入（固定窗口切块 + 检索自检）后所有
> 节点立即可检，RAG 全链路（提问 → `knowledge_search` → 引用知识库回答）真实 LLM 实测命中。
>
> ⚠️ **Session 双写状态（2026-09-02 校准）**：Session 当前仅 Redis 热存（`session:{tenant}:{sid}`
> hash + state/version）；「异步刷盘 SQL 持久化」列为**生产演进**（现有 SQL 层仅 tenant/audit/summary）。
>
> ✅ **运行时按租户后端生效（09-06 修复）**：此前 gateway 只按 demo backends 创建一次固定
> Storage（未接 SQL 引擎），租户行内 `data_backend_config` 在网关链路**从不生效**——执行审计/
> 摘要实际写进程内 InMemory。现引入 `storage/manager.py::StorageManager`：gateway 建
> Redis+SQL 引擎后，Runtime 与 Filter 按租户 `TenantConfig.backends` **懒建并缓存 Storage**
> （`session/memory/summary/audit/knowledge` 各域各自生效；`idempotency/lock` 属平台共享
> 基础设施，Redis key 含租户前缀隔离）；租户配置热更新（Admin+广播）时失效缓存、下一请求按
> 新后端重建。多租户异构后端（如 A 租户 audit 落 SQL、B 租户 audit 落 InMemory）由此真实生效。

### 2.3 数据同步策略

#### A. 多节点并发写入同一 Session 的一致性

> ✅ **实现状态（2026-09-04 修复落地）**：读-改-写横跨整个 LLM 调用周期（handle 开头读
> session → 调 LLM → 写回），**仅给写加锁防不住丢失更新**——09-04 联调实测 mock 12 并发
> 丢 2 轮、framework 6 并发丢 5 条模型回复。修复为「**锁内重读** + 锁等待重试」：
> `Runtime._save_session`（`runtime/runtime.py`）与框架适配层
> `PlatformSessionService._locked_persist`（`storage/framework_adapter.py`，events 按
> 序列化指纹合并）均在锁内重读最新数据为基线再合并写入；锁经
> `acquire_lock_with_retry`（`storage/base.py`，timeout=租约 TTL、wait_seconds=等待预算）
> 获取，超时尽力写 + 告警（可用性优先）。回归：mock/framework 并发单测 + 真实 E2E
> 12/24 条历史零丢失。

```python
# Redis 分布式锁 + 锁内重读 + 版本号自增
async def _save_session(self, storage, tenant, event, session_state, reply, tool_trace):
    lock_key = f"lock:session:{event.tenant_id}:{event.session_id}"
    acquired = await acquire_lock_with_retry(storage.lock, lock_key, ttl=10, wait_seconds=5.0)
    try:
        # 锁内重读最新 state 为基线（handle 开头的快照在 LLM 调用期间已过期）
        fresh = await storage.session.get_session(event.tenant_id, event.session_id)
        state = dict((fresh or {}).get("state") or session_state or {})
        history = list(state.get("history") or [])
        history.append({"role": "user", "content": event.content, "msg_id": event.msg_id})
        history.append({"role": "assistant", "content": reply})
        state["history"] = history
        state["tools"] = tool_trace
        await storage.session.update_state(event.tenant_id, event.session_id, state)  # version+1
    finally:
        if acquired:
            await storage.lock.release(lock_key)
```

#### B. Session Event / State / Summary 的更新顺序

```
1. append-only 写 message_event（带自增 sequence_num + 唯一约束）
2. Worker 消费 event → 读 state → 调 LLM/Tool → 产生新 event
3. 事务/Pipeline 原子提交：写 assistant event + 更新 state + 触发 summary
   Summary 走异步后台任务，不阻塞主链路
```

#### C. Memory 写入后的跨节点可见性

- **Redis**：写后立即可见（单线程线性一致）。
- **向量库**：默认最终一致；写后在 state 记录 `memory_version`，读取时对比落后则强制刷新。

#### D. 后端迁移（Redis→SQL / 本地向量→远端向量）

双写 + 切读四阶段：双写期 → 回填期（checksum 校验）→ 切读期（按 tenant 灰度 5%→50%→100%）→ 下线旧后端。

> ✅ **实现状态（2026-09-06 落地）**：提供可执行迁移工具 `storage/migration.py`
> （`copy_summaries` + `verify_summaries`）与 CLI `migrate-summaries --tenant <id>
> --source redis|sql|inmemory --target redis|sql|inmemory`——对 Summary 数据域做
> 「源全量回填目标 + 集合校验」，迁移后校验一致提示可切换。**单测 + 真实跑通**
> （InMemory→SQLite、Redis→SQLite、SQL→InMemory，copied/matched 一致）。
> 双写期与切读不新增代码：由运维按 Admin 租户 backends 热切（StorageManager.invalidate
> 已支持）执行；copy 幂等（save 先删后插），单条失败由 verify 暴露、重跑收敛。
>
> ⚠️ **范围边界**：当前仅迁移 **Summary 数据域**（Redis/SQL/InMemory 三端均有真实实现，
> 最贴题面「Redis→SQL」例子）。Session/Memory 缺 SQL 后端实现，跨后端迁移需先落地对应
> 目标后端（生产演进，见 §2.2 Session 双写注记）——不虚构"已支持"。

#### E. IM 消息重复投递的幂等

```python
async def handle_webhook(self, body: bytes) -> None:
    msg = parse_im_message(body)
    idem_key = f"idempotency:{msg.channel_type}:{msg.msg_id}"
    if not await self.redis.set(idem_key, "1", nx=True, ex=86400):  # 24h TTL
        return  # 重复消息，静默成功
    event = to_agent_event(msg)
    await self.gateway.enqueue(event)
```

### 2.4 一致性取舍

| 后端       | 一致性           | 延迟     | 成本 | 运维 | 适用场景                 |
| ---------- | ---------------- | -------- | ---- | ---- | ------------------------ |
| InMemory   | 进程内           | <1ms     | 最低 | 最低 | 仅单机开发（多节点禁用） |
| Redis      | 强一致（单 key） | 1-5ms    | 低   | 低   | Session、幂等、缓存、锁  |
| PostgreSQL | 强一致（ACID）   | 5-20ms   | 中   | 中   | 租户、审计、Summary      |
| 向量库     | 最终一致         | 10-50ms  | 中   | 中   | Knowledge、Memory 检索   |
| MinIO/S3   | 最终一致         | 50-200ms | 低   | 低   | Artifact、备份           |

### 2.5 最小数据模型（SQL DDL）

> ⚠️ **落点对照（2026-09-02 校准）**：下表为**目标参考模型**；实际按「数据模型分域落地」
> （§1.3 决策）部署，八类关系全部可表达：
>
> | PRD 表 | 代码实际落点 | 说明 |
> | --- | --- | --- |
> | tenant | ✅ SQL 表（`SqlTenantStore`） | 表级落库 |
> | agent_app | ✅ 嵌套 `tenant.app` JSON | agent 配置随租户 |
> | session | ✅ Redis hash `session:{tenant}:{sid}`（state/version/history） | 热存；SQL 刷盘为生产演进 |
> | message_event | ✅ 嵌套 `session.state.history` | 事件随会话 |
> | memory | ✅ Redis list `memory:{tenant}:{user}` | 表级落库（Redis） |
> | summary | ✅ SQL 表（`SqlSummaryStore`，+InMemory/Redis 实现） | 表级落库 |
> | channel_binding | ✅ 嵌套 `tenant.im` JSON | 通道绑定随租户 |
> | audit_log | ✅ SQL 表（`SqlAuditStore`） | 表级落库 |

```sql
-- 租户（见 1.1）+ agent app
CREATE TABLE agent_app (
    app_id       VARCHAR(64) PRIMARY KEY,
    tenant_id    VARCHAR(64) NOT NULL,
    name         VARCHAR(128) NOT NULL,
    agent_type   VARCHAR(32) NOT NULL,          -- llm / chain / graph
    app_config   JSON NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenant(tenant_id),
    INDEX idx_tenant (tenant_id)
);

CREATE TABLE session (
    session_id   VARCHAR(64) PRIMARY KEY,
    tenant_id    VARCHAR(64) NOT NULL,
    app_id       VARCHAR(64),
    user_id      VARCHAR(64) NOT NULL,
    channel_type VARCHAR(32),
    state        JSON,                          -- 会话状态
    version      BIGINT DEFAULT 0,              -- 乐观锁版本
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_tenant_user (tenant_id, user_id)
);

CREATE TABLE message_event (
    event_id      VARCHAR(64) PRIMARY KEY,
    session_id    VARCHAR(64) NOT NULL,
    tenant_id     VARCHAR(64) NOT NULL,
    sequence_num  BIGINT NOT NULL,
    event_type    VARCHAR(32),                  -- user_message/assistant/tool_call/tool_result
    payload       JSON,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_session_seq (session_id, sequence_num)
);

CREATE TABLE memory (
    memory_id     VARCHAR(64) PRIMARY KEY,
    tenant_id     VARCHAR(64) NOT NULL,
    user_id       VARCHAR(64) NOT NULL,
    content       TEXT,
    embedding_id  VARCHAR(64),                  -- 向量库引用
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_user (tenant_id, user_id)
);

CREATE TABLE summary (
    summary_id    VARCHAR(64) PRIMARY KEY,
    session_id    VARCHAR(64) NOT NULL,
    tenant_id     VARCHAR(64) NOT NULL,
    content       TEXT,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE channel_binding (
    binding_id      VARCHAR(64) PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    channel_type    VARCHAR(32) NOT NULL,       -- wechat_work / feishu / web / wecom_bot / feishu_sdk
    app_id          VARCHAR(128),               -- corp_id / bot_id
    token_encrypted VARCHAR(512),               -- AES-GCM 加密
    secret_encrypted VARCHAR(512),
    webhook_path    VARCHAR(256),
    user_id_mapping JSON,
    UNIQUE KEY uk_tenant_channel (tenant_id, channel_type, app_id)
);

CREATE TABLE audit_log (
    log_id       VARCHAR(64) PRIMARY KEY,
    tenant_id    VARCHAR(64) NOT NULL,
    trace_id     VARCHAR(64),
    channel      VARCHAR(32),
    user_id      VARCHAR(64),
    session_id   VARCHAR(64),
    agent_name   VARCHAR(128),
    tool_name    VARCHAR(128),
    decision     VARCHAR(32),                   -- allow / block / confirm_required
    latency_ms   INT,
    error_type   VARCHAR(64),
    cost         DECIMAL(12,6),
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_time (tenant_id, created_at)
);
```

---

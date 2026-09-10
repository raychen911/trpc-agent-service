# 多后端适配方案：Redis / SQL / 向量库 / 对象存储

## 1. 各类数据的归属

| 数据 | 首选 | 备选 | 说明 |
|------|------|------|------|
| Session（event/state） | Redis | SQL | 热数据、高频读写、TTL 自动过期 |
| Memory（跨会话长期记忆） | Redis（关键词检索） | SQL / 向量库 | 语义检索走向量库（预留 embedding 列） |
| Summary | SQL | Redis | 低频写、随会话生命周期 |
| Artifact（文件） | 对象存储（S3/MinIO） | 本地磁盘 | 框架仅内置 InMemory 实现，平台层封装 |
| Knowledge（RAG） | 向量库 | SQL 全文检索 | 框架仅有抽象 KnowledgeBase，平台对接具体后端 |
| Audit Log | SQL | JSONL 兜底 | 合规要求，长保留周期 |
| 租户/绑定/幂等元数据 | SQL | — | 事务与唯一约束 |

## 2. 适配方式

### 2.1 Session / Memory（已实现）

平台不做自研存储，**直接复用框架三实现**：`InMemorySessionService / RedisSessionService / SqlSessionService` 与对应的 MemoryService。`storage/factory.py` 按 `TenantConfig.storage.session_backend` 装配，租户在 YAML 中选择：

```yaml
storage:
  session_backend: "sql"          # in_memory / redis / sql
  redis_url: "redis://localhost:6379/0"
  sql_url: "mysql+pymysql://user:pass@host/trpc_agent?charset=utf8mb4"
```

多租户混布：不同租户可同时选不同后端，互不影响。

### 2.2 向量库（预留设计）

- 框架 `KnowledgeBase` 只有抽象接口（search/SearchRequest/SearchResult），需自建实现。
- 适配点：`memory` 表已预留 `embedding JSON` 列；迁移路径 = InMemory/Redis 关键词检索 → 嵌入模型向量化 → 向量库（如 Milvus/Qdrant）ANN 检索 → `SearchMemoryResponse` 封装回框架。
- 选择建议：记忆条目 < 10 万用 SQL 全文+关键词即可；超过后引入向量库，避免为小规模引入重运维组件。

### 2.3 对象存储（预留设计）

- 框架 `ArtifactServiceABC` 接口齐全（save/load/list/delete/versions），仅内置内存版。
- 适配点：实现 `S3ArtifactService`（put/get 对象 + 版本前缀 `{app}/{user}/{artifact_id}/{version}`）；IM 图片/文件消息的二进制也走该通道，避免占用 Session 存储。

## 3. 选型决策树

```
租户是否需要多节点部署？
├─ 否（单机/开发）→ InMemory
└─ 是
   ├─ 是否有合规审计/长期查询需求？ → SQL（或 Redis 热数据 + SQL 冷数据）
   ├─ 是否 QPS 高、会话短、允许 TTL 过期？ → Redis
   └─ 是否需要记忆语义检索 / 大文件？ → +向量库 / +对象存储（在 Redis/SQL 之上叠加）
```

## 4. 迁移与兼容

- 后端切换只改租户 YAML 的 `session_backend` 与连接串，热加载生效；数据迁移按 `docs/sync-and-idempotency.md` §5 执行。
- 平台元数据表（tenant/channel_binding/audit_log/idempotency）始终在 SQL，不随后端切换。

## 3.1 租户后端选型的边界

租户在 tenants.yaml 中可选的是其**数据资产**的存储（Session/Memory：
in_memory/redis/sql）；平台的**运行时治理状态**（预算计数、去重、绑定
缓存）由平台统一决定。预算计数使用 Redis 原子 INCR（热路径、按日过期、
双键 TTL 48h）；无 Redis 的多节点部署下降级为节点内存计数（配额近似
生效）。SQL 不做计数后端；按租户成本报表由定时快照/审计聚合落表实现
——计数与报表分离。

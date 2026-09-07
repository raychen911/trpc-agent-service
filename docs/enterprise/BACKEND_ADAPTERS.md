# 多后端适配方案

## 1. 为什么不能使用一个统一实现

平台对上提供统一接口，但不会假设所有后端具有相同事务语义。Redis 适合高频、小对象和原子
状态；SQL 适合关系、事务和长期事实；向量库以近似检索为核心，索引更新通常最终一致；对象
存储适合大文件和不可变 payload，但不能与 SQL 元数据组成原生跨系统事务。因此统一的是
`tenant scope + API contract + observability`，提交、校验、迁移和补偿策略按数据类型实现。

## 2. 数据放置矩阵

| 数据 | 主存储 | 辅助存储 | 原因 |
|---|---|---|---|
| Redis Streams 任务、session lock、幂等 reservation、预算计数 | Redis | SQL receipt/audit | 需要低延迟原子操作和 TTL |
| 热 Session、短期 Memory、回复结果缓存 | Redis | SQL 可作长期后端 | 高频读写、过期和多节点共享 |
| 租户配置、版本、Channel Binding、Event、Summary、Audit | SQL | Redis 缓存 | 需要事务、唯一约束、查询和保留策略 |
| Knowledge Document/Chunk 元数据 | SQL | Redis 缓存 | 文档版本和处理状态是事实记录 |
| Chunk embedding | Qdrant/Milvus/pgvector | SQL 保存 vector_id/version | 需要 ANN 检索与 metadata filter |
| 图片、附件、报告、原始知识文件 | S3/COS/MinIO | SQL 保存 URI/checksum | 大对象成本低，支持生命周期和跨区域复制 |

## 3. 代码接口与租户隔离

`trpc_service.workspace` 提供四类入口：

- `SessionServiceABC`、`MemoryServiceABC`：直接复用 tRPC-Agent-Python 接口和 Redis/SQL 实现；
- `VectorStoreABC`：`upsert/search/delete/close`，记录包含 embedding、正文、metadata 和 version；
- `ObjectStoreABC`：`put/get/head/delete/close`，写入返回 SHA-256、大小和 MIME 类型；
- `TenantStorageRouter`：按租户配置选后端、缓存连接，并包装为 `TenantVectorStore` 或
  `TenantObjectStore`。

向量 namespace 最终为 `{tenant_id}:{knowledge_namespace}`；对象 key 最终为
`{tenant_id}/{logical_key}`。调用方即使忘记加 tenant id，适配器也会在存储边界注入；已经加过
前缀时不会重复添加。生产后端还应使用每租户 collection/partition、bucket prefix、数据库用户
或独立实例进行第二层隔离。

内置实现：

| 实现 | 用途 |
|---|---|
| `InMemoryVectorStore` | 单元测试、单进程 demo；精确 cosine 检索，不用于多节点生产 |
| `QdrantVectorStore` | 生产向量检索；自动创建 collection，强制 tenant payload filter |
| `LocalObjectStore` | Compose/开发；原子 rename，payload 成功后才提交 metadata sidecar |
| `S3CompatibleObjectStore` | AWS S3、MinIO 和支持 S3 API 的 COS；checksum 写入对象 metadata |

Milvus 和 pgvector 可通过 `register_vector_factory()` 注册，私有云对象服务可通过
`register_object_factory()` 注册，不需要修改 Worker 或业务调用代码。

## 4. 租户配置示例

```yaml
storage_config:
  session_backend: redis
  memory_backend: mysql
  redis_url: ${REDIS_URL}
  mysql_url: ${MYSQL_URL}
  vector:
    backend: qdrant
    url: ${VECTOR_URL}
    api_key: ${QDRANT_API_KEY}
    collection: tenant_a_knowledge
    dimensions: 1536
    embedding_model: text-embedding-v3
  object:
    backend: s3
    endpoint_url: ${OBJECT_STORE_ENDPOINT}
    bucket: tenant-a-artifacts
    region: ${OBJECT_STORE_REGION}
    access_key: ${OBJECT_STORE_ACCESS_KEY}
    secret_key: ${OBJECT_STORE_SECRET_KEY}
```

URL、API key 和对象存储凭据使用 `SecretStr`，写入配置仓库时由 `TenantConfigCodec` 单独加密。
不同 embedding 维度或模型不得复用同一个 collection；变更模型时创建新 collection/version，
完成 shadow query 后再切读。

## 5. 一致性和故障处理

- Redis：单 key 操作或 Lua 保证原子性；队列使用 consumer group；锁必须有 lease 和 fencing。
- SQL：Event append 与 Session version CAS 放在事务中；审计 append-only；配置变更使用 outbox。
- 向量库：以稳定 `vector_id` upsert；SQL `knowledge_document.status` 未到 `ready` 前不参与检索；
  向量写失败由 `storage_outbox` 重放。
- 对象存储：先 PUT payload 并校验 checksum，再提交 SQL metadata；删除先 tombstone，再异步删对象；
  orphan GC 只删除超过安全窗口且没有 SQL 引用的对象。

## 6. 依赖安装

基础 Redis/MySQL 能力：

```bash
pip install trpc-agent-service
```

Qdrant 与 S3 兼容对象存储：

```bash
pip install 'trpc-agent-service[vector-storage,object-storage]'
```

生产镜像已安装这两个 extra；测试或只运行最小内存/本地后端时不需要额外客户端。

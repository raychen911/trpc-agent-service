# 采用 PostgreSQL、Redis、PGVector 与 S3 默认存储栈

默认生产存储配置档使用 PostgreSQL 16 高可用集群保存控制面元数据、Session 权威记录、投影、幂等、Outbox 和审计索引，Redis 7 Cluster 保存缓存、限流、fencing 租约及热投影，独立 PostgreSQL/PGVector 保存默认 Knowledge 与语义 Memory 向量，S3 兼容对象存储保存 Artifact、知识源文件、导出和大型归档；开发环境使用 MinIO，InMemory 只允许用于单元测试。平台仍通过 Adapter 支持租户选择其他 SQL、Redis、向量、对象或外部 Memory 后端，默认配置不构成唯一支持范围。

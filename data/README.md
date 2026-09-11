# 数据目录

第 1～3 周使用 InMemory，`data/` 仅作为后续本地 SQL、向量库和 Artifact 的挂载点。生产数据不得提交到 Git；推荐使用 PostgreSQL、Redis、pgvector/Qdrant 和 S3/MinIO，并按 `tenant_id` 做命名空间隔离。

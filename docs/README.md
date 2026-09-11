# 交付文档索引

本目录保存题目要求的正式交付文档；根目录 [README](../README.md) 是安装、运行和验收入口。

- [architecture.md](architecture.md)：总体架构、系统拓扑图、企业微信核心时序图，以及 tRPC-Agent-Python 与平台层的责任边界。
- [data-model.md](data-model.md)：核心实体、关系、SQL 表和共享 Session/Memory 模型。
- [data-sync-idempotency.md](data-sync-idempotency.md)：并发写入、事件顺序、跨节点可见性、幂等和后端迁移策略。
- [backend-strategy.md](backend-strategy.md)：Redis、PostgreSQL、MinIO/S3 与可选向量后端的适用范围和一致性取舍。
- [production-risks.md](production-risks.md)：生产风险、检测方式、缓解措施和对应证据。
- [acceptance-matrix.md](acceptance-matrix.md)：README 要求到实现、测试和验收命令的映射。

实现边界以源码、Alembic 迁移和部署清单为准，多后端扩展职责与一致性策略以本目录设计文档为准。

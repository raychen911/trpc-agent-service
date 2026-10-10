# 原始需求归档与锁定范围

原始题目要求基于 tRPC-Agent-Python 设计并实现一套多租户、可节点化部署的 Agent 平台，
覆盖租户/配置/工具/IM/数据隔离，多节点 Session 路由，多后端 Session、Memory、Summary、
Artifact、Knowledge 与 Audit，至少两类 IM，治理、OpenTelemetry、审计、密钥保护、
故障恢复、灰度回滚、容量评估，以及 Compose/Kubernetes 部署。

验收至少要求：完整架构图和企业微信消息时序；tenant、agent、binding、session、event、memory、
summary、audit 数据关系；Redis/SQL/向量/对象存储取舍；重复消息幂等；不少于八项生产风险；
明确 SDK 复用边界；提供 GitHub 可运行代码。

本仓库将范围进一步锁定为：

- 所有代码、测试、文档和部署仅位于 `E:/trpc-agent-service`。
- Python 3.12 生产基线，CI 覆盖 3.11、3.12、3.13。
- tRPC-Agent 兼容区间 `>=1.1.17,<1.2`，锁文件固定 1.1.19。
- 企业微信使用 AI Bot WebSocket 长连接；飞书使用加密 HTTP 事件回调与 OpenAPI 回复。
- PostgreSQL 是权威日志，Redis Streams 是至少一次通知层，S3/MinIO 与 pgvector 是默认后端。
- Worker 无状态，不依赖 sticky session；Session lease/fencing 防止旧 Worker 提交。
- 共享表 + RLS 为默认隔离，高监管租户可经相同 Repository 接口绑定独立后端。
- 首版不包含管理 UI、Telegram、微信公众号和微信客服。

只有离线、真实 IM、安全、性能、迁移和部署必需门禁均通过，才能标记生产候选。

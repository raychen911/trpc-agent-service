# 文档索引

| 文档 | 内容 |
|------|------|
| [architecture.md](architecture.md) | 系统架构：组件职责、多租户与节点部署、治理监控安全、IM 接入、故障恢复与运维、数据同步 |
| [data-model.md](data-model.md) | 数据模型：tenant / agent_app / tenant_revision / session / message / memory / summary / channel_binding / audit_log / idempotency 表结构，Redis 键结构，实体关系 |
| [sync-and-idempotency.md](sync-and-idempotency.md) | 数据同步与幂等：并发写一致性、event/state/summary 顺序、Memory 跨节点可见性、Redis→SQL 迁移、IM 三层幂等、各后端一致性取舍 |
| [backend-adapter.md](backend-adapter.md) | 多后端适配：Redis / SQL / 向量库 / 对象存储分别适合存什么，预留设计 |
| [risk-list.md](risk-list.md) | 生产风险清单（12 项）与缓解措施 |
| [memory.md](memory.md) | Memory 子系统设计（LoadMemoryTool、跨节点可见性） |
| [framework_api.md](framework_api.md) | trpc-agent-py 框架能力与平台复用边界 |
| [mermaid/architecture.mermaid](mermaid/architecture.mermaid) | 系统架构图 |
| [mermaid/sequence.mermaid](mermaid/sequence.mermaid) | 核心时序图（企业微信消息 → Agent → Tool → Session/Memory → IM 回复） |
| [mermaid/sequence-feishu.mermaid](mermaid/sequence-feishu.mermaid) | 飞书链路时序图（事件订阅 ACK + 异步主动回复） |

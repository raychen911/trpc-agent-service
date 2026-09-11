# 设计文档

- [多租户节点化 Agent 部署平台架构设计](./architecture-design.md)
- [PostgreSQL 核心数据模型](./schema.sql)
- [PostgreSQL 租户 RLS 策略](./rls.sql)
- [RLS 隔离回归测试](./rls-regression.sql)
- [可运行实现清单](./implementation-status.md)

架构设计覆盖多租户隔离、无状态 Worker、Session 路由、Inbox/Outbox 一致性、多后端适配、企业微信与 Telegram 接入、治理与可观测性、故障恢复、容量模型以及生产风险。

数据库验证顺序为 `schema.sql → rls.sql → rls-regression.sql`；最后一个脚本在事务末尾执行 `ROLLBACK`，不会保留测试夹具。

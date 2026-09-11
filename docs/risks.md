# 生产风险清单

| # | 风险 | 缓解措施 |
|---:|---|---|
| 1 | 伪造 tenant_id 或 binding 越权 | 只信已验签 binding；所有 key/SQL/RLS 强制 tenant scope |
| 2 | IM 重复投递导致重复 Agent/副作用 | inbox 唯一键、队列 task 幂等、工具 execution key |
| 3 | 同 Session 并发覆盖上下文 | lease/async lock、event sequence、version CAS/fencing |
| 4 | Redis 丢失导致任务丢失 | PostgreSQL outbox 权威、relay 重放、DLQ 和 backlog 告警 |
| 5 | IM 回调超时或限流 | 快速 ACK、异步发送、分段、抖动退避和最大重试 |
| 6 | 非幂等工具重复产生外部副作用 | 稳定幂等 key；未知结果停止自动重试并人工对账 |
| 7 | Worker 脑裂提交旧结果 | lease epoch/fencing，提交前校验 owner/epoch |
| 8 | 模型超时、成本失控 | timeout/fallback、并发隔离、token/费用预算和熔断 |
| 9 | 配置灰度漂移 | immutable revision、入队消息固定 revision、分批租户发布和回滚；Session 级固定与自动控制器需扩展 |
| 10 | 向量/Memory 投影落后 | source_version、lag 指标、SQL 事实回退和迁移校验 |
| 11 | Artifact 跨租户或孤儿对象 | tenant hash 前缀、checksum、staging TTL、GC |
| 12 | 日志、trace、错误报告泄露密钥 | SecretRef、统一递归脱敏、secret scan、最小权限 |
| 13 | OIDC/JWKS 或 Secret 轮换失败 | TTL cache、fail-closed、旧新版本并行轮换演练 |
| 14 | 数据库迁移破坏滚动发布 | expand-contract、兼容窗口、checkpoint、shadow read |
| 15 | 单租户耗尽共享资源 | 租户限流、连接池/队列配额、成本和 lag 告警 |
| 16 | 备份存在但无法恢复 | PITR/对象版本、按租户恢复演练和 RTO/RPO 门禁 |

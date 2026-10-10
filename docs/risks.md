# 生产风险清单

| # | 风险 | 缓解与验证 |
|---:|---|---|
| 1 | 请求伪造 tenant_id 越权 | 只信已验签 binding；跨租户负向测试 |
| 2 | RLS 被 owner 绕过 | runtime 非 owner、迁移账号分离、直接 SQL 隔离测试 |
| 3 | 同 Session 并发覆盖 state | lease + fencing + 原子 turn commit；四 Worker 故障注入 |
| 4 | Redis 丢数据导致消息丢失 | PostgreSQL Outbox 权威、至少一次补投、队列归零门禁 |
| 5 | IM 重复/乱序 | 平台消息唯一键、Session 接受序、幂等 outbound/tool execution |
| 6 | IM 超时后盲目重发产生重复回复 | ambiguous 状态 + 人工确认 replay |
| 7 | 非幂等工具重复产生外部副作用 | 稳定 execution key；未知结果停止自动重试 |
| 8 | Worker 脑裂提交旧结果 | 单调 fencing token；旧 token commit 必须失败 |
| 9 | 配置灰度过程中漂移 | Inbox 固定 immutable revision；Session HMAC rollout 桶 |
| 10 | Memory/向量投影滞后影响回答 | PG 权威、projection sequence/lag、落后时回退或提示 |
| 11 | Artifact 跨租户路径或残留 | tenant hash prefix、checksum、staging TTL 和孤儿清理 |
| 12 | 日志/trace 泄露消息与密钥 | recursive redaction + PrivacySpanProcessor + canary 扫描 |
| 13 | OIDC 密钥轮换或 JWKS 不可用 | TTL cache、issuer/audience/alg 固定、过期 fail closed |
| 14 | 飞书事件回调被重放或处理超时 | 时间戳/签名/Token/App ID 四重校验，持久接受后立即确认，ack p95 门禁 |
| 15 | 企业微信多副本重复连接 | 每 binding PG advisory lock、断线重连和锁接管测试 |
| 16 | 数据迁移部分成功或无法回滚 | 分阶段 checkpoint/count/checksum、双写观察窗、revision 回滚 |
| 17 | 数据库迁移破坏滚动发布 | Alembic expand-contract、新旧版本兼容测试 |
| 18 | 下游模型/工具雪崩 | timeout、租户 fallback、熔断、预算和并发隔离 |
| 19 | 单租户耗尽共享资源 | tenant 限流/预算、连接池配额、成本和队列 age 告警 |
| 20 | 备份存在但无法恢复 | PG PITR + 对象版本 + 密钥备份，季度按租户恢复演练 |

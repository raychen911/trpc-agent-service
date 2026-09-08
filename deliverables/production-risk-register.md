# 多租户 Agent 平台生产风险清单

> 评估范围：当前项目中的 Gateway、Worker、企业微信/Telegram Channel Adapter、治理 Filter、多后端存储、Transactional Outbox、OpenTelemetry 及部署配置。

## 1. 当前验证基础

当前项目已经完成 Docker Compose 后端联调、PostgreSQL + Redis 真实双节点测试、企业微信智能机器人真实消息与 DeepSeek 回复，以及 Jaeger 完整 Trace 验证。这证明核心链路能够运行，但在生产开放前仍需重点处理以下风险。

## 2. 风险等级

| 等级 | 含义 |
|---|---|
| P0 | 可能造成数据泄露、跨租户访问、未授权操作或大面积不可用，上线前必须解决 |
| P1 | 可能造成消息丢失、重复回复、成本失控或局部不可用，上线前需完成缓解和告警 |

## 3. 十项主要生产风险

| # | 等级 | 风险与当前依据 | 缓解措施 | 验证方法 |
|---:|:---:|---|---|---|
| 1 | P0 | **Trace 或日志泄露敏感信息。** 当前真实 Jaeger Trace 中包含完整模型输入、输出和用户标识，可能暴露 PII、业务数据或提示词。 | 生产默认关闭 prompt/response 正文采集；应用日志和 OTel Collector 同时使用字段白名单；对用户及 session 标识散列；限制 Jaeger 权限和保留周期。 | 发送包含测试手机号、邮箱和假 API Key 的消息，确认日志、Trace、指标和错误报告中均无法检索到原文。 |
| 2 | P0 | **开发默认密码或 Secret 进入生产。** Compose 中仍有 `trpc-dev-only`、`minioadmin`、`compose-change-me` 等开发值。 | 生产启动时拒绝默认值；使用 Vault/KMS/Kubernetes Secret；配置中只保存 Secret 引用；按租户和用途拆分并定期轮换；CI 执行密钥扫描。 | 使用默认值启动 production 必须失败；完成密钥轮换演练；检查 Git、镜像层、日志和 Trace 不含明文。 |
| 3 | P0 | **管理面或节点内部接口被未授权访问。** Admin OIDC 默认关闭，节点间仍可能使用 HTTP 和共享内部 Token。 | 生产强制 OIDC/RBAC并校验租户 Claim；管理端限制入口；节点间启用 mTLS、证书轮换和 NetworkPolicy；内部 Token 仅作为第二道防线。 | 无 Token 返回 401、无权限返回 403、跨租户访问被拒绝；无客户端证书的节点调用必须失败。 |
| 4 | P0 | **PostgreSQL RLS 被绕过导致跨租户访问。** 当前 RLS 测试已通过，但数据库 owner、`BYPASSRLS` 角色或连接池残留上下文仍可能破坏隔离。 | 应用使用非 owner、`NOBYPASSRLS` 账号；租户表启用 `FORCE ROW LEVEL SECURITY`；事务结束时清理 tenant context；保留复合外键；高隔离租户使用独立 Schema 或数据库。 | 对全部租户表执行双租户串读、串写测试，并覆盖连接复用、异常回滚和并发场景。 |
| 5 | P1 | **公网 IM Webhook 不稳定、被重放或遭受流量攻击。** 当前真实验证使用临时 Tunnel，URL 会变化且不具备生产 SLA。 | 使用固定域名、负载均衡、TLS 和 WAF；限制请求体、连接数和账号频率；校验签名、timestamp 和 nonce；保持 `tenant_id:channel:external_message_id` 唯一幂等约束；Channel Adapter 至少双副本。 | 重复回放同一消息只能生成一条 Inbox/Event 和一次回复；监控 401、429、回调 P95 与 Inbox backlog。 |
| 6 | P1 | **模型服务超时、限流或费用失控。** 当前链路依赖 DeepSeek API，429、5xx、网络长尾会延迟回复并形成队列积压。 | 设置连接、首 Token 和总执行超时；使用有界重试、熔断和并发舱壁；配置租户预算与限流；保留备用模型；超时返回明确的降级消息。 | 注入 429、5xx 和超时，检查熔断、预算结算和降级回复；告警模型 P95、错误率、Token 与租户成本。 |
| 7 | P1 | **Session 锁过期造成并发重复执行。** 当前分布式锁 TTL 默认为 60 秒，慢模型或工具可能超过 TTL。 | 增加锁续租和 fencing token；SQL 更新继续使用 `lock_version`/CAS；CAS 冲突后重新读取并有界重试；限制同 session 并发。 | 构造超过锁 TTL 的慢调用并从两个节点同时发送，确认只有一个有效提交和一次回复；监控续租失败与 CAS 冲突。 |
| 8 | P1 | **Redis 或 PostgreSQL 故障造成路由、锁和权威数据不可用。** 当前 Compose 均为单实例，SQL 又承载配置、Event、Summary、审计和 Inbox/Outbox。 | 生产使用 PostgreSQL HA 和 Redis Sentinel/Cluster；启用连接池、超时、AOF/副本和内存告警；协调后端故障时对同 session 写入 fail-closed；执行数据库备份、WAL/PITR 和恢复演练。 | 模拟 Redis 主从切换、数据库短暂不可用和连接池耗尽，确认恢复后无跨租户写入或重复回复；记录 RPO/RTO。 |
| 9 | P1 | **Inbox/Outbox 积压、重复投递或死信无人处理。** 当前已有重试与死信机制，但企业微信智能机器人回复依赖一次性 `response_url`，投递结果还可能处于不确定状态。 | 不同 topic 独立限速与并发；记录发送前后状态；建立死信查询、审批和幂等重放工具；接近 URL 过期时优先投递；配置 backlog、最老消息年龄和死信告警。 | 模拟 429、超时和“服务端已接收但客户端超时”，确认不会重复回复；验证死信可审计、可重放。 |
| 10 | P1 | **向量库、对象存储和异步同步发生数据不一致。** Memory 通过 Outbox 同步 Qdrant，Artifact 存入 MinIO；当前环境为单节点后端。 | SQL 保持事实源；向量写入使用稳定 ID 和版本号；Artifact 保存 checksum；生产启用副本、版本控制和备份；迁移采用双写、回填、校验、切读和回滚流程。 | 定期比对 SQL Outbox、向量点数和 Artifact checksum；删除测试索引后执行全量重建；监控同步延迟和孤儿数据。 |

## 4. 上线前检查重点

1. 清除 Trace、日志和错误报告中的模型输入输出及 Secret；
2. 启用 OIDC/RBAC、mTLS、NetworkPolicy，并替换全部开发凭据；
3. 使用生产数据库账号重新完成全表 RLS 隔离测试；
4. 完成 Redis/PostgreSQL 故障恢复以及 Inbox/Outbox 死信重放测试；
5. 配置模型错误率、IM 成功率、Session 延迟、队列积压、死信、Token 和租户成本告警；
6. 至少完成一次备份恢复和峰值并发压测，明确 RPO、RTO 与扩容阈值。

## 5. 结论

当前系统已经具备核心功能验证基础。生产化的重点应放在数据脱敏、身份认证、租户隔离、依赖高可用和消息可靠性上；关闭 P0 风险并完成 P1 风险的告警与故障演练后，才适合进入受控灰度试运行。

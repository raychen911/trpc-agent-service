# 使用 Kafka 兼容持久执行总线

平台使用 Kafka 兼容消息系统连接 Channel Gateway、Agent Worker 和 Job Worker，生产支持 Apache Kafka；核心链路采用至少一次投递与业务幂等，不宣称端到端 Exactly Once。入站执行以 `(tenant_id, session_id)` 分区以维持会话内顺序，Channel Gateway 通过 PostgreSQL Transactional Outbox 发布，消费者使用幂等状态机；执行结果、IM 投递、Memory 更新、审计、分级重试和死信使用独立 Topic。Redis 只承担缓存、租约和可选 Session 后端，不作为核心执行总线。

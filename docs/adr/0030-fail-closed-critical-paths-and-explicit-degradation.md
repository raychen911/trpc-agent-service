# 关键链路关闭并显式降级增强能力

平台在状态持久化、安全决策、密钥、租约和副作用结果不确定时 fail closed，不在无法记录或授权的情况下继续执行；Kafka 故障可依靠已提交 Outbox 积压，Redis 缓存可绕过但租约不可绕过，Memory 可用 Session 代替，Knowledge、Artifact 和模型 fallback 只有 Agent Release 明确允许时才能降级，IM 回复通过 Outbox 重试。任何降级都必须形成用户可见状态、审计事件、指标和告警，不能静默伪装为完整回答。

# 使用无状态 Worker 与显式 Session 并发控制

平台不使用 Sticky Session，所有 Worker 通过共享 Session 后端继续任意会话。同一 `(tenant_id, agent_app_id, session_id)` 的执行先由 Kafka 分区维持正常顺序，再通过带 fencing token 的可续期租约和 `expected_version` 乐观写入处理重平衡、超时重投与故障恢复产生的并发；丢失租约或版本冲突的 Worker 不得提交结果。平台在 tRPC-Agent-Python v1.1.19 SessionService 外增加这一并发层，工具副作用另以幂等键保护，不能依赖 Session 锁防重。

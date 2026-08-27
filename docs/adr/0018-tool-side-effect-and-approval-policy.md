# 显式分类工具副作用并持久化审批

每个 Tool 必须声明 READ_ONLY、IDEMPOTENT_WRITE、NON_IDEMPOTENT_WRITE 或 HIGH_RISK，并同时声明权限 Scope、超时、费用上限、数据敏感级别和允许主体。平台为调用创建持久 Tool Invocation 与幂等键：只读操作可退避重试，幂等写只有在下游接受幂等键时才可重试，非幂等写超时进入 OUTCOME_UNKNOWN 而不盲目重试，高风险操作必须审批并在受限执行环境运行。平台复用 tRPC-Agent-Python v1.1.19 的 Tool Safety Guard 与 HITL 能力，审批严格绑定工具版本、规范化参数哈希、主体和有效期，结果持久化后才能追加 Tool Result Event。

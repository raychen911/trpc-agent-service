# 使用 OPA Policy Bundle 统一执行治理策略

平台以结构化配置管理租户治理策略，校验、签名并版本化生成 OPA Policy Bundle，由 Gateway、Worker 和 Job Worker 的本地 OPA Sidecar 低延迟执行，tRPC-Agent-Python Filter 负责把 Agent 上下文接入策略判断。策略输出 allow、deny 或 needs_approval 及参数、数据范围、脱敏、预算和解释；敏感访问默认 fail closed，Sidecar 仅能短期使用最近已验证 Bundle，过期后停止敏感操作。关键决策记录策略版本及输入输出摘要，Tool 执行器和存储层仍执行纵深校验，不能把 Filter 当作唯一权限边界。

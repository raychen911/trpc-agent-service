# 异步生成最终一致的 Memory

Memory 由 Job Worker 从已提交 Session Event 异步生成，不阻塞当前 IM 回复，正常跨节点可见延迟目标为 P99 不超过 5 秒。Memory 记录携带租户、主体、来源 Session、来源 Event 范围和生成策略版本，并以来源范围保证幂等；成功写入后发布缓存失效事件。短暂延迟时后续执行继续使用权威 Session 上下文，积压超标则告警和降级；删除或纠正 Memory 留下审计记录，但不修改原始 Session Event。

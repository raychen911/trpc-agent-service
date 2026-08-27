# 采用分类内容生命周期与可证明删除

默认保留加密原始 IM Payload 7 天、Session Event 内容至最后活跃后 90 天、State 与 Summary 随 Session、Memory 至最后使用或验证后 365 天、普通 Artifact 30 天、幂等墓碑 365 天、在线备份 35 天，Knowledge 与被固定 Artifact 按显式生命周期保留；租户可在合规范围内覆盖。删除请求覆盖 SQL、Redis、向量、对象、缓存和派生副本，主存储 24 小时内完成、备份 35 天内失效，并提供状态、重试、对账和证明；删除正文后仅保留无内容墓碑与审计摘要，Legal Hold 可暂停过期且全程受审计。

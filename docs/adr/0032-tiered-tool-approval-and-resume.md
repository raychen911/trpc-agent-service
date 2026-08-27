# 使用分级 Tool 审批与持久恢复

普通受控操作可由具备权限的请求者通过企业微信、飞书卡片或 Web Console 确认，HIGH_RISK 或职责分离规则命中的操作必须由另一名审批角色用户批准。审批请求持久绑定租户、Agent Release、Agent 执行、工具、参数哈希、请求者、策略版本和有效期，默认 15 分钟；参数、Agent Release 或策略变化使其失效。等待期间 Agent 执行进入 WAITING_APPROVAL 且不占 Agent Worker，批准后重新取得 Session 租约从检查点恢复，拒绝、过期和撤销形成工具结果，所有动作进入审计链。

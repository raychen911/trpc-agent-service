# 使用通道范围身份与确定性 Session 隔离

平台先以 `tenant_id + channel_binding_id + external_user_id` 解析 IM 主体，再生成不暴露原始标识的内部 Session ID：单聊按租户、Agent 应用、绑定、主体和 generation 隔离，群聊按租户、Agent 应用、绑定和稳定 chat ID 隔离，飞书话题另加 thread ID；无法取得稳定群标识时拒绝启用共享群 Session。跨群、跨绑定和跨租户默认不共享 Session，只有管理员建立经过验证的主体关联后才允许在租户内共享跨通道 Memory，且群聊仍通过隐私 Filter 禁止注入成员私有 Memory。

# 安全与治理

## 租户边界

非开发环境的 Web 请求需要验证租户 Token。IM 消息根据平台保存的 Binding 确定租户，不接受消息体自行声明的租户身份。`app_name`、内部 user/session ID、对象目录和查询条件都带有租户命名空间。

这里的隔离边界是租户，而不是租户内部的个人用户。持有同一 Tenant Token 的客户端可以访问该租户的任务和资源；如果业务还需要用户级隔离，应在接入层补充个人身份认证和细粒度 ACL。

企微 HTTP decoded Frame 入口被禁止；只能由 WSClient 认证后回调。Telegram 使用 secret header，消息 hash 排除接收时间、Trace 等易变信息，重复不会误判 payload 冲突。

微信客服校验 SHA1 签名、AES Envelope、企业接收者和 `open_kfid`，XML 解析禁用实体扩展。不同 Binding 的 Session/Memory 使用不同身份。

在 `shared` 群聊中，SDK `user_id` 表示群会话归属，真实发言人保存在可信的 `metadata.actor_user_id`。入站 ACL 仍检查原始成员身份，不能按群归属给所有成员统一授权。危险操作通过审批 API 生成一次性令牌，IM 消息不能自行声明已经获批。

## Tool 的三道门

1. 配置只允许注册且 allowed 的 Tool；allow/deny 冲突拒绝配置。
2. TenantBoundaryAgentFilter 比较 contextvar 和 SDK metadata。
3. confirmation_required Tool 依次检查审批参数、执行记录、SDK ToolSafety。

Approval 使用随机、短期、一次性的 Token，数据库只保存其 Hash。Token 同时绑定 tenant、user、session、tool 和参数 Hash。接口验证通过后写入可信的 `approved_arguments`，Tool Filter 再与模型实际调用参数比较并消费本次授权。因此，一次审批不能用于任意参数或后续调用。

`ToolExecutionStore` 使用 tenant、request、tool 和参数 Hash 组成唯一键。执行成功后可以复用结果；异常或崩溃留下的 `running` / `unknown` 状态不会自动重做。生产环境使用 PostgreSQL 保存这些记录。

如果工具会修改外部系统，外部接口仍应提供幂等键、状态查询或补偿机制。平台本地没有成功记录，并不能证明远端操作没有发生。没有配置确认要求的普通工具，也不会自动获得副作用防重能力。

## 预算与用量

Redis Lua 会原子预留请求数、估算的输入/输出 token 和金额。单价来自租户模型配置中的每百万 token 价格，平台不会自动查询供应商账单。价格为 0 时仍可统计请求量和 token，但 `cost_usd` 只会得到 0，不能用来核对真实金额。

Usage Ledger 以 tenant、request 和 model 的组合键避免重复记账。Redis 的差额结算按 `request_id` 去重，因此数据库先写成功、Redis 后结算失败时仍可安全重试。

请求准入时先按估算值预留预算，完成后再根据模型返回的 token 补差额。这套机制用于平台预算保护和趋势统计，不等同于供应商的硬性 token 配额或最终账单。企业微信和微信客服请求也经过同一套预算检查。

## Secret、日志与 Trace

`SecretProviderRegistry` 默认解析 `env://` 引用，也允许接入其他 Secret Provider。空 Secret 按未配置处理。

成功解析的敏感值会登记到脱敏器。日志过滤范围包括 Bearer Token、键值对、邮箱、手机号、URL 和数据库凭据。`LoggingAuditSink` 先脱敏字符串字段再序列化，CLI 根日志也会在格式化后统一过滤。

平台 span 只保留明确白名单属性，丢弃 baggage，不记录原始异常 message/stack，只记异常类型。SDK 与第三方日志由统一日志过滤器处理，交付前的 Secret 扫描同时覆盖日志、错误响应和导出的 Trace 属性。

数据库 URL 属于管理员部署配置，不进入普通租户请求、日志或指标。生产环境通过 Secret/ConfigMap 注入，并限制管理接口访问。

审计覆盖 Chat、Tool、审批、配置、迁移、存储和投递。Redis 与 PostgreSQL 之间没有全局事务，平台依靠阶段记录、幂等和恢复流程保持业务一致性。外部 Tool 或客服发送进入 `UNKNOWN` 时，审计记录为人工核查提供依据。

测试入口包括 `demo governance`、`test_v3_resources_governance.py`、`test_v3_resumption.py` 和真实 Redis/PostgreSQL integration。离线测试不调用收费模型。

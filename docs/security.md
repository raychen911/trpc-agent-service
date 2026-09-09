# 安全与治理

## 租户边界

非开发环境的 Web 请求通过租户 Token 鉴权。IM 消息根据平台保存的 Binding 确定租户。`app_name`、内部 user/session ID、对象目录和查询条件都带有租户命名空间。

平台的基础隔离边界是租户。持有同一 Tenant Token 的客户端可以访问该租户的任务和资源；需要用户级隔离的业务可以在接入层增加个人身份认证和细粒度 ACL。

企业微信消息由认证后的 WSClient 回调进入 Adapter。Telegram 使用 secret header，消息 Hash 采用稳定业务字段，使重复投递可以复用原请求。

微信客服校验 SHA1 签名、AES Envelope、企业接收者和 `open_kfid`，XML 解析禁用实体扩展。不同 Binding 的 Session/Memory 使用不同身份。

在 `shared` 群聊中，SDK `user_id` 表示群会话归属，真实发言人保存在可信的 `metadata.actor_user_id`。入站 ACL 按原始成员身份检查权限。危险操作通过审批 API 生成一次性令牌，服务端验证后再写入可信审批元数据。

## Tool 的三道门

1. 配置服务检查 Tool 注册状态以及 allow/deny 规则，发布有效的工具列表。
2. TenantBoundaryAgentFilter 比较 contextvar 和 SDK metadata。
3. confirmation_required Tool 依次检查审批参数、执行记录、SDK ToolSafety。

Approval 使用随机、短期、一次性的 Token，数据库保存其 Hash。Token 同时绑定 tenant、user、session、tool 和参数 Hash。接口验证通过后写入可信的 `approved_arguments`，Tool Filter 再与模型实际调用参数比较并消费本次授权，使审批范围精确到一次指定参数调用。

`ToolExecutionStore` 使用 tenant、request、tool 和参数 Hash 组成唯一键。执行成功后可以复用结果；异常或崩溃留下的 `running` / `unknown` 状态进入查询或人工核查流程。生产环境使用 PostgreSQL 保存这些记录。

会修改外部系统的工具通过幂等键、状态查询或补偿接口与平台协作。平台遇到远端结果未知时先查询实际状态，再决定复用、补偿或人工处理。普通工具可根据副作用等级选择是否启用这套执行记录。

## 预算与用量

Redis Lua 会原子预留请求数、估算的输入/输出 token 和金额。单价来自租户模型配置中的每百万 token 价格。价格为 0 时仍会统计请求量和 token，`cost_usd` 记为 0；供应商账单作为外部结算依据。

Usage Ledger 以 tenant、request 和 model 的组合键避免重复记账。Redis 的差额结算按 `request_id` 去重，因此数据库先写成功、Redis 后结算失败时仍可安全重试。

请求准入时先按估算值预留预算，完成后再根据模型返回的 token 补差额。这套机制用于平台预算保护和趋势统计，供应商账单负责最终结算。企业微信和微信客服请求也经过同一套预算检查。

## Secret、日志与 Trace

`SecretProviderRegistry` 默认解析 `env://` 引用，也可以注册其他 Secret Provider。解析阶段会检查 Secret 是否存在。

成功解析的敏感值会登记到脱敏器。日志过滤范围包括 Bearer Token、键值对、邮箱、手机号、URL 和数据库凭据。`LoggingAuditSink` 先脱敏字符串字段再序列化，CLI 根日志也会在格式化后统一过滤。

平台 Span 使用属性白名单并记录归一化异常类型。SDK 与第三方日志由统一日志过滤器处理，Secret 扫描覆盖日志、错误响应和导出的 Trace 属性。

数据库 URL 属于管理员部署配置，通过 Secret/ConfigMap 注入。普通租户请求、日志和指标使用经过筛选的连接状态信息，管理接口由管理员权限保护。

审计覆盖 Chat、Tool、审批、配置、迁移、存储和投递。Redis 与 PostgreSQL 之间没有全局事务，平台依靠阶段记录、幂等和恢复流程保持业务一致性。外部 Tool 或客服发送进入 `UNKNOWN` 时，审计记录为人工核查提供依据。

测试入口包括 `demo governance`、`test_v3_resources_governance.py`、`test_v3_resumption.py` 和真实 Redis/PostgreSQL integration。本地测试使用 OfflineModel。

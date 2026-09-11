# 治理、监控与安全

## 租户治理

`TenantPolicyFilter` 在 Worker 内执行输入长度、IM 用户白名单、工具 allowlist/denylist 和危险工具二次确认。预算在调用模型前预留、结束后按估算 token 结算；超限只影响当前租户。审计记录 allow/deny/confirm/execute/fail/fallback，不记录完整密钥；`AuditPolicy.retention_days` 控制租户审计保留期，`allow_export` 默认关闭并约束 Admin 审计查询。

## 可观测性

指标保留低基数标签（tenant、channel、result）：入站量、重复率、队列深度/lag、模型/工具耗时、Session 后端延迟、IM 投递成功率、错误率、token 和租户成本。user/session/message 等高基数字段进入 trace 或审计，不直接作为 Prometheus label。OpenTelemetry span 链为：`im.callback → gateway.verify → queue.consume → runner → model/tool → session.write → memory.write → im.reply`。

## 密钥和脱敏

配置保存 `env://` 或 `file://` 引用，运行时解析；KMS/Vault 通过 CSI/sidecar 注入文件或环境变量，生产拒绝 `literal://`。日志、trace attributes、异常和审计 detail 经过统一 masker，覆盖 API key、Bearer token、secret、password、数据库 URL；SecretStr/环境变量不通过 `repr` 输出。密钥轮换采用新旧引用并行窗口，轮换完成后撤销旧版本。

实现边界：自建 span 使用递归脱敏，并关闭自动 exception event/description，仅记录 error.type 与错误状态；不是所有第三方 SDK/FastAPI 自动 instrumentation 都经过该 helper。生产还须关闭请求正文和认证 header 采集，对第三方日志/exporter 做 secret scan；Secret 轮换窗口由运维实施，不是内置自动轮换服务。

## 访问控制

Admin API 使用服务端绑定的 admin/operator/viewer 独立 token，客户端提交的角色 header 不参与授权；前置网关可进一步接入 OIDC/JWT。配置更新采用 ETag/expected_version；数据库 runtime/control/migration 账号分离，生产启用 SQL RLS。所有查询、缓存 key、对象 key 和导出接口都强制 tenant scope；跨租户访问、伪造签名、危险工具绕过和日志 secret scan 纳入安全门禁。

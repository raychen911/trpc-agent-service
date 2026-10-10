# 安全、治理与遥测隐私

Admin 生产认证固定 OIDC issuer、audience 与算法白名单，从 JWKS 校验 JWT；角色为
`platform_admin`、`tenant_admin`、`auditor`。生产设置拒绝开发 token、literal SecretRef 和内容 trace。
tenant admin 只能操作 token 中授权的 tenant。所有控制面写操作要求 Idempotency-Key，除创建租户外
还要求 If-Match，避免重放和丢失更新。

数据隔离由 API tenant scope、tenant-first Repository、PostgreSQL RLS、Redis key tenant namespace、
S3 tenant hash prefix 和向量 tenant 条件共同执行。迁移账号与运行账号分离；运行账号不是 owner，
不能绕过 RLS。绑定解析函数使用 SECURITY DEFINER，但固定 search_path、最小返回列和显式 EXECUTE
授权。

治理顺序固定为身份/租户授权 → 工具白名单 → 预算预留 → SDK Tool Safety → 危险操作确认 → 执行。
确认令牌绑定 tenant、principal、Session、tool、参数摘要和过期时间，账本确保一次性消费。工具声明
`idempotent`、`non_idempotent` 或 `unknown`：只有幂等操作可自动重试；非幂等调用在结果未知时进入
人工状态，防止重复付款、发信或变更外部系统。

配置仅保存 `SecretRef`。Compose 用 Docker secrets，Kubernetes 用 Secret/ExternalSecret；可扩展
Vault/KMS Provider。JSON logger 递归脱敏 token、password、authorization、cookie、API key 和连接串。
异常响应使用固定错误码，不返回内部 SQL、路径或第三方响应正文。

tRPC-Agent 1.1.19 会把 Runner/Tool/LLM 内容写入 span，本项目在 exporter 前使用
`PrivacySpanProcessor` 删除 input/output/state、tool arguments/result 和 LLM request/response。正文采集
只允许显式开发模式，生产模型校验会拒绝它。审计保留 tenant、channel、principal、session、agent、
tool、decision、latency、error_type、cost、trace_id、config/policy version、idempotency key 和是否脱敏，
不保留原始消息。

建议在 CI 和镜像发布执行锁文件检查、pip-audit、Trivy、SBOM、secret scan；运行时使用非 root、只读
根文件系统、drop ALL capabilities、NetworkPolicy 和默认拒绝 egress，仅放行 DNS、OIDC、托管后端及
明确的模型/工具/IM endpoint。

截至 2026-08-21 的复扫中，`cryptography` 已提升到 50.x 安全基线。`openclaw` extra 需要
`nanobot-ai`；其 0.3.0 元数据仍限制旧版 Dulwich/PyPDF，而这些版本命中可修复高危漏洞。本服务不调用
nanobot 私有 API，因此在 `tool.uv.override-dependencies` 中把两者提升到已修复版本，并用 SDK 契约和
nanobot 导入测试守住兼容性；上游放宽约束后应删除 override。生产部署必须从 `uv.lock` 构建，不能
脱离锁文件单独用 pip 解析该 extra。运行镜像采用 Python 3.12 Alpine 3.24。Docker Scout 对最终镜像的
CRITICAL/HIGH 扫描和 pip-audit 均必须为零；任一依赖变化都要重新执行 SDK 契约、完整回归和镜像
漏洞门禁。

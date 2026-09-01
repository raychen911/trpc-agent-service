# 安全模型、租户隔离与生产风险

## 1. 信任边界

```text
不受信任
  IM 回调头 query body
  租户提交的配置 JSON
  模型输出和 Tool 参数
  Tool 下游响应

受约束信任
  经服务端路由解析的 TrustedBindingContext
  通过验签解密和 schema 校验的 NormalizedInbound
  不可变 TenantSpec 和 AgentAppSpec 版本
  带租约和 fencing token 的 SessionClaim

高权限
  迁移所有者数据库角色
  运行时租户 RLS 数据库角色
  密钥解析器和根密钥
  Outbox Dispatcher
```

核心原则是不让外部数据自带权限上下文。tenant、app、binding 和 secret reference 由服务端根据公开 callback ID 查得；IM payload 只能提供待校验的业务内容。

## 2. 隔离措施

### 配置隔离

- Pydantic 模型使用 `extra=forbid` 和 frozen 语义，未知字段不会静默生效。
- 租户与 Agent 配置按严格递增 revision 发布，同 revision 不同 content hash 拒绝。
- channel 必须引用同一 TenantSpec 中存在的 app revision，callback path 必须等于系统推导的规范路径。
- Worker 使用 Inbox 接收时的 `config_revision` 加载不可变快照，避免队列积压期间配置切换导致 TOCTOU。

### 数据隔离

- 所有业务查询显式带 `tenant_id`，复合外键防止跨租户关联。
- PostgreSQL 对租户表强制 RLS，策略同时带 `USING` 和 `WITH CHECK`，每个事务通过 `set_config` 设置租户 GUC。
- 迁移使用 owner 角色，服务使用 `NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT` 的 runtime 角色。生产 runtime 还必须确保无 `BYPASSRLS`。
- `channel_ingress_route` 是 RLS 前的公开索引，只保留 public ID、tenant、binding、channel、revision 和 status，不存外部身份、payload 或 secret。

### 工具隔离

- `TenantToolSet` 只暴露租户白名单内、且已通过审批门的工具。
- 同一白名单在模型建请求和 SDK 实际解析 Tool 时均检查，并要求 invocation metadata 中存在匹配的 `TenantContext`。
- ToolSet 对象不允许运行时加工具，Agent 和 Filter 每轮重建，减少请求间可变状态泄漏。

当前仍缺少可上线的具体 PII Filter、输入/输出内容策略、IM 用户 ACL、token/成本强制扣费和二次确认工作流。`ToolPolicy` 和 Filter factory 是可用扩展点，不应被文档表述为已实施的完整治理。

## 3. 密钥与敏感数据

- TenantSpec 只允许 `secret://` 引用，不接受明文 token。
- 当前 `EnvironmentSecretResolver` 只能读显式 allowlist 内的 `secret://env/NAME`，防止租户配置演变成任意环境变量读取器。
- `response_url` 和 Telegram 发送坐标用 HKDF 派生的 AES-256-GCM 密钥加密，AAD 绑定 tenant、binding、delivery 和 credential kind。
- 完整 SDK Event 以 tenant/session/event/seq 为 AES-GCM AAD，密文写入租户 RLS 的 append-only `event_object`；`content_ref` 只含上下文哈希和密文哈希。
- principal、conversation、session、thread 和 message reference 用版本化 HMAC 派生，不把外部 ID 直接写入 Inbox。
- `SecretStr` 防止常规 repr 泄漏；日志过滤器递归脱敏密钥字段、Bearer 值和 URL query 凭据，并丢弃 exception message 和 traceback。
- OpenTelemetry 在进程内的终端 exporter 前创建允许列表 span 副本，再交给 OTLP；Collector 侧还有第二层属性删除。

生产不应使用 Compose 中的示例密码或环境变量作为最终密钥管理方案。需实现 Vault/云 Secret Manager/KMS 适配器，给 Gateway、Worker 和 Dispatcher 分配最小解密权限，并制定根密钥与 HMAC key version 的双读轮换方案。

需明确数据分类边界：归一化 Inbox 文本、SessionEvent 的最小可观测 payload、Summary 和显式 Memory 当前仍可能以数据库明文字段存在，依靠 RLS、磁盘加密、备份权限和保留策略保护；它们不等同于完整 SDK Event 的应用层 envelope encryption。高敏租户应增加字段级加密/令牌化、按类别 TTL 和数据主体删除工作流。

## 4. 网络与输入安全

- 生产配置会拒绝 HTTP public base URL、SQLite、短根密钥和默认 Admin key；Worker/Projector 还拒绝将 replay 必需的 event object 放在 local/Redis 后端。
- 入站回调要求精确 `application/json`，按流读取并限制 body 字节数，重复头/query 会拒绝。
- 企微主动回复 URL 在验签后和解密后均限定 HTTPS、主机名、端口、无 userinfo 和无 fragment，HTTP 客户端不跟随重定向。
- Telegram endpoint 由代码固定为 Bot API，model base URL 由平台配置而非租户输入，并要求 HTTPS。
- FastAPI 响应添加 `nosniff` 和 `no-store`，生产关闭 OpenAPI UI，禁止 reload。

生产还需外层 WAF/反向代理的 TLS 终止、源 IP 策略（通道支持时）、按 callback ID 限流、请求超时、连接数上限和 egress allowlist。

## 5. 审计语义

`audit_log` 包含课题要求的 tenant、channel、user、session、agent、tool、decision、latency、error、cost 和 trace，并补充 request、action、resource、config/policy revision 和 idempotency key。PostgreSQL trigger 拒绝 UPDATE/DELETE，即使本地 Compose 的通用 runtime grant 含 UPDATE 也无法改写；生产数据库权限还应显式撤销 audit/event object 的 UPDATE/DELETE，形成权限与 trigger 双层控制。

`record_hash` 是单条记录的规范 JSON SHA-256，可发现非预期内容变化，但它不是 HMAC、不是链式哈希，也未外部封存。具有 owner 权限的攻击者仍可以重写表和重算哈希。高合规环境应将审计流异步写入 WORM 存储或 SIEM，并使用 KMS 签名。

## 6. 生产风险清单

| 编号 | 风险 | 已有缓解 | 上线前剩余工作 |
|---:|---|---|---|
| R1 | 伪造或重放 IM 回调 | 通道验签、时间窗、严格解析、持久去重 | 真实平台联调、WAF 限流、时钟漂移告警 |
| R2 | 租户越权读写 | 显式 tenant 条件、复合外键、FORCE RLS、独立 runtime 角色 | 在目标 PG 集群运行渗透和权限快照测试 |
| R3 | 配置切换导致旧消息用新权限执行 | Inbox 固化 config/app revision，Worker 按版本加载 | 配置保留周期与删除禁令 |
| R4 | 密钥进入日志或 trace | SecretStr、递归脱敏、异常去文本、span 允许列表 | 主动 canary secret 扫描、日志平台检测、事故轮换演练 |
| R5 | 回复 URL 或模型 endpoint 造成 SSRF | 企微 host allowlist、HTTPS、禁止重定向，租户不能指定 model URL | 网络层 egress policy、DNS rebinding 防护和代理审计 |
| R6 | 旧 Worker 延迟写入覆盖新 Worker | 租约、数据库时钟、fencing token、OCC | 生产 PG 高并发和故障注入 |
| R7 | 不可幂等 Tool 重复执行 | Tool Effect 账本保留 `unknown` | 将实际 Tool 执行器全部接到账本，建对账队列 |
| R8 | IM 模糊结果导致重复回复 | Outbox delivery fence，read timeout 和过期 send 进 `unknown` | 对账工具、运营界面、通道原生幂等键调研 |
| R9 | Prompt injection 诱导危险工具 | 工具白名单和审批集合 | 具体 Filter、参数级授权、人工确认、沙箱与 egress 策略 |
| R10 | 静态 Admin key 被泄漏或无法归因 | 常量时间比较、生产强度校验 | 替换为 OIDC/mTLS、RBAC、细粒度动作审计；不信任自报 actor header |
| R11 | 过量请求、token 或工具调用导致成本失控 | body 上限、Runner 有限循环、模型 token ceiling、Tool 次数上限 | 租户令牌桶、硬预算预留/扣减、并发舱壁和告警 |
| R12 | 恶意附件和媒体解压炸弹 | 只传不透明 locator，当前文本 Worker 不下载 | 独立下载沙箱、尺寸/MIME/magic bytes 校验、AV 扫描、对象隔离 |
| R13 | 依赖包或基础镜像供应链污染 | `uv.lock`、关键包精确锁定、CI `--frozen` 和 `pip check` | 镜像 digest、SBOM、签名验证、SCA 和定期升级窗口 |
| R14 | 审计管理员篡改 | append-only trigger 与单条 record hash | WORM/SIEM 外部封存、KMS 签名、分离数据库所有者 |
| R15 | 个人数据过度保留 | HMAC 标识、AuditPolicy retention 建模 | 真实删除/导出 job、法务基线、按数据类别设定 TTL |

## 7. 权威参考

- [PostgreSQL CREATE POLICY](https://www.postgresql.org/docs/current/sql-createpolicy.html)
- [PostgreSQL Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- [OpenTelemetry Python](https://opentelemetry.io/docs/languages/python/)
- [OpenTelemetry Python Exporters](https://opentelemetry.io/docs/languages/python/exporters/)

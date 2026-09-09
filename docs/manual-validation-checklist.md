# 实网与目标环境人工验收清单

这份清单只包含必须由账号所有者或目标环境管理员完成的事项。代码仓库已经能做的自动化验证不在这里重复。建议先完成 Telegram 闭环，再完成企业微信主链；前者配置快，后者是题目重点。

## 0. 证据规则

- 只使用测试租户、测试群和测试账号，不接生产通讯录或真实客户数据。
- token、EncodingAESKey、模型 key 和数据库密码只写入 Secret 管理系统或本机未提交的 `.env`，截图必须遮挡。
- 每轮测试记录：UTC/北京时间、配置 revision、delivery/update ID、request ID、trace ID、预期、实际、截图或日志位置。
- 不用“页面能打开”代替端到端成功；至少核对 Inbox、Run、Session event、Outbox、Audit 五类事实。

## 1. 模型与 Worker

- [ ] 申请一个测试模型 API key，确认 provider、model、endpoint 和额度。
- [ ] 将 key 配置到服务端 allowlist 中的环境变量，TenantSpec 只保留 `secret://env/...` 引用。
- [ ] 将演示 app 的 `mock` provider 改为已批准 provider，发布 revision 2。
- [ ] 启动 Worker、Dispatcher、Projector，确认 readiness 和 `/metrics` 正常。
- [ ] 发送一条不调用 Tool 的消息，核对 AgentRun 完成、token 指标增加、回复进入 Outbox。

## 2. Telegram 最小实网闭环

- [ ] 通过 BotFather 创建仅用于本项目的测试 bot，取得 bot token。
- [ ] 生成独立的 webhook secret，不与 bot token 或管理密钥复用。
- [ ] 准备公网 HTTPS callback：`/v1/channels/telegram/{public_callback_id}/callback`。
- [ ] 将 token/secret 写入环境变量并加入 `TRPC_SERVICE_SECRET_ENV_ALLOWLIST`。
- [ ] 更新 `external_account_id`、secret refs 与 identity allowlist，启用绑定并发布新 revision。
- [ ] 调用 Telegram webhook 配置接口后，分别验证单聊、群聊、topic、重复 update、超长回复分段和 429 限流重试。
- [ ] 人为制造一次发送超时，确认结果进入 `unknown` 或受限重试，而不是盲目重复发送。

## 3. 企业微信主链

- [ ] 在企业微信测试组织创建智能机器人/应用，选择与本仓库适配器一致的 JSON 加密回调模式。
- [ ] 准备 token、EncodingAESKey、企业/机器人账户标识及公网 HTTPS callback。
- [ ] 将 secret 写入允许的环境变量，配置 `external_account_id` 与 identity allowlist，发布并启用新 revision。
- [ ] 通过平台的 URL 验证挑战，再测试加密消息回调、时间窗、防重放与重复 delivery。
- [ ] 分别验证单聊、普通群聊和群成员隔离；确认外部 user/group ID 不以明文进入 Session/Audit。
- [ ] 验证 `response_url` 域名限制、过期回复、超长文本分段和投递失败分类。

## 4. PostgreSQL 与多节点

- [ ] 使用 migration owner 执行迁移，使用非 owner、非 superuser、无 `BYPASSRLS` 的 runtime role 运行服务。
- [ ] 设置 `TEST_POSTGRES_URL`，执行 `pytest -m postgres`，确保当前本机跳过的 5 项合同全部通过。
- [ ] 启动至少 2 个 Worker；并发投递同一 session，验证严格有序且无丢失更新。
- [ ] 在 Agent 执行中杀死 Worker，等待租约接管；核对旧 fencing token 无法提交。
- [ ] 制造数据库短暂不可用，确认 Gateway 不会在 T0 未提交时返回成功 ACK。
- [ ] 演练备份恢复、Outbox/Tool Effect `unknown` 对账和单租户数据导出。

## 5. Kubernetes 与观测

- [ ] 将镜像改为 digest，补齐 Ingress/TLS、External Secrets、托管 PostgreSQL、OTLP endpoint 和 egress allowlist。
- [ ] 运行 server-side dry-run，再部署 migration Job 和四类 workload。
- [ ] 验证 request/trace ID 能从 IM callback 对到 Run、Tool、Session/Memory、Outbox 和回复日志。
- [ ] 为 callback 5xx、oldest Inbox、stale fence、Outbox unknown/dead letter、数据库连接池和每租户 token/成本配置告警。
- [ ] 用目标模型完成基准压测与 30% Worker 故障注入，记录 P50/P95/P99、峰值 QPS、token 和恢复时间。

## 6. 最终提交证据

- [ ] GitHub Actions 的 quality、migration、postgres-contract 全绿。
- [ ] 控制台截图不含密钥或真实个人数据，并能看到 tenant、revision、通道状态和审计 trace。
- [ ] 至少保留一条“企微用户消息 → Agent → Tool/Memory → Outbox → 企微回复”的 trace 证据。
- [ ] 在 README 或答辩记录中明确区分：本地自动化通过、实网通过、设计完成但尚未接入。
- [ ] 提交前运行 `git status` 和 secret scan，确认 `.env`、数据库、日志、截图原图和 token 均未进入版本库。

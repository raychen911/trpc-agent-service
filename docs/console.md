# 可视化控制台与安全演示

## 1. 定位

`/console` 是面向评审和平台运维人员的轻量控制面，不是聊天玩具页。它将本项目最重要的工程对象直接呈现出来：租户、不可变配置版本、跨节点队列、通道绑定、存储选择、审计元数据，以及发布与回滚操作。

前端不依赖 CDN 或第三方脚本，随 Python wheel 一起打包，由同一 FastAPI 进程提供。页面使用严格 Content Security Policy，管理密钥仅保存在当前浏览器标签页的 `sessionStorage`，不会写入 URL、Cookie 或页面日志。

## 2. 本地启动

```bash
uv sync --frozen --all-extras
uv run trpc-agent-service migrate
uv run trpc-agent-service demo-seed
uv run trpc-agent-service serve --host 127.0.0.1 --port 8000
```

打开 `http://127.0.0.1:8000/console`。若未加载 `.env`，开发默认管理密钥为 `development-admin-key`；若使用 `.env.example`，输入其中 `TRPC_SERVICE_ADMIN_API_KEY` 的值。

`demo-seed` 可以重复执行。相同 revision 和内容会返回 `idempotent: true`，不会生成第二份配置。演示租户具备以下安全限制：

- 企业微信与 Telegram 绑定均为 `disabled`；
- 两个绑定的身份默认策略均为 `deny`；
- 模型 provider 为 `mock`，Worker 会拒绝启动；
- 所有凭据均是 `secret://env/...` 引用，种子不包含真实 token；
- 命令在 production 环境直接拒绝执行。

因此，演示种子只能证明控制面、数据模型和页面行为，不能被当作实网联调成功证据。

## 3. 页面与操作

| 页面 | 展示/操作 | 对应接口 |
|---|---|---|
| 运行总览 | 租户数、Session、Inbox、Run、Outbox、Audit 与队列状态 | `GET /v1/admin/overview` |
| 租户与版本 | 活动配置、模型路由、通道启停、存储选择、历史 revision | active、revisions、rollback API |
| 配置发布 | 编辑完整 `TenantSpec` JSON，创建严格递增的新版本 | `PUT /v1/admin/tenants/{tenant_id}` |
| 审计追踪 | decision、action、trace/request、延迟、错误与成本元数据 | activity API |
| 验收证据 | 将核心设计映射到可执行工程证据，并标出实网边界 | 静态说明 |

控制台不会读取或返回消息正文、Prompt、Memory 内容、IM token、模型 key 或数据库密码。审计页面只展示核验链路所需的元数据，减少运维控制面成为第二个敏感数据出口的风险。

## 4. 安全边界

当前 Admin API 使用静态 key，适合本地演示和受控测试环境，不适合直接暴露到公网。生产环境必须在反向代理后接入 OIDC 或 mTLS，实施 RBAC、CSRF/来源约束、操作级审计、短会话和密钥轮换；还应将自报的 `x-admin-actor` 替换为经过认证的主体声明。

页面上的“队列清零”只代表当前数据库快照没有开放记录，不代表模型、Worker、外部 IM 或网络探针健康。真实上线验收应同时检查 `/health/ready`、Prometheus 告警、Worker/Dispatcher/Projector 日志、目标 PostgreSQL 合同测试与外部 IM 探针。

## 5. 需要人工完成的实网步骤

代码无法代替平台账号所有者完成以下操作：

1. 在企业微信或微信客服后台创建测试应用/机器人，取得合法回调 token、EncodingAESKey 与账户标识；
2. 在 Telegram 的 BotFather 创建测试 bot，设置 webhook secret 和 HTTPS callback；
3. 将凭据写入本机或部署平台的 Secret 管理系统，并加入服务端 allowlist；
4. 为允许的真实用户计算/登记内部 principal，发布启用后的新 revision；
5. 使用真实消息验证验签、重复投递、群聊/单聊隔离、限流、超时、分段和失败重试；
6. 在目标 PostgreSQL 与 Kubernetes 环境运行 RLS、故障注入、容量和恢复演练。

这些步骤需要账号权限或目标基础设施，不能用本地 mock 截图替代。

逐项执行与留证模板见 [实网与目标环境人工验收清单](manual-validation-checklist.md)。

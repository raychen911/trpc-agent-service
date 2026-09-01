# 开源组件评估与复用边界

## 1. 基线事实

本仓库的实现基线是提交 `4cda37b`。该提交只有题目 README、空脚本、空包入口和目录占位，共 22 个文件；`trpc_service` 下没有可运行的平台逻辑，`docs/README.md` 与 `data/README.md` 也为空。因此，本分支中的租户模型、IM 适配、可靠性账本、数据迁移和运行角色均属于新增实现，不能把上游骨架描述成已有能力。

这一判断来自本仓库 Git 对象，可用下面两条只读命令复核：

```bash
git show --stat 4cda37b
git ls-tree -r --name-only 4cda37b
```

## 2. 评估方法

组件不是按 Star 数或宣传语选择，而按以下工程问题逐项判断：

1. **职责是否单一**：框架、协议解析、持久化、密码学和遥测分别由相应组件承担，不让某个库越过平台信任边界。
2. **接口是否可封装**：平台只依赖窄接口；外部 API 的版本变化应被兼容层或 Adapter 隔离。
3. **维护与来源是否可核验**：优先官方仓库、官方文档和 PyPI 发布物。
4. **安全默认值是否可约束**：敏感网络请求、密钥解析、反序列化和数据库权限必须由平台再加限制。
5. **能否确定性复现**：运行时关键包精确锁定，完整依赖图由 `uv.lock` 固化，CI 使用 `uv sync --frozen`。
6. **许可证与供应链是否可交付**：依赖许可证需在发布前生成清单；镜像、SBOM 和签名仍应进入生产门禁。

## 3. 采用的组件

| 组件 | 本项目用途 | 采用理由与边界 | 当前约束 |
|---|---|---|---|
| [tRPC-Agent-Python](https://github.com/trpc-group/trpc-agent-python) `1.1.19` | `LlmAgent`、`Runner`、Event、ToolSet、Filter 与模型抽象 | 直接复用 Agent 编排，不重复实现 SDK；多租户隔离、Inbox/Outbox、fence、审计和 IM 协议仍由平台层实现 | tag commit `fd051a475574c900123acf0fa64eab9b3c502842`，wheel SHA-256 `fc94b81305542b25b2318842ffb52671db4a92788b6f886c6c98068c829d9`；`agent.compat` 校验公开签名 |
| [FastAPI](https://fastapi.tiangolo.com/) | Gateway、健康检查、Admin API、IM callback | ASGI 组合清晰，便于做严格请求边界和 OTel 插桩 | 生产关闭文档 UI；大小、Content-Type、认证和异常输出由平台控制 |
| [SQLAlchemy](https://docs.sqlalchemy.org/en/20/) 与 [Alembic](https://alembic.sqlalchemy.org/) | 异步 SQL、事务、模型与迁移 | 同一模型支持 SQLite 开发和 PostgreSQL 生产合同 | SQLite 只用于本地；RLS、`SKIP LOCKED` 等性质必须在 PostgreSQL 验证 |
| [PostgreSQL](https://www.postgresql.org/docs/current/) 16 | 配置、Inbox、Run、EventObject、ProjectionJob、ToolEffect、Outbox、Audit 权威面 | 事务、行锁、约束、RLS 和数据库时钟适合可靠性事实 | 运行角色不得是 owner、superuser 或 `BYPASSRLS`；本机尚无真实 PG 运行证据 |
| [Redis](https://redis.io/docs/latest/) | Session 热投影、Summary 缓存和限流计数 | 低延迟、Lua CAS；可由 SQL 事实重建 | 不是权威事务日志；production Worker/Projector 强制使用 SQL event store |
| [cryptography](https://cryptography.io/en/latest/) `50.0.1` | AES-GCM、HKDF 和企微 AES-CBC 协议实现 | 使用成熟密码原语，不自创加密算法 | 根密钥轮换和 KMS/Vault Adapter 尚未完成 |
| [aiogram](https://docs.aiogram.dev/en/latest/) `3.31.0` | Telegram Update 的严格结构解析 | 复用 Bot API 类型，不启动框架自己的轮询器 | webhook 认证、身份派生和投递由平台实现 |
| [HTTPX](https://www.python-httpx.org/) `0.28.1` | IM Outbox HTTP 投递 | 明确超时、连接池和重定向策略，易于合同测试 | 禁止自动重定向；企微目标还要做 host allowlist |
| [OpenTelemetry Python](https://opentelemetry.io/docs/languages/python/) | FastAPI trace 与 OTLP 导出 | 采用开放协议，Collector 与后端解耦 | 当前只完成入口插桩和出口属性清洗，尚非完整跨进程 trace 树 |
| [Prometheus Python client](https://prometheus.github.io/client_python/) | `/metrics` 与低基数指标 | 拉取模型简单，适合平台运行指标 | 多数 Worker、Tool、Storage 指标已定义但尚未全部接线 |
| [uv](https://docs.astral.sh/uv/) | 锁文件、同步、构建和 CI | `--frozen` 可阻止 CI 静默改锁文件 | `uv.lock` 不等于漏洞扫描或来源证明 |

## 4. tRPC-Agent-Python 的具体复用

平台与 SDK 的边界由代码而不是架构图口号确定：

| 直接调用或实现 SDK 扩展点 | 平台必须新增 |
|---|---|
| `Runner.run_async`，逐个消费完整 `Event` | T0 持久接收、消息去重、session 顺序号 |
| `LlmAgent`、`RunConfig`、模型类 | 不可变 tenant/app/channel revision 与服务端路由 |
| `BaseSessionService` 接口 | 绑定租约和 fencing token 的 SessionService |
| ToolSet、FunctionTool、Filter 工厂 | 租户白名单、审批上下文、Tool Effect 副作用账本 |
| SDK 的 session/event 数据类型 | staged/committed/aborted 可见性和加密事件对象 |
| SDK telemetry 扩展能力 | request/trace 持久关联、日志与 span 出口脱敏 |

本项目没有复制 SDK 内部 Agent 循环，也没有假定 SDK 的 Session 后端可以自动解决跨节点互斥。版本升级流程应先在兼容测试中验证构造签名、事件字段和 Runner 行为，再更新精确锁定版本。

## 5. 借鉴的成熟模式

下列是通用工程模式的独立实现，不是从某个仓库复制代码：

- **Transactional Inbox/Outbox**：输入落库后才 ACK，回复意图与 Agent 完成事务一起提交。
- **Lease + fencing token + OCC**：租约决定当前所有者，fence 拒绝旧所有者，版本号拒绝丢失更新。
- **不可变配置 revision**：发布只增加版本；回滚激活旧快照；Inbox 固定接收时版本。
- **权威事实与投影分离**：SQL 保存不可替代事实，Redis、Summary、Memory 和向量索引按水位异步重建。
- **Expand-contract 数据迁移**：双写、影子校验、切读、观察、停止旧写、延迟清理。
- **最小权限与 RLS**：迁移 owner 和运行角色分离，租户事务同时设置数据库隔离上下文。

这些模式只有在目标 PostgreSQL、真实 IM 和故障注入环境中通过验证后，才可提升为生产保证。

## 6. 未采用的捷径

- 不以 sticky session 代替共享状态和并发控制；节点故障后粘滞路由无法保证正确性。
- 不把 Redis 当作所有数据的唯一权威源；审计、外部副作用和投递模糊状态需要持久事务记录。
- 不把 Telegram 或企微的 HTTP 200 当作端到端处理成功；200 只代表 T0 已持久接收。
- 不允许租户直接提交模型 base URL、IM token 或任意环境变量名；这些是平台运维权限。
- 不把依赖库的能力写成平台已经接通的能力。例如 OTel 已安装不等于 Worker 至 IM 回复已经形成同一分布式 trace。

## 7. 供应链剩余风险

当前已有 `uv.lock`、CI frozen sync、`uv pip check` 和若干关键包精确版本；仍缺少：

1. 基础镜像按 digest 固定，而不是只用可移动 tag；
2. CycloneDX 或 SPDX SBOM；
3. 镜像签名、来源证明和部署侧验签；
4. 依赖许可证归档与漏洞扫描门禁；
5. 定期升级窗口及 tRPC SDK 兼容性审查记录；
6. 对 Compose 中 PostgreSQL、Redis 和 OTel Collector 镜像做同样的来源约束。

因此，当前依赖方案足以支持可重复的工程验证，但还不能作为完整的软件供应链合规证明。

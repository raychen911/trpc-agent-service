# 原题逐条检查记录

代码检查日期：2026-09-10。依据：[README-old.md](../README-old.md)。本次文档修订将 IM 验收范围统一为企业微信和飞书，并按实际存储结构整理数据模型。原题以架构设计为主，不要求完整系统，因此分别评估设计交付、代码实现和目标环境验证。七项验收入口见 [README](../README.md)。

## 1. 具体要求

状态口径：“实现”表示有本地代码及相关测试，不代表真实外部平台联调通过；“设计”表示提供具体落地方案，但不宣称全自动实现。

| 编号 | 原题要求 | 检查结论与证据 |
|---|---|---|
| A1 | 租户含 ID、应用、模型、工具、IM、后端、审计策略 | 实现：`tenant/models.py` 的 TenantConfig/AgentApp/ChannelBinding/StorageProfile/AuditPolicy；[模型](data-model.md) |
| A2 | Gateway/Worker/Channel/Storage/Admin/Collector 拓扑协作 | 实现与设计：[架构图](architecture.md)、Compose/Kustomize；Collector 为独立服务 |
| A3 | 水平扩展及 tenant/session 路由 | 实现：Gateway binding 校验、HMAC Session ID、Redis consumer group、SQL/Redis lease；[架构](architecture.md) |
| A4 | sticky session 与共享 Session/Memory | 无需 HTTP sticky；平台状态 SQL/Redis、框架 Session 共享 Redis；企业微信 Bot WebSocket 例外，当前仅 role=all，不能按通用多 Worker 路径部署 |
| A5 | 配置、数据、工具、日志、密钥隔离 | 实现：revision、RLS/tenant scope、allowlist、SecretRef、脱敏；群级 Memory/多 App 隔离限制见 [IM 边界](im-channels.md) |
| B1 | 不同租户选择后端 | 实现：StorageRouter 按 kind/name 注册 SQL/Redis/pgvector/S3；InMemory 单进程；外部 Memory/其他向量库仅扩展契约 |
| B2 | Session/Memory/Summary/Artifact/Knowledge/Audit 统一访问 | 实现：[一致性文档](consistency.md) 的契约映射与 `storage/router.py`、projector、artifacts、vector |
| B3 | 同一 session 多节点并发写一致性 | 实现：lease/fencing/version CAS；`test_redis_state.py`、`test_reliability.py`，真实 SQL 故障待验 |
| B4 | event/state/summary 更新顺序 | 实现：turn 提交、prepared、Memory、异步 summary 版本门禁；平台事件不含完整 Tool 事件，工具另有审计/账本 |
| B5 | Memory 跨节点可见 | 实现：共享后端主库读写，幂等 source ID；缓存/向量最终一致与故障窗口见 [一致性](consistency.md) |
| B6 | Redis→SQL、本地→远端向量迁移 | 设计已补全冻结、导出、checksum、增量、切换、反向同步；MigrationCoordinator 仅协调器，不是自动数据搬迁器 |
| B7 | IM 重复投递幂等 | 实现：Inbox 唯一键、Session 锁内复查、prepared 回复恢复、Outbound 分段账本；`test_outbox.py`、`test_reliability.py` |
| B8 | 强/最终一致、延迟、成本、运维取舍 | 设计：[一致性后端取舍表](consistency.md)，明确 Redis 故障丢写窗口及对象跨库非原子性 |
| B9 | 最小八实体模型 | 实现与设计：[模型与物理映射](data-model.md)，App/Binding 在 revision JSON，真实 DDL 在 migrations |
| C1 | 至少两类 IM | 企业微信与飞书已有 Adapter 和相关测试。[接入方案](im-channels.md) 说明认证、回复及部署差异 |
| C2 | IM→框架输入、Agent Event→回复 | 实现：InboundMessage→Content/Part，TRPCAgentExecutor 仅最终回答→OutboundMessage 分段；token 流与通用卡片转换是扩展 |
| C3 | webhook/token/secret/验签/去重/身份绑定 | 实现：[IM 接入](im-channels.md)、`web/app.py`、各 Adapter；secret 解析与开发示例不同，生产须 SecretRef |
| C4 | 单群聊 session_id、跨群/租户隔离 | 实现及明确限制：[IM 身份边界](im-channels.md)；Session 跨群隔离，但同租户同用户 Memory 共享；敏感群需拆 tenant |
| C5 | 长度/频率/异步/媒体/撤回/重试 | 实现：分段、限流、快速入队 ACK、媒体、失败账本；飞书撤回已实现，通用撤回/完整 token 流仅设计 |
| D1 | Filter 白名单、脱敏、预算、确认、用户权限 | 实现：TenantPolicyFilter、GovernedToolRuntime、确认/预算账本；为平台 Filter，非直接复用框架 Filter 生命周期 |
| D2 | 请求/模型/工具/投递/错误/token/成本/存储指标 | 实现：`metrics/prometheus.py`；token 和 cost 按文本长度及配置价格估算，不能当 provider 账单，压测待执行 |
| D3 | callback→Runner→Tool→Session/Memory→回复 trace | 修复队列 trace 丢失，补读 span；`test_telemetry.py`；跨进程关联 trace ID，尚不传完整父 span |
| D4 | 审计至少 11 字段 | 模型/SQL 包含 tenant/channel/user/session/agent/tool/decision/latency/error/cost/trace；turn 有默认值，工具审计部分字段使用数据库默认，非全部实测 |
| D5 | IM/API/DB 密钥不进日志/trace/错误 | 修复递归属性/DSN 脱敏及自建 span 自动异常记录；第三方 SDK/FastAPI exporter 仍须 secret scan，不能保证任意第三方异常都安全 |
| E1 | 节点、IM、DB、模型、工具失败降级 | 实现及设计：[故障表](operations.md)、DLQ/Outbox/fallback/确认；纠正为 SQL 持久化失败不得 ACK，熔断自动控制尚非完整实现 |
| E2 | 灰度发布与租户回滚 | 实现 revision/CAS/回滚，补运维分批租户发布步骤；HMAC 百分比调度/阈值自动回滚未集成 |
| E3 | 并发 Session/token/QPS/峰值容量 | 设计：[容量公式和示例](operations.md)；示例不是测量数据，需目标环境压测 |
| E4 | 最小与生产部署 | 实现：CLI demo、Compose、Kustomize Gateway/Worker/Admin/Migration、HPA/PDB/NetworkPolicy；HA 与备份由运维配置 |

路径未带目录前缀的源码位于 `trpc_service/`；测试位于 `tests/`。

## 2. 六项题目难点

| 难点 | 应对与检查结果 |
|---|---|
| 租户不只是 tenant_id | 配置版本、binding、工具确认、预算、RLS、对象前缀、SecretRef、脱敏共同隔离；同租户群内/跨 App 不是独立保密域 |
| 无状态 Worker 与共享上下文 | 平台 SQL/Redis + 框架 Redis 双状态层；共享 Memory；lease/fencing 接管；InMemory 和 Bot 长连接限制已写明 |
| IM 非普通 chat API | 验签、XML/JSON、身份、快速 ACK、分段、媒体、去重、ambiguous；明确乱序按锁获取顺序，不承诺发送顺序 |
| 后端一致性不同 | SQL 权威事务、Redis 原子状态与持久化风险、向量版本投影、对象 checksum/metadata；独立迁移策略 |
| 模型/Tool/MCP/Knowledge/沙箱跨组件观测 | 已覆盖 Runner、受治理工具及状态/回复 span；Knowledge/外部系统可沿当前上下文扩展；沙箱只有 workspace 约定，没有执行服务 |
| 灰度/回滚/限流/成本/合规 | 已有配置回滚、限流、预算、审计导出和保留策略；自动灰度、精准账单、合规认证仍需扩展或外部验收 |

## 3. 八项交付物

| 交付物 | 位置与状态 |
|---|---|
| 架构设计文档（建议 2000–4000 字） | [architecture.md](architecture.md) 为主文，详细策略分拆至专题文档；字数为建议，非硬性门禁 |
| 系统架构图 | [架构第 2 节](architecture.md#2-总体架构与组件职责) 展示 IM、Gateway、Worker、Filter、Storage 与 Telemetry |
| 企业微信完整时序图 | [架构第 9 节](architecture.md#9-完整消息时序与-trace-关联) 展示持久化受理、Tool/MCP、状态提交和 trace 传递 |
| 数据模型 | [data-model.md](data-model.md) + 实际 SQL migration |
| 同步和幂等 | [consistency.md](consistency.md) |
| 多后端适配方案 | consistency 的契约、权威分层、取舍和迁移步骤 |
| 至少 8 项风险 | [risks.md](risks.md) 共 16 项，缓解措施不等于均已自动实施 |
| GitHub 实现代码 | 当前仓库包含实现、测试、构建/启动脚本，git origin 为 `sddzhyc/trpc-agent-service`；本次仅本地修改，未推送或验证远端同步 |

## 4. 验证与剩余门禁

2026-09-11 的代码检查通过 `coverage.sh` 执行测试与覆盖率统计，通过 `lint_flake8.sh` 执行 Ruff 检查，并执行 `uv run python -m compileall -q trpc_service tests`。这些验证不访问真实 IM、模型、SQL 或 Redis，不替代集成测试。

该轮结果为 82 项测试通过，总体语句覆盖率为 67%，出现 2 条第三方依赖弃用警告。Ruff、compileall 和 `git diff --check` 均通过。上述结果对应本地代码检查，不代表真实 IM、模型、SQL、Redis 或多节点生产环境已经完成验收。

进入生产必须完成 [生产运行手册第 6 节](production-runbook.md) 的 RLS/三账号、重放、Worker 故障、Redis 丢通知、ambiguous、灰度回滚及容量测试，同时校准模型 token/cost、验证密钥轮换、备份恢复与日志扫描。未获得证据前，结论为“原题设计交付可验收，核心代码本地可验证，生产运行能力有明确边界”，不得写作“全量生产功能全部完成”。

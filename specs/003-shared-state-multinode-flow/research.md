# Phase 0 Research: 共享状态多节点消息闭环

**Feature**: 003-shared-state-multinode-flow
**Date**: 2026-09-07
**Status**: Complete — all planning questions resolved

## Research Basis

研究依据为项目宪法、第三阶段 spec.md、clarification-decisions.md、第二阶段实现与
契约测试、当前 uv.lock，以及本地安装的 trpc-agent-py 1.1.19 源码。外部能力只参考
官方 Redis、PostgreSQL/Docker、SQLite、SQLAlchemy 与 PyPI 项目资料。

## Decision 1: Shared Runtime Topology

- **Decision**: 使用两个独立 Uvicorn Worker 进程，各自创建 Gateway、AgentExecutor、
  Runner cache 和连接池；两个进程共同连接 Redis 与 PostgreSQL。测试入口在 8001/8002
  之间显式交替或随机选择，不引入 sticky session。
- **Rationale**: 两个进程不能共享 Python 对象，可直接证明正确性来自共享后端。
  显式端口路由比引入负载均衡器更容易进行故障注入，且不扩大阶段范围。
- **Alternatives considered**:
  - 同进程两个 Worker 对象：仍可能隐式共享 dict/lock，不能作为多节点证据。
  - 单 Uvicorn 多 worker 参数：进程选择不可控，难以做确定性节点中断测试。
  - Kubernetes/生产负载均衡：超出本阶段范围。

## Decision 2: Redis for Fencing-Critical Shared State

- **Decision**: Redis 7.4.11 保存幂等记录、message/session lease、generation counter、
  Session metadata 与 Session Event。原子状态转换使用短小、版本化 Lua scripts，
  由 SCRIPT LOAD/EVALSHA 调用；租约有效性基于 Redis key 与 PTTL，不基于 Worker 时间。
- **Rationale**: Redis 官方文档保证脚本原子执行，适合把 compare-generation、
  compare-token、状态检查与写入组合成一个线性化操作；脚本必须足够短以避免阻塞服务。
  Redis transactions 也提供原子执行与 WATCH/CAS，但对本功能的多条件转换，脚本更直接。
- **Alternatives considered**:
  - SET NX 锁 + 客户端 read/check/write：验证与写入之间存在竞争窗口。
  - 仅 WATCH/MULTI：可以实现，但重试与多状态返回更复杂。
  - Worker 本机时间：无法抵抗时钟偏差、暂停和网络分区。
  - Redis Functions：具备原子性与服务端持久化，但增加本地部署初始化；本切片用
    EVALSHA 并处理 NOSCRIPT 重载更简单。
- **Official references**:
  - [Redis Lua atomic execution](https://redis.io/docs/latest/develop/programmability/eval-intro/)
  - [Redis transactions and WATCH](https://redis.io/docs/latest/develop/using-commands/transactions/)
  - [Redis 7.4.11 official image](https://hub.docker.com/layers/library/redis/7.4-alpine/)

## Decision 3: Session Adapter at the Official SDK Boundary

- **Decision**: 实现 FencedRedisSessionService，遵守 trpc_agent_sdk.sessions
  BaseSessionService 的公开 async 契约，并直接使用官方 Session/Event 类型。Gateway
  获得 SessionFence 后由 Worker 在执行上下文中绑定，create_session、append_event、
  update_session 的 Redis 写入在同一原子操作内验证 session generation。
- **Rationale**: 本地固定版 SDK 确认公开导出 BaseSessionService、RedisSessionService、
  SqlSessionService 和 Session。官方 RedisSessionService 可以共享 Session，但其公开
  写方法不携带平台 generation，无法原子拒绝旧 owner。平台 Adapter 保留 Runner 的
  官方接口，同时实现第三阶段特有的 fencing。
- **Alternatives considered**:
  - 直接使用 InMemorySessionService：跨进程不可见。
  - 直接使用官方 RedisSessionService：缺少平台 fencing 条件，旧 owner 仍可追加事件。
  - 修改 SDK RedisSessionService：违反 framework-first 和升级边界。
  - 在写前单独检查 lease：check 与 write 之间仍有 TOCTOU 窗口。

## Decision 4: PostgreSQL for Persistent Configuration, Audit and Recovery

- **Decision**: 使用 PostgreSQL 17.11、SQLAlchemy 2.0.52 AsyncEngine 和 asyncpg 0.31.0。
  Tenant/Agent/Binding 是授权权威；Audit 与 Recovery Marker 同事务持久化。
- **Rationale**: PostgreSQL 支持跨进程事务、约束和行级并发，适合持久元数据与追加
  审计。asyncpg 0.31.0 明确支持 Python 3.12 和 PostgreSQL 9.5–18。选择独立数据库
  服务也避免把“两个 Worker”限制在同一主机文件系统。
- **Alternatives considered**:
  - SQLite WAL：适合轻量本地持久化，但官方文档明确 WAL 要求进程位于同一主机，
    且同一时刻只有一个 writer；不利于展示可替换的多节点边界。
  - MySQL：SDK 已带相关驱动，但当前项目没有既有 MySQL 约束；PostgreSQL 的本地
    Compose 与 async 驱动组合更直接。
  - 把配置/Audit 全放 Redis：不符合长期持久状态与短期协调状态的职责分离。
- **Official references**:
  - [PostgreSQL 17.11 official image tags](https://hub.docker.com/_/postgres/tags?name=17.)
  - [asyncpg 0.31.0 and compatibility](https://pypi.org/project/asyncpg/)
  - [SQLite WAL concurrency limits](https://sqlite.org/wal.html)
  - [SQLAlchemy SQLite notes](https://docs.sqlalchemy.org/en/20/dialects/sqlite.html)

## Decision 5: Async Repository Ports

- **Decision**: 将所有可能访问 Redis/PostgreSQL 的 Repository/Adapter 方法升级为
  async，Gateway 继续保留 async handle_verified_message 外部形态。InMemory 实现也
  实现相同 async Protocol，契约结果和错误类别保持不变。
- **Rationale**: 在 ASGI 事件循环中执行同步网络 I/O 会阻塞其他 Session，违反
  “不同会话可并行”的目标。端口升级属于内部兼容扩展，不改变 HTTP 契约。
- **Alternatives considered**:
  - 同步 redis/sql 客户端：实现较少，但会阻塞事件循环。
  - 每次调用 asyncio.to_thread：可以兼容同步库，但连接生命周期、取消和测试更复杂。
  - 新建完全不同的 shared Gateway：会复制编排并让 002/003 行为漂移。

## Decision 6: Two Independent Ownership Scopes

- **Decision**: message claim 与 Session lease 分开建模。message generation 保证同一
  external_message_id 最多一个执行者；session generation 保证同一 tenant-scoped
  Session 同时只有一条消息修改上下文。任何 Session/Event 写入需同时匹配当前
  SessionFence，终态写入还需匹配 MessageFence。
- **Rationale**: 只按消息加锁不能串行同 Session 的不同消息；只按 Session 加锁不能
  阻止同消息在不同 Session 计算错误或重复执行。两个作用域必须分别可审计。
- **Alternatives considered**:
  - 一个全局锁：破坏不同 Session 并行。
  - 仅 Session 锁：缺少 external_message_id 幂等。
  - 把 external_message_id 当 Session 顺序号：外部通道不保证连续或可信序号。

## Decision 7: Lease Timing and Renewal

- **Decision**: 默认 lease 10 秒，heartbeat 每 3 秒，Session acquire 最多等待 2 秒，
  Agent timeout 继续为 30 秒。测试通过注入使用 100–500ms lease。续期脚本只在
  token/generation 匹配且 PTTL > 0 时成功；过期后旧 generation 永不复活。
- **Rationale**: heartbeat 间隔小于 TTL 的三分之一，允许一次短暂抖动；10 秒又足够
  快地完成本地节点故障演示。测试使用虚拟/短周期避免实际等待。
- **Alternatives considered**:
  - 不设 TTL：节点退出会永久锁住 Session。
  - 到期宽限期：产生两个 owner 都认为合法的模糊区间。
  - 自动无限等待 Session：占用 HTTP 连接且难以控制演示时间。

## Decision 8: Durable Execution-Start Boundary

- **Decision**: Agent prepare 完成后，Gateway 原子验证 MessageFence + SessionFence，
  将消息推进为 EXECUTION_STARTED 并写 execution_trace_id；只有确认成功才调用
  PreparedAgentRun.execute。heartbeat 或进程存活不能替代此记录。
- **Rationale**: 共享状态中的阶段是其他节点唯一可验证的接管证据。执行开始后未知
  结果宁可禁止重放，也不能重复 Agent 或未来 Tool 副作用。
- **Alternatives considered**:
  - 调用 execute 后再标记：崩溃窗口可导致重复执行。
  - 收到首个 Event 后标记：Runner 可能已经写 Session。
  - 节点失联即视为未开始：存活信息不证明副作用未发生。

## Decision 9: Fencing Failure and Diagnostic Audit

- **Decision**: 旧 generation 的业务 Session/Event/terminal/business-audit 写入返回
  StaleFence。平台以当前有效身份追加独立 diagnostic audit，关联 rejected/current
  generation、node 与 trace；该记录不能改变业务状态。
- **Rationale**: 完全忽略迟到写会丢失安全证据，允许旧 owner 写普通 Audit 又可能污染
  终态。业务授权与诊断记录必须分离。
- **Alternatives considered**:
  - 旧写静默丢弃：不可诊断。
  - 允许旧 owner 写普通 audit：可能形成错误业务事实。
  - 因诊断 Audit 失败而放行旧写：直接破坏 fencing。

## Decision 10: Cross-Backend Finalization and Recovery

- **Decision**: 完成 Agent 后，PostgreSQL 单事务写 final Audit 与
  TERMINAL_PENDING Recovery Marker，保存脱敏 ExecutionResult 或安全引用；随后 Redis
  terminal CAS。CAS 成功后 marker 变为 RECONCILED。CAS 失败/超时返回 outcome_unknown。
  Reconciler 只允许按 tenant/message/execution trace/generation 条件复制原结果。
- **Rationale**: 两后端没有分布式事务，不能把部分提交伪装成功。先留下持久恢复证据，
  可以在不重新调用 Agent 的前提下补齐 Redis 状态。
- **Alternatives considered**:
  - 审计成功即返回成功：Redis 幂等终态尚不可靠。
  - Redis 先终态再尽力审计：可能产生无持久审计的成功。
  - 自动重新运行 Agent：会重复副作用。
  - 回滚 Audit：跨系统回滚不可靠且会销毁证据。

## Decision 11: Configuration Cache Is Not an Authority

- **Decision**: PostgreSQL 无法验证配置版本、状态或所有权时返回
  configuration_unavailable，正向缓存不能授权。缓存只在权威读取成功时加速同一版本
  解析、辅助诊断或继续拒绝。
- **Rationale**: TTL 不能证明期间没有 Tenant 停用、Binding 撤销、密钥轮换或所有权
  变更。该阶段选择安全优先的 fail closed。
- **Alternatives considered**:
  - 短 TTL 正向授权：仍存在撤销延迟。
  - stale-while-revalidate：后端长故障会扩大授权窗口。
  - 仅旧 Session 放行：权限强度不应取决于是否已有 Session。

## Decision 12: Migration and Schema Gate

- **Decision**: 使用仓库内顺序 SQL migration 和 schema_migrations 表。显式
  shared-init 命令建表/seed；Worker 启动只验证当前支持版本，不自动迁移。数据库版本
  高于代码支持时 fail closed。
- **Rationale**: 显式初始化更适合答辩演示，也避免两个 Worker 同时启动时竞争迁移。
- **Alternatives considered**:
  - 启动自动 create_all：隐藏 schema 变化，无法演示版本门禁。
  - 本阶段引入 Alembic：能力完整但增加额外依赖与任务量。
  - 忽略未知列/版本继续运行：可能错误解释权限数据。

## Decision 13: Contract and Fault-Test Strategy

- **Decision**: 将 002 的核心端口断言抽成参数化 contract suite，使用 InMemory fixture
  与真实 shared fixture 运行同一测试；vendor 专属测试只验证 Adapter 设置与故障注入。
  双进程 E2E 通过真实 loopback HTTP，故障点包括 kill Worker、暂停续期、Redis/SQL
  stop/start 和响应丢失。
- **Rationale**: 共用业务断言证明可替换性；真实进程/后端测试证明不是共享 Python
  内存造成的假多节点。vendor 测试与业务契约分离避免上层依赖实现表示。
- **Alternatives considered**:
  - 只用 fakeredis/mock SQL：不能证明跨进程。
  - 所有测试都要求 Docker：会破坏 002 快速回归。
  - 只做 E2E：难以覆盖每个状态转换和定位失败。

## Resolved Planning Questions

| Topic | Resolution |
|---|---|
| Shared short-term backend | Redis 7.4.11 |
| Persistent metadata backend | PostgreSQL 17.11 |
| Python storage clients | redis-py 8.1.0, SQLAlchemy 2.0.52, asyncpg 0.31.0 |
| Worker topology | Two independent Uvicorn processes |
| Session SDK integration | Official BaseSessionService boundary with platform fenced Redis adapter |
| Atomicity | Versioned short Redis Lua scripts |
| Lease authority | Redis PTTL/token/generation |
| Safe takeover | Only explicit pre-EXECUTION_STARTED shared state |
| Stale write | Reject business write; append separate diagnostic audit |
| Partial commit | SQL terminal_pending marker then Redis CAS; reconcile without execution |
| Config outage | Fail closed; no positive cache authorization |
| SQL schema | Explicit versioned migrations; startup version gate |
| Test boundary | Parameterized contracts + real Docker backends + two-process E2E |

No unresolved research item remains for Phase 1 design.

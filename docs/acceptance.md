# 验收标准—证据追踪矩阵

本文把题目中的“必须说明”转成可以由评审直接打开文件、运行测试或观察数据库约束来核验的证据。状态含义：**完成**表示仓库存在可执行代码与测试；**设计完成**表示方案和扩展合同完整，但没有伪装成已完成的第三方生产联调。

## 1. 官方七项验收标准

| # | 验收要求 | 状态 | 主要设计证据 | 代码与测试证据 |
|---:|---|---|---|---|
| 1 | 覆盖多租户、节点化、同步、多后端、IM、治理监控、故障恢复 | 完成 | `architecture.md`、`reliability.md`、`operations.md` | `tenant/`、`reliability/`、`backends/`、`channels/`、`runtime/`；CI quality + postgres-contract |
| 2 | tenant、agent、channel、session、event、memory、summary、audit 关系 | 完成 | `data-model.md` ER 图与表字典 | `storage/models.py`、0001–0006 Alembic；`tests/reliability`、`tests/projection` |
| 3 | 至少两种 IM，其中包含微信或企业微信 | 完成 | `channels.md` 差异表与企微时序 | `channels/wecom.py`、`channels/telegram.py`、静态密码向量、HTTP T0 集成测试 |
| 4 | 至少三类后端及同步策略 | 完成/分层 | `data-model.md` SQL/Redis/向量库/对象存储取舍 | SQL 权威面、Redis/InMemory Session 投影和 shadow 迁移已实现；向量/S3 为合同与落地方案 |
| 5 | 完整消息链路，trace/request ID 贯穿 | 完成 | `channels.md` 核心时序图 | Gateway 生成/校验 ID，持久到 Inbox/Run/Audit 和 SDK custom data；全链 parent span 仍列为后续项 |
| 6 | 至少 8 个生产风险与缓解 | 完成 | `security.md` 共 15 项风险、已有控制与上线剩余工作 | 日志脱敏、RLS、防重放、fence、UNKNOWN、输入限制均有测试 |
| 7 | 区分 SDK 复用与平台新增 | 完成 | `architecture.md` 专表、`oss-evaluation.md` | `agent/compat.py` 固定 1.1.19 公共接口；可靠性和租户层均为平台实现 |

## 2. 交付物核对

| 题目交付物 | 文件位置 | 可核验点 |
|---|---|---|
| 架构设计文档 | `docs/architecture.md` | 系统图、组件边界、无 sticky session、SDK 复用边界 |
| 系统架构图 | `README.md`、`docs/architecture.md` | GitHub 可渲染 Mermaid |
| 企业微信核心时序图 | `docs/channels.md` | 验签、T0、Runner、Tool、Session/Memory、T2、回复与 trace ID |
| 数据模型 | `docs/data-model.md`、`storage/models.py` | 关系图、约束、水位、RLS、Alembic |
| 同步和幂等策略 | `docs/reliability.md` | T0/T1/T2、幂等键、fence/OCC、UNKNOWN |
| 多后端适配方案 | `docs/data-model.md`、`backends/` | 合同、能力声明、双读/shadow/cutover/rollback |
| 风险清单 | `docs/security.md` | 15 项风险，不只写口号，区分已有控制与剩余工程 |
| GitHub 代码实现 | 本仓库 | 四角色 CLI、可视化控制台、Compose/K8s、迁移和自动化质量门禁 |

## 3. 高标准自验收

### 3.1 正确性与并发

- 同一 IM delivery key 重投只产生一个 Inbox；相同 key 异内容拒绝。
- 同 session 由 head-of-line、租约、fencing token 和 OCC 四层保证顺序；不同 session 可被多节点并行领取。
- partial SDK event 不持久，完整事件先加密对象化再 staged；只有成功 T2 的 attempt 对下轮回放可见。
- T2 同事务提交 state、event、Outbox、Audit 和 ProjectionJob；失败不产生半发布状态。
- ProjectionJob 具备租约、心跳、fence、退避、死信与单调水位；较旧任务晚完成不会回退 Summary。
- Tool/IM 结果无法确定时进入 `unknown`，不将 at-least-once 包装成虚假的 exactly-once。

### 3.2 隔离与安全

- 外部 payload 不直接决定 tenant；public callback ID 只用于路由，真实 binding 在租户 RLS 事务内加载。
- PostgreSQL 表启用 FORCE RLS；CI 使用非 superuser runtime role，避免 superuser 绕过后仍声称验证成功。
- SDK event ciphertext 按 tenant/session/event/seq 作为 AES-GCM AAD，`event_object` 表按 tenant 主键和 RLS 隔离且 PostgreSQL 禁止 UPDATE/DELETE。
- IM token、AES key、model key 和 response URL 不写普通业务字段；短期回复坐标 envelope encryption 后持久化。
- 日志和 trace 出口采取 allowlist/递归脱敏；Prometheus label 不放 user/session/request/trace 等高基数值。
- Agent 输入在进入 Session/模型前脱敏，输出事件在持久化/回复前再次脱敏；通道 principal/scope ACL 在 Worker 入口强制执行。
- 生产配置拒绝 SQLite、HTTP public URL、默认 root/admin secret；Worker/Projector 拒绝非 SQL 权威 event store。

### 3.3 可运维性

- migration owner 与 runtime DB role 分离；迁移执行 upgrade → downgrade → upgrade → drift check。
- Gateway、Worker、Dispatcher、Projector 可独立运行、扩缩和优雅停机。
- Compose 提供最小联调环境；Kustomize 提供 ServiceAccount、PDB、HPA、NetworkPolicy 与 Secret 边界。
- `/console` 提供租户运行快照、不可变 revision、发布/回滚和内容无关审计视图；演示种子默认禁用所有通道。
- 配置采用不可变 revision；旧消息固定接收时 revision；回滚不会改写历史版本。
- 容量方案以 `lambda × latency`、SQL QPS、token、事件字节和租户公平性建模，不承诺未经压测的吞吐数字。

## 4. 诚实边界与后续工程

| 能力 | 当前边界 | 要达到真实生产上线还需 |
|---|---|---|
| IM 实网 | 协议、密码向量、HTTP 合同与投递分类已测 | 企业微信测试机器人与 Telegram test bot 的真实限流、超时、撤回、媒体联调 |
| Tool/MCP | 白名单 ToolSet 和 ToolEffect 账本已测 | 将实际业务 Tool 逐个标注 effect class，并接审批/对账执行器 |
| 治理 Filter | 确定性敏感字段脱敏、输入/输出词项策略、长度上限、IM principal/scope ACL 已接入并测试 | 语义 DLP、参数级授权、硬预算扣费和危险动作人工确认 |
| Memory/Summary | 常驻 Projector、确定性摘要、显式记忆、租户/用户隔离的 SDK Memory 查询已实现 | 若使用 LLM/向量召回，需版本化 prompt、离线评估、隐私同意和回归集 |
| 向量/对象后端 | 数据模型、路由与迁移协议已设计 | Qdrant/Milvus/pgvector 和 S3/MinIO 的具体驱动、压测与故障注入 |
| Observability | FastAPI 自动 span、ID 贯穿、OTLP 清洗；入站/Agent/token/Memory/租约/投递指标已接线 | Worker/Tool/Storage/Dispatcher 完整 parent context、模型/Tool 耗时与成本账本 |
| 密钥与 Admin | allowlisted env resolver、加密、静态 Admin key | KMS/Vault/External Secrets、轮换版本、OIDC/mTLS/RBAC |
| Kubernetes | 可审阅的 Kustomize 起点 | 目标集群的镜像 digest、托管后端、FQDN egress、队列指标 HPA 和 server dry-run |

## 5. 当前验证记录

本分支当前 Windows/Python 3.12 验证记录：

```text
ruff format --check: passed
ruff check: passed
mypy: 72 source files, no issues
pytest: 281 passed, 5 skipped
coverage: 86.05% (threshold 85%)
Alembic: upgrade -> downgrade base -> upgrade -> check, passed through revision 0006
wheel: console assets, demo seed, memory adapter and migrations included
docker compose config: passed
kubectl kustomize deploy/k8s/base: passed
```

5 项跳过全部依赖真实 PostgreSQL 的 FORCE RLS、append-only、`SKIP LOCKED` 与 fencing 合同。本机结果不能代替它们；`.github/workflows/ci.yml` 的 `postgres-contract` job 会创建非 owner、非 superuser 且无 `BYPASSRLS` 的 runtime role 并执行。提交后的 GitHub Actions 结论优先于本记录。

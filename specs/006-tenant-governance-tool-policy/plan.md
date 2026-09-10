# Implementation Plan: 租户治理与工具安全策略

**Git Branch**: `feature/luwenjie` | **Feature**: `006-tenant-governance-tool-policy` | **Date**: 2026-09-10 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/006-tenant-governance-tool-policy/spec.md`

**Decision Record**: [clarification-decisions.md](./clarification-decisions.md)

## Summary

在第五阶段双 IM、多租户和多节点共享状态主链路上增加强制治理层：可信渠道主体授权、版本化租户策略、入站/出站敏感信息处理、工具白名单、危险操作确认、严格预算预占及治理审计。Gateway 在 Runner 之前执行身份、内容和预算准入；官方 tRPC-Agent 的 Agent/Model/Tool Filter 与 callback 边界负责 Runner 内治理，尤其在工具真正执行前重新读取当前策略版本。所有恢复事实均可被其他节点访问，Worker 不保存唯一业务状态。

ACA 三项决定形成统一安全边界：策略收紧在事务提交后对新判断和未执行工具立即生效；文本编号与 IM 按钮消费同一份确认状态；预算必须完整预占租户配置的单次最大额度，实际结算后才释放差额。

## Implementation Baseline

- 第二阶段：可信 Channel Binding、tenant-scoped Session、幂等、Gateway、Worker、统一回复与 Audit/Metrics。
- 第三阶段：双 Worker、共享 Session、消息/Session 租约、generation、fencing、Redis/PostgreSQL、部分提交恢复。
- 第五阶段：飞书与企业微信 Adapter、可信渠道主体字段、回复交付状态及 Adapter 主动/备用接管。
- 当前 Runner 使用固定依赖 `trpc-agent-py==1.1.19` 与确定性离线模型；本阶段继续不接真实模型或高危外部工具。
- 现有 `AuthoritativeConfigCache` 的正缓存只是提示，每次授权仍调用权威读取，适合作为策略立即失效的基线。

## Technical Context

**Language/Version**: Python 3.12（项目约束 >=3.12,<3.13）

**Primary Dependencies**: trpc-agent-py==1.1.19、Pydantic 2.13.5、SQLAlchemy 2.0.52、asyncpg 0.31.0、redis 8.1.0、既有飞书与企业微信 SDK

**Storage**: PostgreSQL 保存策略版本、主体授权、预算账户/预占/结算、审计与恢复事实；Redis 保存短期待确认状态及原子一次性消费；InMemory 实现仅用于契约和单元测试

**Testing**: pytest、pytest-asyncio、Repository 契约测试、SDK/Runner/Tool 测试替身、Redis/PostgreSQL 双节点集成测试、安全扫描

**Target Platform**: Windows/Linux 本地开发环境；共享后端继续使用既有 Docker Compose profile

**Project Type**: Python 异步服务，由 Channel Adapter、Gateway、Governance、Worker 与 Storage Adapter 组成

**Performance Goals**: 确定性本地环境下，不含 Runner 与 IM 网络耗时的治理准入 p95 小于 100ms；20 个跨节点并发预算请求不超额；10 个重复确认最多执行一次工具

**Constraints**: 默认拒绝；策略收紧立即生效；预算严格预占；无 sticky session；不记录 Secret/原始敏感值；按钮仅用于危险确认；不依赖真实模型、真实危险工具或真实凭证

**Scale/Scope**: 至少 2 个租户、2 个 Worker、飞书与企业微信两种渠道、4 类预算维度、文本与按钮两种确认入口、6 个用户故事

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Pre-Research Gate

| Principle | Result | Evidence |
|-----------|--------|----------|
| I. Framework-First | PASS | 复用官方 Runner、AgentContext、Agent/Model/Tool Filter 和 callback；平台只实现租户治理编排与数据边界 |
| II. Tenant Isolation | PASS | 策略、授权、确认、预算、审计及键均带可信 tenant scope，并包含双租户拒绝测试 |
| III. Stateless Workers | PASS | Worker 只持有请求内上下文；策略、确认、预算与恢复状态均在共享后端 |
| IV. Contract-First | PASS | 新 Repository/Filter/确认契约具有 InMemory 与共享实现的一致行为测试 |
| V. Security by Default | PASS | 未知策略、用户、工具、预算和治理依赖失败全部默认拒绝 |
| VI. Observability | PASS | 决策、工具、预算、确认和错误贯穿现有 trace 并进入审计/低基数指标 |
| VII. Spec-Driven Evidence | PASS | spec、ACA 决策、research、data model、contracts、quickstart 和测试先行任务可追踪 |

**Gate result**: PASS。无宪法例外，无未解决澄清，可以进入研究与设计。

## Framework Reuse and Platform Ownership

### 直接复用官方 tRPC-Agent

- 使用官方 `Runner`、`LlmAgent`、Event/Content/Part 与 Session 边界，不复制 Runner 循环或工具调度器。
- 通过每次调用独立的 `AgentContext.metadata` 传递不可变 `GovernanceContext`，不把租户状态保存在 Worker 单例中。
- 使用官方 model/tool callback（底层由 SDK Filter 机制执行）完成模型前后与工具前后的治理；工具执行前 callback 是最终授权门禁。
- 保留确定性模型与工具替身，使本阶段无需真实模型 API 或真实副作用仍能验证治理语义。

### 复用现有平台能力

- Channel Adapter 与可信 Channel Binding 继续确定 `tenant_id`、Agent、渠道主体和 Session 边界。
- Gateway 继续负责消息幂等、Session 串行、trace、统一回复和审计编排。
- Worker、共享 Session、Redis/PostgreSQL、租约、generation、fencing、恢复标记与交付状态沿用第三、第五阶段实现。
- 现有 `AuthoritativeConfigCache` 只能作为解析/定位提示，不能作为授权依据。

### 本阶段新增的平台能力

- `governance` 包：策略解析、主体授权、内容检查、预算编排、危险操作确认、稳定错误语义。
- `tool` 包：稳定工具描述、确定性工具替身，以及接入官方 tool callback 的治理适配器。
- PostgreSQL Repository：不可变策略版本、有效版本指针、主体授权、预算账户和预算预占/结算。
- Redis Repository：短期待确认事实、一次性声明和跨节点消费。
- Audit/Metrics/Recovery 扩展：记录策略版本、决定、预算、确认与恢复事实，不记录原始敏感值。

## Architecture

### Topology

```text
Feishu / WeCom
      │
      ▼
Channel Adapter ── trusted binding/principal ──► Gateway
                                                   │
                                                   ▼
                                      Governance Coordinator
                                      ├─ Policy + Grant (PostgreSQL)
                                      ├─ Budget ledger (PostgreSQL)
                                      ├─ Confirmation (Redis)
                                      ├─ Content policy
                                      └─ Audit / Metrics / Recovery
                                                   │ GovernanceContext
                                                   ▼
                                            Agent Worker
                                                   │
                                  official tRPC-Agent Runner
                                                   │
                                    official tool callback/filter
                                                   │
                                      deterministic governed tool
                                                   │
                                                   ▼
                                    outbound content check
                                                   │
                                      UnifiedReply → origin Adapter
```

### Request Sequence

1. Adapter 完成既有真实性校验并转换统一入站消息；Gateway 只信任 Channel Binding 解析出的租户、Agent 和主体。
2. Gateway 获取消息幂等所有权和 tenant-scoped Session 租约，再启动治理准入。
3. Governance Coordinator 权威读取当前策略与主体授权，执行入站内容检查；任一事实未知即默认拒绝。
4. Coordinator 在 PostgreSQL 单事务中为请求、Token、工具次数与成本单位完整预占本次最大额度；任一维度不足则全部不占用。
5. Coordinator 生成请求内不可变 `GovernanceContext`，通过 `AgentContext.metadata` 传入现有 Worker 与官方 Runner。
6. Runner 选择工具后，官方 tool callback 再次权威读取有效策略，校验工具白名单、参数内容、主体授权和当前策略版本。
7. 危险工具无有效确认时，先确认本次最大额度已完成预占，再只创建一份绑定该 reservation 的 `PendingConfirmation` 并返回确认提示；文本编号和按钮均转换为统一 `ConfirmationIntent`（FR-017a；DEC-004）。
8. 工具或 Agent 完成后，以 execution_id 幂等结算实际用量、释放差额，并写入审计和恢复事实；已开始执行的结果不得因恢复而再次运行。
9. 出站内容检查通过后生成向后兼容的统一回复，由原 Channel Adapter 发送；发送失败复用既有 Delivery Recovery，不重复 Agent、工具或预算结算。
10. 重复消息、重复确认、等待者与接管节点只读取共享结果或稳定冲突状态，不创建第二份执行事实。

### Governance Context

`GovernanceContext` 至少包含可信 tenant、Agent、binding、渠道主体摘要、Session、message/execution 标识、`trace_id`/`owner_trace_id`/`execution_trace_id`、准入策略版本、预算 reservation_id 与 fencing generation。它只在单次调用内传递，不包含 Secret、原始敏感值或可扩大权限的客户端字段。

## ACA Decision Mapping

| Decision | Architecture | State/Data | Error & Recovery | Required Evidence |
|----------|--------------|------------|------------------|-------------------|
| DEC-001 / A | Gateway 准入与 tool callback 双重权威检查 | 不可变策略版本 + 原子 active 指针；旧缓存无授权效力 | `policy_missing`、`policy_disabled`、`policy_stale` 均在副作用前拒绝；已完成副作用只审计不回滚 | 收紧后新请求拒绝、已选未执行工具拒绝、双节点旧缓存不能放行 |
| DEC-002 / C | 两种 Adapter 输入统一转换为 `ConfirmationIntent` | 文本编号与按钮消费同一 `PendingConfirmation` | 过期、错主体、参数变化、重复消费稳定拒绝；执行前可接管，执行后不确定不得重放 | 文本→按钮及按钮→文本各 10 次重放，工具总执行次数均为 1 |
| DEC-003 / A | Coordinator 在 Runner 前调用单一预算账本 | 原子全维度 reservation，`RESERVED → SETTLED/RELEASED` | 额度不足/状态未知默认拒绝；结算幂等，执行后中断保留占用待恢复 | 20 个跨节点临界并发不超额、差额释放、重启不重复扣减 |
| DEC-004 / A | Confirmation 创建复用 Coordinator 已完成的预算准入 | PendingConfirmation 与 reservation/execution 绑定，确认 TTL 内保持 `RESERVED` | 过期、取消或执行前失配原子释放；确认成功不重新预占；执行后未知进入 `REVIEW_REQUIRED` | 等待确认 reservation 数量为 1，过期可复用额度，文本/按钮重放不重复预占或结算 |

## Project Structure

### Documentation (this feature)

```text
specs/006-tenant-governance-tool-policy/
├── plan.md              # This file ($speckit-plan command output)
├── research.md          # Phase 0 output ($speckit-plan command)
├── data-model.md        # Phase 1 output ($speckit-plan command)
├── quickstart.md        # Phase 1 output ($speckit-plan command)
├── contracts/           # Phase 1 output ($speckit-plan command)
└── tasks.md             # Phase 2 output ($speckit-tasks command - NOT created by $speckit-plan)
```

### Source Code (repository root)

```text
trpc_service/
├── governance/
│   ├── models.py
│   ├── service.py
│   ├── policy.py
│   ├── principal.py
│   ├── budget.py
│   ├── confirmation.py
│   ├── content.py
│   └── errors.py
├── tool/
│   ├── models.py
│   ├── callbacks.py
│   └── deterministic.py
├── channels/
│   ├── contracts.py
│   ├── feishu.py
│   └── wecom.py
├── gateway/service.py
├── worker/service.py
├── audit/models.py
├── metrics/models.py
├── recovery/reconciler.py
└── storage/
    ├── contracts.py
    ├── inmemory.py
    ├── shared.py
    ├── redis_confirmations.py
    ├── postgres/
    │   ├── models.py
    │   ├── repositories.py
    │   └── migrations/005_governance.sql
    └── redis_scripts/
        └── confirmation_claim.lua

tests/
├── unit/governance/
├── contract/governance/
├── integration/
│   └── governance/
├── e2e/
│   └── governance/
└── security/
```

**Structure Decision**: 保持当前单一 Python 包和既有测试布局。治理编排集中在新 `governance` 包；SDK 工具接入集中在现有空 `tool` 包；共享存储、Gateway、Worker、Channel、Audit、Metrics 与 Recovery 只做契约扩展，不创建第二套消息链路。

## Policy and Authorization Design

- PostgreSQL 保存不可变 `GovernancePolicyVersion`，另以 `(tenant_id, scope_type, scope_id)` 唯一 active 指针原子切换版本。
- 生命周期为 `DRAFT → ACTIVE → SUPERSEDED`，有效版本也可转为 `DISABLED`；事务提交即为 DEC-001 的生效点。
- 新请求准入和工具执行前分别权威读取 active 指针。可缓存已校验策略的解析结果，但必须以本次读取到的版本和状态作为授权条件。
- `ChannelPrincipal` 由可信 tenant、channel、binding 与 provider stable subject 规范化；审计和确认只保存租户内不可逆摘要。
- `PrincipalGrant` 可限定 tenant、Agent 和 binding；多层规则取交集，缺失、禁用或过期均拒绝。
- 授权失败不得创建可继续使用的新业务 Session；已存在 Session 也不得被未授权主体读取或写入。

## Dangerous Operation Confirmation State Machine

```text
PENDING ──valid text/button──► CLAIMED ──fenced start──► EXECUTING ──► COMPLETED
   │                            │                         │
   ├──ttl──► EXPIRED            └──lease before start──► PENDING/reclaim
   └──policy/identity change──► CANCELLED                └──owner lost──► OUTCOME_UNKNOWN
```

- `PendingConfirmation` 使用 tenant、channel、binding、principal、Agent、Session、工具/参数摘要、策略版本、过期时间和 opaque nonce 绑定操作；只保存确认码散列。
- 文本编号和按钮 payload 均先变成相同 `ConfirmationIntent`，再执行同一 Redis 原子 claim；任一入口成功后另一入口不能再次声明。
- claim 后、工具开始前发生节点中断可在租约过期后由新节点接管；已经标记 `EXECUTING` 而结果未知时进入人工/恢复审查，绝不自动重放副作用。
- 确认时必须重新校验当前策略和授权。策略版本失效、操作摘要变化、身份/Session 不匹配或 Redis 不可用均拒绝。

## Budget State Machine

```text
REQUESTED ──atomic full reservation──► RESERVED ──actual usage──► SETTLED
    │                                    │
    └──insufficient/unavailable──► DENIED└──failure before execution──► RELEASED
                                          └──post-start uncertainty──► REVIEW_REQUIRED
```

- PostgreSQL 是预算账户、预占与结算的唯一权威来源，避免 Redis 与 SQL 之间形成不可原子判断的双账本。
- `(tenant_id, execution_id, dimension)` 唯一；一个事务对 request、tool_call、token、cost 四个维度执行条件更新，必须全部完整预占，否则全部回滚。
- `RESERVED` 保存当前策略定义的单次最大量；Runner/Tool 也使用同一上限限制实际执行。完成后按 actual 一次性结算并释放差额。
- Agent/工具开始前失败则释放全部占用；开始后结算失败则保留预占并交由恢复流程收敛，不能通过超时自动释放后重新执行。
- 重复消息、确认、回复重试与恢复以同一 execution_id 查询既有 reservation；结算和释放均为幂等条件转换。

## Error Semantics

| Code | Retryable | Execution started | Meaning / Adapter behavior |
|------|-----------|-------------------|----------------------------|
| `policy_missing` / `policy_disabled` | No | No | 安全提示“当前服务不可用”，不暴露策略细节 |
| `policy_stale` | Yes, re-evaluate | No | 重新读取当前版本，不允许旧判断继续执行 |
| `principal_unauthorized` | No | No | 拒绝访问，不创建业务 Session |
| `content_rejected` | No | No | 返回安全内容提示，不回显命中值 |
| `governance_unavailable` | Yes | No | 依赖不可判定，默认拒绝 |
| `budget_exhausted` | No | No | 告知本周期额度不足 |
| `budget_unavailable` | Yes | No | 不进入 Runner，不按无限额度处理 |
| `confirmation_required` | User action | No | 返回同一 confirmation 的文本编号与最小按钮 |
| `confirmation_invalid` / `expired` / `consumed` | No | No | 不执行工具；提供重新发起操作的安全提示 |
| `tool_denied` | No | No | 工具 callback 阻断，不泄露白名单 |
| `governance_outcome_unknown` | No automatic retry | Maybe | 进入恢复/审查，不重新执行 Agent 或工具 |

统一回复只新增向后兼容的可选 `confirmation` 载荷；Adapter 负责文本与最小按钮的渠道映射，不决定权限或重试语义。

## Cross-Node Recovery and Commit Ordering

1. 获取消息幂等与 Session 所有权。
2. 权威校验策略、主体和入站内容。
3. 原子预算预占；若工具需要确认，在创建 PendingConfirmation 前完成并在确认 TTL 内保持同一 `RESERVED` reservation。
4. 在高风险执行前持久化可审计授权/确认事实；失败则默认拒绝。
5. 以 fencing generation 标记 `execution_started`。
6. 执行一次 Agent/工具，并持久化结果或恢复所需摘要。
7. 以 execution_id 幂等完成预算结算、最终审计与恢复标记。
8. 完成消息/确认幂等结果，最后进入现有回复 Delivery 流程。

接管节点只推进共享状态允许的后继转换。旧 generation 的写入被拒绝；`execution_started=false` 的过期 reservation 可安全释放，已有确定结果的 reservation 只补结算，`execution_started=true` 且无结果证据的记录进入 `REVIEW_REQUIRED`。待确认危险操作确认成功时沿用原 reservation，禁止确认后重新预占；任何路径都不以重新调用 Agent 或危险工具来“猜测”结果。

## Security and Observability

- 入站、工具参数摘要、Agent 输出、统一回复、日志与审计共用租户策略的 redact/reject 规则；检查异常默认拒绝。
- Secret、访问令牌、数据库密码和敏感命中原文禁止进入源码、Git、日志、trace、指标标签或明文数据库字段。
- 每个治理决定记录 tenant、channel、principal digest、Session、Agent、tool、policy version、decision、reason code、latency、error、cost 和三类 trace。
- 指标只使用 channel、decision、error_type、tool_class 等低基数标签，不使用 tenant/user/session/message/trace。
- 危险操作必须先形成审计事实才能执行；审计不可用时返回 `governance_unavailable`。

## Test Strategy

严格测试先行，每个行为先提交可重复的失败证据，再实现到通过：

- **Unit**：策略交集、主体规范化、内容脱敏/拒绝、稳定错误、预算与确认状态机、日志脱敏。
- **Contract**：Policy、Grant、Budget、Confirmation Repository 的 InMemory 与 PostgreSQL/Redis 实现共享同一套契约；验证官方 callback 适配器确实在工具函数前阻断。
- **Integration**：两个 Worker + Redis + PostgreSQL 验证立即策略失效、20 个临界预算并发、10 个文本/按钮混合确认、节点在各提交点中断及 fencing 接管。
- **End-to-end**：飞书/企业微信 SDK 测试替身验证统一入站、最小按钮、原会话回复、双租户 Session/成本隔离；真实客户端仅作为非自动化验收。
- **Regression**：第二、第三、第五阶段测试及全量 pytest 必须继续通过。
- **Security**：扫描源码、Git diff、日志、审计、错误与持久化样本，敏感原文命中必须为 0。

## Migration and Rollback

- 数据库 migration 只新增治理表、索引和现有审计/恢复的可空扩展字段，先部署 schema，再部署默认拒绝的代码，最后显式激活租户策略。
- 未迁移租户没有 active policy，因此按规格拒绝；不得自动生成允许全部的兼容策略。
- 回滚应用前必须先停用治理入口或切回不暴露新工具的旧版本；数据库新增表保留，避免删除审计和预算事实。
- 正在 `RESERVED`、`EXECUTING` 或 `REVIEW_REQUIRED` 的记录不得通过回滚脚本直接删除或释放，应由相同版本恢复器或人工审查收敛。

## Post-Design Constitution Check

| Principle | Result | Design evidence |
|-----------|--------|-----------------|
| Framework-First | PASS | 官方 Runner/AgentContext/callback/Filter 与 Tool 边界保持唯一执行链路 |
| Tenant Isolation | PASS | 所有策略、主体、确认、预算、审计和恢复键均以可信 tenant 开头并做双租户测试 |
| Stateless Workers | PASS | PostgreSQL/Redis 保存所有可接管事实，Worker 仅持请求上下文 |
| Contract-First | PASS | 四类新 Repository 及 callback adapter 均定义共享契约测试 |
| Security by Default | PASS | 缺策略、授权、预算、内容检查、确认或审计均在副作用前拒绝 |
| Observability | PASS | 不可变审计与低基数指标覆盖允许、拒绝、待确认、结算和恢复 |
| Spec-Driven Evidence | PASS | ACA 映射到模型、状态机、错误、恢复和明确并发验收 |

**Post-design gate result**: PASS。无宪法例外；可以进入 `$speckit-tasks`。

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

无需要豁免的复杂度或宪法违反。

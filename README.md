# tRPC-Agent 多租户双 IM 节点化服务平台

基于 **tRPC-Agent-Python** 构建的多租户、可水平扩展、支持飞书与企业微信真实消息接入的 Agent 服务平台。

本项目将官方 Agent Runner 从单机 SDK 示例扩展为完整的平台消息闭环：真实 IM 消息经 Channel Adapter 进入 Gateway，由可信 Channel Binding 确定租户与 Agent，经过跨节点幂等、tenant-scoped Session、治理策略和官方 Runner 执行后返回原会话；Redis、PostgreSQL、OpenTelemetry 和运维状态机共同提供共享状态、故障恢复、审计追踪、灰度发布与容量验收能力。

> **当前状态（2026-09-11）**：八个阶段全部完成；真实 Docker Compose 环境最终回归 **620 passed、0 skipped、0 failed**。两条 warning 均来自飞书 SDK 的弃用提示，不影响功能和验收。

## 1. 项目目标与需求

项目解决的核心问题不是“让一个机器人回复消息”，而是让多个租户、多个 Agent、多个 IM 入口和多个 Worker 在同一平台中安全协作。系统围绕以下要求实现：

- **多租户隔离**：Tenant、Agent、Channel Binding、Session、Memory、Summary、Knowledge、Audit 和治理状态全部显式携带租户作用域。
- **节点化部署**：Worker 不依赖 sticky session；任意健康节点可继续同一会话，节点失效后通过租约和 fencing 安全接管。
- **真实双 IM 接入**：飞书与企业微信使用独立长连接 Adapter，支持文本单聊和群聊中明确 @ 机器人的消息。
- **可信身份绑定**：不信任外部 `tenant_id`，只根据经过认证的渠道、企业/应用和机器人复合身份查询 Channel Binding。
- **消息幂等与顺序**：使用飞书 `message_id` 和企业微信 `msgid` 去重；同一会话串行，不同会话并行。
- **共享数据与恢复**：Redis 承担低延迟协调，PostgreSQL 保存权威业务事实；部分提交恢复不得重新执行 Agent 或 Tool。
- **治理和安全**：租户级主体授权、内容检查、工具白名单、危险操作确认、预算 reservation、密钥隔离和审计 fail-closed。
- **可观测与可运维**：trace、指标、结构化日志、健康矩阵、去重告警、灰度/回滚、容量双门禁和可重复故障演练。

完整架构设计、系统架构图、企业微信核心时序图、数据与一致性策略见 [ARCHITECTURE.md](ARCHITECTURE.md)。

## 2. 已实现的端到端链路

```text
飞书 / 企业微信客户端
        ↓
飞书 / 企业微信开放平台
        ↓
Channel Adapter（协议解析、身份校验、自身消息过滤）
        ↓
Gateway（可信 Channel Binding、租户路由、幂等入口）
        ↓
Agent Worker（tenant-scoped Session、租约、generation/fencing）
        ↓
官方 tRPC-Agent Runner → Filter / Policy / Tool
        ↓
Event / Memory / Summary / Audit 持久化
        ↓
统一回复 → 对应 Channel Adapter → 原 IM 会话
```

`trace_id` 贯穿 Adapter、Gateway、Worker、Runner、Tool、数据访问和回复投递。未知或禁用 Binding、配置不可用、审计不可用、认证失败以及无法证明安全的恢复场景均默认拒绝。

## 3. 架构组成

| 层次 | 主要模块 | 作用 |
|---|---|---|
| 渠道接入 | Feishu Adapter、WeCom Adapter、Local HTTP Adapter | 将不同供应商事件转换为统一入站消息，并将统一回复送回原会话 |
| 入口与路由 | Gateway、Channel Binding | 可信身份映射、租户/Agent 路由、稳定错误和 trace 关联 |
| 执行层 | Agent Worker、官方 tRPC-Agent Runner | 多轮会话、Agent/Tool 执行、同会话串行和跨节点接管 |
| 治理层 | Policy、Authorization、Confirmation、Budget、Filter | 入站/出站检查、危险操作确认、工具权限和成本控制 |
| 数据层 | Repository/UoW、Redis、PostgreSQL、Vector/Object 边界 | 幂等、租约、Event、Memory、Summary、Audit、迁移与恢复 |
| 可观测层 | OpenTelemetry、Metrics、Health、Alerts | 14 阶段关联、脱敏采样、角色健康矩阵和去重告警 |
| 运维层 | Release、Routing、Gates、Drain、Capacity | 灰度发布、硬门槛自动回滚、质量门槛暂停、安全排空和容量验收 |

### tRPC-Agent 复用与平台新增

直接复用：

- `trpc-agent-py==1.1.19` 的 `LlmAgent → Runner → Event → SessionService` 主链路；
- 官方 Tool callback 与 Agent 生命周期；
- 官方 OpenTelemetry instrumentation。

平台新增：

- 双真实 IM Channel Adapter 与统一消息契约；
- 可信 Channel Binding 和多租户 Gateway；
- Redis/PostgreSQL Repository、跨节点幂等、租约、generation/fencing 与恢复；
- 租户级治理、预算和危险操作确认；
- Event/Memory/Summary/Artifact/Knowledge 数据边界与迁移状态机；
- 端到端可观测、健康/告警、灰度/回滚、容量和故障演练。

## 4. 开发方法：Spec-Driven Development + TDD

项目不是先堆代码再补文档，而是将每一阶段作为可独立验收的 Feature，执行统一流程：

```text
项目宪法
  → Specify：定义用户故事、FR/NFR、成功标准和范围边界
  → Clarify：识别歧义，记录候选方案、人工选择和判断依据
  → Plan：映射架构、契约、数据模型、状态机和测试策略
  → Tasks：拆分为有依赖关系、可验证的测试先行任务
  → Implement：逐任务执行 Red → Green → Refactor
  → Validate：定向测试、全量回归、真实环境验收和证据归档
```

TDD 执行要求：

1. **Red**：先编写契约、单元、集成或 E2E 断言，保存预期失败及原因；
2. **Green**：只实现使当前契约通过的最小代码，不用测试替身绕过正式主链；
3. **Refactor**：消除重复、稳定端口和错误语义，再运行阶段回归及全部历史回归；
4. **Evidence**：命令、退出码、passed/failed/skipped、环境边界和人工验收写入 `validation-results.md`；
5. **Fail closed**：测试缺失、共享环境未启动或真实凭证未验证时明确标为 skip/待验收，不把“没有执行”写成“已经通过”。

每个 Feature 目录保留 `spec.md`、`clarification-decisions.md`（适用时）、`plan.md`、`research.md`、`data-model.md`、`tasks.md`、`quickstart.md`、`validation-results.md` 和阶段成果记录，形成需求到代码和证据的闭环。

## 5. 八阶段实施成果

| 阶段 | 功能与成果 | 关键证据 |
|---|---|---|
| 1. tRPC-Agent SDK 最小验证 | 固定 SDK 1.1.19；真实经过官方 Agent、Runner、Event 和 Session；确定性模型实现零凭证、零网络、零费用验证 | 阶段验收 18/18；当前 SDK 回归 22 passed |
| 2. 多租户本地消息闭环 | 完成本地 HTTP → HMAC Binding → Gateway → tenant Session → Worker → 官方 Runner → Reply/Audit/Metrics；验证隔离、重复、冲突和失败语义 | 阶段全量 101 passed |
| 3. 多节点共享状态闭环 | Redis 幂等/Session/租约/fence，PostgreSQL 配置/Audit/Recovery；双 Worker 无 sticky session，部分提交恢复不重放 Agent | 真实共享后端 165 passed；200 次随机跨节点路由零错误 |
| 4. 飞书与企业微信 SDK 最小连通性 | 在真实客户端完成机器人创建、权限、版本发布和长连接 SDK 收发，证明“客户端→平台→本地 Python→机器人回复”可用 | 飞书和企业微信 Echo 均人工通过 |
| 5. 双 IM 真实 Channel 闭环 | 将两个 SDK 实现为正式 Adapter；统一入站/回复契约；可信复合身份、群聊 sender Session、Delivery 重试、主备 ownership 和 trace/audit | 共享后端全量 247 passed；双真实客户端及主备接管通过 |
| 6. 租户治理与工具安全 | 主体授权、入站/出站内容检查、工具白名单、危险操作确认、预算预占/结算/释放、治理审计和跨节点恢复 | 严格 fail-closed；全部用例进入最终全量回归 |
| 7. Memory/Summary 与多后端 | tenant-scoped Event、Memory、Summary、Artifact、Knowledge；watermark/CAS；Redis→SQL 迁移与 Vector/Object Adapter 边界 | 真实共享后端和全量回归通过 |
| 8. 可观测性与运维闭环 | W3C trace、脱敏采样和缓冲、健康矩阵、告警状态机、灰度/回滚、容量双门禁、Compose 拓扑、排空和故障演练 | 92/92 任务完成；最终 620 passed、0 skipped |

> 第四阶段属于飞书/企业微信管理后台与真实客户端的外部验证，验证结论随后纳入第五阶段正式 Adapter 验收，因此规格目录从 `003` 跳到 `005`，不是阶段缺失。

## 6. 项目目录

```text
.
├─ ARCHITECTURE.md                         # 最终架构设计、架构图、时序图和一致性说明
├─ README.md                               # 项目总览、运行方式和验收入口
├─ Dockerfile                              # 服务容器镜像
├─ pyproject.toml / uv.lock                # Python 3.12、固定依赖和 CLI
├─ deploy/
│  ├─ local-shared/                        # Redis + PostgreSQL 共享后端
│  └─ local-observable/                    # Gateway + 双 Worker + OTel Collector
├─ trpc_service/
│  ├─ agent/                               # 官方 Runner 接入及确定性验证模型
│  ├─ channels/                            # 飞书、企业微信和通道统一契约
│  ├─ gateway/                             # 可信绑定、路由和消息入口
│  ├─ worker/                              # Agent 执行与会话处理
│  ├─ tenant/                              # Tenant/Agent/Binding 作用域
│  ├─ storage/                             # InMemory、Redis、PostgreSQL、数据端口
│  ├─ governance/                          # 授权、策略、确认和预算
│  ├─ audit/                               # 追加式审计模型
│  ├─ observability/                       # trace、脱敏、采样、指标、健康和告警
│  ├─ operations/                          # 发布、门禁、容量、排空和故障演练
│  ├─ recovery/                            # 部分提交与节点恢复
│  ├─ tool/ / skill/                       # Tool/Skill 扩展边界
│  ├─ config/ / log/ / metrics/            # 配置、日志和指标
│  ├─ web/                                 # HTTP 服务与健康端点
│  └─ _cli.py                              # 本地、共享、通道和诊断 CLI
├─ tests/
│  ├─ unit/                                # 领域模型和纯逻辑
│  ├─ contract/                            # Adapter/Repository/错误契约
│  ├─ integration/                         # 组件与真实共享后端
│  ├─ e2e/                                 # 多节点、双 IM、治理和运维闭环
│  ├─ performance/                         # 1,000 消息容量双门禁
│  ├─ security/                            # Secret、租户隔离和脱敏门禁
│  └─ sdk_validation/                      # 官方 SDK 复用基线
└─ specs/
   ├─ 001-trpc-agent-sdk-validation/
   ├─ 002-multitenant-local-message-flow/
   ├─ 003-shared-state-multinode-flow/
   ├─ 005-dual-im-real-channel-flow/
   ├─ 006-tenant-governance-tool-policy/
   ├─ 007-memory-summary-backend-flow/
   └─ 008-observability-operations-flow/
```

## 7. 快速开始

### 7.1 环境要求

- Python `>=3.12,<3.13`
- [uv](https://docs.astral.sh/uv/)
- Docker Engine / Docker Desktop 与 Compose v2（完整共享后端验收需要）
- Windows PowerShell 7（以下示例）

```powershell
Set-Location E:\grad_files\2026trpc-agent\trpc-agent-service-submit
uv sync --group dev
```

### 7.2 十秒级 SDK 基线

无需 Docker、IM Secret 或模型 API Key：

```powershell
uv run python -m trpc_service.agent.sdk_validation
uv run pytest tests/sdk_validation -q -p no:cacheprovider
```

预期：验证报告 `RESULT: PASS`，当前版本 SDK 测试 `22 passed`。

### 7.3 本地自动化回归

```powershell
uv run pytest -q -p no:cacheprovider
```

未设置 Redis/PostgreSQL URL 时，共享后端测试会明确 skip；该结果适合快速开发反馈，不能替代最终 0 skip 验收。

### 7.4 Redis/PostgreSQL 与完整 0-skip 验收

不要把真实密码、DSN、IM Secret 写入脚本、README、日志或 Git。先按照 [第八阶段 Quickstart 第 3 节](specs/008-observability-operations-flow/quickstart.md#3-共享后端准备)在当前 PowerShell 安全设置：

- `TRPC_DEMO_REDIS_PASSWORD`
- `TRPC_DEMO_POSTGRES_PASSWORD`
- `TRPC_SHARED_REDIS_URL`
- `TRPC_SHARED_DATABASE_URL`

确保本机 `5432`、`6379` 和 `8080` 未被其他测试栈占用，然后执行：

```powershell
$testProject = "trpc-agent-final-" + (Get-Date -Format "yyyyMMddHHmmss")
$composeBase = "deploy/local-shared/compose.yaml"
$composeOps = "deploy/local-observable/compose.yaml"
$pytestTemp = Join-Path $env:TEMP ("trpc-agent-pytest-" + [guid]::NewGuid().ToString("N"))

docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps up -d --build --wait
uv run trpc-agent-shared-init
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps ps

Invoke-RestMethod http://127.0.0.1:8080/health/live
Invoke-RestMethod http://127.0.0.1:8080/health/ready

uv run pytest -q -p no:cacheprovider --basetemp $pytestTemp
uv run pytest tests/security -q -p no:cacheprovider
```

最终验收基准：

```text
620 passed, 0 skipped, 0 failed
Gateway / Worker-A / Worker-B / Redis / PostgreSQL / OTel Collector: healthy
/health/live: alive
/health/ready: ready
```

停止时先确认项目名，只停止本次环境，不默认删除数据卷：

```powershell
$testProject
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps ps
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps down
```

### 7.5 真实飞书与企业微信

真实凭证只通过当前进程或受控 Secret Provider 注入。变量名、双 Adapter 启动、Channel Binding 初始化、真实客户端单聊/群聊和主备接管步骤见：

- [双 IM Quickstart](specs/005-dual-im-real-channel-flow/quickstart.md)
- [运行环境变量示例](specs/005-dual-im-real-channel-flow/runtime-env.example.md)
- [第五阶段真实验收记录](specs/005-dual-im-real-channel-flow/validation-results.md)

## 8. 测试体系与最终结果

| 测试层 | 验证内容 |
|---|---|
| SDK Validation | 官方 SDK 版本、Runner/Event/Session、离线安全和确定性 |
| Unit | 状态机、模型、规范化、采样、预算、容量和恢复逻辑 |
| Contract | Channel/Repository/UoW/错误码/安全边界在不同实现间一致 |
| Integration | Gateway、Worker、Redis、PostgreSQL、迁移、审计和双节点交互 |
| E2E | 双 IM、跨节点 Session、治理、trace、灰度回滚、排空和故障恢复 |
| Performance | 2 租户、2 Worker、100 并发 Session、1,000 消息；正确性零容忍，遥测相对开销 ≤10% |
| Security | 租户越权、Secret/DSN/正文泄露、日志/trace 标签和 fail-closed |

最终验证环境和事实：

| 项目 | 结果 |
|---|---|
| Docker Engine | 29.6.2 |
| PostgreSQL / Redis | healthy，schema-init 退出码 0 |
| Gateway / 双 Worker / Collector | 全部 healthy |
| 共享后端专项 | 33 passed |
| 全量回归 | **620 passed，0 skipped，0 failed，2 warnings** |
| 健康端点 | live=alive，ready=ready |
| Collector 故障 | Gateway 继续 live/ready，恢复后 Collector healthy |
| Worker-A 停止 | 排空/接管 E2E 4 passed，Worker-B 持续服务 |
| Git 敏感材料扫描 | 跟踪文件 0 命中 |
| 证据性质 | 真实本机 Docker/local evidence；不据此宣称生产 SLA |

完整命令、RED→GREEN 记录、故障注入和边界声明见 [第八阶段验证记录](specs/008-observability-operations-flow/validation-results.md)。

## 9. 原始核心要求覆盖情况

| # | 核心要求 | 实现与证据 | 状态 |
|---:|---|---|---|
| 1 | 多租户、节点化、同步、多后端、IM、治理监控、恢复 | 阶段 2–8；架构文档、全量测试和 Docker 故障演练 | 完成 |
| 2 | Tenant、Agent、Binding、Session、Event、Memory、Summary、Audit 数据模型 | 各阶段 `data-model.md`，重点见 [第七阶段数据模型](specs/007-memory-summary-backend-flow/data-model.md) | 完成 |
| 3 | 至少两种 IM，且包含微信/企业微信 | 飞书与企业微信独立 Adapter、SDK 替身测试和真实客户端验收 | 完成 |
| 4 | 至少三类后端 | Redis、PostgreSQL 真实验证；Vector Store、Object Store 契约与迁移边界 | 完成（扩展边界如实标注） |
| 5 | 完整消息链路与 trace_id | [企业微信核心时序图](ARCHITECTURE.md#3-企业微信核心消息时序)及 E2E trace 测试 | 完成 |
| 6 | 至少八项生产风险 | [风险登记](specs/008-observability-operations-flow/risk-register.md)共 9 项，含检测、处置和恢复证据 | 完成 |
| 7 | 明确框架复用与平台新增 | 第一阶段 SDK 基线、第八阶段成果记录及本文第 3 节 | 完成 |

总体验收追踪率：**7/7 = 100%**。详细 FR/NFR/SC → 设计 → 测试 → 证据映射见 [traceability-matrix.md](specs/008-observability-operations-flow/traceability-matrix.md)。

## 10. 最终交付物

| 交付要求 | 文件 |
|---|---|
| 2000–4000 字架构设计 | [ARCHITECTURE.md](ARCHITECTURE.md) |
| 系统架构图 | [ARCHITECTURE.md：系统架构](ARCHITECTURE.md#2-系统架构) |
| 企业微信核心时序图 | [ARCHITECTURE.md：核心消息时序](ARCHITECTURE.md#3-企业微信核心消息时序) |
| 数据模型 | [第七阶段 data-model.md](specs/007-memory-summary-backend-flow/data-model.md)及各阶段模型 |
| 数据同步与幂等策略 | [ARCHITECTURE.md：数据同步、幂等与故障恢复](ARCHITECTURE.md#5-数据同步幂等与故障恢复) |
| 多后端适配方案 | [ARCHITECTURE.md：多后端适配](ARCHITECTURE.md#6-多后端适配方案) |
| 至少八项生产风险 | [risk-register.md](specs/008-observability-operations-flow/risk-register.md) |
| GitHub 实现代码 | 当前仓库 `feature/luwenjie` 分支 |
| 最终验证证据 | [validation-results.md](specs/008-observability-operations-flow/validation-results.md) |
| 阶段答辩摘要 | [阶段成果记录.md](specs/008-observability-operations-flow/阶段成果记录.md) |

## 11. 安全与范围声明

- `.env.local` 已由 Git 忽略；Bot/App Secret、数据库密码、模型 Key、DSN、response URL 和原始消息正文不得进入代码、Git、日志或 trace。
- 自动化测试默认使用确定性模型与 SDK Test Double，不依赖真实模型 API，不产生模型费用。
- Redis/PostgreSQL 已完成真实本地共享后端验证；飞书/企业微信已完成真实客户端最小连通与正式链路验收。
- Vector Store 和 Object Store 当前为正式 Repository 契约与确定性实现边界，不宣称已接入具体生产产品。
- 生产 Kubernetes、跨地域 HA、管理 UI、真实模型供应商和生产绝对 SLA 不在本次范围；生产推荐拓扑见 [deployment-topology.md](specs/008-observability-operations-flow/deployment-topology.md)。

## 12. 结论

项目已经从官方 SDK 最小验证，逐步完成单进程多租户、双节点共享状态、真实双 IM、租户治理、多后端数据语义以及可观测运维闭环。所有核心安全决策都有人工选择记录，所有功能任务都有规格、契约和测试证据，最终共享环境实现零失败、零跳过回归。仓库既提供可运行的本地实现，也明确标注生产建议与范围边界，可用于后续接入真实模型、向量数据库、对象存储和生产编排。

<!--
Sync Impact Report
- Version change: unratified template -> 1.0.0
- Modified principles:
  - Template Principle 1 -> I. Framework-First, Platform-Owned Boundaries
  - Template Principle 2 -> II. Tenant Isolation Is a System Invariant
  - Template Principle 3 -> III. Stateless Workers, Explicit Consistency
  - Template Principle 4 -> IV. Contract-First Channels and Storage
  - Template Principle 5 -> V. Security and Governance by Default
  - Added VI. End-to-End Observability and Failure Accountability
  - Added VII. Spec-Driven Vertical Slices and Evidence
- Added sections:
  - Project-Wide Constraints
  - Development Workflow and Quality Gates
- Removed sections: none
- Template synchronization: not required; Spec Kit resolves this constitution at runtime
- Follow-up TODOs: none
-->

# tRPC-Agent 多租户节点化部署平台 Constitution

## Core Principles

### I. Framework-First, Platform-Owned Boundaries

项目 MUST 将 tRPC-Agent-Python 作为 Agent 运行能力的正式上游依赖，优先复用其
Agent、Runner、Session、Memory、Knowledge、Tool、Filter 和 Telemetry 能力。
作业仓库 MUST 聚焦多租户管理、Gateway、Worker、Channel Adapter、
Storage Adapter、治理审计和运维等平台能力，不得复制官方源码后冒充自研实现。

平台代码 MUST 通过明确的 Adapter、Factory 或 Service 边界接入框架，业务模块
不得散布对框架内部实现细节的依赖。每份功能计划 MUST 列出“框架直接复用能力”
和“平台新增能力”，固定依赖版本，并提供兼容性验证。确需修改上游时，MUST
通过独立 fork、固定 commit、架构决策记录和回退方案管理，不得直接混入作业仓库。

Rationale: 该课题的价值在于把框架能力映射为企业平台，而不是重新实现框架或仅在
项目名称中引用它；清晰边界也降低上游升级风险。

### II. Tenant Isolation Is a System Invariant

任何租户相关请求、配置、事件、存储访问、工具调用、日志、指标和成本记录 MUST
携带可验证的 tenant identity。租户上下文 MUST 从入口贯穿至执行与存储边界；
Repository、缓存键、会话键、审计记录和异步任务 MUST 显式包含租户作用域。

配置、数据、工具权限、IM 绑定、知识库、预算、密钥和审计策略 MUST 按租户隔离，
访问策略 MUST 默认拒绝。任何功能只要涉及租户数据，MUST 包含跨租户拒绝测试和
同租户正常访问测试。不得依赖调用方“自觉传对 tenant_id”作为隔离机制。

Rationale: 多租户隔离是平台的安全边界；单纯增加 tenant_id 字段无法阻止越权、
串话、密钥误用或成本归属错误。

### III. Stateless Workers, Explicit Consistency

Agent Worker MUST 保持可替换和可横向扩展。除单次请求内的临时状态外，继续处理
会话所需的 Session、Memory、Summary、Artifact 和执行状态 MUST 存入明确的共享
后端。生产方案不得依赖某一 Worker 的进程内状态维持会话正确性。

每个涉及消息或状态写入的功能 MUST 说明 session 路由、幂等键、并发控制、事件
顺序、重试边界和失败恢复语义。IM 重复投递不得导致同一业务动作重复执行。同一
session 的并发更新 MUST 有可验证的冲突处理策略。只有经架构决策记录证明必要时
才可采用 sticky session，且 MUST 同时给出节点故障后的恢复路径。

Rationale: 节点化部署的核心不是启动多个进程，而是任意健康节点均能在一致、可
恢复的状态基础上安全接续请求。

### IV. Contract-First Channels and Storage

外部 IM 平台数据 MUST 先转换为平台统一的入站消息契约，Agent 事件 MUST 先转换
为统一出站消息契约，再由具体 Channel Adapter 处理平台差异。核心业务不得直接
依赖某一 IM 的原始载荷、鉴权方式或回复格式。

项目最终方案 MUST 覆盖至少两类独立 IM Channel，其中至少一类为微信或企业微信。
缺少真实外部账号时，相关 Adapter 仍 MUST 完成契约、验签、消息映射、幂等和发送
行为的 fixture 或 mock 验证；同时 MUST 至少有一个本地可运行的端到端消息通道。

Session、Memory、Summary、Artifact、Knowledge 和 Audit Log MUST 通过清晰的数据
访问契约管理。最终方案 MUST 比较并覆盖至少三类后端，逐项说明数据归属、一致性、
延迟、成本、并发和迁移策略。上层业务不得依赖某个供应商专属的数据表示。

Rationale: 契约优先让通道和后端可替换，也使外部条件不足的集成仍能被可靠验证。

### V. Security and Governance by Default

工具白名单、IM 用户权限、预算限制、危险操作确认和敏感信息处理 MUST 在执行前
强制实施，并默认拒绝未声明的能力。来自用户、知识库、工具或外部通道的内容均
不得绕过授权边界。代码执行、文件访问和外部工具 MUST 使用与风险相称的最小权限
和隔离措施。

IM token、模型 API key、数据库密码及其他凭据 MUST 通过密钥引用或运行环境注入，
不得明文进入源码、配置样例、测试数据、日志、trace、错误报告或审计详情。日志和
遥测在输出前 MUST 脱敏。安全相关拒绝、授权决策和危险工具确认 MUST 形成可审计
记录，但审计记录本身不得泄露秘密。

Rationale: 平台同时连接模型、工具、企业数据与外部 IM，安全控制必须成为默认
执行路径，而不是上线前追加的可选功能。

### VI. End-to-End Observability and Failure Accountability

每次外部请求 MUST 在入口创建或继承唯一 trace_id 或 request_id，并贯穿 IM
callback、Gateway、Runner、模型、Tool、Session 或 Memory 读写及 IM 回复。
结构化日志、指标和审计记录 MUST 支持按租户、会话和 trace 查询，同时遵守脱敏
与最小披露原则。

涉及外部依赖的计划 MUST 定义超时、有限重试、退避、降级、熔断或失败转储中的
适用策略，并明确哪些操作可以重试、哪些操作必须依赖幂等保护。指标 MUST 至少能
观察请求量、错误率、关键链路延迟、模型与工具耗时、IM 投递、token 消耗、租户
成本和状态后端延迟。失败不得被吞掉或伪装为成功。

Rationale: 只有能够关联一次完整业务链路并解释失败，平台才具备生产诊断、审计
和容量治理能力。

### VII. Spec-Driven Vertical Slices and Evidence

每项开发工作 MUST 从可验收的 Spec 开始，并按 clarify、plan、tasks、analyze、
implement、converge 的适用阶段推进。Spec MUST 描述用户价值、边界、验收场景和
可度量成功标准；Plan MUST 说明架构、框架复用、数据一致性、安全、可观测性和
测试策略；Tasks MUST 可执行并能追溯至需求。

功能 MUST 以一周内可完成、可运行、可测试、可演示的纵向切片交付。自动化证据
MUST 覆盖正常路径和关键失败路径；外部账号或基础设施不可用时可以使用 mock、
fixture 或受控替代实现，但 MUST 明确标注真实联调未完成，不得将模拟结果表述为
生产验证。没有通过验收测试、文档与演示证据的功能不得声明完成。

Rationale: 该课题范围广，纵向切片和需求追踪可以防止只堆积文档、只搭空骨架或
在没有证据时声称满足生产要求。

## Project-Wide Constraints

- 项目 MUST 以架构设计为主，同时实现足以证明关键边界和完整消息链路的代表性
  代码；不得为了追求“完整平台”而牺牲核心验收项的具体性和可验证性。
- 核心数据模型 MUST 表达 tenant、agent app、channel binding、session、
  message 或 event、memory、summary 与 audit log 的关系、所有权和隔离键。
- 架构 MUST 明确 Agent Gateway、Agent Worker、Channel Adapter、
  Storage Adapter、Admin API 与 Telemetry Collector 的职责和交互。
- 设计 MUST 给出一条从企业微信用户消息到 Agent、Tool、Session 或 Memory 写入
  再到 IM 回复的完整时序，并展示 trace_id 或 request_id 的传播。
- 数据方案 MUST 说明 event、state、summary 的更新顺序，多节点并发写入、
  Memory 可见性、重复投递幂等以及后端迁移。
- 运维方案 MUST 包含最小可运行部署与生产推荐部署、容量估算、灰度发布、
  租户级配置回滚，以及至少八项生产风险和对应缓解措施。
- 依赖 MUST 固定到可复现版本；升级 MUST 经过兼容性验证。参考源码仓库只用于
  阅读与核对，除非按 Principle I 的上游修改流程获得批准。
- InMemory 实现只能作为本地验证或明确限定的单节点方案；生产设计 MUST 说明
  共享状态后端和持久化边界。
- 所有图、Schema、伪代码、接口、实现和 README 之间 MUST 使用一致的组件名称、
  标识符和数据流，不得出现互相矛盾的架构版本。

## Development Workflow and Quality Gates

1. **需求追踪**：每个 Spec MUST 标明其对应的 README 验收项。项目 MUST 维护从
   七项总体验收标准到设计文档、代码、测试和演示证据的可追踪关系。
2. **范围冻结**：开始 Plan 前 MUST 消除影响范围、安全或数据语义的歧义，并明确
   Included、Excluded、依赖和假设。新增范围 MUST 回到 Spec 评审。
3. **计划门禁**：Plan MUST 明确复用与新增边界、组件契约、数据模型、错误语义、
   测试方法和回退路径；重大取舍 MUST 写入 ADR。
4. **任务门禁**：Tasks MUST 按用户故事组织，单项任务必须具有清晰产物或验证
   结果。实现前 MUST 运行一致性分析，处理高严重度遗漏。
5. **测试门禁**：核心领域逻辑 MUST 有单元测试；Channel 和 Storage Adapter
   MUST 有契约测试；跨组件主链路 MUST 有集成测试；幂等、并发、隔离、超时和
   降级 MUST 按对应功能风险提供自动化测试。
6. **安全门禁**：提交前 MUST 检查秘密、跨租户访问、日志脱敏和未授权工具路径。
   发现凭据泄露或可复现的跨租户访问时，该功能不得合并或演示为完成。
7. **完成定义**：功能只有在验收场景通过、测试可重复、文档同步、启动或演示步骤
   可执行、限制真实披露且 converge 未发现阻断项时，才可标记完成。
8. **变更可追溯**：项目工作 MUST 纳入 Git 版本控制。功能变更 MUST 使用独立
   分支和清晰提交；工具或仓库状态暂不支持分支时，MUST 先记录原因并保留等价的
   变更与评审记录，再在条件恢复后补齐版本控制。

## Governance

本 Constitution 是项目内需求、设计、实现和评审的最高工程约束。若其他文档、
临时决定或实现与其冲突，MUST 先修订 Constitution 或修正冲突内容，不得静默
绕过。每次 Spec、Plan 和代码评审 MUST 检查相关原则，并在不适用时记录理由。

修订 MUST 包含变更动机、受影响原则、迁移或兼容影响及 Sync Impact Report。
版本遵循语义化规则：移除原则或不兼容地重定义治理要求为 MAJOR；新增原则或
实质扩大约束为 MINOR；不改变语义的澄清和文字修正为 PATCH。首次正式批准采用
版本 1.0.0。

临时例外 MUST 通过 ADR 记录范围、原因、风险、责任人、补救措施和失效条件。
涉及租户隔离、凭据保护或审计完整性的要求不得以进度为由豁免。每个里程碑结束
时 MUST 对照 README 验收标准和本 Constitution 执行合规审查。

**Version**: 1.0.0 | **Ratified**: 2026-09-03 | **Last Amended**: 2026-09-03

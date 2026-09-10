# Feature Specification: 双 IM 真实消息闭环

**Feature Branch**: `005-dual-im-real-channel-flow`
**Created**: 2026-09-08
**Status**: Draft
**Input**: 将已完成最小连通性验证的飞书和企业微信长连接 SDK 实现为正式 Channel Adapter，接入现有多租户、多节点 Agent 消息闭环。

## Background

第二阶段已经验证单进程多租户消息闭环，第三阶段已经通过 Redis、PostgreSQL、分布式租约、generation 与 fencing 将其扩展为多节点共享状态架构。飞书与企业微信 SDK 最小连通性验证又分别证明了真实客户端、平台、长连接 SDK 与本地程序之间可以完成消息接收和回复。

当前两个验证程序仍是独立 Echo Bot，尚未经过平台的 Channel Binding、租户隔离、消息幂等、tenant-scoped Session、Agent Worker、官方 tRPC-Agent Runner 和 Audit Log。本功能要补齐这一缺口，在不改变既有 Gateway、Worker、Runner 和统一回复语义的前提下，形成两个真实 IM 的端到端消息闭环。

## Clarifications

### Session 2026-09-08

- Q: 系统应使用哪组由 IM 平台认证的身份作为 Channel Binding 唯一键，确保相同机器人标识不会绑定到错误租户？ → A: 选择 A；飞书使用 `feishu + tenant_key + app_id/bot_id`，企业微信使用 `wecom + corp_id + bot_id`，全部组成部分均须来自已认证 SDK 上下文并精确匹配。
- Q: 群聊中的不同成员分别 @ 机器人时，系统应如何划分多轮会话 Session？ → A: 选择 A；按“tenant_id + channel_type + channel_binding_id + group_conversation_id + sender_id”隔离，同群不同成员不共享 Agent 历史上下文。
- Q: Agent 结果已经持久化，但向飞书或企业微信发送回复失败或超时时，系统应采用哪种重试策略？ → A: 选择 A；明确临时失败按 1、2、4 秒最多自动重试 3 次，永久失败不重试，发送结果未知时标记 `delivery_unknown` 且不自动重发，所有恢复均复用既有 Runner 结果。
- Q: 同一个飞书 App 或企业微信 Bot 在多个 Adapter 节点部署时，应如何决定哪个节点建立并持有长连接？ → A: 选择 A；每个 Channel Identity 采用主动/备用模式，仅共享租约持有者建立长连接，接管时递增 generation 并使用 fencing 阻止旧节点继续收发。
- Q: Adapter 收到机器人自身发送的消息，或无法确认发送者是否为当前机器人时，应如何处理？ → A: 选择 A；使用 SDK 已认证的发送者类型、发送者 ID 和运行时机器人身份严格比较，自身消息直接忽略，身份缺失或无法确认时默认拒绝并记录安全审计，两者均不调用 Agent。

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 从两种真实 IM 获得 Agent 回复 (Priority: P1)

作为已绑定租户的飞书或企业微信用户，我希望在客户端中向机器人发送文本消息，并在原会话中收到由平台 Agent 生成的回复，从而直接通过日常 IM 使用 Agent 服务。

**Why this priority**: 这是双 IM 接入的核心价值，也是证明真实 Channel Adapter 已接入平台主链路的最小可交付结果。

**Independent Test**: 分别启动飞书和企业微信 Adapter，在两个客户端中向已绑定机器人发送文本单聊消息，验证消息均经过 Gateway 和官方 tRPC-Agent Runner，并返回到原会话。

**Acceptance Scenarios**:

1. **Given** 飞书机器人已启用、Channel Binding 有效且 Adapter 已连接，**When** 用户向机器人发送文本单聊消息，**Then** 用户在同一飞书会话中收到统一回复转换后的飞书消息。
2. **Given** 企业微信机器人已启用、Channel Binding 有效且 Adapter 已认证，**When** 用户向机器人发送文本单聊消息，**Then** 用户在同一企业微信会话中收到统一回复转换后的企业微信消息。
3. **Given** 机器人已加入群聊，**When** 用户在群聊中明确 @ 机器人并发送文本，**Then** 该消息进入平台主链路并在原群聊中获得回复。
4. **Given** 机器人已加入群聊，**When** 普通群消息未明确 @ 机器人，**Then** Adapter 不调用 Agent，且不会向群聊发送回复。
5. **Given** 同一群聊中的两个成员分别 @ 机器人进行多轮对话，**When** 平台构造 Session，**Then** 两位成员分别使用独立上下文，任何一方的历史消息不得进入另一方的 Runner 输入。
6. **Given** SDK 推送的消息发送者与当前运行时机器人身份一致，**When** Adapter 执行入口过滤，**Then** 消息被标记为自身消息并静默忽略，不创建业务 Session、不调用 Agent 且不发送回复。

---

### User Story 2 - 通过可信绑定保证租户隔离 (Priority: P1)

作为平台管理员，我希望每个飞书应用或企业微信机器人只能访问其 Channel Binding 指定的租户，以防止外部消息伪造 tenant_id 或跨租户访问会话和数据。

**Why this priority**: 真实外部渠道扩大了信任边界；若绑定解析不安全，会直接破坏系统最核心的租户隔离原则。

**Independent Test**: 配置两个租户和两个不同渠道绑定，分别发送消息并查询 Session 与 Audit Log；随后使用未知、禁用和错误绑定发送消息，验证系统默认拒绝且不调用 Agent。

**Acceptance Scenarios**:

1. **Given** 飞书应用身份绑定到租户 A，**When** 该应用收到消息，**Then** 系统仅根据可信 Channel Binding 得出 tenant A，不接受消息内容或调用参数中的 tenant_id。
2. **Given** 企业微信 Bot ID 绑定到租户 B，**When** 该机器人收到消息，**Then** 会话、幂等记录、执行结果和审计记录全部归属租户 B。
3. **Given** 两个渠道具有相同的外部用户 ID 或会话 ID，**When** 它们分别属于不同租户或不同渠道，**Then** Session 和消息记录保持隔离。
4. **Given** 应用或机器人没有绑定、绑定已禁用、租户已禁用或提供方身份与绑定不一致，**When** 收到消息，**Then** 系统以稳定错误语义拒绝，不创建业务 Session、不调用 Agent、不发送正常业务回复，并记录安全审计。

---

### User Story 3 - 在多节点环境保持幂等和会话连续 (Priority: P1)

作为 IM 用户，我希望重复投递、Adapter 重连或 Worker 节点切换不会导致 Agent 重复执行或上下文丢失，并且同一会话中的多轮对话保持正确顺序。

**Why this priority**: 飞书和企业微信事件均可能因网络或重连重复到达；真实接入必须继承第三阶段的共享状态一致性保证。

**Independent Test**: 将同一飞书 message_id 或企业微信 msgid 重复投递给不同处理节点，并让同一会话的多轮消息跨 Worker 执行，验证 Agent 最多执行一次、回复可恢复且上下文连续。

**Acceptance Scenarios**:

1. **Given** 相同飞书 `message_id` 被重复投递，**When** 不同节点并发处理，**Then** Agent 最多执行一次，重复投递复用已存在的处理状态或结果。
2. **Given** 相同企业微信 `msgid` 被重复投递，**When** Adapter 重连后再次收到消息，**Then** Agent 最多执行一次。
3. **Given** 同一 tenant-scoped Session 的连续消息由不同 Worker 处理，**When** 用户进行多轮对话，**Then** 后续轮次能够读取此前上下文。
4. **Given** 同一会话存在并发消息，**When** 多节点接收并处理，**Then** 消息按既有会话串行语义执行；不同会话仍可并行。
5. **Given** Agent 已生成并持久化结果但渠道发送失败，**When** 同一外部消息再次投递或执行恢复，**Then** 系统不得重新调用 Agent，只能基于既有结果执行安全的回复恢复。

---

### User Story 4 - 在渠道或平台故障后安全恢复 (Priority: P2)

作为运维人员，我希望 Adapter 断线、凭证失效、Gateway 短暂不可用或回复发送失败时具有明确、可观察且不会破坏幂等性的恢复行为。

**Why this priority**: 长连接属于持续运行的外部边界，故障不可避免；恢复行为必须与第三阶段的部分提交和 fencing 语义一致。

**Independent Test**: 使用 SDK 测试替身注入断线、认证失败、Gateway 超时、回复超时和发送失败，检查重连、拒绝、重试、状态迁移、Agent 调用次数和审计记录。

**Acceptance Scenarios**:

1. **Given** 长连接意外断开，**When** SDK 或 Adapter 触发重连，**Then** Adapter 恢复接收能力，且重放消息仍受共享幂等保护。
2. **Given** App Secret 或 Bot Secret 无效，**When** Adapter 认证，**Then** Adapter 不进入就绪状态、不接收业务流量，并输出不包含凭证明文的稳定错误和审计信息。
3. **Given** Gateway、Redis 或 PostgreSQL 暂时不可用，**When** Adapter 收到消息，**Then** 系统不得绕过校验或降级为无共享状态执行，失败可重试且不得重复执行 Agent。
4. **Given** 统一回复已生成但渠道返回明确临时失败，**When** Adapter 执行回复恢复，**Then** 分别在 1、2、4 秒后最多自动重试 3 次，只执行渠道发送且记录每次 Delivery Attempt。
5. **Given** 渠道返回永久失败，**When** Adapter 分类错误，**Then** 不自动重试并将交付标记为 `delivery_failed`。
6. **Given** 回复发送超时且无法判断平台是否已接收，**When** Adapter 分类错误，**Then** 将交付标记为 `delivery_unknown`，不得自动重发或重新运行 Agent。
7. **Given** 活动 Adapter 节点中断或失去所有权租约，**When** 租约到期且备用节点成功取得新 generation，**Then** 备用节点建立长连接并恢复接收，旧节点不得继续接收业务消息或发送回复。

---

### User Story 5 - 跨渠道追踪和审计真实消息 (Priority: P2)

作为开发、运维或答辩验收人员，我希望能够通过 trace_id 证明一条真实 IM 消息经过 Adapter、Gateway、Worker、Runner、回复和 Audit Log 的全过程。

**Why this priority**: 可追溯证据是定位真实渠道问题和证明端到端交付结果的基础。

**Independent Test**: 在两个客户端各发送一条消息，从 Adapter 日志或验收输出取得 trace_id，并查询对应审计记录，核对提供方消息标识、租户、会话、执行节点和回复状态。

**Acceptance Scenarios**:

1. **Given** 一条新的真实 IM 消息没有上游 trace_id，**When** Adapter 接收消息，**Then** 系统生成 trace_id 并传播到所有后续组件和审计记录。
2. **Given** 重复消息或恢复流程发生，**When** 查询其处理记录，**Then** owner_trace_id 和 execution_trace_id 延续第三阶段定义的所有权与执行语义。
3. **Given** 回复成功或最终失败，**When** 查询 Audit Log，**Then** 可以识别渠道类型、Channel Binding、外部消息、租户、会话、Worker、Runner 结果和回复交付状态，且日志中不存在 Secret。

### Edge Cases

- 飞书或企业微信发送空文本、仅 @ 机器人但无正文时，系统必须产生稳定结果。
- 相同 `message_id`/`msgid` 出现在不同租户或不同渠道时，幂等键不得碰撞。
- SDK 在断线前已收到消息、但 Adapter 尚未确认处理结果时发生重连，重放必须进入共享幂等流程。
- 用户快速发送多条同会话消息且 SDK 回调顺序与平台到达顺序不一致时，系统仍须遵守既有会话排序契约。
- 回复发送超时且无法判断渠道是否已接收时，系统必须进入 `delivery_unknown`，不得以自动重发制造重复回复；后续状态收敛或显式恢复仍不得重新执行 Agent。
- Adapter 收到图片、文件、语音、卡片、撤回或其他不支持事件时，不得错误地当作文本调用 Agent。
- 群聊消息包含多个 @ 或引用消息时，必须按平台结构化字段识别目标；机器人自身消息必须依靠认证身份过滤，禁止使用文本、名称或前缀猜测。
- SDK 未提供完整发送者身份或 Adapter 无法确认是否为当前机器人时，必须默认拒绝并形成安全审计，不得继续进入 Gateway。
- 同一群聊中的不同发送者即使使用相同文本或相同局部标识，也必须保持 Session 隔离，且幂等键不得相互覆盖。
- Channel Binding 在处理过程中被禁用时，不得以过期配置开始新的 Agent 执行。
- Channel Identity 任一组成部分缺失、来源不可信或与持久化绑定不一致时，必须按未知绑定默认拒绝。
- 旧 Adapter 节点的网络连接仍然存活但所有权租约已失效时，必须立即退出就绪状态并关闭连接；其旧 fencing token 不得完成新的状态写入或回复发送。
- Adapter 重启时不得依赖本地内存恢复租户、会话、幂等或 Agent 执行状态。
- Secret 缺失、格式错误、被轮换或失效时，错误和日志不得暴露原值。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 系统 MUST 提供彼此独立的飞书 Channel Adapter 和企业微信 Channel Adapter。
- **FR-002**: Adapter MUST 封装对应 SDK 的连接生命周期、入站解析和出站回复，不得让 SDK 类型进入 Gateway、领域服务、Repository 或 Agent Worker。
- **FR-003**: 两种 Adapter MUST 将受支持的入站消息转换为同一统一消息契约。
- **FR-004**: 统一入站消息 MUST 至少包含渠道类型、可信渠道身份、Channel Binding、外部消息、外部会话、外部用户、消息类型、规范化文本、接收时间和 trace_id。
- **FR-005**: 飞书 Adapter MUST 使用 `message_id` 作为渠道外部消息标识。
- **FR-006**: 企业微信 Adapter MUST 使用 `msgid` 作为渠道外部消息标识；SDK 帧 `req_id` 仅作为协议回复上下文，不得替代业务消息标识。
- **FR-007**: 全局幂等键 MUST 至少包含 tenant_id、channel_type、Channel Binding 和 external_message_id。
- **FR-008**: Channel Binding MUST 使用由已认证 SDK 上下文提供的复合唯一键：飞书为 `feishu + tenant_key + app_id/bot_id`，企业微信为 `wecom + corp_id + bot_id`；全部组成部分必须精确匹配，系统再从绑定得出 tenant_id。
- **FR-009**: 系统 MUST NOT 接受消息正文、请求参数或其他不可信字段中的 tenant_id 作为授权依据。
- **FR-010**: 未知、禁用、租户失效或身份不匹配的 Channel Binding MUST 默认拒绝，且 MUST NOT 调用 Agent。
- **FR-011**: Adapter MUST 调用既有 Gateway，不得复制或绕过租户校验、幂等、Session、租约、fencing、恢复和审计逻辑。
- **FR-012**: Agent Worker MUST 保持无业务状态，并继续通过官方 tRPC-Agent Runner 处理消息。
- **FR-013**: 本阶段 MUST 使用既有确定性测试 Runner；真实模型 API 不得成为验收依赖。
- **FR-014**: 统一回复 MUST 由原渠道 Adapter 转换并发送到触发消息的原单聊或群聊会话。
- **FR-015**: 两种 Adapter MUST 支持文本单聊和群聊中明确 @ 机器人的文本消息。
- **FR-016**: 未明确 @ 机器人的普通群消息 MUST NOT 触发 Agent 执行或正常业务回复。
- **FR-017**: 不支持的消息或事件类型 MUST 产生稳定的忽略或拒绝结果，不得错误转换为文本。
- **FR-018**: Adapter MUST 使用 SDK 已认证的 sender_type、sender_id 和运行时机器人身份识别自身消息；确认是自身消息时 MUST 静默忽略，身份缺失或无法确认时 MUST 默认拒绝并记录安全审计，所有这些路径 MUST NOT 创建业务 Session、调用 Agent 或发送回复。系统 MUST NOT 使用消息文本、机器人名称或自定义前缀作为自身身份判断依据。
- **FR-019**: tenant-scoped Session MUST 包含 tenant_id、channel_type 和 channel_binding_id；群聊 Session 还 MUST 包含 group_conversation_id 与 sender_id，确保同群不同成员不共享 Agent 历史上下文。
- **FR-020**: 同一 Session MUST 复用第三阶段的跨节点串行机制，不同 Session MUST 保持可并行。
- **FR-021**: 重复事件、Adapter 重连和节点切换 MUST 复用 Redis 的共享幂等、短期 Session 和分布式租约状态。
- **FR-022**: 租户配置、Channel Binding、Audit Log、回复交付状态和恢复记录 MUST 通过既有 Repository/Adapter 边界访问持久化后端。
- **FR-023**: Agent 结果持久化后，任何回复发送重试、状态收敛或显式恢复 MUST 只复用既有 Runner 结果，MUST NOT 再次调用 Agent。
- **FR-024**: Adapter MUST 将回复失败分类为明确临时失败、永久失败或结果未知；明确临时失败 MUST 按 1、2、4 秒最多自动重试 3 次，永久失败 MUST 直接标记 `delivery_failed`，发送超时且结果未知 MUST 标记 `delivery_unknown` 并禁止自动重发。
- **FR-025**: 入站处理与出站交付 MUST 使用有界超时和上述有界重试；每次发送尝试 MUST 持久化为 Delivery Attempt，不得无限阻塞 SDK 回调或形成无界重试风暴。
- **FR-026**: trace_id MUST 贯穿 Adapter、Gateway、共享状态、Worker、Runner、统一回复、渠道发送和 Audit Log。
- **FR-027**: 重复处理和恢复 MUST 延续第三阶段的 owner_trace_id、execution_trace_id 和执行所有权语义。
- **FR-028**: App Secret、Bot Secret 及等价凭证 MUST 通过环境变量或可替换 Secret Provider 注入，不得出现在代码、测试夹具、数据库明文字段、普通日志、异常或 Git 历史中。
- **FR-029**: Channel Binding 只允许保存非敏感元数据和 Secret 引用；日志与验收证据必须脱敏凭证、令牌和长连接票据。
- **FR-030**: 两种 Adapter MUST 提供 SDK 测试替身，以验证入站转换、出站转换、连接生命周期和故障语义。
- **FR-031**: 系统 MUST 为统一 Adapter 契约提供契约测试，并对飞书、企业微信运行同一组行为断言。
- **FR-032**: 集成测试 MUST 证明真实渠道消息复用第三阶段的 Repository、幂等、Session、租约、fencing、恢复与审计实现。
- **FR-033**: 系统 MUST 提供不包含 Secret 的真实客户端验收步骤和证据模板，分别覆盖飞书和企业微信。
- **FR-034**: 本功能 MUST 保持第二、第三阶段现有 HTTP 入口和统一回复结构兼容。
- **FR-035**: 每个 Channel Identity MUST 采用主动/备用长连接模型；同一时刻只有持有共享所有权租约的 Adapter 节点可以建立并维持活动连接。
- **FR-036**: 所有权接管 MUST 递增 generation 并携带 fencing token；旧节点失去租约后 MUST 退出就绪状态、关闭长连接，并且 MUST NOT 接收新的业务处理或发送回复。

### Key Entities

- **Channel Adapter**: 封装某一 IM SDK 的连接、入站转换和出站回复能力。
- **Channel Identity**: 由已认证 SDK 提供的渠道、企业和应用/机器人身份组成的复合唯一键；飞书为 `feishu + tenant_key + app_id/bot_id`，企业微信为 `wecom + corp_id + bot_id`。
- **Channel Binding**: 将可信渠道身份绑定到 tenant_id、启用状态、策略和非敏感配置。
- **Unified Inbound Message**: 与 IM SDK 无关的标准入站消息。
- **Tenant-scoped Session**: 单聊按租户、渠道、绑定与会话标识隔离；群聊额外包含发送者标识，使同群成员拥有独立的多轮上下文。
- **Provider Reply Context**: Adapter 内部使用的最小回复上下文，不得泄漏到领域核心。
- **Unified Reply**: Runner 结果转换后的渠道无关回复。
- **Delivery Attempt**: 一次渠道发送尝试，包含 attempt_no、状态、时间、失败分类、下一重试时间和脱敏错误；交付最终状态至少包含 `delivered`、`delivery_failed` 与 `delivery_unknown`。
- **Audit Record**: 记录消息从接收、绑定解析、执行到回复交付的查询证据。
- **Adapter Ownership Lease**: 按 Channel Identity 建立的共享所有权租约，包含 owner_node_id、generation、fencing_token 和过期时间，用于保证同一渠道身份最多只有一个活动长连接。

## Assumptions

- 飞书与企业微信 SDK 最小连通性验证已经完成，测试凭证有效且机器人对测试人员可见。
- 第三阶段 Redis、PostgreSQL、Repository/Adapter、租约、generation、fencing、恢复和追踪能力继续可用。
- 每个真实机器人在验收环境中至少存在一个有效 Channel Binding。
- 本阶段只保证文本单聊和群聊明确 @ 机器人的消息语义。
- 第四阶段 Echo Bot 代码仅作为可行性证据，不直接作为正式 Adapter 实现。

## Dependencies

- `002-multitenant-local-message-flow` 已完成并通过验收。
- `003-shared-state-multinode-flow` 已完成并通过验收。
- 飞书测试企业、自建机器人、消息权限、长连接事件和发布版本可用。
- 企业微信测试企业、API 模式智能机器人、长连接 Bot ID 和 Secret 可用。
- 验收环境能够访问飞书和企业微信长连接服务。

## Out of Scope

- 真实模型 API 与模型供应商接入。
- 图片、文件、语音、视频、富文本、卡片和流式回复的完整业务支持。
- 邮件、文档、日程、会议、微盘、通讯录等企业办公 API。
- 向量库、知识库和 RAG。
- 管理后台及可视化运维控制台。
- Kubernetes、生产级弹性伸缩和完整生产 Telemetry。
- 面向外部企业或应用市场的正式发布流程。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 测试人员能够在飞书和企业微信客户端各完成至少 3 轮文本会话，所有回复均来自既有 Gateway → Worker → 官方 tRPC-Agent Runner 主链路。
- **SC-002**: 两个租户通过不同渠道并发发送消息时，Session、幂等状态、结果和 Audit Log 的跨租户混淆数量为 0。
- **SC-003**: 对同一飞书 `message_id` 或企业微信 `msgid` 至少 10 次跨节点并发重复投递时，每条业务消息的 Agent 执行次数不超过 1。
- **SC-004**: 未知、禁用和身份不匹配的 Channel Binding 测试全部被拒绝，Agent 调用次数为 0。
- **SC-005**: 同一会话消息跨至少两个 Worker 执行后上下文连续且顺序断言通过；不同会话不被全局串行化。
- **SC-006**: 注入 Adapter 断线、Gateway 暂时不可用、明确临时发送失败、永久失败和未知超时后，实际重试次数及 1、2、4 秒退避符合规定，未知超时自动重发次数为 0，已完成 Agent 结果的重复执行次数为 0。
- **SC-007**: 每条真实 IM 验收消息均可通过 trace_id 查询到 Adapter 接收、绑定解析、Agent 执行和回复交付证据。
- **SC-008**: 自动化测试、日志扫描和 Git 变更扫描中发现的 Secret、访问令牌和长连接票据明文数量为 0。
- **SC-009**: 飞书与企业微信 Adapter 均通过同一套 Adapter 契约测试，既有第二、第三阶段测试保持通过。
- **SC-010**: 不配置真实模型 API、向量库、管理后台或 Kubernetes，也能重复完成双 IM 端到端验收。
- **SC-011**: 主动/备用故障接管测试中，同一 Channel Identity 同时处于就绪状态的 Adapter 数量不超过 1，失效节点成功写入或发送回复的次数为 0。
- **SC-012**: 自身消息、缺失发送者身份和无法确认发送者身份的测试中，Agent 调用和业务回复次数均为 0；身份不确定路径均产生不含敏感信息的安全审计。

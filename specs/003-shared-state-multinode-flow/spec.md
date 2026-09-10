# Feature Specification: 共享状态多节点消息闭环

**Feature Branch**: `feature/luwenjie`

**Created**: 2026-09-07

**Status**: Draft

**Input**: User description: "在第二阶段多租户本地消息闭环基础上，通过既有 Repository/Adapter 边界引入共享状态与持久化状态，使至少两个 Worker 能在不依赖 sticky session 的条件下安全接续同一 tenant-scoped Session，并覆盖跨节点幂等、会话串行、节点故障、租约超时、状态后端短暂不可用和 trace 传播；保持现有 HTTP Channel、Gateway、Worker、官方 tRPC-Agent Runner 与统一回复的外部语义，不包含真实 IM、向量库、Kubernetes、管理后台、真实模型 API 或完整生产 Telemetry。"

## Clarifications

### Session 2026-09-07

- Q: 当租约到期与原 owner 的续期请求并发时，平台应如何确定续期是否有效？ → A: 由共享状态后端原子判断；只有当前 generation 且租约尚未过期时可以续期，过期后原 owner 的续期失败，后续处理必须取得更高 generation。
- Q: 当旧 generation 的 Worker 恢复并尝试写入时，fencing 应如何处理它产生的审计信息？ → A: 拒绝旧 generation 的所有业务写入；由当前平台身份追加独立、不可变的“迟到写入被拒绝”诊断审计，该记录不得修改任何业务状态或结果。
- Q: 节点租约失效后，新节点应根据什么证据判断原 Agent 尚未开始、因此可以安全接管执行？ → A: 以共享状态中由当前 owner 条件写入的持久执行阶段为准；仅明确处于 EXECUTION_STARTED 之前时允许换代接管，阶段无法确认时按 outcome_unknown 处理并禁止重放。
- Q: 当持久化审计已经成功，但共享幂等终态写入超时或结果无法确认时，平台应如何响应并恢复？ → A: 对外返回 outcome_unknown 并禁止自动重放；持久化端保留可恢复的既有执行结果或安全引用，恢复流程只允许条件补齐幂等终态，不得再次调用 Agent。
- Q: 当持久化配置后端暂时不可用时，Worker 是否可以使用缓存中的 Tenant 或 Channel Binding 配置授权新的消息执行？ → A: 不可以；缓存不能作为授权来源，无法确认权威配置时默认拒绝。缓存只能在权威配置可验证时加速读取、辅助诊断或维持拒绝决定。

## User Scenarios & Testing *(mandatory)*

### User Story 1 - 任意健康节点继续租户会话 (Priority: P1)

作为平台开发和验收人员，我希望同一租户会话的连续消息可以由不同 Worker 接续处理，
这样节点切换不会丢失多轮上下文，也不需要把用户固定路由到某一台 Worker。

**Why this priority**: 这是“节点化”区别于启动多个单机副本的核心价值；如果会话仍
依赖某个进程内存，多节点只增加实例数量而没有可恢复性。

**Independent Test**: 启动两个相互不共享进程内业务状态的 Worker，让同一租户、
Agent、Binding、用户和外部会话的保存消息与召回消息分别由不同节点处理；验证第二个
节点得到正确上下文，另一租户使用相同外部标识时仍完全隔离。

**Acceptance Scenarios**:

1. **Given** Worker A 已处理租户 alpha 的首轮消息，**When** 同一会话的第二轮消息
   被路由到 Worker B，**Then** Worker B 使用共享会话状态给出连续且正确的最终回复。
2. **Given** alpha 与 beta 使用相同外部用户和会话标识，**When** 两个租户的消息在
   两个 Worker 间交替处理，**Then** 两者的会话、事件、回复、审计和指标均不串用。
3. **Given** 任一 Worker 在两轮消息之间退出，**When** 另一健康 Worker 接收下一轮
   消息，**Then** 无需恢复原节点或启用会话粘滞即可继续会话。

---

### User Story 2 - 跨节点重复投递只执行一次 (Priority: P1)

作为 Channel 调用方，我希望相同消息同时到达不同节点时仍只发生一次业务执行，
这样负载均衡、客户端重试或节点切换不会产生重复 Agent 副作用和重复回复。

**Why this priority**: 多节点会放大第二阶段进程内幂等的失效风险；跨节点原子处理权
是安全水平扩展的必要前提。

**Independent Test**: 将相同 tenant、Binding 和 external_message_id 的请求同时
发送给两个 Worker，重复至少 50 组；验证每组只有一个执行 owner、一次 Agent 执行、
一份会话业务事件和一次可投递业务回复，其余请求只得到 processing 或缓存终态。

**Acceptance Scenarios**:

1. **Given** 两个 Worker 同时收到相同内容和幂等标识，**When** 它们竞争处理权，
   **Then** 恰好一个 owner 可以跨越执行开始边界，其他节点不得调用 Agent。
2. **Given** 原执行仍在进行，**When** 另一节点收到重复请求，**Then** 返回可安全
   重试的处理中结果，并关联当前 owner trace。
3. **Given** 原执行已完成，**When** 任一节点收到相同内容的重复请求，**Then** 返回
   已保存的首次执行结果、抑制重复投递并关联 execution trace。
4. **Given** 同一幂等标识携带不同内容到达另一节点，**When** 平台校验内容指纹，
   **Then** 返回冲突，不覆盖原内容、原结果或原审计。

---

### User Story 3 - 跨节点保持会话顺序 (Priority: P2)

作为多轮对话用户，我希望同一会话的不同消息即使落到不同节点，也按取得会话处理权
的顺序执行，同时其他会话不被全局阻塞。

**Why this priority**: 会话上下文对顺序敏感；仅有消息去重不能避免两个节点同时修改
同一 Session，也不能阻止失去所有权的旧节点迟到写入。

**Independent Test**: 向两个节点并发发送同一会话的不同消息，并同时发送其他会话
消息；验证同会话最大执行并发为 1，其他会话可并行，租约换代后旧 owner 的会话写入
和终态写入均被拒绝。

**Acceptance Scenarios**:

1. **Given** 同一 tenant-scoped Session 的两条不同消息分别到达 Worker A 和 B，
   **When** 两者竞争会话处理权，**Then** 同一时刻最多一条消息执行并修改该会话。
2. **Given** 两条消息属于不同 tenant-scoped Session，**When** 它们由不同节点处理，
   **Then** 可以并行执行，不受全局会话锁阻塞。
3. **Given** Worker A 的会话租约失效且 Worker B 获得新一代处理权，**When** Worker A
   恢复并尝试迟到写入，**Then** 所有旧代写入都被拒绝并形成可诊断记录。

---

### User Story 4 - 节点和状态后端故障可安全恢复 (Priority: P2)

作为平台操作人员，我希望节点退出、租约超时和状态后端短暂不可用都有明确且安全的
结果，这样平台不会因为自动恢复而重复执行，也不会把未持久化或未审计的结果报告为
成功。

**Why this priority**: 节点化平台必须能解释故障发生在执行前还是执行后；错误恢复比
直接失败更容易造成重复副作用和跨租户污染。

**Independent Test**: 在取得幂等 claim 前后、会话租约前后、Agent 开始前后和最终
状态提交前后注入节点退出及后端不可用；验证每个检查点都得到预定义终态、恢复方式、
审计和 trace，且 Agent 最多执行一次。

**Acceptance Scenarios**:

1. **Given** owner 在 Agent 开始前退出且租约已确认失效，**When** 另一节点重新接收
   相同消息，**Then** 可以在获得新 owner generation 后安全接管执行。
2. **Given** owner 在 Agent 开始后退出或无法证明执行未发生，**When** 租约失效或
   相同消息再次到达，**Then** 保存或返回 outcome_unknown，禁止自动重新执行。
3. **Given** 共享短期状态不可用，**When** 新消息无法可靠取得幂等或会话处理权，
   **Then** 在 Agent 执行前失败关闭，不退回进程内状态继续处理。
4. **Given** 持久化配置不可用，**When** 请求无法验证最新 Tenant/Binding 所有权，
   **Then** 默认拒绝，不使用任何正向缓存授予新的执行权限；缓存只能辅助诊断或维持
   拒绝决定。
5. **Given** 最终审计或终态提交结果不确定，**When** 平台形成对外结果，**Then**
   延续第二阶段的 audit_incomplete 或 outcome_unknown 语义，且相同消息不自动重放。
6. **Given** 最终审计及恢复记录已持久成功但共享幂等终态写入结果不确定，**When**
   请求返回或恢复流程运行，**Then** 对外先返回 outcome_unknown，恢复流程仅使用既有
   执行结果条件补齐终态，不再次调用 Agent。

---

### User Story 5 - 运维人员跨节点追踪与验证替换边界 (Priority: P3)

作为平台评审和运维人员，我希望从任一节点都能按租户、会话和 trace 查询完整处理
证据，并证明共享后端没有改变第二阶段的业务契约。

**Why this priority**: 共享状态只有在故障可定位、租户归属可证明并且替换不改变上层
语义时，才能成为后续真实 IM 和生产设计的可信基础。

**Independent Test**: 对成功、重复、冲突、节点接管、迟到写拒绝和后端故障分别
采样，从另一个节点查询审计、幂等与会话记录；同时对 InMemory 和共享实现运行同一套
端口契约测试。

**Acceptance Scenarios**:

1. **Given** 消息在两个节点间完成处理，**When** 从任一节点按 trace 查询，**Then**
   current、owner 和 execution trace 的关系一致且可以关联所有关键阶段。
2. **Given** 查询携带一个租户作用域，**When** 访问会话、幂等、审计或指标，**Then**
   永远不会返回其他租户记录。
3. **Given** 两套不同状态实现，**When** 运行相同 Repository/Adapter 契约测试，
   **Then** 二者对相同输入产生相同业务状态和错误类别。

### Edge Cases

- 两个节点在几乎同一时刻首次 claim 同一消息时，只允许一个 owner generation 生效。
- 节点在取得幂等 claim 后、取得会话租约前退出时，租约到期后的新 owner 可以安全接管。
- 当前 owner 必须在调用 Agent 前，以匹配当前 generation 的条件写持久记录
  EXECUTION_STARTED；该写入未确认成功时不得调用 Agent。
- 节点已进入 EXECUTION_STARTED 但尚未保存最终结果时退出，或共享状态无法证明仍
  停留在执行开始之前时，必须按结果不确定处理，不得根据节点失联猜测失败后自动重放。
- 旧 owner 在长暂停后恢复，并在新 owner 已产生会话事件或终态后尝试写入时，迟到写
  必须被 fencing generation 拒绝；旧 owner 不得以普通业务审计绕过 fencing。平台
  应以当前有效的平台身份追加独立且不可变的“迟到写入被拒绝”诊断审计，该审计不得
  修改 Session、事件、幂等终态、业务回复或原执行结果。
- 会话租约续期请求与租约到期并发发生时，共享状态后端必须原子校验当前 generation
  与后端认定的有效期；只有尚未过期的当前 owner 可以续期，过期后的续期必须失败，
  后续处理只能取得更高 generation，任一时刻只能有一个有效 owner。
- 共享短期状态在 claim、lease、session read、event append 或 terminal commit 任一
  阶段超时，不得静默退回节点本地数据。
- 持久化状态在读取 Tenant/Binding、写入最终审计或查询审计时不可用，必须返回阶段
  对应的安全错误，且不得泄露连接信息或凭据。
- 持久化配置后端不可用时，即使 Worker 持有未过本地 TTL 的启用配置，也不得据此授权
  新执行；缓存的禁用或拒绝结果可以继续用于拒绝，但不能反向授予权限。
- 最终审计及恢复记录已经持久成功、但共享幂等终态写入超时或结果不确定时，必须返回
  outcome_unknown 并禁止自动重放；恢复流程只允许读取已持久化的既有执行结果或安全
  引用，并以条件写补齐幂等终态，不得再次调用 Agent。
- 共享幂等终态已提交、但持久化端无法证明最终审计成功时，不得回滚或覆盖既有终态，
  必须留下可查询的审计不完整或恢复状态，并禁止把部分提交报告为完整成功。
- 全部 Worker 重启后，已完成幂等终态、会话上下文和持久化审计仍可从共享状态恢复。
- 不同租户使用完全相同的外部消息、用户和会话标识时，所有共享键和查询仍必须隔离。
- 同租户同会话的热点消息不得阻塞其他租户或其他会话。
- Worker 时钟存在合理偏差时，不得仅依赖本机时钟判定锁所有权或接受旧代写入。
- 共享后端中的未知 schema/config version 必须失败关闭，不能由旧节点猜测解释。
- 指标或日志后端失败不得修改已提交的业务终态，也不得改变认证拒绝结果。

## Scope and Boundaries

### Included

- 至少两个可独立退出和重启的 Worker 实例，以及可将消息发送到任一节点的本地验收入口。
- 在多个 Worker 之间共享的幂等处理权、会话租约、Session 状态和最小事件历史。
- 可持久保存并跨 Worker 重启查询的 Tenant、Agent、Channel Binding 元数据和 Audit Log。
- 跨节点会话连续、幂等、串行、fencing、节点接管和状态后端短暂故障的自动化证据。
- 延续第二阶段统一消息、统一回复、租户上下文、错误类别、审计作用域和 trace 语义。
- InMemory 与共享状态实现复用同一业务端口契约的替换性验证。

### Excluded

- 企业微信、微信客服、公众号、Telegram 等真实 IM 账号联调及新增真实 Channel Adapter。
- Memory、Summary、Knowledge、向量检索、对象存储和跨后端数据迁移执行。
- Kubernetes、生产负载均衡、自动扩缩容、跨地域容灾和生产级高可用部署。
- 管理后台、Admin API、租户自助配置、灰度发布 UI 和生产密钥管理系统。
- 真实模型 API、真实 token/成本、Tool/MCP 执行和危险工具治理。
- 完整 OpenTelemetry Collector、生产告警平台或真实 IM 投递指标。
- 对跨多个状态系统提供分布式事务或宣称生产级 exactly-once；本阶段只验证定义的
  owner、fencing、不可重放和可恢复语义。

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: 平台 MUST 保持第二阶段本地 HTTP 入站消息、HMAC Binding 认证、统一
  出站回复及稳定错误类别的外部行为兼容。
- **FR-002**: 平台 MUST 同时运行至少两个可独立退出、重启和接收消息的 Worker；
  两个 Worker 不得共享进程内业务状态来维持正确性。
- **FR-003**: 平台 MUST 允许通过校验的消息被路由到任一健康 Worker，不得依赖
  sticky session 才能保持会话连续、幂等或租户隔离。
- **FR-004**: 每个 Worker MUST 只保留单次请求所需的临时对象；继续会话所需的
  Session 状态、事件、幂等状态和执行所有权必须存在于明确的共享边界中。
- **FR-005**: 所有共享 Session、事件、幂等、租约、配置、审计和指标键或查询 MUST
  显式受已验证 tenant 与 Agent 作用域约束，调用方提供的 tenant_id 不构成授权。
- **FR-006**: 平台 MUST 让同一 tenant、Agent、Binding、用户和外部会话的连续消息
  在不同 Worker 间读取和更新同一 Session，同时拒绝任何跨租户或跨 Agent 访问。
- **FR-007**: 全部 Worker 重启后，已确认提交的会话上下文、幂等终态和审计记录 MUST
  仍可恢复；未确认提交的数据必须通过显式恢复状态呈现，不得伪装为成功。
- **FR-008**: 平台 MUST 以 tenant、Binding 和 external_message_id 组成跨节点唯一
  幂等作用域，并以消息内容指纹识别同 ID 不同内容的冲突。
- **FR-009**: 不同 Worker 对同一幂等作用域进行顺序或并发 claim 时，MUST 原子地产生
  至多一个有效 owner generation，且只有该 owner 能进入 Agent 执行。
- **FR-010**: 平台 MUST 保存 first_claim_trace_id、当前 owner_trace_id、
  execution_trace_id、owner generation、执行阶段、租约状态和最终结果，以支持跨节点
  processing、duplicate、conflict 和故障恢复判断。
- **FR-011**: 已完成重复请求 MUST 从任一节点返回原执行终态并抑制业务投递；处理中
  请求 MUST 返回当前 owner trace；冲突请求不得覆盖原记录或触发执行。
- **FR-012**: 平台 MUST 为每个 tenant-scoped Session 提供跨节点互斥处理权，使同一
  会话不同消息串行执行，同时允许不同会话并行。
- **FR-013**: 会话处理权 MUST 具有可识别的 owner generation；任何已失效 generation
  的 Session event、状态、业务审计终态或幂等终态写入都必须被拒绝。拒绝发生后，只能
  由当前有效的平台身份追加独立且不可变的迟到写入诊断审计；该诊断记录不得改变任何
  业务状态、业务终态或回复结果。
- **FR-014**: 会话处理权的释放、续期、到期和接管 MUST 由共享状态后端原子裁决并
  具有单一明确结果。只有匹配当前 generation 且按后端时间仍未过期的 owner 可以续期；
  租约一旦过期，原 owner 的续期必须失败，后续处理只能取得更高 generation；不得仅凭
  Worker 本机认为租约仍有效就接受续期或状态写入。
- **FR-015**: 当前 owner MUST 在调用 Agent 前，以匹配当前 generation 的条件写将
  共享幂等记录持久推进至 EXECUTION_STARTED；该写入未确认成功时不得调用 Agent。
  Worker 在此阶段之前退出或失去处理权时，相同消息仅可在共享状态明确证明尚未开始、
  旧 owner 已确认失效且新 owner generation 已成功建立后重新竞争执行。
- **FR-016**: 共享状态已记录 EXECUTION_STARTED，或无法确认是否仍处于该阶段之前时，
  Worker 退出、超时或失去处理权 MUST 使消息进入 outcome_unknown，并禁止相同
  external_message_id 自动重放；Worker 存活状态或心跳不得作为“Agent 尚未开始”的证明。
- **FR-017**: 共享幂等或会话状态不可用时，平台 MUST 在无法可靠取得处理权的情况下
  失败关闭，不得回退到进程内幂等、进程内锁或进程内 Session 继续执行。
- **FR-018**: 持久化租户或 Binding 配置不可用、版本未知或所有权无法确认时，平台
  MUST 默认拒绝新业务执行。正向缓存不得在权威配置不可验证时成为授权来源，即使其
  本地 TTL 尚未结束；缓存只能在权威版本与新鲜度可确认时加速读取、辅助诊断或继续
  执行拒绝决定，不得扩大访问权限。
- **FR-019**: Tenant、Agent Application 和 Channel Binding 元数据 MUST 跨 Worker
  一致可读，并保留启用状态、所有权、配置版本及不含明文秘密的 secret reference。
- **FR-020**: Audit Log MUST 持久保存成功、拒绝、重复、冲突、节点接管、迟到写拒绝、
  执行失败和结果不确定记录，并继续支持 TenantScope/PreAuthScope 及按 tenant、session、
  trace 的隔离查询。迟到写拒绝记录必须与被拒绝的旧 generation 和当前有效 generation
  相关联，并与能够形成业务终态的普通审计明确区分。
- **FR-021**: 最终审计和幂等终态 MUST 延续“最终审计先成功、再条件提交业务终态”的
  顺序；最终审计失败为 audit_incomplete，终态提交不确定为 outcome_unknown，两者均
  禁止相同消息自动重放。持久化最终审计成功时，MUST 同时保留足以恢复既有终态的脱敏
  结果或安全引用；共享幂等终态写入不确定时，对外不得返回完整成功。
- **FR-022**: 跨两个状态边界发生部分成功时，平台 MUST 保存或产生可查询的恢复标记，
  使操作人员能区分未开始、处理中、已执行但审计不完整、终态不确定和已完成。恢复流程
  只能将已持久化的既有执行结果以匹配 tenant、message、execution trace 和 generation
  的条件写补齐共享幂等终态，不得再次调用 Agent、重建结果或覆盖不同 generation 的终态。
- **FR-023**: 平台 MUST 为共享 Session event 定义单调顺序和唯一身份；同一 Session
  不得因跨节点重试出现重复业务事件或接受旧 owner 的迟到事件。
- **FR-024**: HTTP 入口、Gateway、Worker、共享状态访问、持久化状态访问、统一回复、
  审计和指标 MUST 使用同一当前 delivery trace；幂等记录同时保留 owner 与 execution
  trace，跨节点接管不得用 first claim 冒充实际执行。
- **FR-025**: 共享后端实现 MUST 遵守第二阶段已定义的数据访问契约和稳定错误类别；
  如需表达租约 generation 或条件写入，只能进行保持上层语义兼容的扩展。
- **FR-026**: 第二阶段 InMemory 实现和本阶段共享实现 MUST 运行同一核心
  Repository/Adapter 契约测试；测试不得依赖供应商专属返回结构来判断业务结果。
- **FR-027**: 平台 MUST 记录 tenant、node、session、处理阶段和结果维度的请求量、
  错误量、共享状态延迟、持久化状态延迟、租约竞争/失效、接管和迟到写拒绝指标；
  未发生的真实模型、Tool 和 IM 指标仍标记为零或 not_applicable。
- **FR-028**: 共享状态与持久化状态的连接凭据 MUST 仅通过运行时秘密引用解析，不得
  进入源码、配置样例、测试数据、日志、trace、审计详情或错误响应。
- **FR-029**: 后端异常、超时和条件写失败 MUST 映射为稳定、安全且可审计的业务错误，
  不得向调用方披露连接地址、凭据、查询文本、堆栈或供应商异常详情。
- **FR-030**: 本阶段所有验收 MUST 在没有真实 IM 账号、真实模型凭据和外部模型调用
  的环境中重复执行，并明确区分本地多节点验证与生产高可用保证。

### Key Entities

- **Worker Node**: 可独立接收和执行消息的节点身份；包含不授予租户权限的 node_id、
  生命周期状态和最后活动时间，用于诊断 owner 与故障接管。
- **Shared Session**: tenant 与 Agent 作用域下跨 Worker 共享的会话状态；具有平台
  session_id、版本、最近事件序号和当前处理权信息。
- **Session Event**: Session 中有唯一身份和单调顺序的业务事件；关联 tenant、Agent、
  session、message、execution trace 和 owner generation。
- **Processing Lease**: 某一幂等记录或 Session 的限时处理权；包含 owner node、
  generation、由共享状态后端裁决的有效期和 fencing 身份。只有尚未过期的当前
  generation 可以续期；过期既不授权旧 owner 继续写入，也不允许旧 generation 复活。
- **Shared Idempotency Record**: 跨节点消息处理状态；包含租户作用域键、内容指纹、
  three-trace 关系、owner generation、执行阶段、租约信息和不可变终态。
  EXECUTION_STARTED 必须由当前 generation 在调用 Agent 前条件写入；只有共享状态
  明确证明尚未到达该阶段时，租约换代后才允许新 owner 接管执行。
- **Persistent Tenant Configuration**: Tenant、Agent Application 和 Channel Binding 的
  持久元数据及版本关系，是新业务执行授权的权威来源；秘密只保存引用。节点缓存不具备
  独立授权能力，在权威配置不可验证时只能辅助诊断或维持拒绝。
- **Persistent Audit Record**: 可跨 Worker 重启查询的脱敏处理证据；记录 tenant、node、
  session、trace、owner generation、决策、延迟、错误类别和恢复状态；诊断型迟到写
  拒绝记录不可变且不具备修改业务终态的能力。
- **Recovery Marker**: 跨状态边界部分提交或结果未知时的可查询记录；表达故障阶段、
  是否允许接管、是否禁止重放及关联 trace；在最终审计已成功的部分提交场景中，还包含
  足以校验和恢复既有终态的脱敏结果或安全引用，且不能作为重新执行 Agent 的许可。
- **Node-Scoped Metric Snapshot**: 按 tenant 和 node 聚合的本地验收指标；用于证明跨节点
  路由、状态延迟、租约竞争和故障恢复，不冒充完整生产遥测。

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 两个独立 Worker 交替处理同一会话至少 20 轮时，正确上下文召回率为
  100%，会话中断、错误租户回复和跨租户状态泄漏均为 0。
- **SC-002**: 对同一消息执行至少 100 次顺序重复和 50 组双节点并发重复时，每组
  Agent 执行、会话业务事件和可投递业务回复数量均为 1。
- **SC-003**: 至少 50 组同会话跨节点并发测试中，同会话最大业务执行并发为 1、迟到
  写入成功数为 0；至少 20 组不同会话测试能够观察到并行执行。
- **SC-004**: 在 Agent 开始前和开始后各执行至少 20 次 owner 节点退出或租约失效
  故障注入时，开始前场景均可安全接管，开始后场景均不自动重放，重复副作用为 0。
- **SC-005**: 全部 Worker 重启后，已确认提交的 20 组多轮会话、幂等终态和审计样本
  均可由任一新 Worker 查询或继续，恢复成功率为 100%。
- **SC-006**: 共享状态和持久化状态在每个关键读写阶段的短暂不可用测试中，100% 返回
  预定义安全结果；未经可靠授权或处理权确认而启动 Agent 的次数为 0。
- **SC-007**: 成功、重复、冲突、接管、迟到写拒绝、后端故障和结果不确定样本中，
  100% 可从另一节点使用 tenant/session/trace 关联到一致的审计和状态证据。
- **SC-008**: 第二阶段 InMemory 实现与本阶段共享实现通过同一核心端口契约套件，
  供应商专属断言数量为 0，上层统一消息与回复兼容性回归通过率为 100%。
- **SC-009**: 在不使用 sticky session 的随机节点路由下，至少 200 次连续消息的正确
  tenant/session 归属率为 100%，因节点选择导致的会话失败数量为 0。
- **SC-010**: 评审人员能够在 10 分钟内完成双节点启动、跨节点多轮、并发重复、节点
  退出接管和跨节点审计查询的本地演示，并获得确定的通过或失败结论。
- **SC-011**: 源码、测试、配置样例、日志、trace、审计和错误响应的敏感信息扫描中，
  共享后端凭据、通道秘密、模型凭据和完整敏感正文泄漏数量为 0。
- **SC-012**: 全部验收在无真实 IM、无真实模型和无外部模型调用的环境中可重复执行，
  并明确报告本地验证范围，误宣称生产高可用或生产 exactly-once 的交付物数量为 0。

## Assumptions

- 第二阶段 `002-multitenant-local-message-flow` 的 HTTP、HMAC、租户上下文、统一回复、
  幂等状态和审计错误语义作为兼容基线，不在本阶段重新设计。
- 本地验收环境能够运行一个共享短期状态服务和一个持久化关系数据服务；用户提出的
  Redis 与 SQL 是计划阶段的首选实现类别，而不是本规格对供应商产品的绑定。
- “两个 Worker”指可独立退出和重启、没有共享进程内业务状态的实例；仅在同一进程
  创建两个引用同一内存字典的对象不能作为最终多节点验收证据。
- 不采用 sticky session；测试入口可以显式选择节点或随机分发，以证明任一节点接续。
- 同一 Session 的顺序延续第二阶段 D-001：以成功取得有效会话处理权的顺序为准，
  不承诺按外部网络到达时间重新排序。
- 故障重试延续第二阶段 D-002/D-006：只有能证明 Agent 尚未开始的失败允许接管；
  Agent 开始后无法确认结果时，选择不自动重放。
- 最终审计顺序延续第二阶段 D-005；指标或日志故障不反向改写业务终态。
- 租约接管必须包含 generation/fencing 语义；只依赖超时时间而不拒绝旧 owner 写入
  不满足本阶段验收。
- Session 在本阶段只覆盖支撑确定性多轮验证所需的最小状态和事件；Memory、Summary、
  Knowledge 与 Artifact 的共享和迁移在后续功能中定义。
- 本阶段可以使用本地进程、容器或受控测试服务提供共享后端，但不得把单机验证结果
  表述为生产集群高可用证明。

## Dependencies and Constraints

- 依赖第一阶段固定版本官方 tRPC-Agent Runner/Event/Session 兼容性基线。
- 依赖第二阶段已通过的统一消息、HMAC、Gateway、Worker、SessionIdentity、幂等、
  Audit、Metrics 和端口契约；本阶段必须保持这些回归测试通过。
- 共享 Session 实现必须通过上游正式支持的 Session 边界接入，不得复制或修改官方
  Runner 内部源码来绕过共享状态问题。
- 新状态实现必须可清理并提供确定性测试隔离，测试数据不得依赖开发者机器上已有状态。
- 本阶段不要求生产证书、云托管服务或真实账号；连接秘密仍必须按生产安全原则处理。

## Requirement Traceability

| 来源 | 本功能覆盖 | 本阶段边界 |
|---|---|---|
| README 验收标准 1 | 两个 Worker、任意节点路由、无 sticky session、节点故障接管 | 不包含 Kubernetes、生产自动扩缩容与灰度发布 |
| README 验收标准 2 | tenant、agent、binding、session、event、idempotency、lease、audit 的共享/持久关系 | Memory、Summary、Knowledge、Artifact 留待后续 |
| README 验收标准 4 | 共享短期状态与持久关系状态的职责和一致性取舍基础 | 向量库、对象存储和正式迁移工具不在本阶段 |
| README 验收标准 5 | trace 跨节点、Runner、Session、状态边界、回复与审计传播 | Tool、Memory 和真实 IM callback trace 留待后续 |
| README 验收标准 7 | 继续复用官方 Runner/Session 边界，新增共享状态、租约和故障恢复平台能力 | 不把共享后端能力表述为上游框架原生能力 |
| Constitution I | 官方 Agent 能力保持固定，通过 Adapter 接入共享 Session | 具体适配方式在 plan 核对 SDK 后决定 |
| Constitution II | 所有共享键、查询、审计和指标显式 tenant scoped | 生产 IAM 与密钥系统不在本阶段 |
| Constitution III | Stateless Worker、共享 Session、跨节点幂等、串行和 fencing | 不宣称跨地域或生产级 exactly-once |
| Constitution IV | 复用 Storage/Session 契约并验证 InMemory/共享实现一致 | 真实双 IM 与三类完整后端组合尚未完成 |
| Constitution V–VI | 默认拒绝、秘密引用、故障可见、跨节点 trace 与指标 | 完整治理 Filter 和生产 Telemetry 留待后续 |
| Constitution VII | 可运行、可测试、可演示的第三个纵向切片 | 完成仍需 clarify、plan、tasks、analyze、implement、converge 证据 |

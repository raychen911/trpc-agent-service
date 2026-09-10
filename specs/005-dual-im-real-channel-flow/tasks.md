# Tasks: 双 IM 真实消息闭环

**Input**: specs/005-dual-im-real-channel-flow 下的 spec.md、plan.md、
research.md、data-model.md、clarification-decisions.md、contracts/ 和 quickstart.md
**Baseline**: 005-dual-im-real-channel-flow 分支，基于第三阶段提交 1bbf202
**Tests**: 本功能明确要求测试先行；每个故事先写测试并确认失败，再写实现并运行对应测试
**Decision IDs**: DEC-001 Channel Binding；DEC-002 群聊 Session；
DEC-003 回复重试；DEC-004 长连接 HA；DEC-005 自身消息过滤

## Format

- [P] 表示可在不修改同一文件且不依赖未完成任务时并行。
- [US1] 至 [US5] 对应 spec.md 的五个用户故事。
- 每项任务括号内列出主要 FR、决策和验收证据。

## Phase 1: Setup — 正确基线与安全前置

**Purpose**: 确认第五阶段建立在已完成的第三阶段代码上，并消除真实联调前的凭证风险。

- [X] T001 完成飞书和企业微信测试凭证轮换，并仅将轮换时间、操作者确认和“未记录新值”的结论写入 specs/005-dual-im-real-channel-flow/validation-results.md（FR-028、FR-029；安全门禁）
- [X] T002 在 pyproject.toml 固定 lark-channel-sdk==1.4.0 和 wecom-aibot-python-sdk==1.0.2，并更新 uv.lock（FR-001、FR-030）
- [X] T003 [P] 创建 trpc_service/channels/identity.py、base.py、feishu.py、wecom.py、delivery.py、runtime.py 及 tests/unit/channels/、tests/contract/channels/、tests/integration/channels/ 包结构（FR-001、FR-002）
- [X] T004 [P] 在 specs/005-dual-im-real-channel-flow/runtime-env.example.md 记录仅含变量名和占位符的运行配置，不写入任何 Secret、token、ticket 或连接 URL（FR-028、FR-029）
- [X] T005 运行 uv run pytest -q，并将 139 passed、共享后端跳过原因或新的实际结果写入 specs/005-dual-im-real-channel-flow/validation-results.md（FR-034；基线证据）

**Checkpoint**: 分支基于 1bbf202，依赖可复现，凭证已轮换，001/002/003 回归无新增失败。

---

## Phase 2: Foundational — 所有故事的阻断性契约与模型

**Purpose**: 先建立 SDK 无关的身份、消息、交付、所有权和安全边界。

### Tests first

- [X] T006 [P] 先编写并确认失败：ChannelIdentity、RuntimeBotIdentity、AuthenticatedSender 的完整性、复合摘要和不可打印敏感字段单测，文件 tests/unit/channels/test_identity_models.py（FR-008、FR-018；DEC-001、DEC-005）
- [X] T007 [P] 先编写并确认失败：DeliveryRecord、DeliveryAttempt 状态转换、终态不可逆和 attempt_no 约束单测，文件 tests/unit/channels/test_delivery_models.py（FR-023—FR-025；DEC-003）
- [X] T008 [P] 先编写并确认失败：ChannelAdapterPort、ProviderClientPort、DeliveryRepository、AdapterOwnershipRepository 的 runtime protocol 契约测试，文件 tests/contract/channels/test_channel_ports.py（FR-002、FR-022、FR-030、FR-031）
- [X] T009 [P] 先编写并确认失败：仅通过 SecretProvider/环境变量加载、repr/异常脱敏和缺失凭证 fail closed 单测，文件 tests/unit/channels/test_channel_settings_security.py（FR-028、FR-029）
- [X] T010 [P] 先编写并确认失败：003_dual_im.sql 的表、复合唯一约束、外键和向后兼容 schema 测试，文件 tests/contract/channels/test_dual_im_schema.py（FR-008、FR-022；DEC-001）

### Implementation

- [X] T011 扩展 trpc_service/channels/contracts.py 的 Channel 枚举与统一入站/回复字段，同时保持 LOCAL_HTTP 序列化兼容（FR-003—FR-007、FR-034）
- [X] T012 实现 trpc_service/channels/identity.py 中的 ChannelIdentity、RuntimeBotIdentity、AuthenticatedSender、ProviderReplyContext 和长度前缀摘要（FR-004、FR-008、FR-018；DEC-001、DEC-005）
- [X] T013 实现 trpc_service/channels/base.py 的 SDK 无关 Adapter/Provider 端口、readiness、稳定错误和 ProviderSendAck（FR-001、FR-002、FR-030）
- [X] T014 扩展 trpc_service/storage/models.py 与 trpc_service/storage/contracts.py，加入 Delivery、AdapterFence 领域对象及 Repository 端口（FR-022—FR-025、FR-035、FR-036；DEC-003、DEC-004）
- [X] T015 实现 trpc_service/config/settings.py 的双渠道配置和 SecretProvider 引用加载，确保配置对象与错误统一脱敏（FR-028、FR-029）
- [X] T016 扩展 trpc_service/audit/models.py 的 Adapter、过滤和 Delivery 决策值及非敏感关联字段（FR-026、FR-027、FR-029）
- [X] T017 在 trpc_service/storage/postgres/migrations/003_dual_im.sql 新增 Binding 身份列、delivery_records、delivery_attempts 和索引，不破坏 001/002 schema（FR-008、FR-022；DEC-001、DEC-003）
- [X] T018 在 trpc_service/storage/postgres/repositories.py 实现复合身份查询和 DeliveryRepository 基础 CRUD/条件写（FR-008、FR-022、FR-025；DEC-001、DEC-003）
- [X] T019 [P] 在 tests/support_channels.py 建立不含真实凭证的 Feishu/WeCom SDK 测试替身、虚拟 Clock/Sleeper 和 Agent 调用计数器（FR-030、FR-031）
- [X] T020 运行 T006—T010 对应测试并在 specs/005-dual-im-real-channel-flow/validation-results.md 记录先失败、后通过的命令与结果（测试先行证据）

**Checkpoint**: 公共领域模型、端口、配置和迁移契约通过，才能开始任何用户故事。

---

## Phase 3: User Story 1 — 两种真实 IM 获得 Agent 回复 (Priority: P1) MVP

**Goal**: 飞书和企业微信文本单聊、群聊明确 @ 均进入既有 Gateway/Runner 并回复原会话。

**Independent Test**: 用两种 SDK 替身各发送单聊文本和群聊 @ 文本，验证统一消息、
确定性 Runner 与原会话回复；未 @、自身消息和不支持消息均不调用 Agent。

### Tests first

- [X] T021 [P] [US1] 先编写并确认失败：飞书 message_id、chat_id、sender、结构化 mention、文本规范化和发送映射单测，文件 tests/unit/channels/test_feishu_mapping.py（FR-003—FR-005、FR-014—FR-017）
- [X] T022 [P] [US1] 先编写并确认失败：企业微信 msgid、chatid/userid、sender、req_id 分离和发送映射单测，文件 tests/unit/channels/test_wecom_mapping.py（FR-003、FR-004、FR-006、FR-014—FR-017）
- [X] T023 [P] [US1] 先编写并确认失败：对两种 Adapter 参数化运行同一收发与生命周期行为断言，文件 tests/contract/channels/test_adapter_contract.py（FR-001、FR-002、FR-030、FR-031）
- [X] T024 [P] [US1] 先编写并确认失败：自身消息、身份不确定、群聊未 @、仅 @ 无正文和不支持事件不调用 Agent，文件 tests/contract/channels/test_inbound_filters.py（FR-016—FR-018；DEC-005）
- [X] T025 [P] [US1] 先编写并确认失败：飞书和企业微信有效单聊经 Gateway→Worker→官方 Runner 的集成测试，文件 tests/integration/channels/test_dual_im_happy_path.py（FR-011—FR-015）
- [X] T026 [US1] 先编写并确认失败：成功 UnifiedReply 只向原会话发送一次且重复 reply 为 suppress，文件 tests/integration/channels/test_successful_delivery.py（FR-014、FR-023）

### Implementation

- [X] T027 [P] [US1] 在 trpc_service/channels/feishu.py 实现 lark-channel-sdk ProviderClient 封装、事件字段提取和文本发送，不泄漏 SDK 对象（FR-001—FR-005、FR-014）
- [X] T028 [P] [US1] 在 trpc_service/channels/wecom.py 实现 wecom-aibot-python-sdk ProviderClient 封装，确保 msgid 为业务 ID、req_id 仅作协议上下文（FR-001—FR-003、FR-006、FR-014）
- [X] T029 [US1] 在 trpc_service/channels/base.py 实现统一入口过滤、消息转换、Gateway 调用和回复路由骨架（FR-002—FR-004、FR-011、FR-017、FR-018；DEC-005）
- [X] T030 [US1] 完成 trpc_service/channels/feishu.py 的单聊、群聊结构化 @、自身身份过滤和统一 Adapter 行为（FR-015—FR-018；DEC-005）
- [X] T031 [US1] 完成 trpc_service/channels/wecom.py 的单聊、群聊结构化 @、自身身份过滤和统一 Adapter 行为（FR-015—FR-018；DEC-005）
- [X] T032 [US1] 扩展 trpc_service/tenant/session_identity.py，使群聊 Session 包含 channel、binding、group 和 sender，回复目标仍为原群（FR-019、FR-020；DEC-002）
- [X] T033 [US1] 在 trpc_service/channels/delivery.py 实现成功结果的单次 DeliveryRecord 创建与渠道发送，不在交付路径调用 Runner（FR-014、FR-023；DEC-003）
- [X] T034 [US1] 在 trpc_service/channels/service.py 组合 Adapter、Binding 解析、Gateway 和 DeliveryService，保持 trpc_service/gateway/service.py 不依赖供应商 SDK（FR-002、FR-011、FR-034）
- [X] T035 [US1] 在 trpc_service/_cli.py 增加 feishu/wecom channel-serve 入口和安全 readiness 输出（FR-001、FR-028）
- [X] T036 [US1] 运行 tests/unit/channels、tests/contract/channels/test_adapter_contract.py、test_inbound_filters.py 和双 IM happy-path 测试并记录 US1 证据到 specs/005-dual-im-real-channel-flow/validation-results.md

**Checkpoint**: US1 可用 SDK 替身独立演示双渠道主链路；真实客户端验收留到最终阶段。

---

## Phase 4: User Story 2 — 可信 Binding 与租户隔离 (Priority: P1)

**Goal**: 只根据认证复合 Channel Identity 得出 tenant，未知、禁用、错误绑定全部拒绝。

**Independent Test**: 两租户、两渠道和相同外部 ID 并发处理无混淆；篡改外部 tenant_id
或身份任一组成部分均拒绝，Agent 调用为 0。

### Tests first

- [X] T037 [P] [US2] 先编写并确认失败：按飞书 tenant_key+app/bot 与企微 corp_id+bot 精确查询、任一字段缺失/错配拒绝的 Repository 契约测试，文件 tests/contract/channels/test_channel_binding_identity.py（FR-008—FR-010；DEC-001）
- [X] T038 [P] [US2] 先编写并确认失败：未知/禁用 Binding、禁用 Tenant/Agent 和配置后端不可验证时 fail closed 集成测试，文件 tests/integration/channels/test_binding_rejection.py（FR-010、FR-022）
- [X] T039 [P] [US2] 先编写并确认失败：伪造 tenant_id、跨渠道相同用户/会话/消息 ID 不发生 Session、幂等和 Audit 碰撞，文件 tests/integration/channels/test_tenant_isolation.py（FR-007—FR-010、FR-019）
- [X] T040 [US2] 先编写并确认失败：Binding 拒绝和 sender identity 不确定生成脱敏 pre-auth 安全审计且 Agent 为 0，文件 tests/integration/channels/test_preauth_security_audit.py（FR-010、FR-018、FR-029；DEC-001、DEC-005）

### Implementation

- [X] T041 [US2] 完成 trpc_service/storage/postgres/repositories.py 的 ChannelIdentity 一致性查询、active/ownership/config_version 校验和统一非披露错误（FR-008—FR-010、FR-022；DEC-001）
- [X] T042 [US2] 在 trpc_service/channels/service.py 实现 tenant_id 不可信字段丢弃、VerifiedBindingScope 签发和默认拒绝路径（FR-008—FR-011；DEC-001）
- [X] T043 [US2] 在 trpc_service/audit/models.py 与 PostgreSQL Audit Repository 实现 binding_rejected、sender_identity_unverified 等 pre-auth 审计，保存摘要而非原值（FR-010、FR-018、FR-026、FR-029）
- [X] T044 [US2] 运行 T037—T040 测试并将两租户隔离、所有拒绝 Agent=0 的证据写入 specs/005-dual-im-real-channel-flow/validation-results.md（SC-002、SC-004、SC-012）

**Checkpoint**: US2 可独立证明外部消息不能指定租户，复合身份是唯一授权入口。

---

## Phase 5: User Story 3 — 多节点幂等、会话连续与恢复 (Priority: P1)

**Goal**: 真实 IM 重放和跨节点执行继承第三阶段最多一次、Session 串行与部分提交恢复。

**Independent Test**: 同一 message_id/msgid 跨节点并发 10 次 Agent<=1；同一会话跨
Worker 多轮连续且串行，不同会话并行；回复恢复不重新运行 Agent。

### Tests first

- [X] T045 [P] [US3] 先编写并确认失败：IdempotencyKey 包含 tenant、channel、binding、external_message_id 且跨 scope 不碰撞，文件 tests/unit/channels/test_im_idempotency_identity.py（FR-005—FR-007）
- [X] T046 [P] [US3] 先编写并确认失败：飞书 message_id 与企微 msgid 各跨两个 Adapter/Worker 并发重放 10 次，文件 tests/integration/channels/test_cross_node_im_idempotency.py（FR-021、FR-023）
- [X] T047 [P] [US3] 先编写并确认失败：同一单聊及同群同 sender 跨 Worker 三轮连续、同群不同 sender 隔离，文件 tests/integration/channels/test_cross_node_im_session.py（FR-019—FR-021；DEC-002）
- [X] T048 [P] [US3] 先编写并确认失败：同 Session 最大并发 1、不同 Session 可并行，文件 tests/integration/channels/test_im_session_serialization.py（FR-020、FR-021）
- [X] T049 [US3] 先编写并确认失败：Agent 结果已持久化后 Adapter 重放、发送失败和 Recovery Marker 恢复均不增加 Agent 调用，文件 tests/integration/channels/test_im_partial_commit_recovery.py（FR-023、FR-027；DEC-003）

### Implementation

- [X] T050 [US3] 扩展 trpc_service/storage/models.py 和 Redis codec，使幂等摘要显式包含 channel 且保持 LOCAL_HTTP 兼容（FR-007、FR-021）
- [X] T051 [US3] 完成 trpc_service/tenant/session_identity.py 的 direct/group 长度前缀摘要和 ownership 校验（FR-019、FR-020；DEC-002）
- [X] T052 [US3] 在 trpc_service/channels/service.py 接入第三阶段 Redis claim、Session lease、fencing、Recovery Marker 和 owner/execution trace，不建立 Adapter 本地业务状态（FR-020、FR-021、FR-027）
- [X] T053 [US3] 在 trpc_service/channels/delivery.py 通过 create_or_get 复用已有 ExecutionResult，禁止 duplicate/recovery 路径重新调用 Agent（FR-023；DEC-003）
- [X] T054 [US3] 运行 T045—T049、tests/integration/shared 和既有 HTTP 幂等/Session 回归，将 Agent<=1 与并行性证据写入 specs/005-dual-im-real-channel-flow/validation-results.md（SC-003、SC-005）

**Checkpoint**: US3 证明真实渠道语义没有削弱第三阶段一致性保证。

---

## Phase 6: User Story 4 — 渠道故障与主动/备用恢复 (Priority: P2)

**Goal**: 断线、认证、Gateway 和发送故障具有明确语义；同一 Channel Identity 只有一个活动连接。

**Independent Test**: 用虚拟时钟和两个 Adapter runtime 注入全部故障，验证重连、1/2/4
秒交付重试、delivery_unknown 不重发、接管 generation 和旧 fence 拒绝。

### Tests first

- [X] T055 [P] [US4] 先编写并确认失败：临时失败 1/2/4 秒、总 attempt<=4、永久失败和 unknown 不重试的虚拟时钟单测，文件 tests/unit/channels/test_delivery_retry_policy.py（FR-023—FR-025；DEC-003）
- [X] T056 [P] [US4] 先编写并确认失败：DeliveryRepository 条件转换、并发 claim、read-back 和终态不可逆契约，文件 tests/contract/channels/test_delivery_repository.py（FR-022—FR-025；DEC-003）
- [X] T057 [P] [US4] 先编写并确认失败：AdapterOwnershipRepository acquire/renew/release/generation/fence 契约，文件 tests/contract/channels/test_adapter_ownership.py（FR-035、FR-036；DEC-004）
- [X] T058 [P] [US4] 先编写并确认失败：两个 Adapter 同身份 ready<=1、活动节点终止后接管、旧节点发送/写入=0，文件 tests/integration/channels/test_adapter_takeover.py（FR-035、FR-036；DEC-004）
- [X] T059 [P] [US4] 先编写并确认失败：网络断线按 1/2/4/8/16/30 秒封顶+jitter 重连、稳定 60 秒重置、失租取消，认证失败等待配置变化，文件 tests/unit/channels/test_connection_lifecycle.py（FR-025、FR-035、FR-036）
- [X] T060 [US4] 先编写并确认失败：Gateway/Redis/SQL 暂不可用不降级、不无界排队且平台重放仍幂等，文件 tests/integration/channels/test_channel_backend_outages.py（FR-021—FR-025）

### Implementation

- [X] T061 [P] [US4] 在 trpc_service/channels/base.py 实现 transient/permanent/unknown/auth/connection 稳定错误分类，未知 vendor 异常默认 unknown（FR-024、FR-025；DEC-003）
- [X] T062 [US4] 完成 trpc_service/storage/postgres/repositories.py 的 Delivery attempt 原子条件写、due 查询与 read-back 收敛（FR-022—FR-025；DEC-003）
- [X] T063 [US4] 在 trpc_service/channels/delivery.py 实现可取消虚拟时钟重试器、1/2/4 秒策略和不经过 Runner 的恢复（FR-023—FR-025；DEC-003）
- [X] T064 [US4] 在 trpc_service/storage/redis_adapter_leases.py 及 trpc_service/storage/redis_scripts/adapter_lease_*.lua 实现 Adapter ownership acquire/renew/release/ready 与 fencing（FR-035、FR-036；DEC-004）
- [X] T065 [US4] 在 trpc_service/channels/runtime.py 实现 STANDBY→AUTHENTICATING→READY→DRAINING 生命周期、续租 heartbeat、失租关连接和新 generation 接管（FR-035、FR-036；DEC-004）
- [X] T066 [P] [US4] 在 trpc_service/channels/feishu.py 和 trpc_service/channels/wecom.py 接入统一重连退避、认证永久失败与可取消 close 语义（FR-025、FR-035）
- [X] T067 [US4] 在 trpc_service/_cli.py 和 trpc_service/web/app.py 增加 Adapter readiness/standby 状态，输出不得包含身份原值或凭证（FR-028、FR-035、FR-036）
- [X] T068 [US4] 运行 T055—T060 故障测试并把 attempt、退避、delivery_unknown、接管和旧 fence 证据写入 specs/005-dual-im-real-channel-flow/validation-results.md（SC-006、SC-011）

**Checkpoint**: US4 可独立证明故障恢复不会重复 Agent 或形成双活发送。

---

## Phase 7: User Story 5 — 跨渠道 Trace 与 Audit (Priority: P2)

**Goal**: 一条真实消息可由 trace_id 串起 Adapter、Gateway、Worker、Runner 和 Delivery，且证据无 Secret。

**Independent Test**: 两种渠道各处理一条成功、重复和失败消息，通过 trace 查询核对
五类 trace、Adapter generation、Session、执行节点与交付状态。

### Tests first

- [X] T069 [P] [US5] 先编写并确认失败：trace_id、first_claim、owner、execution、delivery trace 跨组件传播测试，文件 tests/integration/channels/test_im_trace_propagation.py（FR-026、FR-027）
- [X] T070 [P] [US5] 先编写并确认失败：按 tenant/trace 查询 Adapter、Binding、Execution、Delivery Audit 且跨租户拒绝，文件 tests/contract/channels/test_im_audit_query.py（FR-026、FR-027）
- [X] T071 [P] [US5] 先编写并确认失败：连接、过滤、接管、交付指标仅使用低基数安全标签，文件 tests/unit/channels/test_channel_metrics.py（FR-026、FR-029）
- [X] T072 [US5] 先编写并确认失败：日志、异常、Audit、测试证据和 Git diff 中 Secret/token/ticket/access_key/完整 WebSocket URL 为 0，文件 tests/unit/channels/test_channel_redaction.py（FR-028、FR-029）

### Implementation

- [X] T073 [US5] 完成 trpc_service/audit/models.py 与 PostgreSQL Audit Repository 的 Adapter/Delivery 关联字段、租户作用域查询和 diagnostic late-ack/fence 记录（FR-026、FR-027、FR-029）
- [X] T074 [P] [US5] 在 trpc_service/metrics/models.py 与 trpc_service/metrics/shared.py 增加 adapter_connection、filter、takeover、delivery attempt/success/unknown/latency 指标（FR-026、FR-029）
- [X] T075 [US5] 在 trpc_service/_cli.py 增加按 trace 输出脱敏验收摘要的诊断命令，不输出原始用户/消息/连接信息（FR-026、FR-029、FR-033）
- [X] T076 [US5] 运行 T069—T072，并将飞书/企微各一条替身消息的完整脱敏 trace 证据写入 specs/005-dual-im-real-channel-flow/validation-results.md（SC-007、SC-008）

**Checkpoint**: US5 可独立用脱敏证据解释成功、重复、拒绝、失败和接管。

---

## Phase 8: Polish & Cross-Cutting Validation

**Purpose**: 完成全量回归、真实客户端验收、安全检查和答辩记录。

- [X] T077 运行 uv run pytest -q，确认 001/002/003/005 离线测试全部通过并记录总数到 specs/005-dual-im-real-channel-flow/validation-results.md（FR-034、SC-009）
- [X] T078 启动第三阶段 Redis/PostgreSQL profile 后运行所有 shared_backend、tests/integration/channels 和 tests/e2e 测试，将未跳过结果写入 specs/005-dual-im-real-channel-flow/validation-results.md（FR-021、FR-022、FR-032）
- [X] T079 按 quickstart.md 完成飞书真实客户端三轮单聊、群聊 @、未 @ 和 trace 验收，并将脱敏截图索引写入 specs/005-dual-im-real-channel-flow/validation-results.md（FR-033、SC-001）
- [X] T080 按 quickstart.md 完成企业微信真实客户端三轮单聊、群聊 @、未 @ 和 trace 验收，并将脱敏截图索引写入 specs/005-dual-im-real-channel-flow/validation-results.md（FR-033、SC-001）
- [X] T081 完成双 Adapter 主动/备用人工接管演示，记录 ready<=1、generation 递增和旧节点发送/写入=0 的结果到 specs/005-dual-im-real-channel-flow/validation-results.md（SC-011；DEC-004）
- [X] T082 执行工作区、Git diff 与历史敏感信息扫描，确认新旧凭证明文为 0，并把工具、范围和结论写入 specs/005-dual-im-real-channel-flow/validation-results.md（FR-028、FR-029、SC-008）
- [X] T083 [P] 更新 README.md 的双 IM 启动、限制和证据入口，并生成 specs/005-dual-im-real-channel-flow/阶段成果记录.md，区分自动化替身与真实客户端证据（FR-033）
- [X] T084 逐项执行 specs/005-dual-im-real-channel-flow/quickstart.md，核对 SC-001—SC-012 后完成最终验收记录，未通过项不得标记完成

---

## Dependencies & Execution Order

### Phase Dependencies

~~~text
Phase 1 Setup
  -> Phase 2 Foundation
      -> US1 双渠道主链路
          -> US2 Binding/租户隔离
          -> US3 多节点幂等/Session/恢复
          -> US4 故障与主动/备用
          -> US5 Trace/Audit
              -> Phase 8 全量与真实验收
~~~

- US1 是 MVP，提供两种渠道的最小正式主链路。
- US2 和 US3 在 US1 后可并行，但两者测试分别保持独立。
- US4 依赖 US1 的发送入口和 Phase 2 的 Delivery/ownership 端口。
- US5 可在 US1 后开展，但最终证据依赖 US2—US4 的拒绝、恢复与接管事件。
- 任何实现任务都不得跳过同阶段先行测试及“确认失败”证据。

### User Story Requirement Mapping

| Story | Main FR | Decisions | Independent evidence |
|---|---|---|---|
| US1 | FR-001—FR-006、FR-011—FR-018、FR-030—FR-031 | DEC-002、DEC-005 | 两种 Adapter 统一契约、单聊/群聊 @ 主链路 |
| US2 | FR-007—FR-010、FR-018—FR-019、FR-022、FR-029 | DEC-001、DEC-005 | 两租户隔离、未知/禁用/错配 Agent=0 |
| US3 | FR-005—FR-007、FR-019—FR-023、FR-027、FR-032 | DEC-002、DEC-003 | 重复 10 次 Agent<=1、跨 Worker 多轮 |
| US4 | FR-021—FR-025、FR-035—FR-036 | DEC-003、DEC-004 | 退避/unknown/接管/旧 fence 故障矩阵 |
| US5 | FR-026—FR-029、FR-033 | DEC-001—DEC-005 | 跨组件 trace、Audit 查询、Secret=0 |

## Parallel Opportunities

- T003/T004、T006—T010、T021—T025、T037—T039、T045—T048、
  T055—T059、T069—T071 可按标记并行。
- T027 飞书 Provider 与 T028 企业微信 Provider 可并行，之后汇合到 T029。
- T041/T042/T043 不并行修改同一授权路径，按顺序完成以避免契约漂移。
- T061 与 T064 可并行；T063 等待 T061/T062，T065 等待 T064。
- T079 与 T080 可在自动化和安全门禁通过后分别执行。

### Parallel Example: US1

~~~text
Task A: T021 飞书映射测试 -> T027 飞书 Provider
Task B: T022 企业微信映射测试 -> T028 企业微信 Provider
Task C: T023 参数化 Adapter 契约
汇合: T029 通用编排 -> T030/T031 -> T036
~~~

### Parallel Example: US4

~~~text
Task A: T055/T056 -> T062/T063 Delivery 恢复
Task B: T057/T058 -> T064/T065 Adapter ownership
Task C: T059 -> T066 SDK 重连
汇合: T067 readiness -> T068 故障验收
~~~

## Implementation Strategy

### MVP First

1. 完成 Phase 1 和 Phase 2。
2. 完成 US1，并只使用 SDK 替身和确定性 Runner 验证。
3. 停止并检查 Adapter 契约、SDK 隔离、self-message 和群聊 @。
4. 再加入 US2/US3 的安全与一致性能力；不得用 MVP 结果宣称第五阶段完成。

### Incremental Delivery

1. Foundation：身份、端口、Secret、Delivery/Lease 模型。
2. US1：双 IM 主链路。
3. US2：可信 Binding 与租户隔离。
4. US3：共享状态幂等、Session 与恢复。
5. US4：交付故障和主动/备用接管。
6. US5：Trace/Audit。
7. 全量 shared 测试和两个真实客户端验收。

## Completion Rules

- 所有任务必须保持严格的 checkbox、Txxx、可选 [P]、用户故事 [USx] 和文件路径格式。
- 同一故事的测试任务必须在实现前运行并确认失败。
- 自动化测试替身结果与真实客户端结果必须分开记录。
- T001、T005、T020、T036、T044、T054、T068、T076、T077—T084 是阶段门禁。
- 未完成凭证轮换、共享后端验收、真实双客户端验收或 Secret 扫描时，不得声明完成。

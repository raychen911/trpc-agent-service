# Implementation Plan: 双 IM 真实消息闭环

**Branch**: 005-dual-im-real-channel-flow
**Date**: 2026-09-08
**Spec**: [spec.md](./spec.md)
**Decision Record**: [clarification-decisions.md](./clarification-decisions.md)

## Summary

本阶段把已经完成最小连通性验证的飞书和企业微信长连接 SDK 封装为两个独立
Channel Adapter，并接入第三阶段既有 Gateway、可信 Channel Binding、多租户
幂等、tenant-scoped Session、无状态 Agent Worker、官方 tRPC-Agent Runner、
Redis/PostgreSQL 共享状态、generation/fencing、恢复和审计链路。

两个 Adapter 先在入口完成认证身份提取、自身消息过滤、消息类型与群聊 @ 判断，
再生成统一入站消息；Gateway 只从可信复合 Channel Identity 查询绑定并推导
tenant_id。Runner 结果先按第三阶段语义持久化，再由原渠道执行独立的回复交付。
明确临时发送失败按 1、2、4 秒最多重试 3 次，永久失败终止，未知发送结果进入
delivery_unknown 且禁止自动重发，任何交付恢复均不得重新调用 Agent。

## Implementation Baseline

Git 基线已整理完成：

1. 实现仓库为 trpc-agent-service-submit。
2. 功能分支为 005-dual-im-real-channel-flow。
3. 分支继承第三阶段提交 1bbf202，未改写历史。
4. specs/005-dual-im-real-channel-flow 已完整迁入该仓库。
5. 基线回归结果为 139 passed、26 skipped；跳过项均因本次未启动
   Redis/PostgreSQL shared profile，需在最终共享后端验收时补跑。

仍有一项安全门禁：独立 Echo 验证目录曾出现凭证明文注释，相关飞书和企业微信
凭证必须由操作者轮换并确认清理后，才能启动真实 Adapter、执行真实客户端验收或
提交第五阶段实现。

## Technical Context

**Language/Version**: Python 3.12，项目约束 >=3.12,<3.13
**Primary Dependencies**: trpc-agent-py==1.1.19、
lark-channel-sdk==1.4.0、wecom-aibot-python-sdk==1.0.2、
Starlette==1.6.0、Uvicorn==0.52.4、Pydantic==2.13.5、
redis==8.1.0、SQLAlchemy==2.0.52、asyncpg==0.31.0
**Storage**: Redis 7.4.11 保存幂等、短期 Session、消息/Session 租约和
Adapter Ownership Lease；PostgreSQL 17.11 保存租户、Channel Binding、
Audit、Delivery Record/Attempt 和 Recovery Marker
**Testing**: pytest、pytest-asyncio、SDK 测试替身、参数化 Adapter 契约测试、
真实 Redis/PostgreSQL 集成测试、双 Adapter 节点故障接管测试、人工真实客户端验收
**Target Platform**: Windows/Linux 本地开发机；Redis/PostgreSQL 使用既有本地
shared profile；Adapter 需要访问飞书和企业微信 WebSocket 服务
**Project Type**: Python 异步服务，由 Channel Adapter、Gateway 和 Worker 组成
**Performance Goals**: 确定性 Runner 下非故障文本消息本地处理 p95 小于 2 秒；
Adapter 回调不得阻塞事件循环；不同 Session 可并行；同一 Session 最大执行并发为 1
**Constraints**: 不依赖 sticky session；tenant_id 不来自外部消息；同一业务消息
Agent 最多执行一次；同一 Channel Identity 活动连接不超过 1；所有 Secret 仅通过
环境变量或 Secret Provider 注入；不支持媒体、卡片、流式业务回复或真实模型 API
**Scale/Scope**: 飞书与企业微信各 1 个测试机器人、至少 2 个租户、每种渠道单聊与
群聊 @、两个 Adapter 节点、两个 Worker、10 路重复投递、3 轮真实多轮会话

所有产品语义歧义已由 DEC-001 至 DEC-005 解决。SDK 字段兼容性和基线同步属于
实现前验证项，不是未决产品决策。

## Constitution Check

### Pre-Research Gate

| Principle | Result | Evidence |
|---|---|---|
| I. Framework-First | PASS | 固定 trpc-agent-py 1.1.19，继续通过既有 AgentExecutor 使用官方 Runner，不复制 SDK |
| II. Tenant Isolation | PASS | tenant_id 仅由已认证复合 Channel Identity 对应的 Binding 推导；所有键和审计保持 tenant scope |
| III. Stateless Workers | PASS | Adapter/Worker 不保存业务状态；Redis/PostgreSQL 承担共享状态和恢复 |
| IV. Contract-First | PASS | 两种 SDK 均先映射统一入站/出站契约；SDK 类型不进入 Gateway、Worker 或 Repository |
| V. Security by Default | CONDITIONAL PASS | 设计满足 Secret 引用、脱敏和默认拒绝；实现前必须完成已发现测试凭证的轮换与明文清理 |
| VI. Observability | PASS | trace_id、owner_trace_id、execution_trace_id、delivery trace 和 Adapter generation 可关联 |
| VII. Vertical Slice | PASS | SDK 替身、共享后端集成和两个真实客户端验收分别提供证据 |
| Version Control Gate | PASS | 005 功能分支继承 1bbf202，第五阶段文档已位于正确 Git 仓库 |

**Planning gate result**: Git 与设计门禁通过；真实凭证轮换仍是开始真实连接前的
安全硬门禁。不存在可用进度豁免的租户隔离或凭据保护例外。

## Framework Reuse and Platform Ownership

### 直接复用 tRPC-Agent

- LlmAgent、Runner、Event、Content、Part 和最终回复选取语义。
- BaseSessionService 公共边界及第三阶段 FencedRedisSessionService 适配。
- 确定性离线模型和第一阶段 SDK 兼容性验证。

### 直接复用第二、第三阶段平台能力

- GatewayService 的授权、幂等、Session 串行、Worker 调用和统一回复。
- RedisIdempotencyRepository、RedisSessionLeaseManager、
  FencedRedisSessionService 及 generation/fencing。
- PostgreSQL 配置、租户、Binding、Audit 和 Recovery Repository。
- owner_trace_id、execution_trace_id、部分提交恢复和双 Worker 无状态运行方式。
- 既有 HTTP v1 入口和统一 OutboundReply 兼容语义。

### 第五阶段新增的平台能力

- FeishuChannelAdapter 与 WeComChannelAdapter。
- ChannelIdentity、RuntimeBotIdentity、ProviderReplyContext 和统一解析结果。
- 通过复合渠道身份查询 Binding 的 Repository 端口与 SQL 唯一约束。
- DeliveryRecord、DeliveryAttempt、失败分类、有限重试和状态收敛。
- AdapterOwnershipLease 主动/备用连接编排，复用第三阶段租约和 fencing 原语。
- 两种 SDK 测试替身、共享 Adapter 契约套件和真实客户端证据模板。

## Architecture

### Runtime Topology

~~~text
Feishu Client                         WeCom Client
     |                                     |
Feishu Platform                        WeCom Platform
     | WebSocket                           | WebSocket
     v                                     v
Feishu Adapter A/B                   WeCom Adapter A/B
  active / standby                     active / standby
     | trusted ChannelIdentity              |
     +------------------+-------------------+
                        v
                    Gateway
       Binding -> Idempotency -> Session Lease
                        |
                 Agent Worker A/B
                        |
             official tRPC-Agent Runner
                        |
             persisted Unified Reply
                        |
       original Adapter -> provider delivery
                        |
                Audit / Delivery Evidence

Redis: idempotency, session, message/session/adapter leases, fencing
PostgreSQL: tenant, binding, audit, delivery attempts, recovery
~~~

### Inbound Sequence

1. Adapter 节点按 Channel Identity 竞争共享所有权租约。
2. 只有当前 generation 的租约持有者进入 ready 并建立 SDK 长连接。
3. SDK 回调提供认证连接上下文和原始事件，Adapter 先提取 RuntimeBotIdentity。
4. 校验 sender_type/sender_id；自身消息静默忽略，身份不确定则安全拒绝并写入
   pre-auth 安全审计。
5. 仅接受文本单聊和群聊中结构化字段确认已 @ 当前机器人的消息。
6. Adapter 从认证上下文构造 ChannelIdentity，不读取外部 tenant_id。
7. PostgreSQL 按完整复合身份解析唯一活动 Binding；缺失、禁用或不匹配默认拒绝。
8. Adapter 生成 trace_id，将平台 message_id/msgid、会话、用户和规范化文本转换为
   UnifiedInboundMessage 与 ProviderReplyContext。
9. Gateway 复用第三阶段幂等、Session lease、Worker、Runner、finalization 和恢复。
10. Runner 结果持久化成功后返回 UnifiedReply；重复投递若已有结果只复用结果，
    不重新调用 Agent。

### Outbound Sequence

1. DeliveryService 基于原 Channel Binding 和 ProviderReplyContext 创建 DeliveryRecord。
2. 校验当前 Adapter Ownership Lease、generation 和 fencing token。
3. 原 Adapter 将 UnifiedReply 映射为渠道文本回复并执行一次 DeliveryAttempt。
4. 成功回执转为 delivered；明确临时失败按 1、2、4 秒最多重试 3 次。
5. 永久失败直接进入 delivery_failed；ACK 超时或结果不可确认进入
   delivery_unknown，禁止自动重发。
6. 每次 attempt 与最终状态写入 PostgreSQL，并关联 trace_id、
   execution_trace_id、binding_id 和 adapter_generation。
7. 任何出站恢复只读取已保存的 UnifiedReply，不允许回到 Runner 路径。

## Clarification Decision Mapping

| Decision | Architecture | Data Model | State Machine / Error | Tests |
|---|---|---|---|---|
| DEC-001 Binding 身份键 | Adapter 从认证上下文构造身份，Repository 精确查 Binding | ChannelIdentity 和 SQL 复合唯一键 | 任一字段缺失或错配为 binding_rejected，Agent 0 次 | 渠道、企业、机器人任一字段变更均拒绝；跨租户碰撞为 0 |
| DEC-002 群聊 Session | Gateway 的 Session Key Builder 在群聊加入 group+sender | SessionScope 区分 direct/group | 同群不同 sender 不共享历史；回复仍回原群 | 双用户同群多轮隔离、单用户上下文连续 |
| DEC-003 回复重试 | 执行与交付解耦，DeliveryService 只消费持久化结果 | DeliveryRecord/Attempt | transient: 1/2/4 秒；permanent: failed；unknown: 不自动重发 | 虚拟时钟验证次数/退避；所有恢复 Agent 增量 0 |
| DEC-004 长连接 HA | 每身份主动/备用，共享租约决定唯一连接 | AdapterOwnershipLease 含 generation/fence | 失租立即 not_ready/close；旧代发送和写入拒绝 | 双节点活动数 <=1，接管后旧节点成功操作 0 |
| DEC-005 自身消息过滤 | 统一转换前用认证 sender 与 runtime bot 比较 | RuntimeBotIdentity/AuthenticatedSender | self_message ignored；identity_unverified rejected+audit | 自身、缺失身份、伪造显示名分别验证 |

## Channel and Contract Design

详细契约见 contracts/：

- channel-adapter.md：生命周期、可信身份、入站解析、出站发送和 SDK 隔离。
- unified-message.md：统一入站消息、回复上下文、Session 和幂等键规则。
- delivery-and-ownership.md：交付状态机、重试、主动/备用租约和 fencing。

核心约束：

- Channel 枚举新增 FEISHU 和 WECOM，但保持 LOCAL_HTTP 不变。
- VerifiedBindingScope 只能由认证并匹配 Binding 的 Adapter 工厂签发。
- ProviderReplyContext 只能存在于 Channel/Delivery 层；Gateway 不接触 SDK frame。
- 企业微信 req_id 仅作为协议回复上下文，msgid 才是业务幂等 ID。
- 原始 payload 不进入日志、Audit、Redis key 或普通 SQL 字段。

## Shared State and Persistence

### Redis

- Adapter lease key 按规范化 Channel Identity digest 分区，并使用独立单调 generation。
- 复用第三阶段原子 acquire/renew/release/fence 语义；默认租约 10 秒、3 秒续期。
- 继续使用 tenant+channel+binding+external_message_id 的幂等键。
- Session key 对单聊包含 tenant+channel+binding+conversation；群聊再加入 sender。
- 旧 Adapter generation 不能发送、创建 DeliveryAttempt 或推进最终状态。

### PostgreSQL

- channel_bindings 增加 provider_tenant_key、provider_app_or_bot_id 和
  channel_identity_digest，建立按渠道身份的活动唯一约束。
- 新增 delivery_records 和 delivery_attempts；回复正文使用既有安全结果引用或
  受控字段，不保存 Secret、token、ticket 或原始 SDK frame。
- Audit 增加 adapter_node_id、adapter_generation、delivery_status 和
  provider_message_digest 等非敏感字段。
- Migration 只通过显式 init 命令运行；启动时只验证 schema，不自动迁移。

## Error and Recovery Semantics

| Condition | Stable Result | Retry | Agent |
|---|---|---|---:|
| 认证失败/Secret 无效 | adapter_auth_failed, not_ready | 连接层有界退避，人工修复凭证 | 0 |
| 未知或禁用 Binding | binding_rejected | 不自动重试业务消息 | 0 |
| 自身消息 | self_message_ignored | 无 | 0 |
| 发送者身份不确定 | sender_identity_unverified | 无，安全审计 | 0 |
| 不支持消息/群聊未 @ | unsupported_or_not_addressed | 无业务回复 | 0 |
| Gateway/共享后端执行前不可用 | 既有 retryable 503 语义 | 同 ID 由平台重放后重试 | 0 |
| 明确临时发送失败 | delivery_retrying/failed | 1、2、4 秒，最多 3 次 | 不增加 |
| 永久发送失败 | delivery_failed | 0 | 不增加 |
| 发送 ACK 超时/结果不明 | delivery_unknown | 自动重发 0 | 不增加 |
| Adapter 失租/旧 fence | adapter_lease_lost | 新 generation 接管 | 旧节点 0 |

连接重连与业务重试分离：活动 owner 遇到明确网络断线时按
1、2、4、8、16、30 秒上限并附加最多 20% jitter 重连；连接稳定 60 秒后重置退避。
只要仍持有租约，30 秒封顶的重连循环可以继续；失租或进程关闭必须立即取消。
明确凭证无效属于永久认证失败，Adapter 进入 not_ready，不自动反复认证，只有
配置版本/Secret 引用变化或人工重启后才重新认证。所有平台重放事件仍进入共享幂等
流程。Gateway 不可用时 Adapter 不在内存中建立无界消息队列，也不绕过共享状态；
让平台重放或记录可观察失败。

## Security and Secret Handling

- 生产 Adapter 仅接受 SecretProvider 返回的短生命周期 SecretBytes。
- 环境变量只保存引用约定：飞书 LARK_APP_ID/LARK_APP_SECRET，企业微信
  WECOM_BOT_ID/WECOM_BOT_SECRET；示例只写变量名和占位符。
- Channel Binding 持久化 secret_ref，不保存 secret value。
- 对异常、日志、trace、Audit 和测试快照执行统一 redaction。
- 禁止记录 WebSocket URL 查询参数、access_key、ticket、完整 token、SDK frame。
- 实现前必须轮换已经写入 Echo 验证脚本注释的真实凭证，清理文件并执行历史扫描；
  轮换完成前真实 Adapter 不得启动。

## Observability

每条消息至少关联：

- trace_id：本次 Adapter 收到的投递。
- first_claim_trace_id/owner_trace_id/execution_trace_id：复用第三阶段语义。
- adapter_node_id/adapter_generation：当前长连接所有者。
- provider_message_digest、channel、binding_id_digest、session_id。
- delivery_id、attempt_no、delivery_status、failure_class 和安全错误码。

指标新增 adapter_connection_state、adapter_takeover_total、
channel_message_total、channel_filter_total、delivery_attempt_total、
delivery_success_total、delivery_unknown_total 和 delivery_latency_ms。
指标标签不得包含原始 tenant/user/message/conversation ID 或任何 Secret。

## Testing Strategy

### Test-First Order

1. 扩展统一 Channel、ChannelIdentity、Session key 和 Delivery 状态机的失败单测。
2. 建立参数化 ChannelAdapter 契约套件和 Feishu/WeCom SDK 测试替身。
3. 实现飞书入站解析、自身过滤、Binding 解析和出站映射。
4. 实现企业微信对应能力，并让同一契约套件通过。
5. 实现 Delivery Repository、失败分类和 1/2/4 秒虚拟时钟重试。
6. 实现 Adapter Ownership Lease 和双节点 fencing 测试。
7. 接入真实第三阶段 Gateway/Redis/PostgreSQL，验证幂等、Session、恢复和 trace。
8. 最后执行两个真实客户端的手工验收与证据脱敏检查。

### Required Layers

- Unit：字段解析、规范化、@ 判断、Session key、错误分类、状态转换、redaction。
- Contract：同一套 Adapter 行为断言对飞书和企业微信运行；不依赖 vendor payload。
- Integration：真实 Redis/PostgreSQL、两个 Adapter runtime、两个 Worker，
  重复消息、跨节点多轮、同群不同用户隔离。
- Fault injection：断线、认证失败、Gateway outage、发送临时/永久/未知失败、
  Adapter 失租和旧 fence。
- Regression：001、002、003 全量测试以及既有 HTTP v1 契约继续通过。
- Security：未知绑定、跨租户、身份不确定、self-loop、Secret/URL/ticket 扫描。
- Manual acceptance：飞书和企业微信分别完成单聊 3 轮、群聊 @、trace 查询。

## Local Operations and Rollback

- Adapter 使用独立 CLI 入口或 runtime profile 启动，不改变既有 HTTP shared serve。
- 未配置某一渠道 Secret 时，该 Adapter 为 disabled/not_ready，不影响另一渠道及
  HTTP profile。
- 回滚仅停止第五阶段 Adapter 与 Delivery worker，第三阶段 Gateway/Worker/Redis/
  PostgreSQL 保持运行；数据库新增表和字段向后兼容，不做破坏性 down migration。
- 凭证轮换时先使旧 Binding/Adapter not_ready，再更新 Secret Provider 并重启；
  不把 Secret 写入数据库或命令历史。
- 任一后端不可验证时 fail closed，禁止退回 InMemory 处理真实 IM。

## Project Structure

### Documentation

~~~text
specs/005-dual-im-real-channel-flow/
├── spec.md
├── clarification-decisions.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── channel-adapter.md
│   ├── unified-message.md
│   └── delivery-and-ownership.md
└── tasks.md                      # 由 speckit-tasks 生成
~~~

### Source Code

~~~text
trpc_service/
├── _cli.py                       # 增加 feishu/wecom adapter 启动入口
├── channels/
│   ├── contracts.py              # 扩展 Channel、统一入站/出站契约
│   ├── identity.py               # ChannelIdentity 与认证身份
│   ├── base.py                   # ChannelAdapter/ProviderClient 端口
│   ├── feishu.py                 # 飞书 SDK Adapter
│   ├── wecom.py                  # 企业微信 SDK Adapter
│   ├── delivery.py               # 回复状态机和有限重试
│   └── runtime.py                # 主动/备用生命周期与 readiness
├── gateway/service.py            # 复用主链路，仅接收可信 scope
├── tenant/session_identity.py    # 群聊按 group+sender 隔离
├── storage/
│   ├── contracts.py              # 身份查询、Delivery、Adapter lease 端口
│   ├── models.py                 # Delivery 与 Adapter lease 领域模型
│   ├── redis_leases.py           # 复用/扩展 ownership generation/fence
│   └── postgres/
│       ├── repositories.py       # Binding 身份查询、Delivery Repository
│       └── migrations/
│           └── 003_dual_im.sql
└── audit/models.py               # 渠道与交付审计字段

tests/
├── unit/channels/
├── contract/channels/
├── integration/channels/
├── integration/shared/
└── e2e/
    ├── test_feishu_client_acceptance.py
    └── test_wecom_client_acceptance.py
~~~

**Structure Decision**: 保持现有单 Python 包结构；所有供应商代码只进入
trpc_service/channels，核心 Gateway、Worker 和 Storage 继续面向平台契约。

## Post-Design Constitution Check

| Gate | Result |
|---|---|
| 框架复用/平台新增边界 | PASS，职责已逐项列明 |
| 租户与 Session 隔离 | PASS，可信身份解析与群聊 sender scope 已进入模型和契约 |
| 多节点一致性 | PASS，直接复用第三阶段幂等/Session，并新增 Adapter ownership fence |
| Channel/Storage 契约 | PASS，三份契约明确阻止 SDK 类型泄漏 |
| 安全 | CONDITIONAL PASS，设计完整；凭证轮换与清理仍是实现前硬门禁 |
| Trace/Audit | PASS，执行与交付两个阶段可由 trace 关联 |
| 测试与证据 | PASS，自动化与真实客户端证据边界明确 |
| Git 可追溯性 | PASS，005 分支基于 1bbf202 且全部规划产物已进入正确仓库 |

## Complexity Tracking

没有申请宪法例外。新增 Delivery 状态机与 Adapter Ownership Lease 是满足已澄清的
故障恢复和唯一活动连接要求所必需；它们复用既有 Repository、Redis lease 和
fencing 模式，不形成第二套业务执行链路。

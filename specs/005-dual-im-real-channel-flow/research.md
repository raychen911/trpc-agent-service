# 第五阶段技术研究与选型

**功能**：005-dual-im-real-channel-flow
**日期**：2026-09-08

## 研究结论

第五阶段不存在未解决的规划问题。产品边界由 DEC-001 至 DEC-005
固定；技术选型以第三阶段提交 1bbf202、两个已验证 Echo 程序及其本地锁定依赖为依据。

## R-001：正式实现基线

**Decision**：实现基于 trpc-agent-service-submit 的第三阶段提交 1bbf202；
已创建 005-dual-im-real-channel-flow 分支并把第五阶段文档同步进入该 Git 仓库。

**Rationale**：该提交基线已经包含 Gateway、Worker、Redis/PostgreSQL、
Repository、fencing、恢复和完整 002/003 测试。早期副本无提交且缺少这些模块，
在其中实施会造成重复实现并失去回归证据。

**Alternatives considered**：

- 在早期副本重新实现 002/003：拒绝，会制造架构分叉。
- 在旧副本继续维护第五阶段：拒绝，当前已明确所有代码和文档以 submit 仓库为准。

## R-002：飞书 SDK 依赖

**Decision**：固定 lark-channel-sdk==1.4.0，通过 FeishuChannel 封装连接、
message 事件和 send，不让 SDK 消息对象越过 FeishuChannelAdapter。

**Rationale**：该版本已在本机完成真实长连接 Echo 验证；固定精确版本满足可复现
要求。正式 Adapter 只依赖经测试确认的公开入口，并在本包边界转换为平台 DTO。

**Alternatives considered**：

- 使用浮动 >=1.4.0：拒绝，升级可能改变字段和错误语义。
- 直接在 Gateway 中使用 FeishuChannel：拒绝，违反 Channel 契约边界。
- 改用 URL callback：拒绝，本阶段已明确复用已验证长连接。

## R-003：企业微信 SDK 依赖

**Decision**：固定 wecom-aibot-python-sdk==1.0.2，封装 WSClient、
WSClientOptions、message.text、reply/reply_stream 或 send_message。
业务幂等仅使用 msgid，req_id 只作为协议回复关联。

**Rationale**：该版本已完成真实认证、接收和回复验证。SDK 源码表明回调回复透传
headers.req_id，而主动发送使用 chatid；因此必须把协议关联与业务消息身份分开。

**Alternatives considered**：

- 用 req_id 作为 external_message_id：拒绝，req_id 是协议帧关联，不代表业务消息。
- 保存完整 frame 供恢复：拒绝，frame 含不必要字段、不可稳定持久化且可能泄密。
- 只支持回调内即时 Echo：拒绝，不能接入异步 Gateway 与持久化交付恢复。

## R-004：Adapter 契约边界

**Decision**：每个 SDK 分成 ProviderClientPort 与 ChannelAdapter 两层。前者只处理
SDK 连接/发送，后者负责认证身份、过滤、标准化、Gateway 调用和回复映射。

**Rationale**：测试替身可以完全替换 ProviderClientPort；Gateway 只看到统一
消息与可信 VerifiedBindingScope。SDK 升级只影响具体 ProviderClient。

**Alternatives considered**：

- 一个通用 Adapter 内用大量 provider 条件分支：拒绝，供应商语义相互污染。
- 把原始 payload 传给 Gateway：拒绝，破坏统一契约和日志安全。

## R-005：可信 Channel Identity 与 Binding 查询

**Decision**：按 DEC-001 使用复合身份：

- 飞书：channel + tenant_key + app_id_or_bot_id。
- 企业微信：channel + corp_id + bot_id。

所有字段来自已认证连接或 SDK 事件上下文，先规范化再计算 digest，并由 PostgreSQL
一次一致性查询验证 Binding、Tenant、Agent 的所有权和 active 状态。

**Rationale**：企业身份和机器人身份同时参与，可防止跨企业和同企业多机器人误绑。
tenant_id 仅由查询结果产生，外部负载无法提升权限。

**Alternatives considered**：只用 bot/app、只用企业 ID、只用人工 binding_id 均因
身份边界不完整而被 DEC-001 拒绝。

## R-006：Session 与幂等身份

**Decision**：

- 单聊 Session：tenant + channel + binding + direct conversation。
- 群聊 Session：tenant + channel + binding + group conversation + sender。
- 幂等：tenant + channel + binding + external_message_id。

所有外部标识进入长度前缀哈希，不直接用分隔符拼接或写入 Redis key。

**Rationale**：满足同群不同用户上下文隔离，同时保留个人多轮；渠道和 Binding
参与幂等避免不同渠道或租户外部 ID 碰撞。

**Alternatives considered**：整群共享 Session、每条消息新 Session、可配置共享群
上下文均已由 DEC-002 拒绝或延期。

## R-007：Agent 执行与渠道交付解耦

**Decision**：Gateway 先完成并持久化 ExecutionResult/UnifiedReply，随后创建独立
DeliveryRecord。交付恢复只消费已保存结果，不能调用 prepare/execute。

**Rationale**：外部发送失败不是 Agent 失败。解耦后可以同时保证 Agent 最多执行
一次和渠道有限恢复，并与第三阶段 terminal/recovery 语义一致。

**Alternatives considered**：

- 整条流水线作为一次重试：拒绝，会重复运行 Agent。
- 所有发送错误都重试：拒绝，未知 ACK 可能重复回复。
- 所有发送错误都不重试：拒绝，明确瞬时故障不能自动恢复。

## R-008：交付错误分类

**Decision**：供应商异常只在 Adapter 边界映射为 TRANSIENT、PERMANENT 或 UNKNOWN。
TRANSIENT 在 1、2、4 秒后最多重试 3 次；PERMANENT 立即 delivery_failed；
UNKNOWN 立即 delivery_unknown 且自动重发为 0。测试使用注入 Clock/Sleeper。

**Rationale**：固定策略可测试、可审计，避免重试风暴。UNKNOWN 保守处理平台可能
已接收但 ACK 丢失的情况。

**Alternatives considered**：无限重试、统一三次重试、完全人工恢复均被 DEC-003
拒绝。

## R-009：长连接高可用

**Decision**：每个 Channel Identity 采用主动/备用。Redis 保存
AdapterOwnershipLease，默认 10 秒租约、3 秒续期；取得新所有权递增 generation。
只有当前 fence 可进入 ready、保持连接、创建发送尝试或发送回复。

**Rationale**：复用第三阶段租约、generation 和 fencing 原语，既避免双活重复收发，
又允许节点中断后接管。连接仍存活不等于拥有业务权限。

**Alternatives considered**：固定单节点、多节点同时连接、两种 IM 使用不同 HA 模型
均被 DEC-004 拒绝。

## R-010：机器人自身消息和 @ 过滤

**Decision**：转换前使用认证 sender_type、sender_id 与 RuntimeBotIdentity 精确
比较。自身消息静默忽略；字段缺失或身份无法确认默认拒绝并写安全审计。群聊必须
使用 SDK 结构化 mention 字段确认目标，禁止通过文本或显示名称猜测。

**Rationale**：自身回复通常会获得新的消息 ID，幂等无法阻止循环；认证身份比文本
和值得变化的显示名称可信。

**Alternatives considered**：名称/前缀判断、假设平台不回推、交给 Gateway 幂等处理
均被 DEC-005 拒绝。

## R-011：Secret 管理与凭证轮换

**Decision**：正式代码仅通过 SecretProvider/环境变量读取 Secret，不保存实际值。
实现前轮换两个 Echo 测试中曾以明文出现的凭证，删除源文件明文，并扫描工作区和
Git 历史。日志统一屏蔽 token、secret、ticket、access_key 和 WebSocket 查询串。

**Rationale**：删除文件中的旧值不能使已泄露凭证重新安全；轮换是恢复可信状态的
必要步骤。Channel Binding 只保存 secret_ref。

**Alternatives considered**：

- 仅加入 .gitignore：拒绝，不能撤销已经写入或泄露的值。
- 在数据库加密保存本阶段 Secret：拒绝，增加密钥管理范围且非当前必要。

## R-012：测试与真实验收边界

**Decision**：自动化测试全部使用 SDK 测试替身和确定性 Runner；真实账号只执行
人工验收，不进入 CI。证据只保存脱敏 trace、消息摘要、状态和截图，不保存凭证。

**Rationale**：确保测试可重复、离线且不受平台账户限制，同时保留真实客户端端到端
证明。模拟结果不得表述为真实平台验证。

**Alternatives considered**：

- CI 直接连接真实机器人：拒绝，凭证、安全、稳定性和限额不可控。
- 只做 Echo 手工测试：拒绝，不能证明 Gateway、隔离、幂等和恢复。

## R-013：长连接重连边界

**Decision**：活动 owner 的明确网络断线使用 1、2、4、8、16、30 秒封顶并带最多
20% jitter 的可取消退避，稳定连接 60 秒后重置；明确凭证无效不自动循环认证，
只在配置版本/Secret 引用变化或人工重启后恢复尝试。失租立即取消重连。

**Rationale**：长连接服务必须能长期自愈，因此限制的是单次等待上限而非永久停止；
jitter 避免多个备用实例同时冲击平台。认证失败通常需要配置修复，热循环既无效又
可能触发平台限制。

**Alternatives considered**：

- 固定间隔无限重连：拒绝，易形成同步重连风暴。
- 网络断线达到次数后永久停止：拒绝，降低长期可用性。
- 凭证错误持续自动认证：拒绝，不能自行恢复且制造无意义请求。

## Dependency Pinning

目标 pyproject.toml 应固定：

~~~toml
lark-channel-sdk==1.4.0
wecom-aibot-python-sdk==1.0.2
~~~

其余依赖沿用第三阶段锁定版本。升级任一 SDK 前必须重新运行 Adapter 契约测试、
异常分类测试和至少一次对应真实客户端冒烟测试。

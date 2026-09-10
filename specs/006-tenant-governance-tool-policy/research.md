# 第六阶段研究记录

**功能编号**：`006-tenant-governance-tool-policy`
**日期**：2026-09-10
**输入**：[spec.md](./spec.md)、[clarification-decisions.md](./clarification-decisions.md)、现有第二/三/五阶段实现与 `trpc-agent-py==1.1.19`

## 研究结论

所有规划问题均已解决，没有遗留 `NEEDS CLARIFICATION`。核心原则是：复用官方 Runner 和 callback/Filter 执行边界；平台只提供租户治理数据、状态机与编排；任何无法判断的安全或成本状态均默认拒绝。

## R-001：官方 tRPC-Agent 治理接入点

**Decision**：继续使用官方 `Runner` 与 `LlmAgent`，通过 `AgentContext.metadata` 传入请求级 `GovernanceContext`，使用官方 model/tool callback（SDK Filter 机制）做模型前后和工具前后的治理。

**Rationale**：当前依赖已经提供 `before_model_callback`、`after_model_callback`、`before_tool_callback` 和 `after_tool_callback`。工具前 callback 位于真实工具函数执行之前，适合承载 DEC-001 的最终授权门禁，同时不需要复制 SDK 调度代码。

**Alternatives considered**：

- 在 Gateway 中一次性检查后直接信任 Runner：无法覆盖 Agent 推理期间策略收紧的竞态。
- 自建 Tool Runner：违反 Framework-First，并产生第二套事件、错误和生命周期语义。
- 修改第三方 SDK 源码：升级困难且不必要。

## R-002：策略立即生效与缓存边界（DEC-001/A）

**Decision**：PostgreSQL 保存不可变策略版本和原子 active 指针。请求准入与工具执行前都权威读取当前 active 版本；缓存只复用已经确认版本的解析结果，不具有授权效力。

**Rationale**：事务提交是跨节点清晰可观察的生效点。双重读取覆盖“准入后、工具前”收紧策略的窗口，并延续现有 `AuthoritativeConfigCache` 的 hint-only 思路。

**Alternatives considered**：

- TTL 缓存允许短暂旧策略：会在窗口内继续授权被撤销工具。
- Session 固定策略版本：长会话可能长期保留过期权限。
- 发布消息主动清缓存但不权威复查：消息丢失时仍会错误放行。

## R-003：渠道主体与授权交集

**Decision**：主体由可信 `(tenant, channel, binding, provider_subject)` 规范化；授权可分别限定 tenant、Agent、binding，最终权限取所有适用层级的交集。审计与确认使用租户内不可逆主体摘要。

**Rationale**：同一字符串在飞书和企业微信中不是同一身份；binding 和 tenant 作用域可防止跨渠道、跨租户授权串用。权限交集保证更窄授权不能扩大上层权限。

**Alternatives considered**：

- 使用显示名或用户输入字段：可伪造且不稳定。
- 只按 tenant 授权：无法限制 Agent 或机器人应用。
- 合并不同渠道的相同字符串 ID：存在错误授权风险。

## R-004：双入口危险确认（DEC-002/C）

**Decision**：Redis 保存唯一 `PendingConfirmation`。文本编号和 IM 按钮都解析为 `ConfirmationIntent`，通过同一原子 claim 脚本消费；按钮只覆盖危险操作确认，不扩展通用卡片系统。

**Rationale**：文本是跨渠道可靠基线，按钮改善实际演示体验。共用状态和一次性声明可防止两个入口分别授权同一副作用。

**Alternatives considered**：

- 两套确认表：会产生双消费和恢复歧义。
- 只支持文本：简单但实际 IM 易输入错误。
- 只支持按钮：渠道能力差异使自动化和降级路径较弱。

## R-005：严格预算账本（DEC-003/A）

**Decision**：预算账户、预占和结算全部以 PostgreSQL 为权威；在单事务中完整预占 request、tool_call、token、cost 四维最大额度。实际完成后按 execution_id 幂等结算并释放差额。

**Rationale**：条件更新/行锁能提供跨节点单一原子准入；单一 SQL 账本避免 Redis 额度与 PostgreSQL 审计发生双写不一致。严格最大值预占可证明批准量永不突破预算。

**Alternatives considered**：

- Redis 原子扣减后异步落 SQL：吞吐高但恢复时存在双账本歧义。
- 估算预占并允许超额：第一版无法给出不可突破的成本边界。
- 仅统计实际用量：并发请求可同时越过上限。

## R-006：跨节点恢复边界

**Decision**：所有治理推进都由共享状态与 fencing 决定。执行前中断可释放/接管；已经开始且结果未知的危险工具不自动重放，进入 `REVIEW_REQUIRED`；已有确定结果时只补结算、审计和交付。

**Rationale**：外部副作用通常无法由平台安全判断或回滚。宁可保守挂起，也不能用“重试”制造重复副作用。该方案复用第三阶段 generation/fencing 和部分提交恢复。

**Alternatives considered**：

- 超时后一律释放并重试：可能重复执行工具与重复消耗。
- 永久保留所有预占：会无界冻结预算。
- 要求工具全部支持分布式事务：超出本阶段且现实不可行。

## R-007：敏感信息与可观察性

**Decision**：入站、工具参数摘要、模型输出、统一回复、日志和审计使用同一租户内容规则，支持 redact/reject；持久化只保存类别、规则、长度/位置摘要和不可逆摘要。指标仅用低基数、非身份标签。

**Rationale**：敏感信息可以从任一边界泄漏，单独保护回复不足以满足 FR-021/022。审计需要可解释，但不需要原文。

**Alternatives considered**：

- 只在 Adapter 回复前脱敏：日志、工具和审计仍可泄漏。
- 保存密文原文用于排错：引入密钥和访问控制范围，且当前验收不需要。
- 将 tenant/trace 放入指标标签：导致高基数并扩大身份暴露面。

## R-008：稳定错误与确认回复契约

**Decision**：治理层返回稳定 domain error；统一回复增加可选 `confirmation` 载荷，Adapter 只映射安全文案和最小按钮。后端、策略细节和敏感命中值不出现在对外错误中。

**Rationale**：把安全决定留在核心治理层可保证双 IM 行为一致；加可选字段可保持第五阶段文本回复兼容。

**Alternatives considered**：

- Adapter 自行决定重试和权限：两渠道会出现语义漂移。
- 使用原始数据库/Redis 异常：泄露基础设施且调用方无法稳定处理。
- 新建独立卡片回复协议：超出最小确认范围。

## R-009：测试与证据策略

**Decision**：采用 test-first 的单元、契约、双节点集成、SDK 替身端到端、回归和敏感扫描六层证据。真实 IM 仅验证确认展示，不作为自动化前置条件。

**Rationale**：治理正确性依赖并发与故障点，仅单元测试不足；真实渠道和模型又会降低可重复性。共享后端集成与确定性替身能同时覆盖一致性和稳定执行次数。

**Alternatives considered**：

- 只做真实客户端人工验证：无法稳定重现并发和节点中断。
- 只做 mock 单元测试：不能证明 SQL/Redis 原子性和跨节点接管。

## 已解决的规划问题

- 策略生效点：PostgreSQL active 指针事务提交。
- 工具最终门禁：官方 `before_tool_callback`。
- 按钮与文本关系：同一待确认事实、同一 claim。
- 预算权威来源：PostgreSQL 单一账本。
- 执行后未知结果：禁止自动重放，进入恢复审查。
- 向后兼容：统一回复仅新增可选确认字段。
- 缓存语义：只优化解析，不参与授权。

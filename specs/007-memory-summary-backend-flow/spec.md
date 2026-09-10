---
title: "完整数据抽象与同步策略"
feature: "007-memory-summary-backend-flow"
status: Draft
created: 2026-09-10
---

# Feature Specification: 完整数据抽象与同步策略

## 1. Feature Summary

本阶段为多租户、节点化 tRPC-Agent 平台建立统一的数据访问和同步语义，覆盖
Session Event、Message、Memory、Summary、Artifact、Knowledge 和 Audit Log。
上层 Gateway、Worker、Channel Adapter 和治理模块只依赖异步 Repository/Adapter
端口，不直接依赖 Redis、PostgreSQL、向量数据库或对象存储 SDK。

本阶段以可运行的 InMemory 参考实现证明领域不变量，并实现 PostgreSQL 的 Event、
Memory、Summary、Audit 和迁移链路，复用既有 Redis 完成锁、幂等和缓存协调。向量库
与对象存储只实现供应商无关契约和确定性替身，不把替身结果表述为真实服务验证。

## Clarifications

### Session 2026-09-10

- Q: Session Event 应由哪个后端作为唯一权威来源，并如何提交 event、session watermark 和 audit？ → A: 选择 A；PostgreSQL 是 Session Event 唯一权威源，event append、session watermark 和 audit 在同一数据库事务提交；Redis 只承担缓存、锁与幂等，不作为可回退的业务权威源。
- Q: 同一 Session 收到乱序事件，或收到相同 Summary watermark 但内容不同的更新时，应如何处理？ → A: 选择 A；不连续事件返回可重试的 `sequence_gap` 且不写入、不缓冲；Summary 同 watermark、同 digest 幂等返回原记录，不同 digest 返回 `summary_conflict`。
- Q: 迁移 Redis 遗留数据到 PostgreSQL 时，如果业务仍可能产生新写入，应如何完成切换和回滚？ → A: 选择 A；按 tenant/stream 短暂停写，锁定源 watermark 后迁移并双读校验，再原子切换到 PostgreSQL；首次 PostgreSQL 新写入前允许回滚，之后只能前向修复。
- Q: Memory、Artifact 和 Knowledge 应采用哪组统一的内容存储与租户隔离规则？ → A: 选择 A；Memory 在 PostgreSQL 保存 tenant-scoped 规范化 JSON、版本和 digest；Artifact 使用临时对象、digest 校验、metadata CAS 与 TTL 孤儿清理；Knowledge 必须在查询层先执行 tenant filter，不支持则拒绝。
- Q: 第七阶段应实现哪些真实共享后端，并且 Audit 不可用时哪些数据操作必须拒绝？ → A: 选择 A；真实实现 PostgreSQL Event/Memory/Summary/Audit/迁移并复用 Redis 协调，向量库和对象存储只做契约与替身；Audit 不可用时拒绝写入、迁移和原文读取，仅允许脱敏 metadata/digest 诊断。

## 2. User Scenarios and Testing

测试分为 Unit、Contract、Integration 和 E2E 四层。所有实现任务必须先运行对应
RED 测试，再运行同一命令 GREEN。共享 Redis/PostgreSQL 未配置时只能记录 skip，
并明确未完成真实共享后端验收。

### US1 - 租户隔离的数据模型和 Repository（P1）

**Goal**：每一条数据都能由 tenant_id、资源类型和稳定键唯一确定，任何跨租户读取
都不能返回数据。

**Independent Test**：为两个租户写入相同 key 的 Memory、Artifact 和 Knowledge，
分别从两个节点读取，验证只返回本租户记录。

**Preconditions**：Repository 已初始化；调用方已持有可信租户上下文。

**Main Flow**：调用方传入 tenant_id 和资源键；Repository 校验作用域、写入版本并
返回不可变记录；读取使用同样的租户作用域。

**Failure Flow**：缺少 tenant_id、作用域不匹配、后端不可用或版本冲突时返回稳定
错误，不返回其他租户数据，不回退到本地状态。

**Security Constraints**：tenant_id 不能由消息正文、display name 或不可信 metadata
覆盖；日志只允许记录脱敏租户摘要。

**Acceptance Criteria**：跨租户读取为 0；同 key 在两个租户下互不覆盖；所有端口为
异步 Protocol；InMemory 契约与共享适配器结果形状一致。

### US2 - Session Event 顺序与幂等（P1）

**Goal**：同一 Session 的事件严格按 sequence 可重放，重复投递不产生重复事件。

**Independent Test**：并发提交相同 event_id、乱序 sequence 和合法连续 sequence，
验证只有连续事件进入日志，重复 event_id 返回原记录。

**Preconditions**：session key 已由可信租户和会话身份派生。

**Main Flow**：追加请求携带 event_id、sequence、payload digest 和 trace_id；在
PostgreSQL 同一事务内完成条件追加、session watermark 更新和 audit 写入，全部成功后
才返回 committed；Redis 只承担缓存、锁与幂等。

**Failure Flow**：sequence 跳跃或回退返回可重试的 `sequence_gap`，且不写入、不缓冲；
payload digest 不一致、fencing 过期或 audit 写入失败时拒绝或回滚整个事务；不得留下
只有 event、watermark 或 audit 的部分提交，也不得回退到 Redis 或 Worker 本地状态。

**Security Constraints**：事件 payload 只在租户范围内可读；敏感正文进入存储前按
治理规则处理，日志只记录 digest。

**Acceptance Criteria**：event_id 重放幂等；合法 sequence 单调递增；乱序写入可重试
且不会污染状态；不同租户的相同 session key 不可见。

### US3 - 跨节点 Memory 可见性（P1）

**Goal**：Worker 无状态，节点 A 写入的 Memory 在提交成功后可被节点 B 读取。

**Independent Test**：共享 Repository 上由两个独立节点交替写入和读取同一租户
Memory，验证版本和内容一致。

**Preconditions**：真实 PostgreSQL 共享后端可用；写入带 version 或 compare-and-set
条件；Redis 只用于协调，不保存 Memory 权威副本。

**Main Flow**：节点 A 将规范化 JSON 内容、digest 和版本原子写入 PostgreSQL；返回
committed version；节点 B 按 tenant、namespace 和 key 读取相同内容与最新版本。

**Failure Flow**：CAS 冲突返回 stale/conflict；后端超时返回 unavailable；不在 Worker
进程中创建第二份业务副本。

**Security Constraints**：namespace、key 和版本查询都必须包含 tenant_id；Memory 内容
只能通过授权 Repository 返回，原文不得进入指标标签、普通日志或审计详情；超过既定
Memory 大小限制的内容必须改用 Artifact，不得隐式转存。

**Acceptance Criteria**：提交后的跨节点读取不落后于 committed version；并发更新不
丢失且冲突可解释；后端不可用时 fail closed。

### US4 - Summary watermark 和状态一致性（P1）

**Goal**：Summary 始终说明它覆盖到哪个 event sequence，不允许水位回退。

**Independent Test**：先写入 sequence=5 的 Summary，再提交 sequence=4、6 的更新，
验证回退失败、前进成功，读取不会宣称覆盖未确认事件。

**Preconditions**：Event stream 已存在；summary 生成器提供 event_sequence。

**Main Flow**：Summary 以 session key 和 event_sequence 写入；读取时返回内容、版本
和覆盖水位；重建任务从 event watermark 继续。

**Failure Flow**：水位回退或事件读取不完整时拒绝提交；相同 watermark 与相同 digest
幂等返回原 Summary，相同 watermark 但 digest 不同返回 `summary_conflict`。所有拒绝
均保留旧的有效 Summary 并产生诊断记录。

**Security Constraints**：Summary 遵循租户内容策略，不能绕过敏感信息检查。

**Acceptance Criteria**：watermark 单调；同 watermark 同 digest 幂等、不同 digest
稳定冲突；Summary 版本不落后于声明覆盖的事件；恢复任务可从最后确认水位继续且
不重复应用事件。

### US5 - Artifact 与 Knowledge 的后端适配（P1）

**Goal**：大对象和检索文档分别通过对象存储、向量库/SQL metadata 端口管理，业务
层不依赖供应商格式。

**Independent Test**：为两个租户写入相同 artifact key 和 document_id，分别读取和
检索，验证租户隔离、digest 校验和有限结果集。

**Preconditions**：Artifact metadata 与内容存储引用分离；Knowledge 文档有稳定版本；
本阶段对象存储与向量库验收使用确定性契约替身，不要求真实外部服务。

**Main Flow**：Artifact 先计算 digest，以 tenant-scoped 临时 key 写入对象存储并校验，
再以 CAS 原子更新 SQL metadata 引用；未引用临时对象按 TTL 清理。Knowledge 写入结构化
metadata 和 embedding_ref，向量查询在相似度计算和返回候选前执行 tenant filter。

**Failure Flow**：内容 digest 不匹配或 metadata CAS 失败时不改变有效引用，临时对象留给
幂等 TTL 清理；对象读取失败、embedding 不可用、索引落后或后端不能保证查询前 tenant
filter 时默认拒绝，不允许用查询后过滤作为降级。

**Security Constraints**：原文不进入日志、trace、指标和审计详情；下载必须再次校验
租户权限和 digest。

**Acceptance Criteria**：Artifact metadata 与内容可校验且中断后孤儿对象最终清理；
Knowledge 检索在后端查询层强制 tenant filter；向量库和对象存储只需契约与替身，不
伪造真实服务结果。

### US6 - 多后端迁移（P1）

**Goal**：Redis 到 SQL、Local Vector 到 Remote Vector 的迁移可暂停、校验、重放和回滚。

**Independent Test**：构造源/目标版本不一致、重复迁移和中断恢复场景，验证水位、双
读校验和冲突保护。

**Preconditions**：迁移任务有 tenant、stream、watermark 和 schema version。

**Main Flow**：tenant/stream 进入短暂停写并记录源 watermark；迁移器扫描到该水位、
写入 PostgreSQL、执行 source/target 双读 digest 校验，再原子切换 authority 状态并恢复
写入；切换后第一笔 PostgreSQL 新写入同时关闭反向回滚资格。

**Failure Flow**：校验失败停在上一个水位并解除停写、继续使用遗留源；目标落后时
幂等重放，目标冲突时不覆盖新版本。仅在 PostgreSQL 尚未接收切换后的新写入时允许
回滚到源端；之后故障必须暂停写入并前向修复，不得反向覆盖或丢弃 PostgreSQL 新数据。

**Security Constraints**：迁移日志只包含数量、版本和摘要，不包含正文和连接凭证。

**Acceptance Criteria**：迁移可幂等重跑；停写作用域仅限目标 tenant/stream；水位不
跳跃；冲突不静默覆盖；authority 切换原子；首笔新写前可回滚、之后只能前向修复。

### US7 - 后端故障、冲突和恢复（P1）

**Goal**：各后端或节点故障时选择可解释的安全后继，不产生重复事件、Memory 写入或
错误 Summary。

**Independent Test**：在 event append、memory write、summary commit 和 migration 各
阶段注入超时、断连、旧 generation 和结果未知故障。

**Preconditions**：每次写入都有 trace_id、fencing generation 和幂等键。

**Main Flow**：记录已确认阶段；新节点读取 durable marker；只重试幂等操作；结果未知
的非幂等操作进入 review，而不是自动重放。

**Failure Flow**：后端不可判定时 fail closed；Worker 不使用本地旁路状态；恢复任务
以 CAS 和 generation 保护旧节点。

**Security Constraints**：错误响应只暴露稳定错误码；后端异常、SQL、URL 和凭证不
进入客户端或审计详情。

**Acceptance Criteria**：恢复后 event、Memory、Summary 的终态最多一次；旧节点写入
被 fencing 拒绝；所有故障都有可查询诊断摘要。

### US8 - 数据访问审计与敏感信息保护（P2）

**Goal**：数据读写、迁移和拒绝决定可按 trace 解释，同时任何可观察面不泄露原文。

**Independent Test**：使用测试手机号、邮箱、token 和 secret marker 穿过 event、
Memory、Artifact、Knowledge、日志、指标和审计边界，验证 redact/reject 与 0 原文命中。

**Preconditions**：租户内容策略和 Audit Repository 已配置。

**Main Flow**：在入站、写入、读取、迁移和出站边界执行内容检查；写入治理审计摘要，
保留 tenant、resource、version、decision、trace 和 latency。

**Failure Flow**：检查器或 Audit 不可用时，所有业务写入、迁移以及会返回 Memory/
Artifact/Knowledge/Event 原文的读取默认拒绝；只读取 metadata、digest、水位和状态的
脱敏运维诊断可以继续，并记录不含原文的最小本地运行事件，不能伪装为正式 Audit。

**Security Constraints**：metric label 不得包含 tenant/user/session/message/trace 原值；
Artifact/Knowledge 原文不进入错误、日志、trace 或 Git。

**Acceptance Criteria**：审计不可变且可按 tenant/trace 查询；Audit 故障时写入、迁移
和原文读取执行次数为 0，metadata/digest 诊断仍可用；敏感 marker 在扫描面命中为 0；
拒绝路径不创建未授权业务状态。

## 3. Functional Requirements

- **FR-001**：所有数据模型 MUST 包含 tenant-scoped 所有权和稳定资源键。
- **FR-002**：Repository MUST 提供异步、供应商无关的读写接口；InMemory 与真实 PostgreSQL 实现 MUST 通过同一契约测试，向量库和对象存储使用确定性契约替身。
- **FR-003**：跨租户读取 MUST 返回空结果或稳定 scope error，不得返回数据。
- **FR-004**：Session Event MUST 包含 event_id、sequence、payload digest、版本和 trace_id；PostgreSQL MUST 作为其唯一业务权威源，Redis 只能承担缓存、锁与幂等。
- **FR-005**：同一 session 的 sequence MUST 严格连续且单调递增；sequence 跳跃或回退 MUST 返回可重试的 `sequence_gap`，不得写入或缓冲；event append、session watermark 更新和对应 audit MUST 在同一 PostgreSQL 事务提交，任一步失败必须整体回滚。
- **FR-006**：相同 event_id 和相同 digest 的重放 MUST 幂等返回原记录。
- **FR-007**：相同 event_id 但 digest 不同 MUST 返回冲突错误。
- **FR-008**：Memory MUST 在 PostgreSQL 中保存 tenant-scoped namespace、key、version、updated_at、规范化 JSON 内容和内容 digest；超过配置大小限制的内容必须显式改用 Artifact。
- **FR-009**：Memory CAS 冲突 MUST 可识别，旧版本不得覆盖新版本。
- **FR-010**：Memory 提交后共享节点 MUST 能读取 committed version、相同规范化内容和 digest；不得依赖 Worker 本地副本或对象存储隐式引用。
- **FR-011**：Summary MUST 记录 event watermark，watermark 不得回退；相同 watermark 与相同 digest MUST 幂等返回原记录，相同 watermark 但 digest 不同 MUST 返回 `summary_conflict`。
- **FR-012**：Summary 不得声明覆盖未确认或缺失的 event sequence。
- **FR-013**：Artifact MUST 将 SQL metadata 与对象内容引用分离并保存 digest；写入 MUST 使用 tenant-scoped 临时 key，校验成功后以 metadata CAS 发布有效引用，未引用临时对象 MUST 由幂等 TTL 清理。
- **FR-014**：Artifact 读取 MUST 校验租户作用域、有效 metadata 引用和内容 digest；CAS 或 digest 失败不得改变旧引用。
- **FR-015**：Knowledge Document MUST 保存 tenant、document_id、版本和 embedding 引用。
- **FR-016**：Knowledge 检索 MUST 在向量/SQL 查询层、相似度计算和候选返回之前执行 tenant filter；后端不能保证预过滤时 MUST 默认拒绝，不得用查询后过滤降级。
- **FR-017**：外部后端适配器 MUST 不把供应商对象泄漏到 Gateway、Worker 或领域模型；Memory、Artifact 和 Knowledge 必须返回统一领域对象及稳定错误。
- **FR-018**：Worker MUST 不保存租户业务状态副本或隐式 sticky session 状态。
- **FR-019**：Redis/PostgreSQL/向量库/对象存储 MUST 声明一致性、延迟、成本和并发取舍；本阶段 MUST 实现真实 PostgreSQL 数据适配器并复用真实 Redis 协调适配器，真实向量库与对象存储实现不在范围内。
- **FR-020**：迁移 MUST 使用 tenant/stream/schema/watermark 标识并支持幂等重跑；最终切换期间 MUST 只暂停目标 tenant/stream 的新写入。
- **FR-021**：迁移 MUST 锁定源 watermark，在 authority 切换前完成到该水位的 source/target 双读 digest 校验，并以原子状态转换切换到 PostgreSQL。
- **FR-022**：迁移冲突 MUST 停止推进并保留旧权威版本，不得静默覆盖；仅在 PostgreSQL 尚未接收切换后的第一笔新写入时允许回滚，之后 MUST 采用暂停写入和前向修复。
- **FR-023**：迁移中断 MUST 可从最后确认 watermark 恢复；重复执行不得跨过未校验水位，也不得重新开放已关闭的回滚资格。
- **FR-024**：后端不可用或结果未知时 MUST fail closed，不得回退本地旁路数据。
- **FR-025**：可重试操作 MUST 具备幂等键；非幂等结果未知 MUST 进入 review。
- **FR-026**：节点恢复 MUST 使用 fencing generation 拒绝旧节点写入。
- **FR-027**：数据访问 MUST 传播 trace_id、tenant 摘要和资源版本。
- **FR-028**：Audit Log MUST 通过真实 PostgreSQL Repository 记录读写、冲突、迁移和拒绝决定的最小摘要。
- **FR-029**：Artifact/Knowledge/Event 原文 MUST 不进入日志、指标、trace 和错误详情。
- **FR-030**：内容检查或 Audit 不可用时，所有业务写入、迁移和返回 Event/Memory/Artifact/Knowledge 原文的读取 MUST 默认拒绝；只返回 metadata、digest、水位或状态的脱敏诊断读取 MAY 继续，但不得记为正式 Audit 成功。
- **FR-031**：Repository 错误 MUST 映射为稳定、无后端细节的 domain error。
- **FR-032**：测试替身 MUST 不依赖真实模型、真实 IM、真实向量库、真实对象存储或真实 Secret；替身验证不得表述为真实外部服务验证。
- **FR-033**：真实 PostgreSQL/Redis 共享后端测试在缺少环境时 MUST 明确记录 skip，而不是通过；正式共享验收必须在两者可用时执行。
- **FR-034**：实现 MUST 保持与既有 Session、Audit、Channel 和 Governance 契约兼容。

## 4. Non-Functional Requirements

- NFR-001：确定性本地 Repository 的普通读写 p95 目标小于 50ms（不含网络）。
- NFR-002：单租户 event append 的并发冲突必须可观测，不能静默丢事件。
- NFR-003：所有时间使用 UTC-aware datetime；版本和 sequence 使用非负整数。
- NFR-004：适配器必须支持超时、有限重试、熔断或安全失败策略。
- NFR-005：数据模型和接口必须可序列化为 JSON，但不得序列化 SecretBytes 或原始凭证。

## 5. Data Model Requirements

核心实体为 Tenant、AgentApplication、Session、SessionEvent、Memory、Summary、
Artifact、KnowledgeDocument、ChannelBinding 和 AuditLog。每个实体通过 tenant_id
归属租户；SessionEvent 为 append-only，Summary 通过 event watermark 关联事件，
Memory 在 PostgreSQL 保存规范化 JSON 内容、版本和 digest；Artifact metadata 通过
storage_ref 指向 tenant-scoped 对象内容，KnowledgeDocument 通过 embedding_ref 指向
带 tenant filter 的向量索引。授权领域读取可返回 Memory/Artifact/Knowledge 内容，但
日志、指标、trace 和审计只携带 digest、引用或脱敏摘要。

## 6. Repository and Adapter Boundary

统一端口至少包括 `append_event`、`get_events`、`put_memory`、`get_memory`、
`put_summary`、`get_summary`、Artifact put/get、Knowledge upsert/search；所有方法
均为异步并接收 tenant_id。InMemory 是本地参考实现；Redis 适合短延迟事件/幂等和租约；
PostgreSQL 是 Session Event、session watermark、审计和迁移水位的权威存储；Redis
只用于缓存、幂等和租约；向量库只负责带 tenant filter 的索引；
对象存储负责大 Artifact 内容，SQL 保存 metadata。

## 7. Event Ordering and Idempotency Semantics

写入顺序为：验证租户和 session → 检查 fencing → 检查 event_id/digest → 条件追加
sequence → 更新 session watermark → 写入 audit。后三项使用同一 PostgreSQL 事务，
任何一步失败都整体回滚；事务提交后才能返回 committed。
重复 event_id 且 digest 相同返回原事件；digest 不同返回 `idempotency_conflict`。
sequence 不等于当前 watermark 的下一个值时返回可重试的 `sequence_gap`，Repository
不得保存 pending buffer，也不得在后台根据到达顺序自动补写。

## 8. Memory / Summary Consistency Semantics

Memory 使用版本 CAS；Summary 使用 event watermark CAS。Summary 的
`event_sequence <= confirmed_session_sequence`，且新写入 watermark 必须大于等于旧值。
同 watermark 同 digest 幂等返回原记录，同 watermark 不同 digest 返回
`summary_conflict`；摘要生成失败保留旧 Summary，不能用不完整事件覆盖有效摘要。

## 9. Cross-Node Visibility Requirements

Worker 不要求 sticky session。请求可以被任意健康 Worker 接收，凭借共享 Session、
Memory、Summary 和 fencing 状态完成一致访问。写入响应只有在共享后端确认后才可被
视为 committed；本地缓存只能作为解析提示，不能授权旧版本。

## 10. Redis-to-SQL Migration Requirements

迁移按 tenant/stream 分片，记录 source、target、watermark、schema_version、digest、
authority_state 和 rollback_eligible。最终迁移前仅冻结目标 tenant/stream，锁定 Redis
源 watermark，复制 append-only event 到 PostgreSQL，再构建 Memory/Summary 索引并
执行双读 digest 校验；校验通过后原子切换 authority 并恢复写入。Redis 只作为遗留
数据源。PostgreSQL 接收切换后第一笔新写入时原子设置 `rollback_eligible=false`；此前
可回滚并解除停写，此后只能暂停相关 stream 并前向修复。

## 11. Vector Store and Object Storage Migration Requirements

向量迁移保留 document_id、tenant_id、embedding version 和 source digest；远端检索
必须在相似度计算和候选返回前过滤 tenant，无法保证预过滤时默认拒绝。对象迁移使用
tenant-scoped 临时 key，校验 digest 后以 CAS 原子更新 metadata 引用；失败时不改变
有效引用，未被 metadata 引用的临时对象由可重入 TTL 清理任务删除。

## 12. Failure and Recovery Semantics

后端连接失败、超时、schema 不兼容和 fencing 失败统一映射为安全 domain error。
执行前失败可以释放临时状态；已提交 event/Memory 不自动重复写入；结果未知的非幂等
操作进入 review。Recovery marker 记录最后确认 stage 和 watermark，恢复任务只执行
幂等补偿。

## 13. Security and Tenant Isolation Requirements

tenant_id 来自已验证 binding/context，不接受入站正文声明。所有 key 使用摘要避免
暴露外部标识。原始用户 ID、Artifact 内容、Knowledge 文本、数据库 URL、IM token
和模型 key 不得进入日志、trace、指标、错误或版本库。

## 14. Observability Requirements

每次数据操作传播 trace_id、tenant digest、resource type、version、watermark、node
和 outcome。指标只使用有限集合标签：resource_type、backend_type、operation、
outcome；不得使用 tenant/user/session/message/trace 原值。Audit 保存不可变的最小
摘要并支持按 tenant、trace、session、resource 查询。

## 15. Acceptance Scenarios

1. 两个租户使用相同 Memory key，互相读取均为空。
2. 同 event_id 同 digest 重放一次，事件数量保持 1。
3. 同 event_id 不同 digest 被拒绝并记录冲突审计。
4. sequence=2 在 sequence=1 前到达时不改变有效 session 状态。
5. 节点 A 写入 Memory 后节点 B 读取相同 version/value。
6. Summary watermark 从 3 更新到 2 被拒绝，从 3 更新到 4 成功。
7. Artifact digest 不匹配时读取失败且不泄露内容。
8. Knowledge 检索结果只包含当前 tenant 文档。
9. Redis→SQL 迁移中断后从上次 watermark 继续且不重复事件。
10. 向量迁移冲突暂停，不覆盖目标较新版本。
11. 后端不可用时不创建本地旁路状态。
12. 敏感 marker 在日志、trace、指标、审计和样本扫描中命中为 0。

## 16. Success Criteria

- **SC-001**：Unit/contract/integration/e2e 确定性测试全部通过，且不依赖真实 Secret。
- **SC-002**：跨租户读取和写入覆盖为 0。
- **SC-003**：重复 event_id 不产生重复事件，冲突 digest 有稳定错误码。
- **SC-004**：Event sequence 和 Summary watermark 在并发与恢复测试中始终单调。
- **SC-005**：跨节点 Memory 读取达到 committed version，旧 generation 写入为 0。
- **SC-006**：迁移可暂停、双读校验、幂等重跑和安全回滚均有自动化证据。
- **SC-007**：Artifact/Knowledge 原文在所有可观察面扫描命中为 0。
- **SC-008**：共享后端缺失时所有 skip 都有明确环境原因，不被计入通过数。
- **SC-009**：README、spec、plan、tasks 和代码中的实体、端口和数据流命名一致。

## 17. Explicit Out of Scope

- 不部署真实向量数据库、对象存储或外部 Memory 服务；仅提供契约、确定性替身和迁移语义验证。
- 不实现生产级 embedding 模型、RAG 质量评估或知识内容治理后台。
- 不迁移用户真实数据，不读取真实 IM 凭证或模型 API key。
- 不在本阶段实现 Kubernetes、自动扩缩容、跨地域灾备和完整 Admin UI。
- 不把 InMemory 测试结果表述为 Redis/PostgreSQL 生产可用性证明。

## 18. Dependencies and Assumptions

- 复用既有 tenant、Session、Channel Binding、Audit、Metrics、Governance 和 Worker 端口。
- Python 依赖版本沿用仓库锁定版本；任何 SDK 升级须重新执行兼容性验证。
- 共享 Redis/PostgreSQL 是本阶段真实集成范围，测试需要由运行者显式注入非仓库环境变量；连接信息不写入文件。缺少环境时允许明确 skip，但不得声明共享验收完成。
- 真实后端的性能、可用性和容量结论必须在对应基础设施上重新测量。
- 所有后续实现任务必须从本规格的 FR/SC 和用户故事验收场景反向追踪。

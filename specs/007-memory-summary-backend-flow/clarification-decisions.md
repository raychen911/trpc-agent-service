# 第七阶段澄清与决策记录

**功能编号**：`007-memory-summary-backend-flow`

**功能名称**：完整数据抽象与同步策略

**记录日期**：2026-09-10

**状态**：已完成

**用户最终选择**：`AAAAA`

## 记录目的

本文单独记录第七阶段中影响租户隔离、事件幂等、跨节点可见性、Summary 一致性和迁移恢复的关键问题。每项记录包含候选方案、用户选择、判断逻辑、实现边界和验收影响，用于后续 plan、tasks、代码评审和答辩。

## 决策索引

| 决策编号 | 决策主题 | 用户选择 | 最终决定 |
|----------|----------|----------|----------|
| DEC-001 | Session Event 权威存储与提交原子性 | A | PostgreSQL 唯一权威；event、watermark、audit 同事务提交 |
| DEC-002 | 乱序 Event 与 Summary 同水位冲突 | A | event gap 不落库；Summary 同水位按 digest 幂等或冲突 |
| DEC-003 | Redis→PostgreSQL 迁移切换与回滚 | A | tenant/stream 短暂停写；首笔新写前可回滚，之后前向修复 |
| DEC-004 | Memory/Artifact/Knowledge 内容边界 | A | SQL Memory、临时对象发布、向量查询前 tenant filter |
| DEC-005 | 真实后端范围与 Audit 故障边界 | A | 真实 PostgreSQL/Redis；向量/对象替身；敏感操作 fail closed |

## DEC-001：PostgreSQL 作为 Session Event 唯一权威源

### 问题

Session Event 应由哪个后端作为唯一权威来源，并如何提交 event、session watermark 和 audit？

### 候选方案

| 方案 | 说明 | 主要影响 |
|------|------|----------|
| A | PostgreSQL 唯一权威；event、watermark、audit 同事务，Redis 只作缓存/锁/幂等 | 一致性和恢复边界最清楚，写入受 PostgreSQL 可用性约束 |
| B | Redis 权威；event/watermark 原子写 Redis，Audit 异步写 PostgreSQL | 延迟低，但存在持久化和审计时间窗口 |
| C | PostgreSQL 保存 event/watermark，Audit 通过 outbox 异步补写 | 主状态一致，但审计不是立即可见 |
| D | Redis 与 PostgreSQL 同步双写，两个后端都成功才确认 | 表面语义强，但部分提交和故障恢复最复杂 |

### 用户选择

选择方案 A。

### 最终决定

- PostgreSQL 是 Session Event 和 session watermark 的唯一业务权威源。
- event append、watermark 更新和对应 audit 在同一 PostgreSQL 事务中提交。
- 任一步失败则整体回滚，只有事务提交后才能向调用方返回 committed。
- Redis 只承担缓存、锁和幂等，不得在 PostgreSQL 不可用时成为业务旁路。
- Redis→SQL 迁移中的 Redis 是遗留数据源，不再接收本阶段的新权威 Event 写入。

### 判断逻辑

1. Event、watermark 和 audit 共同描述一次已确认状态变化，拆分提交会产生无法解释的中间状态。
2. PostgreSQL 可用单事务和唯一约束同时保证顺序、幂等、审计和恢复事实。
3. Redis 更适合短期协调，不应同时承担可回退的第二套业务权威状态。
4. 牺牲部分写入可用性换取明确的一致性，符合项目的 fail-closed 和无本地旁路原则。

### 边界场景与验收影响

- audit 写入失败时，event 与 watermark 必须同时回滚。
- event 已存在且 digest 相同的重放返回原记录，不重复写 audit 成功事实。
- PostgreSQL 不可用时拒绝写入，Redis 命中也不能宣称 committed。
- 节点在事务提交前中断不产生部分状态；提交后响应丢失可按 event_id 幂等读取原结果。
- 双节点测试必须证明相同 session 的 sequence、watermark 和 audit 一致推进。

## DEC-002：拒绝乱序 Event，并以 digest 判定 Summary 同水位幂等

### 问题

同一 Session 收到乱序事件，或收到相同 Summary watermark 但内容不同的更新时，应如何处理？

### 候选方案

| 方案 | 说明 | 主要影响 |
|------|------|----------|
| A | gap 返回 `sequence_gap` 且不写入、不缓冲；Summary 同水位同 digest 幂等，不同 digest 冲突 | 状态最少、确定性最强，调用方负责按序重试 |
| B | 乱序 Event 放入共享 pending buffer；Summary 同水位由先写成功者获胜 | 可吸收乱序，但增加缓冲清理、租约和恢复状态 |
| C | 允许乱序写入并后台重排；Summary 由最高版本覆盖 | 吞吐灵活，但有效状态和重放结果可能随时变化 |
| D | Repository 持锁等待缺失 sequence，超时拒绝；Summary 不同 digest 冲突 | 减少调用方重试，但会延长锁持有并增加雪崩风险 |

### 用户选择

选择方案 A。

### 最终决定

- 新 Event 的 sequence 必须等于当前 session watermark 的下一个连续值。
- sequence 跳跃或回退返回可重试的 `sequence_gap`，不写入 PostgreSQL，也不进入 Redis/pending buffer。
- 相同 event_id、相同 digest 仍按既有幂等语义返回原 Event；相同 event_id、不同 digest 返回 `idempotency_conflict`。
- Summary 同 watermark、同 digest 返回原记录；同 watermark、不同 digest 返回 `summary_conflict`。
- 水位回退或冲突都保留已有有效 Summary，不由“最后到达者”覆盖。

### 判断逻辑

1. Repository 内引入乱序缓冲会增加新的 TTL、容量、接管和清理状态机，超出本阶段必要范围。
2. 立即拒绝 gap 能确保 PostgreSQL Event Log 始终是可顺序重放的确定状态。
3. Summary 的 watermark 只能说明覆盖范围，必须再结合 digest 才能判断内容是否是同一事实。
4. 拒绝同水位不同内容可以暴露摘要生成不确定性，避免静默覆盖审计证据。

### 边界场景与验收影响

- sequence=2 先于 sequence=1 时，Event 数量和 watermark 均不改变；1 成功后可重试 2。
- 两个节点同时提交同一个连续 sequence，只允许一个新事实提交；另一个按 event_id/digest 得到幂等或冲突结果。
- Summary watermark=5、digest 相同的重试返回同一版本；digest 不同不创建新版本。
- 恢复器不得把 gap Event 自动排序后写入，只能从调用方或事件源重新取得连续输入。

## DEC-003：按 tenant/stream 停写迁移并限制回滚边界

### 问题

迁移 Redis 遗留数据到 PostgreSQL 时，如果业务仍可能产生新写入，应如何完成切换和回滚？

### 候选方案

| 方案 | 说明 | 主要影响 |
|------|------|----------|
| A | tenant/stream 短暂停写，锁定水位、迁移校验、原子切换；首笔新写前可回滚，之后前向修复 | 无双写歧义，停写范围可控，回滚边界明确 |
| B | 迁移期间 Redis/PostgreSQL 同步双写，读 Redis，追平后切换 | 可持续写入，但需要解决每次双写部分提交 |
| C | Redis 继续写入并通过变更日志追赶 PostgreSQL，可做反向同步 | 可用性高，但引入 CDC、追赶和反向冲突状态机 |
| D | 平台全局停写并统一迁移所有租户 | 实现直观，但故障域和停机范围过大 |

### 用户选择

选择方案 A。

### 最终决定

- 迁移与 authority 状态均按 tenant/stream 隔离，其他租户和 stream 不停写。
- 切换前进入短暂停写并锁定 Redis 源 watermark；目标只需迁移并校验到该确定水位。
- 双读 digest 校验通过后，使用原子状态转换将 PostgreSQL 设为 authority，再恢复写入。
- PostgreSQL 接收切换后的第一笔新写入前，可回滚到只读保留的 Redis 源并解除停写。
- 第一笔新写入提交时关闭 rollback eligibility；此后发生故障必须暂停目标 stream 并前向修复，不能反向覆盖或丢弃 PostgreSQL 新数据。

### 判断逻辑

1. 短暂停写避免同步双写带来的 Redis/SQL 部分提交和冲突恢复问题。
2. tenant/stream 粒度把停机影响限制在正在迁移的数据分片。
3. 固定源 watermark 使双读校验有稳定边界，重跑不会追逐不断变化的源。
4. 在目标产生新事实后禁止反向回滚，可防止把已经确认的新数据静默丢失。

### 边界场景与验收影响

- 停写期间的新请求返回稳定、可重试的 `migration_write_paused`，不得写入任一业务权威源。
- 校验失败或切换前节点中断时，从最后确认水位幂等重跑或安全回滚。
- authority 状态切换和 `rollback_eligible` 关闭均使用 CAS/fencing，旧迁移节点不能覆盖新状态。
- 首笔 PostgreSQL 新写入之后模拟故障，系统只能进入 `forward_repair_required`，不得恢复 Redis 写入。
- 两个租户并行迁移时，一个 tenant/stream 的停写不得阻塞另一个。

## DEC-004：按数据形态划分内容存储并强制查询前租户过滤

### 问题

Memory、Artifact 和 Knowledge 应采用哪组统一的内容存储与租户隔离规则？

### 候选方案

| 方案 | 说明 | 主要影响 |
|------|------|----------|
| A | Memory 存 PostgreSQL JSON；Artifact 临时上传后 CAS 发布并 TTL 清理；Knowledge 查询前 tenant filter | 数据边界清楚，可验证跨节点内容与租户隔离 |
| B | 三类原文都存对象存储，SQL 只存引用；向量结果查询后过滤 | 引用统一，但对象依赖扩大且可能接触跨租户候选 |
| C | 本阶段全部存 PostgreSQL，不保留对象/向量端口 | 实现较少，但无法证明后端替换边界 |
| D | Memory 只存 digest；Artifact 直接最终 key；Knowledge 查询后过滤 | 实现简单，但不能满足内容读取、原子发布和隔离要求 |

### 用户选择

选择方案 A。

### 最终决定

- 小型 Memory 以 tenant、namespace、key 为作用域，在 PostgreSQL 保存规范化 JSON 内容、version 和 digest。
- 超过配置大小限制的内容必须由调用方显式使用 Artifact，不做隐式转存。
- Artifact 先写 tenant-scoped 临时 key，校验 digest 后以 SQL metadata CAS 发布 storage_ref。
- 上传失败或 CAS 冲突不改变旧引用；未被引用的临时对象由可重入 TTL 清理器删除。
- Knowledge 向量/SQL 查询必须在相似度计算和候选返回前强制 tenant filter；后端不能保证时默认拒绝。
- Gateway、Worker 只接收统一领域对象和稳定错误，不接触供应商对象。

### 判断逻辑

1. Memory 通常是小型结构化状态，和 version/digest 一起存 PostgreSQL 最容易保证 CAS 与跨节点可见性。
2. 大内容交给 Artifact 可避免把数据库变成无边界对象仓库。
3. 临时上传加 metadata CAS 能避免失败上传覆盖当前有效对象，TTL 清理能安全回收孤儿。
4. 查询前 tenant filter 可确保跨租户候选从未进入应用进程，强于查询后过滤。

### 边界场景与验收影响

- 两租户相同 Memory key 写入不同 JSON，跨节点读取只返回各自内容和 digest。
- Memory 超限返回稳定错误，不自动生成未审计 Artifact。
- 对象上传后节点中断、digest 失败或 CAS 冲突时旧引用继续有效，临时对象最终只清理一次。
- 模拟不支持 tenant pre-filter 的向量后端时，检索结果为空并返回安全错误，不能拉取全局候选后过滤。
- 日志、指标、trace 和审计不得出现三类原始内容。

## DEC-005：真实 PostgreSQL/Redis 交付与 Audit 故障时分级拒绝

### 问题

第七阶段应实现哪些真实共享后端，并且 Audit 不可用时哪些数据操作必须拒绝？

### 候选方案

| 方案 | 说明 | 主要影响 |
|------|------|----------|
| A | 真实 PostgreSQL 数据链路与 Redis 协调；向量/对象只做契约替身；Audit 故障拒绝写入、迁移和原文读取 | 能证明共享一致性，同时控制外部依赖范围和泄露风险 |
| B | 所有新增 Repository 只做 InMemory，Audit 故障边界同 A | 实现最少，但不能证明真实共享数据链路 |
| C | PostgreSQL、Redis、向量库和对象存储全部真实实现，Audit 故障时所有读写拒绝 | 覆盖最广，但超出阶段范围且诊断可用性过低 |
| D | 真实 PostgreSQL/Redis，但 Audit 故障时继续业务操作并事后补写 | 可用性高，但会形成不可审计的数据访问窗口 |

### 用户选择

选择方案 A。

### 最终决定

- 实现真实 PostgreSQL Session Event、Memory、Summary、Audit 和迁移 Repository。
- 复用第三阶段已有 Redis 锁、幂等和缓存协调，不在 Redis 建立第二套业务权威数据。
- 向量库和对象存储只实现供应商无关 Adapter 契约与确定性测试替身。
- Audit 不可用时，所有业务写入、迁移及会返回原始内容的数据读取均默认拒绝。
- 不返回原文的 metadata、digest、水位和状态诊断可继续，但只能产生最小本地运行事件，不能伪装成正式 Audit。

### 判断逻辑

1. 项目已有可运行 Redis/PostgreSQL 基础设施，真实实现可证明跨节点和事务语义，而不是只验证接口形状。
2. 真实向量库和对象存储会引入账号、网络与供应商差异，不是本阶段核心一致性验证的必要条件。
3. 数据变更和原文读取若无法审计，会形成无法追责的安全窗口，必须 fail closed。
4. 允许纯 metadata/digest 运维诊断，可在不暴露业务内容的前提下保留故障定位能力。

### 边界场景与验收影响

- 自动化必须分别运行 InMemory 和 PostgreSQL/Redis 契约/集成测试；缺少共享环境只能明确 skip。
- 模拟 Audit Repository 断连时，Event/Memory/Summary/Artifact/Knowledge 写入和迁移调用次数为 0。
- 原文读取在 Audit 故障时拒绝，metadata/digest 查询仍返回租户作用域内的脱敏结果。
- 向量/对象替身的结果必须在测试和文档中标记为 fixture，不得宣称生产后端已验证。
- 敏感扫描覆盖错误、运行事件、测试输出和 PostgreSQL 审计样本，原文命中为 0。

## 综合评审结论

五项 A 决策共同形成一条保守但可证明的数据一致性路线：PostgreSQL 承担唯一持久权威和事务边界，Redis 只负责协调；Repository 不接受乱序或隐式冲突；迁移通过 tenant/stream 停写和受限回滚避免双权威；内容按 Memory、Artifact、Knowledge 的数据形态分离并在查询前隔离租户；真实后端范围聚焦 PostgreSQL/Redis，Audit 故障时不允许未审计的数据变化或原文访问。

这些决定均由用户作出，AI 负责识别风险、比较方案并将选择转化为可验证的需求、边界场景和后续设计输入。

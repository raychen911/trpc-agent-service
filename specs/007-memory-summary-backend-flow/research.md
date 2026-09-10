# 第七阶段技术研究与设计依据

**功能编号**：`007-memory-summary-backend-flow`

**日期**：2026-09-10

**输入**：[spec.md](./spec.md)、[clarification-decisions.md](./clarification-decisions.md)

## 研究结论

本阶段不再保留待确认技术项。所有影响一致性和安全的选项均由 DEC-001～DEC-005 固定：PostgreSQL 是持久权威，Redis 只协调；Event 严格连续；迁移短暂停写并限制回滚；内容按数据形态分后端；真实交付聚焦 PostgreSQL/Redis，Audit 故障时敏感操作 fail closed。

## R-001：Event、watermark 与 Audit 的原子边界

**Decision**：使用 PostgreSQL 单事务和 tenant/session 行级串行化，原子提交 Event、session watermark 和 mutation Audit。

**Rationale**：三者共同描述一个已确认事实。若拆分到 Redis、异步 outbox 或双写，节点在任一步中断都会产生“有事件无水位”“有状态无审计”或双权威。单数据库事务配合唯一约束能同时覆盖顺序、幂等与恢复。

**Implementation notes**：

- 唯一键 `(tenant_id, session_key, event_id)`；另有 `(tenant_id, session_key, sequence)` 唯一约束。
- 先查询相同 event_id 判 digest 幂等/冲突，再对 watermark 行加锁并验证 `sequence=current+1`。
- 初始水位视为 0；sequence=1 创建流。
- Audit append 与状态更新使用同一 SQLAlchemy AsyncConnection/transaction，不通过独立 Repository 连接。
- 正常幂等重放返回原提交结果，不重复产生 mutation success Audit。

**Alternatives rejected**：Redis 权威+异步 Audit 会形成不可审计窗口；outbox 不满足本阶段“对应 Audit 同事务”；Redis/PG 同步双写无法获得跨后端原子性。

**Decision source**：DEC-001、DEC-002。

## R-002：规范化、digest 与冲突判断

**Decision**：JSON 内容使用确定性规范化后计算 SHA-256；digest 是幂等事实的一部分，而不是只作日志字段。

**Rationale**：同 ID/同 watermark 不代表相同内容。稳定规范化可让不同节点、重启和迁移得到相同摘要，暴露非确定性生成或错误重放。

**Implementation notes**：UTF-8、对象键排序、无多余空白、禁止 NaN/Infinity；时间与标识先转领域约定字符串。digest 采用 64 位小写十六进制。Event、Memory、Summary、Artifact metadata 和 Knowledge metadata 都保存 digest。

**Alternatives rejected**：直接比较 Python dict 不能跨进程/语言稳定；last-write-wins 会吞掉并发冲突；只比较版本无法证明内容相同。

**Decision source**：DEC-002、DEC-004。

## R-003：Memory、Artifact、Knowledge 的存储边界

**Decision**：Memory 规范化 JSON 存 PostgreSQL；大内容由调用方显式转为 Artifact；Artifact 先临时对象后 SQL metadata CAS；Knowledge 必须底层 pre-filter tenant。

**Rationale**：Memory 是小型结构化状态，适合关系数据库的版本 CAS；大对象不应无界进入 SQL；对象存储无法和 PostgreSQL 做本地事务，因此用“不可见暂存+权威 metadata 发布+孤儿清理”收敛；Knowledge 查询后过滤已让跨租户候选进入进程，不满足强隔离。

**Implementation notes**：

- Memory 大小按规范化后的 UTF-8 字节数判断，限制来自可信配置。
- 对象临时 key 包含 tenant digest、artifact id、upload nonce；日志不得输出完整 key。
- 只有 PostgreSQL 中 `PUBLISHED` metadata 指向的 storage_ref 可读；旧引用在 CAS 失败时不变。
- VectorStorePort 必须接受 tenant filter 作为不可选条件，并暴露 pre-filter capability；不支持时在查询前拒绝。
- 对象/向量仅实现 deterministic fake，测试和文档必须标注 fixture。

**Alternatives rejected**：全部原文放对象存储会扩大故障域；全部放 PG 无法验证替换边界；查询后过滤违反租户隔离。

**Decision source**：DEC-004、DEC-005。

## R-004：跨后端副作用的恢复方式

**Decision**：不伪造分布式事务；以 PostgreSQL 状态机控制对象/向量副作用的可见性和幂等恢复。

**Rationale**：对象与向量后端通常不参与 PostgreSQL 两阶段提交。可恢复状态加幂等 key 能明确判断“未发布”“待索引”“可清理”，比把两个成功响应当作原子提交可靠。

**Artifact protocol**：先执行 Audit/权威后端 readiness gate；`STAGED → digest verified → PUBLISHED`；失败或冲突保持旧 metadata，未引用 temporary object 在 TTL 后成为 GC candidate。初始 gate 失败时不得调用对象端；gate 后竞态故障产生的孤儿按 TTL 收敛。GC 使用 tenant scope 和删除幂等性。

**Knowledge protocol**：先通过 Audit/权威后端 readiness gate，再执行 `PENDING_INDEX → INDEXED`；只有 `INDEXED` 可搜索。初始 gate 失败时不得调用向量端；向量 upsert 使用 `(tenant_id, document_id, digest)` 幂等；后续失败保留 pending，由恢复器补齐。

**Decision source**：DEC-004。

## R-005：Redis→PostgreSQL 迁移方案

**Decision**：按 tenant/stream 短暂停写，持久化源水位与摘要，幂等复制/校验，CAS 切换 authority；首笔 PG 新写之前可回滚，之后只能 forward repair。

**Rationale**：这一方案不需要业务双写或 CDC，停写故障域限定在单个 tenant/stream；固定 source watermark 让验证边界稳定。目标出现新事实后反向回滚会丢数据，因此必须永久关闭回滚资格。

**Implementation notes**：

- 迁移 lease 继续复用 Redis，但 authority/migration state 位于 PostgreSQL。
- 每个阶段保存 expected state、generation、source watermark、checkpoint、source/target digest 和更新时间。
- 切换后首笔 Event/Memory/Summary 写入必须在业务事务内将 `rollback_eligible=false`。
- forward repair 只允许暂停写入、从权威事实补目标或人工确认；不得重新启用 Redis 新写。

**Alternatives rejected**：同步双写有部分提交；CDC/反向同步超出本阶段；全局停写放大故障域。

**Decision source**：DEC-003。

## R-006：Audit 故障与内容访问

**Decision**：Audit 不可用时，拒绝所有业务写入、迁移以及会返回原始 Event/Memory/Summary/Artifact/Knowledge 内容的读取；只允许 tenant-scoped metadata/digest/watermark/status 诊断。

**Rationale**：未审计的数据变化或原文访问形成不可追责窗口。纯 metadata 诊断不含业务内容，可以保留最低运维能力，但本地 operational event 不能冒充正式 Audit。

**Implementation notes**：

- PostgreSQL 内的 mutation Audit 与状态写同事务。
- 原文读取必须通过 Data Access Facade 的 audited read API；直接 adapter read 不暴露给 Gateway/Worker。
- 对象读取需先写访问 Audit，取得内容后若完成态 Audit 失败仍不返回内容。
- operational event 只允许 error_type、repository、operation、timestamp 和 trace digest 等低基数字段。

**Alternatives rejected**：事后补写允许不可审计窗口；全部诊断拒绝不利于恢复且并非安全所需。

**Decision source**：DEC-005。

## R-007：Repository 形状与框架复用

**Decision**：上层使用平台 vendor-neutral 领域对象和异步 Protocol，通过单一 Data Access Facade 适配官方 tRPC-Agent 公开 Session/Memory/Knowledge 对象。

**Rationale**：项目宪法要求 Framework-First 和 Contract-First。官方框架负责 Agent 运行语义，平台负责可信 tenant、共享一致性、审计和后端替换。让 Gateway/Worker 直接调用 SQL/Redis 或供应商 SDK 会破坏这两个边界。

**Compatibility strategy**：固定 `trpc-agent-py==1.1.19`；增加 SDK compatibility tests 验证公开对象映射和 Runner 会话连续性；升级依赖前必须先通过这些测试。

## R-008：测试与验收边界

**Decision**：InMemory 是行为 oracle，PostgreSQL 必须通过相同 contract suite；真实 Redis/PostgreSQL 集成测试是阶段完成条件，对象/向量只验证 fake contract。

**Rationale**：只有 InMemory 通过不能证明跨节点事务；缺 Docker 时 skip 是环境事实，不是通过证据。对象/向量未实现真实后端，不能在阶段报告中宣称其生产可用。

**Required evidence**：

- 每项任务同一测试命令的 RED/GREEN 记录。
- Docker 环境下 PostgreSQL Repository、双节点、迁移和故障注入 PASS。
- 对象/向量测试名称和结果含 `fake`/`fixture` 标识。
- 全量回归和敏感材料扫描 0 命中。

## Open Questions

无。具体数值类配置（Memory 最大字节数、Artifact orphan TTL、批量大小和超时）可在 tasks 实施时以配置默认值落地，但不得改变以上安全与一致性语义。

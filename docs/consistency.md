# 一致性与多后端

*本文定义 Mailbox v2 的权威数据、SessionReady 唤醒、租约 fencing 和重放语义。*

---

## 🧭 权威来源与一致性等级

正确性判断只依赖 PostgreSQL。Redis、InMemory 和各类投影都可以丢失或重建，但不能反过来
决定 Session 是否可执行、谁拥有执行权或某个 turn 是否已经提交。

| 数据或动作 | 唯一权威 | 一致性 | 读写规则 |
|---|---|---|---|
| 入站消息和外部幂等 | PostgreSQL `inbound_messages` | 强一致事务 | `(tenant_id, channel, account_id, external_message_id)` 去重 |
| Session 待处理顺序 | PostgreSQL `session_mailboxes` | 单 Session 串行 | `accepted_sequence > resolved_sequence` 表示仍有工作 |
| Session 业务快照 | PostgreSQL `sessions` | fenced commit | mailbox lease 成功后读取；与 mailbox 按固定锁顺序更新 |
| 事件、turn 和最终状态 | PostgreSQL | 强一致、连续 sequence | 只接受当前 `lease_owner + lease_epoch` 的提交 |
| SessionReady outbox | PostgreSQL `outbox_events` | 事务内持久 | 先提交，再由 relay 至少一次发布 |
| SessionReady Stream | Redis `trpc:session-ready:v2` | 至少一次、可重复 | 只作唤醒；不保存 inbound 正文或业务锁 |
| Redis PEL | Redis consumer group | 短交接窗口 | 只由 Reclaimer 恢复 Claim/ACK 未完成的通知 |
| 长任务恢复 | PostgreSQL Lease Sweeper | 强一致接管 | 只扫描过期 `RUNNING` mailbox，并递增 fencing epoch |
| 到期重试 | PostgreSQL Retry Scheduler | 强一致状态转移 | `RETRY_WAIT → QUEUED` 时生成新 generation |
| 丢通知修复 | PostgreSQL Reconciler | 强一致修复 | stale `QUEUED` 复用原 generation；不接管普通过期 `RUNNING` |
| Memory / Summary / Knowledge | PostgreSQL 事实记录 + 投影 | 事实强一致、向量最终一致 | 投影落后回退 PostgreSQL；禁止 sequence 倒退 |
| Artifact bytes | S3/MinIO | 元数据提交后可见 | staging、checksum、提交元数据、孤儿清理 |
| InMemory adapter | 当前进程 | 单节点开发/测试 | 不作为多节点生产权威 |

`sessions` 中的 lease 字段是与旧运行时和业务快照配套的镜像；Mailbox v2 的调度判断以
`session_mailboxes` 为准。清理两处 lease 时必须同时匹配旧的 `lease_owner` 和
`lease_epoch`，不能用 tenant/session 单独清空新 Worker 的 lease。

## 🔐 Mailbox 状态不变量

所有状态转移都在 tenant-scoped PostgreSQL 事务中锁定 mailbox 行；Gateway 接收新消息、
Worker Claim、fenced commit、Sweeper、Retry Scheduler 和 Reconciler 使用同一锁顺序。

```text
session_mailboxes
→ sessions
→ session_turns
→ session_mailbox_items
→ inbound_messages
→ session_events / outbound_messages / outbox_events
```

```text
resolved_sequence <= accepted_sequence
RUNNING  => processing_sequence = resolved_sequence + 1
RUNNING  => processing_inbound_id、lease_owner、lease_expires_at 均非空
非 RUNNING => processing_sequence、processing_inbound_id、lease_owner、lease_expires_at 均为空
有效执行权 => lease_owner + lease_epoch + 未过期 PostgreSQL lease
```

因此：

- `IDLE` 不得拥有未解决 inbound；若 `accepted_sequence > resolved_sequence`，Reconciler 必须重新使其 `QUEUED`。
- `QUEUED` 表示 PostgreSQL 中确实有可执行工作，并且存在或可以重建对应 SessionReady outbox。
- `RETRY_WAIT` 不占用 Agent permit；只有 Scheduler 到期后才转 `QUEUED`。
- `RUNNING` 的失效只能由 Lease Sweeper 或允许直接接管的 Claim 路径处理，Reconciler 不与其竞争。
- 旧 Worker 即使恢复，也只能用原 epoch 访问；epoch 已变化时 renew、commit、retry 和 fail 都必须失败。

## 🔁 SessionReady 的 Claim/ACK 顺序

```mermaid
sequenceDiagram
    accTitle: SessionReady consistency boundary
    accDescr: Redis delivers a hint, PostgreSQL authenticates and claims the mailbox, and Redis is acknowledged only after the short claim transaction succeeds.

    participant redis as Redis Stream
    participant worker as Worker coordinator
    participant pg as PostgreSQL mailbox
    participant agent as Agent turn

    worker->>worker: reserve permit
    redis->>worker: XREADGROUP new or Reclaimer delivery
    worker->>pg: validate event_id + generation
    worker->>pg: SELECT mailbox FOR UPDATE; claim once
    alt claimed
        pg-->>worker: RUNNING + lease_epoch
        worker->>redis: bounded XACK
        worker->>agent: execute one turn
        agent->>pg: fenced commit
    else stale / running / empty
        pg-->>worker: no executable claim
        worker->>redis: bounded XACK
        worker->>worker: release permit; no sleep/reacquire loop
    else database error
        pg-->>worker: error
        worker->>worker: leave delivery pending
    end
```

生产 PostgreSQL Claim 必须同时验证：

1. `event_id` 是该租户、该 Session 的 `session.ready.v2` outbox ID；
2. outbox payload 的 `generation` 与通知一致；
3. mailbox 当前 generation 与通知一致；
4. mailbox 有未解决的下一条 inbound，且没有其他未过期 lease。

Claim 失败但数据库可用时，通知是 stale、already running 或 empty，应 ACK 并释放 permit。
Claim 事务异常时不能 ACK，交给独立 Reclaimer 继续处理。Claim 成功后 ACK 采用有界超时；
ACK 失败不会回滚 PG lease，后续重复通知会被 PG 状态安全去重。

## 🏷️ Generation 与重放语义

`queue_generation` 是可执行唤醒的版本号，不是 Redis fencing token，也不是模型尝试次数。

| 场景 | generation | outbox 行为 | 目的 |
|---|---:|---|---|
| `IDLE → QUEUED` 新工作 | 加一 | 写新 `session.ready.v2` | 新的可执行状态 |
| `RETRY_WAIT` 到期 | 加一 | 写新 `session.ready.v2` | 新的调度机会 |
| 一个 turn 完成后仍有 backlog | 加一 | 写新 `session.ready.v2` | 让出执行权并重新排队 |
| 长期 `QUEUED`，Redis 丢通知 | 不变 | 重开/补回同 generation | 恢复唤醒，不制造 generation 风暴 |
| 已发布事件未被消费 | 不变 | 仅在没有活动 claim 时重新可用 | 不抢占正在交接的事件 |
| Redis 重复投递同一事件 | 不变 | 不新增业务 turn | PG Claim 负责去重 |

PostgreSQL 使用 `(event_type, tenant_id, aggregate_id, generation)` 唯一约束保护
`session.ready.v2`；`ON CONFLICT DO NOTHING` 使 relay、Reconciler 和重复重启安全。InMemory
契约实现必须保持同样的可观察语义：旧状态为 `QUEUED` 时复用 generation，只有从
`RETRY_WAIT` 或 `IDLE` 转为 `QUEUED` 时才生成新 generation。

## ♻️ 四类恢复职责

| 组件 | 扫描/处理范围 | 不得做的事 |
|---|---|---|
| Redis Notification Reclaimer | `XAUTOCLAIM` 发现的 stale PEL SessionReady | 不执行模型、不把 PEL 当业务 lease、不在主消费循环中隐式恢复 |
| PostgreSQL Lease Sweeper | `status='RUNNING' AND lease_expires_at <= clock_timestamp()` | 不处理 Redis 普通通知、不清理新 owner |
| Retry Scheduler | `RETRY_WAIT AND retry_at <= clock_timestamp()` | 不在 Worker permit 内 sleep |
| PostgreSQL Reconciler | 非 RUNNING 异常、committed self-heal、长期 QUEUED 和丢通知 | 不扫描/接管普通过期 RUNNING |

Reclaimer 保存 `XAUTOCLAIM` cursor，并且只覆盖尚未完成 PostgreSQL Claim/ACK 的短窗口。
Redis PEL 不需要在模型执行期间 heartbeat；Agent 长任务只续 PostgreSQL lease。Recovery service
可以多实例运行，行锁和 fencing 负责竞争安全。

## 🧯 故障与可见性矩阵

| 故障 | 可接受结果 | 最终收敛路径 |
|---|---|---|
| Gateway 在 PG commit 前宕机 | IM 重试或返回非 2xx | Inbox 幂等键保证一次接受 |
| XADD 成功、outbox published 更新前宕机 | Redis 出现重复 SessionReady | 同 generation + PG Claim 去重 |
| XREADGROUP 后、PG Claim 前宕机 | 通知留在 PEL | Redis Reclaimer 接管 |
| PG Claim 成功、XACK 前宕机 | 重复通知可能再次到达 | 看到 `RUNNING` 后 ACK，不重复执行 |
| XACK 后、Agent 执行中宕机 | PG lease 过期 | Lease Sweeper 递增 epoch、重新 QUEUED |
| Redis Stream 或 group 全丢 | 唤醒暂时消失 | outbox relay / Reconciler 从 PG 重建 |
| 模型或幂等工具暂时失败 | mailbox 进入 `RETRY_WAIT` | Scheduler 到期生成新 generation |
| 非幂等工具结果未知 | 不自动重放副作用 | effect ledger、对账或人工确认 |

任何故障下都允许重复通知，但不允许两个有效 PG lease，也不允许旧 epoch 提交可见状态。
模型和工具调用期间不得持有数据库事务；最终事件、Session state、outbound 和后续 outbox
必须在当前 lease 的一个 fenced commit 中完成。

## 🧪 契约验证

离线契约至少覆盖：不同租户相同 session 标识的隔离、重复/乱序 SessionReady、Claim 后
ACK 失败、旧 epoch 提交、Lease Sweeper 接管、Retry Scheduler 新 generation、Reconciler
同 generation 重放、Redis 全丢后的 PG 重建，以及不同 Session 并行。生产门禁还应分别记录
Redis PEL 短窗口延迟、PG lease 年龄、queued age、retry wait 数量和 stale epoch 拒绝数。

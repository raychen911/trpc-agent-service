# 一致性、幂等与故障语义

## 1. 可靠性目标

本系统不宣称端到端 exactly-once。跨 PostgreSQL、模型服务、Tool 下游和 IM 平台的分布式事务不存在统一提交点。平台提供的是可审计语义：

- 外部消息采用 at-least-once 接收，通过幂等键压缩为一个 Inbox。
- 每个 session 同时只允许一个有效写者，旧 Worker 的延迟写入被 fence 拒绝。
- SDK 事件可重放，但只有成功完成的 run 事件对下一轮可见。
- 副作用和 IM 投递都保留 `UNKNOWN`，无法证明未发生时不盲目重试。
- 业务回复只能由持久 Outbox 发出，不在 Agent 执行过程中直接调用 IM。

## 2. 三个事务时点

### T0：持久化接收后 ACK

在一个 SQL 事务中：

1. 以 `(tenant_id, binding_id, external_delivery_id)` 查重。
2. 锁定或创建 `session`，分配不跳号的 `accepted_seq`。
3. 写 `inbox_message`，并将当时的 `config_revision` 和 Agent revision 冻结在输入上。
4. 如回调携带敏感回复路由，用 AES-GCM 加密后写 `channel_reply_credential`。
5. 事务提交后 HTTP 边缘才返回 2xx。

同一投递键但不同 payload hash、session、principal、revision 或回复凭据指纹会报幂等冲突，不会被当成正常重试。

### T1：领取与分阶段事件

Worker 按 tenant 调用 `claim_next`。PostgreSQL 路径使用行锁和 `SKIP LOCKED`，并排除同 session 中尚有更早未完成的 Inbox，从而保持 head-of-line 顺序。领取成功时：

- `session.fencing_token` 加一；
- 设置 `lease_owner` 和数据库时钟上的 `lease_expires_at`；
- 创建或复用该 Inbox 的唯一 `agent_run`；
- takeover 时废弃旧 attempt 的 staged event；
- 向 Worker 返回只读 `SessionClaim`。

SDK 流中的 partial event 只用于运行时展示，不持久。每个完整 event 先通过带 tenant/session/event/seq AAD 的加密 codec 封存，然后按以下条件追加：

```text
session.log_version == expected_version
session.fencing_token == claim.fencing_token
session.lease_owner == claim.worker_id
session.lease_expires_at > database_now
```

事件初始为 `staged`。`event_id` 和 `(run_id, event_key)` 可重试；但相同键如果内容、哈希、角色或 state delta 不同，会拒绝。

### T2：原子发布

Worker 在完整消费 `Runner.run_async` 后，在一个 SQL 事务中完成：

1. 再次检查租约、fence、attempt 和 run 状态。
2. 将当前 attempt 的 staged event 转为 `committed`。
3. 写入最终 session state，并使 `state_version == log_version`。
4. 固化文本 Outbox schema，按 `(reply_id, part_no)` 写入。
5. 写入一条带 record hash 的 AuditLog。
6. 写入与该 run 唯一绑定的 ProjectionJob。
7. 将 Run 和 Inbox 标记成功，释放 session 租约。

任一步失败则整个 T2 回滚，不会出现“状态已可见但没有回复意图”或“Outbox 已发布但审计缺失”。

## 3. 关键幂等键

| 边界 | 幂等键 | 内容绑定 | 冲突策略 |
|---|---|---|---|
| IM 入站 | tenant + binding + external delivery ID | payload hash、session、principal、config/app revision、credential fingerprint | 相同内容返回 duplicate；异内容报冲突 |
| Agent Run | tenant + inbox | request、session、app revision | takeover 复用同一 run，attempt 递增 |
| Event | tenant + event ID；tenant + run + event key | 完整规范化事件哈希 | 完全一致则 already appended；否则拒绝 |
| Tool Effect | tenant + idempotency key | tool/version/effect class/args hash | 成功结果复用；不可幂等的超时执行进入 unknown |
| Reply Outbox | tenant + reply ID + part number | payload hash | 严格按 part number 投递；变更负载拒绝 |
| 后端投影 | tenant + 对象 ID + version/watermark | 规范 JSON 或字节哈希 | 退水位拒绝；同版本异内容拒绝 |

## 4. 异常结果不等于失败重试

### Tool Effect

工具按副作用分类：

- `read`、`idempotent_write`：租约过期后可用新 execution token 重领，旧 token 的延迟完成被拒绝。
- `non_idempotent_write`：如果执行超时而结果不明，状态变为 `unknown`，系统不自动再执行。

当前仓储层已实现该账本，但 `TenantToolSet` 还没有将每个实际 Tool 包装到 Tool Effect 执行器中。因此这是已验证的可靠性原语，尚不是所有 Tool 的端到端保证。

### IM 投递

Outbox 每次领取生成或复用 stable delivery token，并通过 dispatcher ID、attempt 和过期时间防止旧进程回写。分片只有在所有更小 `part_no` 已 `sent` 后才可领取。

- 明确未发送，例如 connect 失败或 Telegram 429：可有界退避重试。
- 明确被拒绝，例如确定的 4xx 或非法路由：`dead_letter`。
- 请求可能已到达，例如 read/write timeout、模糊 5xx、租约过期：`unknown`，进入对账而不是盲目重试。

企业微信 `response_url` 有效期短且只能使用一次，任何模糊结果都不自动再发。Telegram 对明确连接失败可重试，但 read/write timeout 仍进入 `unknown`。

## 5. Session event、state、summary 与 memory 顺序

```text
Inbox accepted
  -> Event staged and log_version advances
  -> Run finalized
  -> Event committed and state_version catches log_version
  -> Summary projection advances through_seq
  -> Memory projection writes source_event_id plus extractor_version
  -> Vector or cache projection advances its own watermark
```

Summary 只能用更大 `through_seq` 更新；同一水位内容不同会冲突。Memory 以 `(source_event_id, extractor_version)` 写一次，从而容许抽取任务重放。跨节点可见性以持久后端的成功提交为准；不把进程内缓存视为可见性证据。

T2 在同一事务中创建唯一 ProjectionJob。常驻 Projector 使用独立 lease、heartbeat 和 fencing token 领取任务；算法失败有界退避，永久错误或超限进入 dead letter。默认窗口摘要只复述已提交 event，不引入模型幻觉；默认 Memory 仅接受用户以 `remember:`、`记住:` 或 `请记住:` 明确发出的记忆指令。摘要与 Memory 算法都带不可变版本，可替换为经过离线评估的语义算法。

## 6. 故障矩阵

| 故障 | 持久状态 | 恢复方式 | 禁止行为 |
|---|---|---|---|
| Gateway 在 T0 前崩溃 | 无 Inbox | IM 重投后重新验签接收 | 未提交却返回成功 ACK |
| Gateway 在 T0 后、ACK 前崩溃 | Inbox 已在 | IM 重投命中 duplicate | 创建第二个 Inbox |
| Worker 失联 | Run/Inbox 仍 running，租约过期 | 新 Worker 增加 fence、废弃旧 staged event、复用 run | 信任旧 Worker 的延迟完成 |
| 模型超时 | attempt 未发布 | 废弃 staged event，在有限 attempt 内重试；超限固化通用错误回复 | 把 provider 错误原文写日志或回用户 |
| 数据库短暂不可用 | 以最后成功事务为准 | 由 IM 重投或 Worker 租约超时恢复 | 使用本地内存冒充权威提交 |
| 不可幂等 Tool 超时 | ToolEffect `unknown` | 人工/对账任务查下游 ID | 自动再执行 |
| IM 投递结果模糊 | Outbox 与 credential `unknown` | 通道对账或人工处置 | 把模糊结果当成已失败并盲目重试 |
| Redis/向量投影丢失 | SQL 权威 event 与 ProjectionJob 仍在 | 按 committed watermark 重建 | 用较旧投影覆盖 SQL state |

## 7. 证据边界

SQLite 测试覆盖去重、顺序、OCC、takeover、原子 finalize、Outbox、Tool Effect、ProjectionJob、投影乱序和加密事件对象状态机。PostgreSQL 专属测试覆盖 `SKIP LOCKED`、fencing、FORCE RLS、append-only audit/event object，并由 CI 服务配置触发。当前开发机没有真实 PostgreSQL 通过记录；这一限制不应被 SQLite 结果替代。

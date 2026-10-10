# Session 调度器 v1/v2 切换运行手册

*本文规定旧 inbound 调度协议与 Mailbox v2 的停入站、排空、启用和回滚顺序。*

---

## 🧭 切换原则

v1 和 v2 是两套不同的 Redis 消息协议，不能依靠滚动发布在同一个数据库上并行处理新
入站。v1 只允许用于兼容排空；生产新流量应使用 v2：

| 项目 | v1（legacy drain only） | v2（Mailbox runtime） |
|---|---|---|
| Redis Stream | `trpc:inbound:v1` | `trpc:session-ready:v2` |
| Consumer group | `trpc-workers-v1` | `trpc-session-ready-v2` |
| Outbox event | `inbound.accepted` | `session.ready.v2` |
| Redis payload | inbound 级旧任务 | SessionReady 唤醒字段 |
| PostgreSQL 权威 | 旧 inbound/Session lease 路径 | `session_mailboxes` + `lease_epoch` |
| Worker 顺序 | 旧版兼容处理 | permit → XREADGROUP → PG Claim once → bounded XACK → one turn |
| 恢复组件 | 旧版排空路径 | Redis Reclaimer、PG Lease Sweeper、Retry Scheduler、PG Reconciler |
| 应急 Stream | `trpc:emergency:v1` | `trpc:emergency:v2` |

v2 Worker 不读取 v1 Stream，v1 Worker 不读取 SessionReady。切换前后保留旧 Stream、group、
outbox 和审计记录，不使用 `DEL`、`XTRIM` 或 `XGROUP DESTROY` 伪造排空。

## 🛡️ 不可违反的运行规则

1. 同一数据库在切换窗口不能让 v1 和 v2 Gateway、Connector、Outbox relay、Worker 同时接受新消息。
2. 只有同一协议、同一数据库契约和同一配置语义的镜像升级才可使用 Kubernetes `RollingUpdate`；v1↔v2 必须先停入站并将相关副本降为零。
3. `session_mailboxes` 是 v2 调度权威；不得手工把 `QUEUED` 改成 `IDLE`，不得倒退 `accepted_sequence` 或 `resolved_sequence`。
4. Redis 只负责唤醒。v2 的 PEL 只覆盖通知交接，不覆盖 Agent 执行周期；禁止为 v2 增加模型执行期间的 Redis PEL heartbeat 或 Redis 长期 owner。
5. v2 Worker 每个 Claim 默认只执行一个 turn。Claim 成功后立即进行有界 `XACK`，再开始模型/工具调用；`BUSY`、`STALE`、`EMPTY` 都必须释放 permit，不能 sleep 后循环抢锁。
6. Redis 通知可重复。`event_id + tenant_id + session_id + generation` 必须通过 PostgreSQL outbox/mailbox 校验；同 generation 的重放不得制造新的 generation 风暴。
7. 全局统计必须使用受审计的 DBA/只读报告会话。运行账号需要先设置 `SET LOCAL app.tenant_id`，不能以未设置租户上下文的业务连接判断全局 `0`。

## 🔍 切换前记录与判据

先记录版本、镜像、配置和副本数：

```bash
NS=<namespace>
kubectl -n "$NS" get configmap trpc-service-config \
  -o jsonpath='{.data.TRPC_SERVICE_SCHEDULER_VERSION}{"\n"}{.data.TRPC_SERVICE_REDIS_STREAM}{"\n"}{.data.TRPC_SERVICE_REDIS_CONSUMER_GROUP}{"\n"}'
kubectl -n "$NS" get deploy \
  trpc-gateway trpc-wecom-connector trpc-worker \
  trpc-outbox-dispatcher trpc-session-recovery -o wide
kubectl -n "$NS" get deploy trpc-gateway trpc-wecom-connector \
  trpc-worker trpc-outbox-dispatcher trpc-session-recovery \
  -o custom-columns='NAME:.metadata.name,REPLICAS:.spec.replicas,IMAGE:.spec.template.spec.containers[0].image'
```

使用受控数据库会话检查；`<tenant_id>`、`<redis-cli-wrapper>`、`<namespace>` 仅是占位符，
真实密钥不得写入仓库或 shell 历史：

```sql
-- 由 DBA/报告角色执行，并先确认能看到一个已知租户。
SELECT count(*) AS v1_unpublished
  FROM outbox_events
 WHERE event_type = 'inbound.accepted'
   AND published_at IS NULL;

SELECT count(*) AS v2_unpublished
  FROM outbox_events
 WHERE event_type = 'session.ready.v2'
   AND published_at IS NULL;

SELECT
  (SELECT count(*) FROM sessions WHERE lease_owner IS NOT NULL) AS session_leases,
  (SELECT count(*) FROM session_turns WHERE status = 'processing') AS processing_turns,
  (SELECT count(*) FROM inbound_messages WHERE status = 'processing') AS processing_inbounds,
  (SELECT count(*) FROM session_mailboxes WHERE status = 'RUNNING') AS running_mailboxes;

SELECT status, count(*)
  FROM session_mailboxes
 GROUP BY status
 ORDER BY status;

SELECT count(*) AS unresolved_mailboxes
  FROM session_mailboxes
 WHERE accepted_sequence <> resolved_sequence
    OR status <> 'IDLE';
```

Redis 只读检查使用部署侧 secret-aware wrapper：

```bash
REDIS=<redis-cli-wrapper>

$REDIS XPENDING trpc:inbound:v1 trpc-workers-v1
$REDIS XINFO STREAM trpc:inbound:v1
$REDIS XINFO GROUPS trpc:inbound:v1
$REDIS XPENDING trpc:session-ready:v2 trpc-session-ready-v2
$REDIS XINFO STREAM trpc:session-ready:v2
$REDIS XINFO GROUPS trpc:session-ready-v2
$REDIS XPENDING trpc:emergency:v1 trpc-emergency-drainers-v1
$REDIS XPENDING trpc:emergency:v2 trpc-emergency-drainers-v2
```

对每个待排空 Stream，必须同时满足：

- `XPENDING` summary 的 pending 为 `0`；
- group `last-delivered-id` 已追上 stream `last-generated-id`；若 stream 不存在，还要证明对应 PostgreSQL outbox 为 `0`；
- 对应 outbox 的 `published_at IS NULL` 为 `0`；
- v1 旧 lease、v2 `session_mailboxes.status='RUNNING'`、processing turn 和 processing inbound 均为 `0`。

v2 的 PEL 年龄只代表 `XREADGROUP → PG Claim → XACK` 的通知交接窗口，不能用模型执行时长
解释或通过 Redis heartbeat 延长。Claim 后已经 ACK 的 Agent turn 由 PostgreSQL lease
heartbeat 和 Lease Sweeper 负责。

## 🔄 v1 → v2

### 1. 停止产生新的入站

先在外部负载均衡器阻断飞书/企业微信 callback，再保存副本数并停止会建立入站连接或
接收 callback 的角色。IM 平台重试必须保持可重试，不能在入口仍可达时切换：

```bash
kubectl -n "$NS" scale deployment/trpc-gateway trpc-wecom-connector --replicas=0
kubectl -n "$NS" rollout status deployment/trpc-gateway --timeout=120s
kubectl -n "$NS" rollout status deployment/trpc-wecom-connector --timeout=120s
```

### 2. 排空 v1

保持 v1 Worker 和 v1 Outbox dispatcher 运行，直到 v1 的 PostgreSQL、Redis 和应急通道
判据全部满足。然后停止 v1 消费者；不要删除 v1 Stream 或 PEL：

```bash
kubectl -n "$NS" scale deployment/trpc-worker trpc-outbox-dispatcher --replicas=0
kubectl -n "$NS" rollout status deployment/trpc-worker --timeout=120s
kubectl -n "$NS" rollout status deployment/trpc-outbox-dispatcher --timeout=120s
```

停止后重新检查。若仍有 processing 或 lease，先等待已批准的恢复路径完成，不要手工清理。
v2 尚未启动时，已经存在的 `session_mailboxes.status='QUEUED'` 必须保留，交给 v2 Worker。

### 3. 在所有相关 Pod 为零时切换配置

先审核无密钥 ConfigMap patch，只改变调度器三元组：

```json
{"data":{"TRPC_SERVICE_SCHEDULER_VERSION":"v2","TRPC_SERVICE_REDIS_STREAM":"trpc:session-ready:v2","TRPC_SERVICE_REDIS_CONSUMER_GROUP":"trpc-session-ready-v2"}}
```

```bash
kubectl -n "$NS" patch configmap trpc-service-config \
  --type merge --patch-file cutover-v2-config.patch.json
```

若同时升级镜像，也要在副本数为零时更新所有读取该 ConfigMap 的角色。启动顺序固定为：

1. v2 `session-recovery`，启动 Lease Sweeper、Retry Scheduler 和 PG Reconciler；
2. v2 `outbox-dispatcher`，将 `session.ready.v2` outbox 发布到 SessionReady；
3. v2 `worker`，启动 permit-bound `XREADGROUP` 和独立 PEL Reclaimer；
4. 确认 Pod 配置为 v2 后，恢复 Gateway 和 WeCom Connector 原副本数。

```bash
kubectl -n "$NS" scale deployment/trpc-session-recovery --replicas=1
kubectl -n "$NS" scale deployment/trpc-outbox-dispatcher --replicas=<saved_outbox_replicas>
kubectl -n "$NS" scale deployment/trpc-worker --replicas=<saved_worker_replicas>
kubectl -n "$NS" rollout status deployment/trpc-session-recovery --timeout=120s
kubectl -n "$NS" rollout status deployment/trpc-outbox-dispatcher --timeout=120s
kubectl -n "$NS" rollout status deployment/trpc-worker --timeout=120s
kubectl -n "$NS" scale deployment/trpc-gateway --replicas=<saved_gateway_replicas>
kubectl -n "$NS" scale deployment/trpc-wecom-connector --replicas=<saved_wecom_replicas>
```

恢复入口后发送一条专用验收消息，确认：PG 先落 Inbox/mailbox；outbox 产生
`session.ready.v2`；Worker 在 Claim 后 ACK；只执行一个 turn；最终 commit 后状态为
`IDLE` 或新的 `QUEUED` generation。旧 v1 Stream/group 至少保留完整观察窗口。

## ↩️ v2 → v1 回滚

v1 不理解 SessionReady，也不会根据 mailbox 状态恢复工作。因此不能直接把 ConfigMap 改回
v1。只有以下条件全部满足时才允许回滚：

- 所有 `session_mailboxes.status` 为 `IDLE`；
- 每个 mailbox `accepted_sequence = resolved_sequence`；
- `session.ready.v2` 未发布 outbox 为 `0`；
- v2 Stream/group 和 v2 emergency group 的 unread/pending 为 `0`；
- `sessions.lease_owner`、processing turn、processing inbound 均为 `0`；
- 没有运行中的 v2 turn，也没有待人工确认的未知外部副作用。

若任一条件不满足，继续运行 v2 并先排空 mailbox、retry 和 outbox。禁止把
`session_mailbox_items` 手工转换成 v1 Redis inbound 消息；未排空回滚必须另行实现并验收
按租户的 mailbox→v1 migration/control hook，本手册不授权手工转换。

满足条件后：

1. 阻断 callback，缩容 Gateway/WeCom Connector；
2. 让 v2 Outbox/Worker 完成最后一轮 Claim/ACK/commit，然后缩容为零；
3. 确认 v2 SQL/Redis 判据仍为零；
4. 停止 `session-recovery`；
5. 在相关 Pod 全部为零时，将调度器三元组改回 `v1`、`trpc:inbound:v1`、`trpc-workers-v1`；
6. 启动 v1 Outbox/Worker，确认 v1 group 正常消费；
7. 最后恢复 Gateway/WeCom Connector，并观察 v1 lease、inbound outbox 和 PEL。

回滚期间保留 v2 Stream、group、outbox 和审计记录。若 v2 已产生 `ambiguous` outbound，
先完成人工确认；切换调度器版本不能把未知投递当作可安全重发。

## ✅ 切换后验证与观察窗口

```bash
kubectl -n "$NS" get configmap trpc-service-config \
  -o jsonpath='{.data.TRPC_SERVICE_SCHEDULER_VERSION}{" "}{.data.TRPC_SERVICE_REDIS_STREAM}{" "}{.data.TRPC_SERVICE_REDIS_CONSUMER_GROUP}{"\n"}'
kubectl -n "$NS" get pods -l app.kubernetes.io/part-of=trpc-agent-service \
  -o custom-columns='NAME:.metadata.name,READY:.status.containerStatuses[0].ready,IMAGE:.spec.containers[0].image'
```

观察窗口内持续记录以下指标和报告：

- `session_ready_stream_lag`、PEL 数和短交接最大年龄；
- `session_state_count`、queued age、retry wait 数量和 lease expired 数；
- stale generation/Claim、stale epoch commit rejection 和 Reconciler 重放次数；
- callback confirmation latency、turn latency、outbound delivery lag 和错误率。

验收消息必须证明重复 SessionReady 只产生一个有效 lease，重复回调不增加 inbound sequence，
杀死已 ACK 的 Worker 后由 Lease Sweeper 重新排队，Redis 清空后 Reconciler 可重建唤醒，
不同 Session 仍可并行。只有停入站记录、排空计数、Pod 状态和 JSON 验收报告均归档后，才能
关闭 v1/v2 变更窗口。

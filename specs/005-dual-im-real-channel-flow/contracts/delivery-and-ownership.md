# Contract: 回复交付与 Adapter 所有权

**Feature**：005-dual-im-real-channel-flow
**Purpose**：保证回复可恢复但不重复运行 Agent，并保证同一渠道身份只有一个活动连接

## 1. DeliveryRepository

~~~text
async create_or_get(
  tenant_scope,
  binding_scope,
  execution_result,
  reply_context,
  adapter_fence
) -> DeliveryRecord

async begin_attempt(
  tenant_scope,
  delivery_id,
  expected_status,
  adapter_fence,
  trace_id
) -> DeliveryAttempt

async finish_attempt(
  tenant_scope,
  attempt_id,
  outcome,
  safe_error_code?,
  retry_delay_seconds?,
  adapter_fence
) -> DeliveryRecord

async get(tenant_scope, delivery_id) -> DeliveryRecord
async list_due(tenant_scope, now, limit) -> list[DeliveryRecord]
~~~

规则：

- create_or_get 对同一执行结果幂等。
- begin/finish 必须在 PostgreSQL 条件写中校验当前状态和 Adapter fence。
- list_due 只返回 RETRY_WAIT 且 next_attempt_at 到期的记录。
- DELIVERY_UNKNOWN 不进入 list_due。
- Repository 不提供任何会重新调用 Agent 的接口。

## 2. Delivery State Machine

~~~text
PENDING -> SENDING
SENDING -> DELIVERED
SENDING -> RETRY_WAIT
SENDING -> DELIVERY_FAILED
SENDING -> DELIVERY_UNKNOWN
RETRY_WAIT -> SENDING
~~~

终态：

- DELIVERED：平台明确确认。
- DELIVERY_FAILED：永久失败或明确临时失败达到最大次数。
- DELIVERY_UNKNOWN：请求可能到达平台但结果无法确认。

非法转换必须抛 ConditionalWriteFailed，并在 read-back 后收敛；不得重新发送猜测结果。

## 3. Retry Policy

attempt_no 语义：

| Attempt | Trigger delay | Allowed |
|---:|---:|---|
| 1 | 0 | initial |
| 2 | 1s | previous transient |
| 3 | 2s | previous transient |
| 4 | 4s | previous transient |

第 4 次仍 transient 后进入 DELIVERY_FAILED。PERMANENT 直接失败；UNKNOWN 在任何
attempt 直接进入 DELIVERY_UNKNOWN。

测试必须注入 Clock/Sleeper，不得用真实 7 秒 sleep。

## 4. AdapterOwnershipRepository

~~~text
async acquire(identity_digest, node_id, lease_ms)
  -> AdapterLeaseHandle | LeaseBusy

async AdapterLeaseHandle.renew(lease_ms) -> AdapterFence
async AdapterLeaseHandle.mark_ready(runtime_bot_identity) -> AdapterFence
async AdapterLeaseHandle.release(reason) -> None
async inspect(identity_digest) -> AdapterOwnershipState
~~~

AdapterFence：

~~~text
identity_digest
node_id
generation
owner_token
expires_at
~~~

owner_token 不得序列化、记录或返回管理接口。

## 5. Ownership Rules

- acquire、generation 增加和 lease 创建为 Redis 原子操作。
- Redis PTTL 是有效性裁决，不依赖节点本机时间。
- renew 仅允许当前未过期 generation/token/node。
- mark_ready 只能由已认证、身份匹配的当前 owner 调用。
- 失租、续租不确定或发现更高 generation 时立即 not_ready 并关闭连接。
- 旧 owner release 不得删除新 owner lease。
- 所有发送和 Delivery 条件写必须携带当前 AdapterFence。

## 6. Active/Standby Runtime

~~~text
STANDBY
  -> ACQUIRING
  -> AUTHENTICATING
  -> CONNECTING
  -> READY

READY -- renew success --> READY
READY -- lease lost/disconnect --> DRAINING -> STANDBY
AUTH failure --> NOT_READY
standby wins expired lease --> new generation AUTHENTICATING
~~~

认证失败不应快速无限重试；使用有界指数退避并保留 not_ready 证据。具体连接退避
参数由配置提供，测试只断言有界、可取消和无 Secret。

## 7. Failure Classification

| Failure | Delivery transition | Automatic resend |
|---|---|---:|
| explicit success ACK | DELIVERED | 0 |
| explicit transient before/with safe negative ACK | RETRY_WAIT or DELIVERY_FAILED at max | <=3 |
| explicit permanent negative ACK | DELIVERY_FAILED | 0 |
| ACK timeout after request emission | DELIVERY_UNKNOWN | 0 |
| connection loss before emission is proven | RETRY_WAIT | <=3 |
| emission status cannot be proven | DELIVERY_UNKNOWN | 0 |
| stale Adapter fence | no provider call; fence rejected | 0 |

未知 vendor exception 默认 UNKNOWN。

## 8. Audit Requirements

每次状态变化记录 tenant、channel、binding digest、execution trace、delivery trace、
adapter node/generation、attempt number、outcome 和 safe error code。

不得记录：

- 回复原文以外的原始入站 payload。
- Secret、secret_ref 的解析值、token、ticket、access_key。
- owner_token、完整 WebSocket URL、SDK frame 或 vendor stack。

## 9. Recovery Rules

- 重放相同外部消息先进入第三阶段幂等；不得重新运行 Agent。
- 若已有 DeliveryRecord：
  - DELIVERED：不发送。
  - RETRY_WAIT：仅到期任务发送。
  - DELIVERY_FAILED：不自动发送。
  - DELIVERY_UNKNOWN：不自动发送，等待显式人工/平台确认流程。
- Adapter 接管后可处理到期 RETRY_WAIT，但必须以新 generation 条件 claim。
- 旧节点的迟到 ACK 只作为 diagnostic evidence，不能覆盖新代已提交状态。

## 10. Contract Tests

1. 同 execution result 重复 create 只产生一条 DeliveryRecord。
2. 1、2、4 秒计划及最多 4 个总 attempt。
3. permanent/unknown 自动重试次数均为 0。
4. 任一交付路径 Agent call 增量为 0。
5. 两节点同时 acquire，READY 数最大 1。
6. 租约到期后 generation 单调增加。
7. 旧 fence 的 send/begin/finish 全部拒绝。
8. 失租节点连接关闭且 readiness=false。
9. 接管后重放消息仍由第三阶段幂等抑制。
10. 日志、Audit 和异常扫描无敏感信息。

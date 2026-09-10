# 预算、结算与跨节点恢复契约

## 1. BudgetRepository

```python
class BudgetRepository(Protocol):
    async def reserve_maximum(
        self, *, tenant_id: str, execution_id: str,
        policy: ActiveGovernancePolicy,
        maximums: Mapping[UsageDimension, Decimal],
        owner_generation: int, trace_id: str
    ) -> BudgetReservationSet: ...

    async def mark_execution_started(
        self, *, tenant_id: str, execution_id: str,
        owner_generation: int
    ) -> BudgetReservationSet: ...

    async def settle(
        self, *, tenant_id: str, execution_id: str,
        actuals: Mapping[UsageDimension, Decimal],
        owner_generation: int
    ) -> BudgetSettlement: ...

    async def release_before_execution(
        self, *, tenant_id: str, execution_id: str,
        owner_generation: int, reason: str
    ) -> BudgetReservationSet: ...

    async def get_by_execution(
        self, *, tenant_id: str, execution_id: str
    ) -> BudgetReservationSet | None: ...
```

## 2. 严格预占语义（DEC-003/A）

- `maximums` 必须来自当前 active policy，不接受客户端覆盖。
- request、tool_call、token、cost 四维在同一 PostgreSQL 事务中全部预占；任一剩余量不足则全部回滚并返回 `budget_exhausted`。
- 不变量为每个账户 `settled + reserved <= hard_limit`。并发节点必须通过条件更新/行锁保证该式始终成立。
- `(tenant_id, execution_id, dimension)` 唯一；同 execution_id 的重复 reserve 返回原 reservation，不再次占用。
- 实际值不得超过已预占 maximum；Runner/Tool 的执行限制与 maximum 使用同一策略来源。
- settle 一次性把 actual 加到 settled、从 reserved 释放 maximum，并返回差额；重复 settle 返回原结果。
- 只有 `execution_started=false` 才能 release；开始后的失败不得伪装成零用量。

稳定异常：`BudgetExhausted`、`BudgetUnavailable`、`BudgetConflict`、`FencingRejected`、`UsageExceedsReservation`。

## 3. GovernanceRecoveryRepository

```python
class GovernanceRecoveryRepository(Protocol):
    async def record_stage(self, marker: GovernanceRecoveryMarker) -> None: ...
    async def list_recoverable(self, *, before: datetime, limit: int) -> list[GovernanceRecoveryMarker]: ...
    async def claim(self, *, marker_id: str, node_id: str, generation: int) -> GovernanceRecoveryMarker: ...
    async def complete(self, *, marker_id: str, generation: int, outcome: str) -> None: ...
```

恢复 marker 与既有 message/session/reply recovery 关联，至少区分：`ADMITTED`、`RESERVED`、`EXECUTION_STARTED`、`RESULT_DURABLE`、`SETTLED`、`AUDITED`、`DELIVERY_PENDING`、`REVIEW_REQUIRED`。

## 4. 故障矩阵

| Failure point | Safe action after recovery | Forbidden action |
|---------------|----------------------------|------------------|
| policy/grant/content read before reserve | fail closed; retry whole admission with same message id | 使用缓存宽松放行 |
| some budget dimension cannot reserve | transaction rollback; return exhausted/unavailable | 保留部分维度占用 |
| node lost after reserve, before start | new generation releases or resumes same reservation | 创建第二个 execution_id |
| node lost after execution started, before result | mark/retain `REVIEW_REQUIRED` | 自动重新运行 Agent/危险工具 |
| durable result exists, settle missing | settle actual once and complete audit | 重新产生 actual usage |
| settle committed, audit/final result missing | append missing audit/result using same execution id | 第二次 settle |
| outbound reply failed | existing delivery retry sends safe cached reply | 回到 Agent、Tool 或 reserve |
| old owner resumes | reject by fencing generation | 覆盖新 owner 状态 |

## 5. 审计原子性边界

- 高风险执行前必须有可持久的授权/确认审计；写入失败则不执行。
- 最终 budget settle 与治理审计尽量在同一 PostgreSQL 事务完成。
- 跨 Redis 确认与 PostgreSQL 预算不宣称分布式原子事务；用 execution_id、状态机、fencing 和 recovery marker 收敛。
- 确认已 `EXECUTING` 后 Redis 丢失不能授权第二次执行；PostgreSQL recovery 事实是保守阻断依据。

## 6. 测试契约

- InMemory 与 PostgreSQL 对 reserve/settle/release/fencing 返回相同 domain 语义。
- 20 个跨节点并发请求竞争临界额度，只有完整最大预占可成功，最终不变量成立。
- 剩余额度低于 maximum 时，即便确定性 actual 更低也拒绝。
- actual 小于 maximum 后差额可供下一请求使用。
- 相同 execution_id reserve/settle/recover 重复至少 10 次只扣一次。
- 在每个恢复 stage 终止一个 Worker，健康 Worker 只执行安全后继动作。
- 策略、预算、审计或恢复存储不可用时不进入受控执行。
- 回复失败和消息重复不改变已结算用量。

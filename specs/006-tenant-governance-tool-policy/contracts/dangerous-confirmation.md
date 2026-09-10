# 危险操作确认契约

## 1. 目标

飞书和企业微信同时支持一次性文本编号与最小结构化按钮，但两种入口必须引用并消费同一份 `PendingConfirmation`，形成单一授权状态与最多一次工具执行。

## 2. PendingConfirmationRepository

```python
class PendingConfirmationRepository(Protocol):
    async def create_once(
        self, pending: PendingConfirmation
    ) -> PendingConfirmation: ...

    async def claim(
        self, *, intent: ConfirmationIntent,
        owner_node_id: str, owner_generation: int, now: datetime
    ) -> ConfirmationClaim: ...

    async def mark_executing(
        self, *, confirmation_id: str,
        claim_token: str, owner_generation: int
    ) -> PendingConfirmation: ...

    async def complete(
        self, *, confirmation_id: str, claim_token: str,
        owner_generation: int, result_digest: str
    ) -> PendingConfirmation: ...

    async def cancel(
        self, *, confirmation_id: str, reason: str
    ) -> PendingConfirmation: ...
```

## 3. 绑定与验证

claim 必须同时匹配：

- trusted tenant_id、channel、binding_id；
- 原渠道稳定主体摘要；
- agent_name 与 tenant-scoped session_id；
- tool_name、operation_digest 与 arguments_digest；
- 当前 active policy_id/version；
- 未过期 TTL、`PENDING` 状态和正确确认码散列/button token；
- 当前节点持有的 fencing generation。

任一不匹配均不得返回执行权。Repository 不保存明文确认码、完整参数、Secret 或可伪造 tenant 字段。

## 4. 两种输入的统一转换

```python
def parse_confirmation_text(message: UnifiedInboundMessage) -> ConfirmationIntent | None: ...
def parse_confirmation_button(event: TrustedChannelCallback) -> ConfirmationIntent: ...
```

- 文本形态示例为安全固定前缀加一次性短码；短码只用于定位并验证 hash。
- 按钮携带 opaque token，不嵌入 tenant、权限、参数或 Secret。
- 两个 parser 输出同一 domain type，并调用同一 Repository `claim`。
- Adapter 不决定是否允许工具，仅负责验证 SDK 回调并构造可信渠道字段。

## 5. 状态与接管语义

| Current | Event | Next | Rule |
|---------|-------|------|------|
| PENDING | valid claim | CLAIMED | 原子一次；其余获得 stable conflict/cached result |
| PENDING | ttl elapsed | EXPIRED | 不执行；用户重新发起原操作 |
| PENDING | policy/identity/action changed | CANCELLED | 默认拒绝 |
| CLAIMED | mark before tool | EXECUTING | 必须校验 claim token 与 generation |
| CLAIMED | owner lost before start | PENDING | 租约到期可由新 generation 接管 |
| EXECUTING | durable result | COMPLETED | 结果摘要与 execution_id 固化 |
| EXECUTING | owner lost, result unknown | OUTCOME_UNKNOWN | 禁止自动重放，进入恢复审查 |

`COMPLETED/EXPIRED/CANCELLED/OUTCOME_UNKNOWN` 为终态；旧 generation 的任何写入被拒绝。

## 6. 回复契约

危险操作第一次触发时，核心返回：

```json
{
  "status": "confirmation_required",
  "safe_message": "该操作需要确认，请在有效期内回复确认编号或点击确认按钮。",
  "confirmation": {
    "display_code": "opaque-one-time-code",
    "button_token": "opaque-token",
    "expires_at": "UTC timestamp"
  }
}
```

该字段是对既有 `UnifiedReply` 的可选扩展。Feishu/WeCom Adapter 映射为各自最小按钮；不支持按钮时仍可使用文本编号。通用业务卡片不在本阶段。

## 7. 稳定结果

- `confirmation_required`：尚未执行，等待用户动作。
- `confirmation_invalid`：身份、Session、动作或 token 不匹配。
- `confirmation_expired`：超过有效期。
- `confirmation_consumed`：已经由文本或按钮消费；如已有结果可复用安全结果。
- `policy_stale`：确认创建后策略已变化，取消旧确认并重新评估。
- `governance_outcome_unknown`：已开始副作用但结果不能证明，不自动重试。
- Redis 不可用：`governance_unavailable`，不得从消息正文重建确认状态。

## 8. 必需测试

- 飞书、企业微信的文本与按钮 parser 产生等价 `ConfirmationIntent`。
- 先文本后按钮、先按钮后文本各重复至少 10 次，工具执行计数均为 1。
- 两节点同时 claim，只有一个获得执行权。
- 其他租户、渠道、用户、Session、参数及过期确认全部拒绝。
- 准备执行前策略收紧，旧确认取消且工具调用为 0。
- CLAIMED 节点中断可接管；EXECUTING 结果未知不能接管执行。
- InMemory 与 Redis 实现具有相同状态和错误语义。

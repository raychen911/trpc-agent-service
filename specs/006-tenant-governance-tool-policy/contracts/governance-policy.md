# 治理策略与主体授权契约

## 1. 目的

定义 Governance Coordinator 与策略、主体授权、内容规则之间的端口。InMemory 与 PostgreSQL 实现必须通过相同契约测试；调用方不得直接读取实现内部表或缓存。

## 2. GovernancePolicyRepository

```python
class GovernancePolicyRepository(Protocol):
    async def get_active(
        self, *, tenant_id: str, agent_name: str, binding_id: str
    ) -> ActiveGovernancePolicy: ...

    async def create_version(
        self, *, tenant_id: str, scope: PolicyScope,
        document: GovernancePolicyDocument, actor_digest: str
    ) -> GovernancePolicyVersion: ...

    async def activate(
        self, *, tenant_id: str, policy_id: str, expected_generation: int
    ) -> ActiveGovernancePolicy: ...

    async def disable(
        self, *, tenant_id: str, policy_id: str, expected_generation: int
    ) -> ActiveGovernancePolicy: ...
```

语义：

- `get_active` 必须权威读取当前有效指针；没有、禁用、跨租户或存储不可用时不能返回宽松默认值。
- `activate/disable` 以 compare-and-set generation 原子提交；成功提交后所有新判断和工具前检查立即遵守新状态。
- 策略版本内容不可修改；更新必须创建新版本。
- 缓存实现可复用同版本解析结果，但每次授权仍需确认 active 版本与状态。

稳定异常：`PolicyMissing`、`PolicyDisabled`、`PolicyStale`、`GovernanceUnavailable`。

## 3. PrincipalGrantRepository

```python
class PrincipalGrantRepository(Protocol):
    async def evaluate(
        self, *, principal: ChannelPrincipal,
        agent_name: str, binding_id: str, at: datetime
    ) -> PrincipalGrantDecision: ...

    async def put(self, grant: PrincipalGrant) -> PrincipalGrant: ...
    async def disable(
        self, *, tenant_id: str, grant_id: str, at: datetime
    ) -> PrincipalGrant: ...
```

语义：

- 查询必须完整匹配 tenant、channel、binding、provider subject；显示名不参与身份。
- tenant、Agent、binding 多层授权取交集；任何适用的显式拒绝、禁用或过期均拒绝。
- 无匹配授权、主体不完整、跨租户查询或存储不可用均默认拒绝。
- 被拒绝主体不得创建或读取业务 Session。

稳定异常：`PrincipalUnauthorized`、`PrincipalInvalid`、`GovernanceUnavailable`。

## 4. ContentPolicyPort

```python
class ContentPolicyPort(Protocol):
    async def inspect(
        self, *, tenant_id: str, policy: ActiveGovernancePolicy,
        boundary: ContentBoundary, content: str
    ) -> ContentInspection: ...
```

`ContentBoundary` 至少包括 `INBOUND`、`TOOL_ARGUMENTS`、`AGENT_OUTPUT`、`CHANNEL_REPLY`、`LOG_OR_AUDIT`。返回 `ALLOW`、`REDACT` 或 `REJECT`，发现项不得包含命中原文。检查异常映射为 `GovernanceUnavailable` 并默认拒绝。

## 5. Governance Coordinator 准入

```python
class GovernanceCoordinator(Protocol):
    async def admit(self, inbound: UnifiedInboundMessage) -> GovernanceAdmission: ...
    async def inspect_outbound(
        self, *, context: GovernanceContext, reply: UnifiedReply
    ) -> UnifiedReply: ...
```

准入顺序固定为：可信 binding → active policy → principal grant → inbound content → budget reservation → pre-execution audit。任何失败都不得进入 Runner。

## 6. Tool Callback 契约

```python
class ToolGovernanceCallback(Protocol):
    async def before_tool(
        self, *, context: GovernanceContext,
        descriptor: ToolDescriptor, arguments: Mapping[str, object]
    ) -> ToolGateResult: ...
```

回调必须：

1. 再次权威读取当前 active policy 和主体授权；
2. 检查工具白名单、参数内容和预算维度；
3. 比较准入版本与当前版本；收紧/禁用时拒绝，而不是使用旧上下文；
4. 危险工具缺确认时返回 `CONFIRMATION_REQUIRED`，工具函数调用次数保持 0；
5. 所有决定写入相同 trace 的审计。

实现必须挂接官方 `LlmAgent.before_tool_callback`/Filter 路径，不建立旁路 Runner。

## 7. 契约测试矩阵

- 两租户同名策略/主体/工具不串用。
- 缺失、禁用、过期和存储故障全部 fail closed。
- active generation 冲突不覆盖新版本。
- 准入后收紧策略，before-tool 阻断工具且调用计数为 0。
- 旧缓存存在时仍以权威 active 指针为准。
- 内容 redact/reject 适用于五个边界且原始测试 Secret 在日志/审计中 0 命中。
- InMemory 与 PostgreSQL 返回相同 domain result/error。

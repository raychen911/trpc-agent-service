from dataclasses import replace
from datetime import datetime, timezone

import pytest

from tests.test_agent_task_queue import _request
from tests.test_trpc_agent_runner import _context
from trpc_service.agent.contracts import (
    AgentReply,
    AgentRunResult,
    AgentRuntimeConfig,
    AgentToolCall,
    AgentToolKind,
    AgentToolResult,
    PolicyAction,
)
from trpc_service.agent.governance import (
    GovernanceContextBuilder,
    GovernanceOutputFilter,
    GovernedToolInvoker,
    TenantGovernancePolicy,
    ToolApprovalRequired,
)
from trpc_service.agent.ports import AgentToolInvoker
from trpc_service.agent.ports import AgentContextBuilder
from trpc_service.agent.usage import UsageReader, UsageTotals
from trpc_service.channels import MessageKind
from trpc_service.log import SensitiveDataRedactor
from trpc_service.metrics import PlatformTelemetry
from trpc_service.storage import SessionEvent


@pytest.mark.anyio
async def test_tenant_governance_denies_a_blocked_principal() -> None:
    """A tenant deny-list is enforced before any model or Tool execution."""

    request = _request()
    request = replace(
        request,
        incoming=replace(request.incoming, principal_id="blocked-principal"),
    )
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        policy={
            "governance": {
                "denied_principals": ["blocked-principal"],
            },
            "limits": {},
        },
    )

    decision = await TenantGovernancePolicy().evaluate(request, config)

    assert decision.action is PolicyAction.DENY
    assert decision.reason == "principal is denied by tenant policy"
    assert decision.attributes["reason_code"] == "PRINCIPAL_DENIED"


@pytest.mark.anyio
async def test_tenant_governance_requires_an_explicitly_allowed_principal() -> None:
    """An IM identity allow-list is enforced independently of deny rules."""

    request = _request()
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        policy={
            "governance": {
                "allowed_principals": ["another-principal"]
            },
            "limits": {},
        },
    )

    decision = await TenantGovernancePolicy().evaluate(request, config)

    assert decision.action is PolicyAction.DENY
    assert decision.attributes["reason_code"] == "PRINCIPAL_NOT_ALLOWED"


@pytest.mark.anyio
async def test_tenant_governance_enforces_input_budget() -> None:
    """An oversized prompt is rejected before consuming model quota."""

    request = _request()
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        policy={
            "governance": {},
            "limits": {
                "max_input_chars": 3
            },
        },
    )

    decision = await TenantGovernancePolicy().evaluate(request, config)

    assert decision.action is PolicyAction.DENY
    assert decision.attributes["reason_code"] == "INPUT_BUDGET_EXCEEDED"


@pytest.mark.anyio
async def test_tenant_governance_requires_review_for_unbacked_daily_budget() -> None:
    """A configured budget is never silently treated as enforced."""

    request = _request()
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        policy={
            "governance": {},
            "limits": {
                "daily_tokens": 1000
            },
        },
    )

    decision = await TenantGovernancePolicy().evaluate(request, config)

    assert decision.action is PolicyAction.REVIEW
    assert decision.attributes["reason_code"] == "BUDGET_LEDGER_REQUIRED"


@pytest.mark.anyio
async def test_tenant_governance_enforces_durable_daily_budgets() -> None:

    class FixedUsage(UsageReader):

        async def totals(self, tenant_id, *, since):  # type: ignore[no-untyped-def]
            del tenant_id, since
            return UsageTotals(calls=10, total_tokens=800)

    request = _request()
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        model={
            "context_window_tokens": 100,
            "max_output_tokens": 20,
        },
        policy={
            "governance": {},
            "limits": {
                "daily_calls": 10,
                "daily_tokens": 1000
            },
        },
    )

    decision = await TenantGovernancePolicy(usage_reader=FixedUsage()).evaluate(request, config)

    assert decision.action is PolicyAction.DENY
    assert decision.attributes["reason_code"] == "DAILY_CALL_BUDGET_EXCEEDED"


@pytest.mark.anyio
async def test_daily_token_budget_requires_a_trusted_context_window() -> None:
    """Hard token budgets fail closed when no provider upper bound exists."""

    class EmptyUsage(UsageReader):

        async def totals(self, tenant_id, *, since):  # type: ignore[no-untyped-def]
            del tenant_id, since
            return UsageTotals(calls=0, total_tokens=0)

    request = _request()
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        policy={
            "governance": {},
            "limits": {
                "daily_tokens": 1000
            }
        },
    )

    decision = await TenantGovernancePolicy(usage_reader=EmptyUsage()).evaluate(request, config)

    assert decision.action is PolicyAction.DENY
    assert decision.attributes["reason_code"] == "POLICY_INVALID"


@pytest.mark.anyio
async def test_output_filter_redacts_model_reply_and_session_event() -> None:
    """Sensitive model output is sanitized before Session and Outbox commit."""

    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            policy={"governance": {
                "redact_pii": True
            }},
        ),
    )
    result = AgentRunResult(
        replies=(AgentReply(
            kind=MessageKind.TEXT,
            text="邮箱 alice@example.com，密钥 sk-example-secret-123456",
        ), ),
        events=(SessionEvent(
            event_id="event-1",
            event_type="agent.replied",
            occurred_at=datetime.now(timezone.utc),
            payload={"text": "请拨打 13800138000"},
        ), ),
    )

    filtered = await GovernanceOutputFilter(SensitiveDataRedactor()).apply(context, result)

    assert filtered.replies[0].text == "邮箱 [REDACTED_EMAIL]，密钥 [REDACTED_SECRET]"
    assert filtered.events[0].payload["text"] == "请拨打 [REDACTED_PHONE]"


@pytest.mark.anyio
async def test_tool_governance_enforces_allowlist_and_risk_level() -> None:
    """Only allowlisted low-risk Tools reach their concrete adapter."""

    class RecordingTool(AgentToolInvoker):

        def __init__(self) -> None:
            self.calls: list[str] = []

        async def invoke(self, context, call):  # type: ignore[no-untyped-def]
            del context
            self.calls.append(call.name)
            return AgentToolResult(call_id=call.call_id, content="ok")

    delegate = RecordingTool()
    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "allowlist": ["ticket.read", "ticket.delete"],
                "risk_levels": {
                    "ticket.read": 0,
                    "ticket.delete": 3
                },
            },
        ),
    )
    invoker = GovernedToolInvoker(delegate)
    read_call = AgentToolCall("call-1", "ticket.read", AgentToolKind.TOOL, 0)
    delete_call = AgentToolCall("call-2", "ticket.delete", AgentToolKind.TOOL, 1)
    unknown_call = AgentToolCall("call-3", "unknown", AgentToolKind.TOOL, 2)

    result = await invoker.invoke(context, read_call)
    with pytest.raises(ToolApprovalRequired):
        await invoker.invoke(context, delete_call)
    with pytest.raises(PermissionError, match="not allowlisted"):
        await invoker.invoke(context, unknown_call)

    assert result.content == "ok"
    assert delegate.calls == ["ticket.read"]


@pytest.mark.anyio
async def test_capability_governance_applies_scoped_grants_to_every_execution_kind() -> None:
    """MCP, Skill, and Workspace calls cannot bypass the shared grant boundary."""

    class RecordingCapability(AgentToolInvoker):

        def __init__(self) -> None:
            self.calls: list[AgentToolCall] = []

        async def invoke(self, context, call):  # type: ignore[no-untyped-def]
            del context
            self.calls.append(call)
            return AgentToolResult(call_id=call.call_id, content="sunny")

    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "grants": [{
                    "kind": "mcp",
                    "name": "weather.lookup",
                    "actions": ["execute"],
                    "resources": ["city:shanghai"],
                    "risk_level": 1,
                }]
            },
        ),
    )
    delegate = RecordingCapability()
    invoker = GovernedToolInvoker(delegate)
    allowed = AgentToolCall(
        call_id="call-mcp-1",
        name="weather.lookup",
        kind=AgentToolKind.MCP,
        logical_call_index=0,
        action="execute",
        resource="city:shanghai",
    )
    wrong_resource = AgentToolCall(
        call_id="call-mcp-2",
        name="weather.lookup",
        kind=AgentToolKind.MCP,
        logical_call_index=1,
        action="execute",
        resource="city:beijing",
    )
    ungranted_skill = AgentToolCall(
        call_id="call-skill-1",
        name="report.writer",
        kind=AgentToolKind.SKILL,
        logical_call_index=2,
    )

    result = await invoker.invoke(context, allowed)
    with pytest.raises(PermissionError, match="not granted"):
        await invoker.invoke(context, wrong_resource)
    with pytest.raises(PermissionError, match="not granted"):
        await invoker.invoke(context, ungranted_skill)

    assert result.content == "sunny"
    assert delegate.calls == [allowed]


@pytest.mark.anyio
async def test_tool_governance_records_execution_and_denial() -> None:
    """Tool observability contains stable metadata but never model arguments."""

    class RecordingTool(AgentToolInvoker):

        async def invoke(self, context, call):  # type: ignore[no-untyped-def]
            del context
            return AgentToolResult(call_id=call.call_id, content="ok")

    class RecordingAudit:

        def __init__(self) -> None:
            self.records: list[dict[str, object]] = []

        async def record(self, request, config, **fields):  # type: ignore[no-untyped-def]
            del request, config
            self.records.append(fields)

    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "allowlist": ["ticket.read"],
                "risk_levels": {}
            },
        ),
    )
    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    audit = RecordingAudit()
    invoker = GovernedToolInvoker(
        RecordingTool(),
        telemetry=telemetry,
        audit=audit,  # type: ignore[arg-type]
    )

    await invoker.invoke(
        context,
        AgentToolCall("call-1", "ticket.read", AgentToolKind.TOOL, 0),
    )
    with pytest.raises(PermissionError):
        await invoker.invoke(
            context,
            AgentToolCall(
                "call-2",
                "ticket.unknown",
                AgentToolKind.TOOL,
                1,
                arguments={"api_key": "must-not-be-recorded"},
            ),
        )

    metrics = telemetry.render_prometheus().decode()
    assert 'result="success",tool_name="ticket.read"' in metrics
    assert 'action="deny",reason_code="TOOL_NOT_ALLOWLISTED"' in metrics
    assert 'result="error",tool_name="_not_allowlisted"' in metrics
    assert 'tool_name="ticket.unknown"' not in metrics
    assert audit.records[0]["tool_name"] == "ticket.read"
    assert audit.records[1]["reason_code"] == "TOOL_NOT_ALLOWLISTED"
    assert "must-not-be-recorded" not in str(audit.records)


@pytest.mark.anyio
async def test_policy_decision_is_audited_and_counted() -> None:
    """Every governance outcome reaches both durable audit and metrics seams."""

    class RecordingAudit:

        def __init__(self) -> None:
            self.records: list[dict[str, object]] = []

        async def record(self, request, config, **fields):  # type: ignore[no-untyped-def]
            del request, config
            self.records.append(fields)

    request = _request()
    audit = RecordingAudit()
    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    policy = TenantGovernancePolicy(telemetry=telemetry, audit=audit)  # type: ignore[arg-type]
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        policy={
            "governance": {
                "denied_principals": [request.incoming.principal_id]
            },
            "limits": {},
        },
    )

    decision = await policy.evaluate(request, config)
    metrics = telemetry.render_prometheus().decode()

    assert decision.action is PolicyAction.DENY
    assert audit.records[0]["action"] == "agent.policy.evaluate"
    assert audit.records[0]["decision"] == "deny"
    assert audit.records[0]["reason_code"] == "PRINCIPAL_DENIED"
    assert 'action="deny",reason_code="PRINCIPAL_DENIED"' in metrics


@pytest.mark.anyio
async def test_context_builder_redacts_input_before_model_execution() -> None:
    """PII and credentials do not leave the platform in a model prompt."""

    base_context = _context()

    class StaticContextBuilder(AgentContextBuilder):

        async def build(self, request, config, policy, claim):  # type: ignore[no-untyped-def]
            del config, policy, claim
            # Storage may enrich a text intent with a tenant-scoped attachment
            # staged by the preceding IM file message.
            return replace(
                base_context,
                request=replace(
                    request,
                    incoming=replace(
                        request.incoming,
                        artifact_refs=("restored-artifact", ),
                    ),
                ),
            )

    config = replace(
        base_context.config,
        policy={"governance": {
            "redact_pii": True
        }},
    )
    builder = GovernanceContextBuilder(StaticContextBuilder(), SensitiveDataRedactor())
    request = replace(
        base_context.request,
        incoming=replace(
            base_context.request.incoming,
            text="alice@example.com 的 key 是 sk-example-secret-123456",
        ),
    )

    context = await builder.build(
        request,
        config,
        base_context.policy,
        base_context.claim,
    )

    assert context.request.incoming.text == ("[REDACTED_EMAIL] 的 key 是 [REDACTED_SECRET]")
    assert context.request.incoming.artifact_refs == ("restored-artifact", )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "policy_config",
    [
        [],
        {
            "governance": []
        },
        {
            "governance": {
                "denied_principals": "employee-1"
            }
        },
        {
            "governance": {
                "allowed_principals": [""]
            }
        },
        {
            "governance": {},
            "limits": []
        },
        {
            "governance": {},
            "limits": {
                "max_input_chars": 0
            }
        },
        {
            "governance": {},
            "limits": {
                "monthly_calls": 1
            }
        },
        {
            "governance": {},
            "limits": {
                "daily_calls": True
            }
        },
    ],
)
async def test_tenant_governance_fails_closed_for_malformed_policy(policy_config: object, ) -> None:
    """Flexible tenant JSON never bypasses policy validation through coercion."""

    request = _request()
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        policy=policy_config,  # type: ignore[arg-type]
    )

    decision = await TenantGovernancePolicy().evaluate(request, config)

    assert decision.action is PolicyAction.DENY
    assert decision.attributes["reason_code"] in {
        "POLICY_INVALID",
        "BUDGET_LIMIT_UNSUPPORTED",
    }


@pytest.mark.anyio
async def test_tenant_governance_enforces_daily_token_reservation() -> None:

    class ExistingUsage(UsageReader):

        async def totals(self, tenant_id, *, since):  # type: ignore[no-untyped-def]
            del tenant_id, since
            return UsageTotals(calls=0, total_tokens=950)

    request = _request()
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        model={
            "context_window_tokens": 100,
            "max_output_tokens": 20,
        },
        policy={
            "governance": {},
            "limits": {
                "daily_tokens": 1000
            },
        },
    )

    decision = await TenantGovernancePolicy(usage_reader=ExistingUsage()).evaluate(request, config)

    assert decision.action is PolicyAction.DENY
    assert decision.attributes["reason_code"] == "DAILY_TOKEN_BUDGET_EXCEEDED"


@pytest.mark.anyio
async def test_output_filter_rejects_non_boolean_pii_policy() -> None:
    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            policy={"governance": {
                "redact_pii": "yes"
            }},
        ),
    )

    with pytest.raises(ValueError, match="redact_pii must be a boolean"):
        await GovernanceOutputFilter(SensitiveDataRedactor()).apply(
            context,
            AgentRunResult(),
        )


@pytest.mark.anyio
async def test_legacy_allowlist_accepts_bounded_workspace_capability() -> None:

    class RecordingWorkspaceTool(AgentToolInvoker):

        async def invoke(self, context, call):  # type: ignore[no-untyped-def]
            del context
            return AgentToolResult(call_id=call.call_id, content="ok")

    context = _context()
    delegate = RecordingWorkspaceTool()
    governed = GovernedToolInvoker(delegate)
    context = replace(
        context,
        config=replace(
            context.config,
            tools={"allowlist": ["workspace.read"]},
        ),
    )
    call = AgentToolCall(
        "workspace-call",
        "workspace.read",
        AgentToolKind.WORKSPACE,
        0,
        resource="inputs/spec.txt",
    )

    result = await governed.invoke(context, call)

    assert result.call_id == "workspace-call"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("permissions", "call", "error_type"),
    [
        (
            {
                "allowlist": ["weather"]
            },
            AgentToolCall("legacy-kind", "weather", AgentToolKind.MCP, 0),
            PermissionError,
        ),
        (
            {
                "grants": "invalid"
            },
            AgentToolCall("grant-array", "weather", AgentToolKind.TOOL, 0),
            ValueError,
        ),
        (
            {
                "grants": [[]]
            },
            AgentToolCall("grant-object", "weather", AgentToolKind.TOOL, 0),
            ValueError,
        ),
        (
            {
                "grants": [{
                    "kind": 1,
                    "name": "weather",
                    "actions": ["execute"]
                }]
            },
            AgentToolCall("grant-name", "weather", AgentToolKind.TOOL, 0),
            ValueError,
        ),
        (
            {
                "grants": [{
                    "kind": "tool",
                    "name": "weather",
                    "actions": []
                }]
            },
            AgentToolCall("grant-actions", "weather", AgentToolKind.TOOL, 0),
            ValueError,
        ),
        (
            {
                "grants": [{
                    "kind": "tool",
                    "name": "weather",
                    "actions": ["execute"],
                    "risk_level": True,
                }]
            },
            AgentToolCall("grant-risk", "weather", AgentToolKind.TOOL, 0),
            ValueError,
        ),
    ],
)
async def test_tool_governance_rejects_malformed_capability_grants(
    permissions: dict[str, object],
    call: AgentToolCall,
    error_type: type[Exception],
) -> None:

    class UnexpectedTool(AgentToolInvoker):

        async def invoke(self, context, delegated_call):  # type: ignore[no-untyped-def]
            del context, delegated_call
            raise AssertionError("invalid grant reached concrete Tool")

    context = _context()
    context = replace(context, config=replace(context.config, tools=permissions))

    with pytest.raises(error_type):
        await GovernedToolInvoker(UnexpectedTool()).invoke(context, call)

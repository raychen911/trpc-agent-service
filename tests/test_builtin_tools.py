from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import SecretStr

from tests.test_trpc_agent_runner import _context
from trpc_service.agent import (
    AgentExecutionContext,
    AgentToolCall,
    AgentToolInvoker,
    AgentToolKind,
    AgentToolResult,
)
from trpc_service.agent.approval import ApprovalRequestSnapshot, ApprovalStatus
from trpc_service.agent.adapters.trpc_tools import CapabilityCallSequence, TRPCToolBridge
from trpc_service.agent.adapters.trpc import TRPCAgentRunner, _close_sdk_runtime
from trpc_service.agent.governance import GovernedToolInvoker, ToolApprovalRequired
from trpc_service.config import Settings
from trpc_service.skill import BuiltinSkillCatalog
from trpc_service.tool import BuiltinToolInvoker


def _bridge(context, invoker):  # type: ignore[no-untyped-def]
    return TRPCToolBridge(context, invoker, CapabilityCallSequence())


@pytest.mark.anyio
async def test_builtin_calculator_executes_through_the_governed_tool_port() -> None:
    """An allowlisted calculation crosses the same port used by future MCP tools."""

    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "allowlist": ["calculate"],
                "risk_levels": {
                    "calculate": 0
                },
            },
        ),
    )
    invoker = GovernedToolInvoker(BuiltinToolInvoker())

    result = await invoker.invoke(
        context,
        AgentToolCall(
            call_id="request-1:0:calculate",
            name="calculate",
            kind=AgentToolKind.TOOL,
            logical_call_index=0,
            arguments={"expression": "(12.5 + 7.5) * 3"},
        ),
    )

    assert result.content == "60"


@pytest.mark.anyio
async def test_builtin_calculator_rejects_code_and_unregistered_tools() -> None:
    """The local test Tool evaluates arithmetic only and fails closed otherwise."""

    context = _context()
    invoker = BuiltinToolInvoker()

    with pytest.raises(ValueError, match="unsupported calculation"):
        await invoker.invoke(
            context,
            AgentToolCall(
                call_id="request-1:0:calculate",
                name="calculate",
                kind=AgentToolKind.TOOL,
                logical_call_index=0,
                arguments={"expression": "__import__('os').getcwd()"},
            ),
        )
    with pytest.raises(ValueError, match="outside the supported range"):
        await invoker.invoke(
            context,
            AgentToolCall(
                call_id="request-1:1:calculate",
                name="calculate",
                kind=AgentToolKind.TOOL,
                logical_call_index=1,
                arguments={"expression": "1e101"},
            ),
        )
    with pytest.raises(PermissionError, match="not registered"):
        await invoker.invoke(
            context,
            AgentToolCall(
                call_id="request-1:1:unknown",
                name="unknown",
                kind=AgentToolKind.TOOL,
                logical_call_index=1,
            ),
        )


@pytest.mark.anyio
async def test_trpc_tool_bridge_exposes_only_allowlisted_builtin_tools() -> None:
    """The SDK can call platform Tools without bypassing tenant governance."""

    context = _context()
    disabled = _bridge(context, BuiltinToolInvoker())
    assert disabled.functions() == ()

    enabled_context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "allowlist": ["calculate"],
                "risk_levels": {
                    "calculate": 0
                },
            },
        ),
    )
    bridge = _bridge(
        enabled_context,
        GovernedToolInvoker(BuiltinToolInvoker()),
    )

    functions = bridge.functions()

    assert len(functions) == 1
    assert functions[0].__name__ == "calculate"
    assert await functions[0](expression="8 * (3 + 2)") == {"result": "40"}

    granted_context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "grants": [{
                    "kind": "tool",
                    "name": "calculate",
                    "actions": ["execute"],
                    "resources": [],
                    "risk_level": 0,
                }]
            },
        ),
    )
    granted = _bridge(
        granted_context,
        GovernedToolInvoker(BuiltinToolInvoker()),
    ).functions()

    assert len(granted) == 1
    assert await granted[0](expression="21 * 2") == {"result": "42"}


@pytest.mark.anyio
async def test_trpc_tool_bridge_binds_knowledge_grants_to_the_selected_base() -> None:
    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "grants": [{
                    "kind": "tool",
                    "name": "knowledge.search",
                    "actions": ["execute"],
                    "resources": ["handbook"],
                    "risk_level": 0,
                }]
            },
        ),
    )

    class RecordingInvoker(AgentToolInvoker):

        def __init__(self) -> None:
            self.call: AgentToolCall | None = None

        async def invoke(
            self,
            runtime_context: AgentExecutionContext,
            call: AgentToolCall,
        ) -> AgentToolResult:
            del runtime_context
            self.call = call
            return AgentToolResult(call_id=call.call_id, content="matched")

    delegate = RecordingInvoker()
    functions = _bridge(context, GovernedToolInvoker(delegate)).functions()

    assert [function.__name__ for function in functions] == ["knowledge_search"]
    assert await functions[0](knowledge_base_name="handbook", query="年假") == {"result": "matched"}
    assert delegate.call is not None
    assert delegate.call.resource == "handbook"


@pytest.mark.anyio
async def test_trpc_tool_bridge_hides_knowledge_add_even_when_granted() -> None:
    """A stale policy cannot re-enable IM-side knowledge mutation."""

    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            knowledge={"knowledge_base_names": ["handbook"]},
            tools={
                "grants": [{
                    "kind": "tool",
                    "name": "knowledge.add",
                    "actions": ["execute"],
                    "resources": ["handbook"],
                    "risk_level": 0,
                }]
            },
        ),
    )
    functions = _bridge(
        context,
        GovernedToolInvoker(BuiltinToolInvoker()),
    ).functions()

    assert functions == ()


@pytest.mark.anyio
async def test_trpc_tool_bridge_hides_knowledge_add_without_an_upload() -> None:
    """Missing attachments cannot make an obsolete mutation function visible."""

    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            knowledge={"knowledge_base_names": ["handbook"]},
            tools={
                "grants": [{
                    "kind": "tool",
                    "name": "knowledge.add",
                    "actions": ["execute"],
                    "resources": ["handbook"],
                    "risk_level": 2,
                }]
            },
        ),
    )
    functions = _bridge(
        context,
        GovernedToolInvoker(BuiltinToolInvoker()),
    ).functions()

    assert functions == ()


@pytest.mark.anyio
async def test_trpc_tool_bridge_hides_knowledge_delete_even_for_a_file_message() -> None:
    """The model cannot select a destructive RAG operation from an IM message."""

    base_context = _context()
    context = replace(
        base_context,
        request=replace(
            base_context.request,
            incoming=replace(
                base_context.request.incoming,
                text="把刚才上传的文件添加到知识库",
                artifact_refs=("staged-artifact", ),
            ),
        ),
        config=replace(
            base_context.config,
            knowledge={"knowledge_base_names": ["handbook"]},
            tools={
                "grants": [{
                    "kind": "tool",
                    "name": "knowledge.delete",
                    "actions": ["execute"],
                    "resources": ["handbook"],
                    "risk_level": 3,
                }]
            },
        ),
    )
    functions = _bridge(
        context,
        GovernedToolInvoker(BuiltinToolInvoker()),
    ).functions()

    assert functions == ()


@pytest.mark.anyio
async def test_trpc_tool_bridge_returns_a_safe_pending_approval_to_the_model() -> None:
    """A dangerous call becomes a user confirmation prompt, not an SDK failure."""

    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "allowlist": ["calculate"],
                "risk_levels": {
                    "calculate": 3
                },
            },
        ),
    )
    pending = ApprovalRequestSnapshot(
        approval_id=uuid4(),
        short_code="A1B2C3D4",
        tenant_id=context.request.tenant.tenant_id,
        agent_app_id=context.request.tenant.agent_app_id,
        binding_id=context.request.channel.binding_id,
        principal_id=context.request.incoming.principal_id,
        session_id=context.request.session_id,
        tool_call_id="request-1:0:calculate",
        capability_kind="tool",
        capability_name="calculate",
        action="execute",
        resource=None,
        arguments_hash="a" * 64,
        risk_level=3,
        status=ApprovalStatus.PENDING,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )

    class PendingInvoker(AgentToolInvoker):

        async def invoke(self, runtime_context, call):  # type: ignore[no-untyped-def]
            del runtime_context, call
            raise ToolApprovalRequired("approval required", approval=pending)

    function = _bridge(context, PendingInvoker()).functions()[0]

    result = await function(expression="1 + 1")

    assert result == {
        "result": ("该操作需要用户确认。请停止继续调用工具，并请原请求人在当前会话回复："
                   "确认 A1B2C3D4")
    }


@pytest.mark.anyio
async def test_agent_factory_registers_allowlisted_functions_as_sdk_tools() -> None:
    """The real SDK must receive BaseTool adapters rather than raw callables."""

    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            model={"api_key_ref": "env://DASHSCOPE_API_KEY"},
            tools={
                "allowlist": ["calculate"],
                "risk_levels": {
                    "calculate": 0
                }
            },
        ),
    )
    invoker = GovernedToolInvoker(BuiltinToolInvoker())

    runner = TRPCAgentRunner(
        Settings(_env_file=None, dashscope_api_key=SecretStr("test-key")),
        skills=BuiltinSkillCatalog(),
    )
    runtime = await runner._build_runtime(context, invoker)

    await _close_sdk_runtime(runtime)

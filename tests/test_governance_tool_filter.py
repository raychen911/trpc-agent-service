"""Stage 6A1 Task 3: real SDK BaseFilter governance tests (RED first).

Interface evidence (installed SDK == local trpc-agent-python, editable):
- ``filter/_base_filter.py:197-239`` ``BaseFilter.run``: awaits ``_before``
  first; ``if result.error or not result.is_continue: return result`` — the
  handle (the actual tool function) is never invoked.
- ``filter/_run_filter.py`` ``run_filters``: unpacks the final FilterResult;
  an ``error`` raises and logs — so governance blocks must set ``rsp`` +
  ``is_continue=False`` with ``error=None`` (same pattern the SDK's own
  ``agents/_callback.py:298-300`` uses).
- ``tools/_base_tool.py:207`` sets the current tool contextvar before the
  filter chain, so ``tools.get_tool_var().name`` resolves the tool name.
- ``FunctionTool(func, filters=[...])`` (``tools/_function_tool.py:88-91``)
  injects instance filters; ``filters_name``/``register_tool_filter`` (the
  global registry) are deliberately unused.
"""

from __future__ import annotations

import logging

import pytest
from trpc_agent_sdk.filter import BaseFilter as SdkBaseFilter
from trpc_agent_sdk.filter import FilterType
from trpc_agent_sdk.sessions import InMemorySessionService, SessionServiceConfig
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.context import AgentContext, InvocationContext
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.types import Ttl

from tests.tenant_helpers import FakeLLMModel

from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.tool_registry import AllowedToolRegistry
from trpc_service.config.tenant import ToolDecision


async def _invocation_context() -> InvocationContext:
    session_service = InMemorySessionService(session_config=SessionServiceConfig(ttl=Ttl(enable=False)))
    session = await session_service.create_session(app_name="gov-test", user_id="u1")
    agent = LlmAgent(name="gov_test_agent", model=FakeLLMModel())
    return InvocationContext(
        invocation_id="inv-test",
        session_service=session_service,
        agent=agent,
        agent_context=AgentContext(),
        session=session,
    )


def _counting_tool(calls: list[str], name: str = "echo_probe"):

    def echo_probe(text: str) -> str:
        """Echo the given text back."""
        calls.append(text)
        return "tool-ran"

    echo_probe.__name__ = name
    return FunctionTool(echo_probe)


def _governed_tool(decisions: dict[str, ToolDecision], calls: list[str]):
    from trpc_service.governance.tool_filter import TenantToolGovernanceFilter

    tool = _counting_tool(calls)
    tool.add_filters([TenantToolGovernanceFilter(decisions)])
    return tool


class TestTenantToolGovernanceFilter:

    def test_is_real_sdk_filter_of_tool_type(self):
        from trpc_service.governance.tool_filter import TenantToolGovernanceFilter

        f = TenantToolGovernanceFilter({"x": "deny"})
        assert isinstance(f, SdkBaseFilter)
        assert f.type == FilterType.TOOL

    def test_decisions_are_captured_immutably(self):
        from trpc_service.governance.tool_filter import TenantToolGovernanceFilter

        source = {"x": "deny"}
        f = TenantToolGovernanceFilter(source)
        source["x"] = "allow"
        # mutating the caller's dict must not change the filter's policy
        assert f._decisions["x"] == "deny"  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_deny_blocks_before_function_runs(self):
        calls: list[str] = []
        tool = _governed_tool({"echo_probe": "deny"}, calls)
        assert any(isinstance(f, SdkBaseFilter) for f in tool.filters)

        result = await tool.run_async(tool_context=await _invocation_context(), args={"text": "raw-arg-secret"})

        assert calls == []
        assert result == {"status": "denied"}

    @pytest.mark.asyncio
    async def test_review_blocks_before_function_runs(self):
        calls: list[str] = []
        tool = _governed_tool({"echo_probe": "review"}, calls)

        result = await tool.run_async(tool_context=await _invocation_context(), args={"text": "raw-arg-secret"})

        assert calls == []
        assert result == {"status": "approval_required"}

    @pytest.mark.asyncio
    async def test_allow_runs_function_once(self):
        calls: list[str] = []
        tool = _governed_tool({"echo_probe": "allow"}, calls)

        result = await tool.run_async(tool_context=await _invocation_context(), args={"text": "hello"})

        assert calls == ["hello"]
        assert result == "tool-ran"

    @pytest.mark.asyncio
    async def test_tool_omitted_from_decisions_defaults_to_allow(self):
        calls: list[str] = []
        tool = _governed_tool({"other_tool": "deny"}, calls)

        result = await tool.run_async(tool_context=await _invocation_context(), args={"text": "hello"})

        assert calls == ["hello"]
        assert result == "tool-ran"

    @pytest.mark.asyncio
    async def test_deny_does_not_log_error_or_arguments(self, caplog):
        calls: list[str] = []
        tool = _governed_tool({"echo_probe": "deny"}, calls)

        with caplog.at_level(logging.DEBUG):
            await tool.run_async(tool_context=await _invocation_context(), args={"text": "DO-NOT-LOG"})

        text = caplog.text
        assert "DO-NOT-LOG" not in text
        # the SDK only debug-logs when FilterResult.error is set; a governance
        # block must not surface as a tool runtime error at all
        assert "run_filters error" not in text


class TestRegistryWithGovernance:

    def test_build_tools_requires_tool_decisions(self):
        registry = AllowedToolRegistry.default()
        with pytest.raises(TypeError):
            registry.build_tools(("get_current_time", ))  # old signature gone

    def test_build_tools_attaches_governance_filter_to_every_tool(self):
        registry = AllowedToolRegistry.default()
        tools = registry.build_tools(("get_current_time", ), {"get_current_time": "review"})
        tool = tools[0]
        assert isinstance(tool, FunctionTool)
        governance = [f for f in tool.filters if f.__class__.__name__ == "TenantToolGovernanceFilter"]
        assert len(governance) == 1
        assert governance[0].type == FilterType.TOOL

    def test_each_runtime_gets_private_filter_instances(self):
        registry = AllowedToolRegistry.default()
        decisions = {"get_current_time": "deny"}
        tools_a = registry.build_tools(("get_current_time", ), decisions)
        tools_b = registry.build_tools(("get_current_time", ), decisions)
        assert tools_a[0] is not tools_b[0]
        assert tools_a[0].filters is not tools_b[0].filters
        f_a = tools_a[0].filters[0]
        f_b = tools_b[0].filters[0]
        assert f_a is not f_b

    def test_unknown_allowed_tool_still_config_error(self):
        registry = AllowedToolRegistry.default()
        with pytest.raises(TenantAgentConfigurationError):
            registry.build_tools(("no_such_tool", ), {})

    def test_decision_for_non_allowed_tool_is_config_error(self):
        registry = AllowedToolRegistry.default()
        with pytest.raises(TenantAgentConfigurationError):
            registry.build_tools((), {"ghost_tool": "deny"})

    def test_inconsistent_decision_value_is_config_error(self):
        registry = AllowedToolRegistry.default()
        with pytest.raises(TenantAgentConfigurationError):
            registry.build_tools(("get_current_time", ), {"get_current_time": "approve"})  # type: ignore[dict-item]


class TestGlobalRegistryAvoidance:

    def test_filter_not_registered_in_global_sdk_registry(self):
        from trpc_agent_sdk.filter import get_tool_filter

        from trpc_service.governance.tool_filter import TenantToolGovernanceFilter

        _ = TenantToolGovernanceFilter({"x": "deny"})
        assert get_tool_filter("tenant_tool_governance") is None
        assert get_tool_filter("TenantToolGovernanceFilter") is None


class TestFailClosedContext:
    """P1-2 (Codex review): governance must fail CLOSED when the tool
    context cannot be resolved — never let the function run unjudged."""

    @pytest.mark.asyncio
    async def test_get_tool_var_none_blocks(self, monkeypatch):
        from trpc_service.governance import tool_filter as tf

        calls: list[str] = []
        tool = _governed_tool({}, calls)  # empty policy: allow path would run the function
        monkeypatch.setattr(tf, "get_tool_var", lambda: None)

        result = await tool.run_async(tool_context=await _invocation_context(), args={"text": "arg-x"})

        assert calls == []
        assert result == {"status": "denied"}

    @pytest.mark.asyncio
    async def test_get_tool_var_raises_blocks(self, monkeypatch):
        from trpc_service.governance import tool_filter as tf

        def boom():
            raise RuntimeError("context internals must not leak")

        calls: list[str] = []
        tool = _governed_tool({"echo_probe": "allow"}, calls)
        monkeypatch.setattr(tf, "get_tool_var", boom)

        result = await tool.run_async(tool_context=await _invocation_context(), args={"text": "arg-y"})

        assert calls == []
        assert result == {"status": "denied"}
        assert "context internals" not in str(result) and "arg-y" not in str(result)

    @pytest.mark.asyncio
    async def test_tool_without_valid_name_blocks(self, monkeypatch):
        from trpc_service.governance import tool_filter as tf

        calls: list[str] = []
        tool = _governed_tool({"": "deny"}, calls)

        class NamelessTool:
            name = None  # illegal/missing name

        monkeypatch.setattr(tf, "get_tool_var", lambda: NamelessTool())

        result = await tool.run_async(tool_context=await _invocation_context(), args={"text": "arg-z"})

        assert calls == []
        assert result == {"status": "denied"}

    @pytest.mark.asyncio
    async def test_normal_path_still_resolves_real_tool_name(self):
        """The fail-closed change must not break the healthy path."""
        calls: list[str] = []
        tool = _governed_tool({"echo_probe": "allow"}, calls)
        result = await tool.run_async(tool_context=await _invocation_context(), args={"text": "ok"})
        assert calls == ["ok"]
        assert result == "tool-ran"


class TestApprovedToolExecutor:
    """P1-4: explicit approved-execution boundary (new module)."""

    import asyncio as _a

    def _executor(self):
        from trpc_service.governance.approved_execution import ApprovedToolExecutor
        return ApprovedToolExecutor

    def test_sync_function_runs_off_event_loop_thread(self):
        import asyncio
        import threading

        from trpc_service.governance.approved_execution import ApprovedToolExecutor

        main = threading.current_thread()
        seen = {}

        def probe() -> str:
            seen["thread"] = threading.current_thread()
            return "done"

        ex = ApprovedToolExecutor({"probe": probe})
        result = asyncio.run(ex.execute("probe", {}))
        assert result == "done"
        assert seen["thread"] is not main

    def test_unknown_tool_rejected(self):
        import asyncio

        from trpc_service.agent.errors import TenantAgentConfigurationError
        from trpc_service.governance.approved_execution import ApprovedToolExecutor

        ex = ApprovedToolExecutor({})
        with pytest.raises(TenantAgentConfigurationError):
            asyncio.run(ex.execute("rm_rf", {}))

    def test_missing_required_arg_rejected_without_call(self):
        import asyncio

        from trpc_service.agent.errors import TenantAgentConfigurationError
        from trpc_service.governance.approved_execution import ApprovedToolExecutor

        calls: list = []

        def need_arg(x: str) -> str:
            calls.append(x)
            return "ran"

        ex = ApprovedToolExecutor({"need_arg": need_arg})
        with pytest.raises(TenantAgentConfigurationError):
            asyncio.run(ex.execute("need_arg", {}))
        assert calls == []

    def test_unknown_extra_arg_rejected_without_call(self):
        import asyncio

        from trpc_service.agent.errors import TenantAgentConfigurationError
        from trpc_service.governance.approved_execution import ApprovedToolExecutor

        calls: list = []

        def fixed() -> str:
            calls.append(1)
            return "ran"

        ex = ApprovedToolExecutor({"fixed": fixed})
        with pytest.raises(TenantAgentConfigurationError):
            asyncio.run(ex.execute("fixed", {"surprise": 1}))
        assert calls == []

    def test_tool_context_functions_rejected_not_executed(self):
        """Tools that need the SDK InvocationContext are explicitly unsupported."""
        import asyncio

        from trpc_service.agent.errors import TenantAgentConfigurationError
        from trpc_service.governance.approved_execution import ApprovedToolExecutor

        calls: list = []

        # defaulted context param: signature validation alone would PASS it —
        # only the explicit unsupported-context guard may reject this
        def ctx_tool(tool_context=None) -> str:
            calls.append(1)
            return "ran"

        ex = ApprovedToolExecutor({"ctx_tool": ctx_tool})
        with pytest.raises(TenantAgentConfigurationError):
            asyncio.run(ex.execute("ctx_tool", {}))
        assert calls == []

    def test_coroutine_function_awaited(self):
        import asyncio

        from trpc_service.governance.approved_execution import ApprovedToolExecutor

        async def slow(x: int) -> int:
            await asyncio.sleep(0)
            return x * 2

        ex = ApprovedToolExecutor({"slow": slow})
        assert asyncio.run(ex.execute("slow", {"x": 21})) == 42

    def test_non_mapping_args_rejected(self):
        import asyncio

        from trpc_service.agent.errors import TenantAgentConfigurationError
        from trpc_service.governance.approved_execution import ApprovedToolExecutor

        ex = ApprovedToolExecutor({"get_current_time": (lambda: "t")})
        with pytest.raises(TenantAgentConfigurationError):
            asyncio.run(ex.execute("get_current_time", "not-a-dict"))

    def test_registry_default_executes_get_current_time_via_boundary(self):
        import asyncio
        import datetime

        from trpc_service.agent.tool_registry import AllowedToolRegistry

        out = asyncio.run(AllowedToolRegistry.default().execute_approved("get_current_time", {}))
        assert isinstance(out, str)
        datetime.datetime.fromisoformat(out)  # parses as ISO-8601

"""Exact Runner registry tests."""

import pytest

from trpc_service.agent import RunnerNotFoundError
from trpc_service.agent import RunnerRegistrationError
from trpc_service.agent import RunnerRegistry
from trpc_service.agent import UnknownToolError
from trpc_service.agent import select_tools_for_runner
from trpc_service.agent import tenant_app_name
from trpc_service.tenant import ToolPolicy

from trpc_agent_sdk.tools import FunctionTool


class FakeRunner:
    def __init__(self, app_name: str):
        self.app_name = app_name


def test_runtime_registry_resolves_complete_key():
    registry = RunnerRegistry()
    runner = FakeRunner(tenant_app_name("tenant_acme", "customer_support"))
    registry.register("tenant_acme", "customer_support", 3, runner)

    assert registry.resolve("tenant_acme", "customer_support", 3) is runner


def test_runtime_registry_does_not_fall_back_to_another_version():
    registry = RunnerRegistry()
    registry.register(
        "tenant_acme",
        "customer_support",
        3,
        FakeRunner(tenant_app_name("tenant_acme", "customer_support")),
    )

    with pytest.raises(RunnerNotFoundError, match="runner not found"):
        registry.resolve("tenant_acme", "customer_support", 4)


def test_runtime_registry_rejects_duplicate_key():
    registry = RunnerRegistry()
    runner = FakeRunner(tenant_app_name("tenant_acme", "customer_support"))
    registry.register("tenant_acme", "customer_support", 3, runner)

    with pytest.raises(RunnerRegistrationError, match="already registered"):
        registry.register("tenant_acme", "customer_support", 3, runner)


def test_tenant_app_name_is_stable_across_config_versions():
    assert tenant_app_name("tenant_acme", "customer_support") == "tenant_acme:customer_support"


def named_tool(name):
    def implementation():
        return None

    implementation.__name__ = name
    return FunctionTool(implementation)


def test_tool_policy_exposes_only_allowed_tools_without_confirmation():
    tools = [named_tool("search_order"), named_tool("execute_refund"), named_tool("internal_admin")]
    policy = ToolPolicy(
        allow=frozenset({"search_order", "execute_refund"}),
        require_confirmation=frozenset({"execute_refund"}),
        deny=frozenset({"internal_admin"}),
    )

    selected = select_tools_for_runner(tools, policy)

    assert [tool.name for tool in selected] == ["search_order"]


def test_tool_policy_default_allow_still_applies_deny():
    tools = [named_tool("search_order"), named_tool("internal_admin")]
    policy = ToolPolicy(default_deny=False, deny=frozenset({"internal_admin"}))

    assert [tool.name for tool in select_tools_for_runner(tools, policy)] == ["search_order"]


def test_tool_policy_rejects_unknown_tool_references():
    with pytest.raises(UnknownToolError, match="missing_tool"):
        select_tools_for_runner(
            [named_tool("search_order")],
            ToolPolicy(allow=frozenset({"search_order", "missing_tool"})),
        )

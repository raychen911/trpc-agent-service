# mypy: disable-error-code="import-untyped"
"""Tenant ToolSet and AgentFactory isolation tests."""

from types import SimpleNamespace

import pytest

from trpc_service.agent.factory import AgentConfigurationError, AgentFactory
from trpc_service.tenant.models import ToolPolicy
from trpc_service.tool import (
    TENANT_CONTEXT_METADATA_KEY,
    TenantToolSet,
    ToolAuthorizationError,
    ToolConfigurationError,
)

from .helpers import StreamingFakeModel, echo, make_app, make_context


def test_toolset_is_allowlisted_approval_aware_and_immutable() -> None:
    policy = ToolPolicy(
        allowed=frozenset({"echo"}),
        requires_approval=frozenset({"echo"}),
        max_calls_per_turn=1,
    )
    withheld = TenantToolSet(
        tenant_id="tenant-a",
        policy=policy,
        registered_tools={"echo": echo},
    )
    approved = TenantToolSet(
        tenant_id="tenant-a",
        policy=policy,
        registered_tools={"echo": echo},
        approved_tools={"echo"},
    )

    assert withheld.tool_names == ()
    assert approved.tool_names == ("echo",)
    with pytest.raises(TypeError, match="immutable"):
        approved.add_tools([])


@pytest.mark.asyncio
async def test_toolset_fails_closed_for_unknown_tool_and_wrong_tenant() -> None:
    with pytest.raises(ToolConfigurationError, match="unregistered"):
        TenantToolSet(
            tenant_id="tenant-a",
            policy=ToolPolicy(allowed=frozenset({"missing"})),
            registered_tools={"echo": echo},
        )

    tool_set = TenantToolSet(
        tenant_id="tenant-a",
        policy=ToolPolicy(allowed=frozenset({"echo"})),
        registered_tools={"echo": echo},
    )
    foreign_agent_context = SimpleNamespace(
        get_metadata=lambda key: (
            make_context(tenant_id="tenant-b") if key == TENANT_CONTEXT_METADATA_KEY else None
        )
    )
    invocation_context = SimpleNamespace(agent_context=foreign_agent_context)
    with pytest.raises(ToolAuthorizationError, match="does not match"):
        await tool_set.get_tools(invocation_context)

    missing_context = SimpleNamespace(agent_context=SimpleNamespace(get_metadata=lambda key: None))
    with pytest.raises(ToolAuthorizationError, match="missing"):
        await tool_set.get_tools(missing_context)

    assert [tool.name for tool in await tool_set.get_tools()] == ["echo"]


def test_factory_rejects_cross_revision_context() -> None:
    factory = AgentFactory(
        model_resolver=lambda context, app: StreamingFakeModel(),
        registered_tools={"echo": echo},
    )

    with pytest.raises(AgentConfigurationError, match="app_revision"):
        factory.build_for_context(
            tenant_context=make_context(app_revision=4),
            app=make_app(),
        )


def test_factory_rejects_cross_app_empty_prompt_and_invalid_model() -> None:
    factory = AgentFactory(
        model_resolver=lambda context, app: StreamingFakeModel(),
        registered_tools=(echo,),
    )

    with pytest.raises(AgentConfigurationError, match="app_id"):
        factory.build_for_context(
            tenant_context=make_context(app_id="other"),
            app=make_app(),
        )
    with pytest.raises(AgentConfigurationError, match="instruction"):
        factory.build_for_context(
            tenant_context=make_context(),
            app=make_app().model_copy(update={"prompt": " "}),
        )

    invalid_factory = AgentFactory(
        model_resolver=lambda context, app: 42,
        registered_tools=(echo,),
    )
    with pytest.raises(AgentConfigurationError, match="model_resolver"):
        invalid_factory.build_for_context(
            tenant_context=make_context(),
            app=make_app(),
        )

    invalid_filter_factory = AgentFactory(
        model_resolver=lambda context, app: StreamingFakeModel(),
        registered_tools=(echo,),
        filter_factories=(lambda context, app: object(),),
    )
    with pytest.raises(AgentConfigurationError, match="filter factory"):
        invalid_filter_factory.build_for_context(
            tenant_context=make_context(),
            app=make_app(),
        )


def test_tool_configuration_edge_cases_fail_closed() -> None:
    with pytest.raises(ToolConfigurationError, match="tenant_id"):
        TenantToolSet(
            tenant_id="",
            policy=ToolPolicy(),
            registered_tools=(echo,),
        )
    with pytest.raises(ToolConfigurationError, match="do not require approval"):
        TenantToolSet(
            tenant_id="tenant-a",
            policy=ToolPolicy(allowed=frozenset({"echo"})),
            registered_tools=(echo,),
            approved_tools={"echo"},
        )
    disabled = TenantToolSet(
        tenant_id="tenant-a",
        policy=ToolPolicy(
            allowed=frozenset({"echo"}),
            max_calls_per_turn=0,
        ),
        registered_tools=(echo,),
    )
    assert disabled.tenant_id == "tenant-a"
    assert disabled.tool_names == ()

    with pytest.raises(ToolConfigurationError, match="invalid tool name"):
        TenantToolSet(
            tenant_id="tenant-a",
            policy=ToolPolicy(),
            registered_tools={"bad-name": echo},
        )
    with pytest.raises(ToolConfigurationError, match="does not match"):
        TenantToolSet(
            tenant_id="tenant-a",
            policy=ToolPolicy(),
            registered_tools={"other": echo},
        )
    with pytest.raises(ToolConfigurationError, match="duplicate"):
        TenantToolSet(
            tenant_id="tenant-a",
            policy=ToolPolicy(),
            registered_tools=(echo, echo),
        )

    class CallableWithoutName:
        def __call__(self) -> None:
            return None

    with pytest.raises(ToolConfigurationError, match="stable __name__"):
        TenantToolSet(
            tenant_id="tenant-a",
            policy=ToolPolicy(),
            registered_tools=(CallableWithoutName(),),
        )

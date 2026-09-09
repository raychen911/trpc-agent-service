# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import pytest
from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.context import new_agent_context
from trpc_agent_sdk.tools import FunctionTool

from trpc_service.config import ToolPolicy
from trpc_service.tenant import TenantBoundaryAgentFilter
from trpc_service.tenant import TenantContext
from trpc_service.tenant import tenant_scope
from trpc_service.tool import ToolRegistry


@pytest.mark.asyncio
async def test_tenant_boundary_filter_accepts_matching_trusted_context():
    sdk_context = new_agent_context(metadata={"tenant_id": "tenant", "app_id": "app"})
    result = FilterResult()
    agent_filter = TenantBoundaryAgentFilter("tenant", "app")
    with tenant_scope(TenantContext(tenant_id="tenant", app_id="app", config_version=1, request_id="r")):
        await agent_filter._before(sdk_context, None, result)
    assert result.error is None
    assert result.is_continue


def test_tool_registry_wraps_callable_with_sdk_filters():
    tools = ToolRegistry().resolve(ToolPolicy(allowed=["current_utc_time"], confirmation_required=["current_utc_time"]))
    assert len(tools) == 1
    assert isinstance(tools[0], FunctionTool)
    assert len(tools[0].filters) == 4


def test_confirmation_tool_must_be_allowed():
    with pytest.raises(ValueError, match="must also be allowed"):
        ToolPolicy(confirmation_required=["dangerous"])

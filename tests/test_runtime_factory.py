# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import pytest

from trpc_service.agent import TenantRuntimeFactory
from trpc_service.config import AgentAppConfig
from trpc_service.config import TenantConfig


@pytest.mark.asyncio
async def test_runtime_factory_uses_tenant_sdk_namespace():
    app = AgentAppConfig(
        app_id="assistant",
        agent_name="assistant",
        model={
            "model_name": "test-model",
            "api_key": "not-used"
        },
        tools={"allowed": ["current_utc_time"]},
    )
    tenant = TenantConfig(tenant_id="tenant-a", apps={"assistant": app})
    runtime = await TenantRuntimeFactory().create(tenant, app)
    try:
        assert runtime.runner.app_name == "tenant:tenant-a:app:assistant"
        assert runtime.runner.agent.name == "assistant"
        assert len(runtime.runner.agent.tools) == 1
        assert runtime.runner.agent.filters[0].name == "tenant_boundary"
    finally:
        await runtime.close()

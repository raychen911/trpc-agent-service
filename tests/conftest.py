# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Shared test configuration builders."""

import pytest

from trpc_service.config import AgentAppConfig
from trpc_service.config import TenantConfig


@pytest.fixture
def tenant_config() -> TenantConfig:
    return TenantConfig(
        tenant_id="tenant-a",
        version=1,
        apps={
            "assistant": AgentAppConfig(
                app_id="assistant",
                agent_name="assistant",
                model={"model_name": "test-model"},
            )
        },
    )

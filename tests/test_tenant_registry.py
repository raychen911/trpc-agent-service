# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import pytest

from trpc_service.tenant import InMemoryTenantRegistry


@pytest.mark.asyncio
async def test_publish_and_rollback_keep_snapshots_immutable(tenant_config):
    registry = InMemoryTenantRegistry([tenant_config])
    fetched = await registry.get("tenant-a")
    fetched.name = "mutated outside"
    assert (await registry.get("tenant-a")).name != "mutated outside"

    version_two = tenant_config.model_copy(update={"version": 2, "name": "Version Two"}, deep=True)
    await registry.publish(version_two)
    assert (await registry.get("tenant-a")).version == 2
    await registry.rollback("tenant-a", 1)
    assert (await registry.get("tenant-a")).version == 1


@pytest.mark.asyncio
async def test_publish_rejects_duplicate_version(tenant_config):
    registry = InMemoryTenantRegistry([tenant_config])
    with pytest.raises(ValueError, match="already exists"):
        await registry.publish(tenant_config)

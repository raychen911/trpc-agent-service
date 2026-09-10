from __future__ import annotations

import asyncio

import pytest

from trpc_service.governance.models import PolicyScope
from trpc_service.governance import policy


def test_tenants_cannot_read_each_others_policy_and_disabled_is_fail_closed() -> None:
    async def scenario() -> None:
        repository = policy.InMemoryGovernancePolicyRepository()
        document = policy.parse_policy_document({"allowed_tools": ["lookup"]})
        version = await repository.create_version(tenant_id="tenant-a", scope=PolicyScope.TENANT, document=document, actor_digest="a" * 64)
        await repository.activate(tenant_id="tenant-a", policy_id=version.policy_id, expected_generation=0)
        with pytest.raises(Exception):
            await repository.get_active(tenant_id="tenant-b", agent_name="agent", binding_id="binding")
        await repository.disable(tenant_id="tenant-a", policy_id=version.policy_id, expected_generation=1)
        with pytest.raises(Exception):
            await repository.get_active(tenant_id="tenant-a", agent_name="agent", binding_id="binding")

    asyncio.run(scenario())

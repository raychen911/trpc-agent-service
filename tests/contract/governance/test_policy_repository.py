from __future__ import annotations

import asyncio

from trpc_service.governance.models import PolicyScope
from trpc_service.governance import policy


def test_policy_repository_async_contract_is_tenant_scoped() -> None:
    repository = policy.InMemoryGovernancePolicyRepository()
    document = policy.parse_policy_document({"allowed_tools": ["lookup"]})
    version = asyncio.run(repository.create_version(tenant_id="tenant-a", scope=PolicyScope.TENANT, document=document, actor_digest="a" * 64))
    asyncio.run(repository.activate(tenant_id="tenant-a", policy_id=version.policy_id, expected_generation=0))
    active = asyncio.run(repository.get_active(tenant_id="tenant-a", agent_name="agent", binding_id="binding"))
    assert active.tenant_id == "tenant-a"

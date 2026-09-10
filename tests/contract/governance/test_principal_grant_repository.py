from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from trpc_service.governance import principal
from trpc_service.governance.models import PrincipalGrant


def test_grant_repository_requires_tenant_binding_and_subject_match() -> None:
    async def scenario() -> None:
        repository = principal.InMemoryPrincipalGrantRepository()
        subject = principal.issue_principal(tenant_id="tenant-a", channel="feishu", binding_id="b", provider_subject="u")
        grant = PrincipalGrant(
            grant_id="g1", tenant_id="tenant-a", channel="feishu", binding_id="b",
            provider_subject_digest=subject.subject_digest, agent_name="agent",
            permissions=frozenset({"use_agent"}), created_at=datetime.now(timezone.utc),
        )
        await repository.put(grant)
        assert (await repository.evaluate(principal=subject, agent_name="agent", binding_id="b", at=datetime.now(timezone.utc))).allowed
        other = principal.issue_principal(tenant_id="tenant-b", channel="feishu", binding_id="b", provider_subject="u")
        assert not (await repository.evaluate(principal=other, agent_name="agent", binding_id="b", at=datetime.now(timezone.utc))).allowed

    asyncio.run(scenario())

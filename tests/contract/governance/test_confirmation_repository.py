from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from trpc_service.governance import confirmation


def test_confirmation_repository_is_one_shot() -> None:
    async def scenario() -> None:
        repository = confirmation.InMemoryConfirmationRepository()
        pending = confirmation.create_pending(
            tenant_id="tenant-a", channel="feishu", binding_id="b",
            principal_digest="a" * 64, session_id="s", tool_name="delete",
            arguments_digest="x" * 64, policy_version=1, reservation_id="r1",
            now=datetime.now(timezone.utc), ttl_seconds=30,
        )
        await repository.create_once(pending)
        intent = confirmation.intent_for(pending)
        first = await repository.claim(intent=intent, owner_node_id="worker-a", owner_generation=1, now=datetime.now(timezone.utc))
        second = await repository.claim(intent=intent, owner_node_id="worker-b", owner_generation=1, now=datetime.now(timezone.utc))
        assert first is not None
        assert second is None

    asyncio.run(scenario())

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.support_channels import VirtualClock
from trpc_service.channels.contracts import Channel
from trpc_service.channels.identity import RuntimeBotIdentity
from trpc_service.storage.contracts import LeaseBusy, LeaseLost
from trpc_service.storage.redis_adapter_leases import InMemoryAdapterOwnershipRepository


@pytest.mark.asyncio
async def test_adapter_lease_acquire_renew_release_generation_and_fence() -> None:
    clock = VirtualClock()
    repository = InMemoryAdapterOwnershipRepository(now=clock.now)
    identity = "a" * 64
    first = await repository.acquire(identity, "node-a", 10_000)
    with pytest.raises(LeaseBusy):
        await repository.acquire(identity, "node-b", 10_000)
    bot = RuntimeBotIdentity(
        channel=Channel.FEISHU,
        sender_type="bot",
        sender_id="bot-a",
        channel_identity_digest=identity,
        authenticated_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
    )
    ready = await first.mark_ready(bot)
    assert ready.generation == 1
    assert (await repository.inspect(identity)).phase.value == "ready"
    renewed = await first.renew(10_000)
    assert renewed.generation == 1
    await first.release("normal")

    second = await repository.acquire(identity, "node-b", 10_000)
    assert second.fence.generation == 2
    assert not await repository.validate_fence(ready)
    with pytest.raises(LeaseLost):
        await first.renew(10_000)

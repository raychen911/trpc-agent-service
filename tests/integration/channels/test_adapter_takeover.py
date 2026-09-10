from __future__ import annotations

import asyncio

import pytest

from tests.integration.channels.multinode_support import NOW
from tests.support_channels import FakeProviderClient, VirtualClock, dual_im_settings
from trpc_service.channels.contracts import Channel
from trpc_service.channels.feishu import FeishuChannelAdapter
from trpc_service.channels.identity import RuntimeBotIdentity
from trpc_service.channels.runtime import ManagedChannelRuntime
from trpc_service.storage.contracts import SecretBytes
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.redis_adapter_leases import InMemoryAdapterOwnershipRepository


class MessageService:
    delivery_service = object()


@pytest.mark.asyncio
async def test_only_one_adapter_is_ready_and_takeover_fences_old_owner() -> None:
    _, identities = dual_im_settings()
    identity = identities[Channel.FEISHU]
    clock = VirtualClock()
    ownership = InMemoryAdapterOwnershipRepository(now=clock.now)
    runtimes = []
    providers = []
    for node_id in ("adapter-a", "adapter-b"):
        provider = FakeProviderClient(
            RuntimeBotIdentity(
                channel=Channel.FEISHU,
                sender_type="bot",
                sender_id="runtime-bot",
                channel_identity_digest=identity.identity_digest,
                authenticated_at=NOW,
            )
        )
        adapter = FeishuChannelAdapter(
            provider=provider,
            credential_secret=SecretBytes(b"placeholder"),
            message_service=MessageService(),
            now=clock.now,
        )
        runtimes.append(
            ManagedChannelRuntime(
                adapter, identity, NodeIdentity(node_id=node_id), ownership,
                lease_ms=10_000,
            )
        )
        providers.append(provider)

    assert (await runtimes[0].start_once()).value == "ready"
    assert (await runtimes[1].start_once()).value == "standby"
    assert sum(runtime.readiness().value == "ready" for runtime in runtimes) == 1
    old_fence = runtimes[0].fence
    await runtimes[0].stop("node_stopped")
    assert (await runtimes[1].start_once()).value == "ready"
    assert runtimes[1].fence.generation == old_fence.generation + 1
    assert not await ownership.validate_fence(old_fence)
    assert providers[0].close_count == 1
    await runtimes[1].stop("test_complete")


@pytest.mark.asyncio
async def test_slow_provider_start_renews_lease_before_marking_ready() -> None:
    _, identities = dual_im_settings()
    identity = identities[Channel.FEISHU]
    ownership = InMemoryAdapterOwnershipRepository()

    class SlowProvider(FakeProviderClient):
        async def connect(self, on_event, on_disconnect, on_error) -> None:
            await asyncio.sleep(0.2)
            await super().connect(on_event, on_disconnect, on_error)

    provider = SlowProvider(
        RuntimeBotIdentity(
            channel=Channel.FEISHU,
            sender_type="bot",
            sender_id="slow-runtime-bot",
            channel_identity_digest=identity.identity_digest,
            authenticated_at=NOW,
        )
    )
    adapter = FeishuChannelAdapter(
        provider=provider,
        credential_secret=SecretBytes(b"placeholder"),
        message_service=MessageService(),
    )
    runtime = ManagedChannelRuntime(
        adapter,
        identity,
        NodeIdentity(node_id="slow-adapter"),
        ownership,
        lease_ms=100,
        heartbeat_ms=50,
    )

    assert (await runtime.start_once()).value == "ready"
    await runtime.stop("test_complete")

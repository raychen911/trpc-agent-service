from __future__ import annotations

import pytest

from tests.integration.channels.multinode_support import TwoNodeIMHarness
from trpc_service.channels.contracts import Channel
from trpc_service.storage.contracts import ConfigurationUnavailable, StateBackendUnavailable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [ConfigurationUnavailable(), StateBackendUnavailable()]
)
async def test_backend_outage_is_bounded_fail_closed_and_never_uses_local_fallback(error) -> None:
    harness = await TwoNodeIMHarness.create(Channel.FEISHU)
    calls = 0

    async def unavailable(*args, **kwargs):
        nonlocal calls
        del args, kwargs
        calls += 1
        raise error

    harness.platform.resolve_by_channel_identity = unavailable
    event = harness.event(
        message_id="outage-message",
        conversation_id="outage-chat",
        sender_id="outage-user",
        text="Remember validation token OUTAGE.",
    )
    try:
        first = await harness.adapters[0].handle_provider_event(event)
        second = await harness.adapters[1].handle_provider_event(event)
        assert first.safe_code == second.safe_code == "binding_rejected"
        assert calls == 2
        assert harness.agent_calls == 0
        assert sum(len(provider.sent) for provider in harness.providers) == 0
    finally:
        await harness.close()

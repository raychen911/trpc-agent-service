from __future__ import annotations

import pytest
from redis.asyncio import Redis

from tests.support import inbound_message_data
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.redis_session import SharedSessionBackendFactory
from trpc_service.tenant.session_identity import derive_session_identity
from trpc_service.worker.service import AgentExecutor


@pytest.mark.shared_backend
async def test_session_is_scoped_ordered_and_readable_after_adapter_restart(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    context = InMemoryPlatformAdapters(build_demo_settings()).context_for_test(
        "binding-alpha", "shared-user", inbound_message_data()["trace_id"]
    )
    identity = derive_session_identity(context, "direct", "shared-conversation")
    first = AgentExecutor(SharedSessionBackendFactory(redis, namespace=shared_namespace))
    second = AgentExecutor(SharedSessionBackendFactory(redis, namespace=shared_namespace))

    stored = await (await first.prepare(context, identity, "Remember validation token ALPHA.")).execute()
    recalled = await (await second.prepare(context, identity, "Recall the validation token.")).execute()
    assert (stored.final_text, recalled.final_text) == ("stored:ALPHA", "recalled:ALPHA")

    await first.close()
    restarted = AgentExecutor(SharedSessionBackendFactory(redis, namespace=shared_namespace))
    after_restart = await (await restarted.prepare(context, identity, "Recall the validation token.")).execute()
    assert after_restart.final_text == "recalled:ALPHA"
    await second.close()
    await restarted.close()
    await redis.aclose()

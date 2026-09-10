from __future__ import annotations

from uuid import UUID

from tests.support import FIXED_UTC
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.contracts import ConditionalWriteFailed
import pytest


async def test_atomic_claim_processing_conflict_and_pre_start_reclaim() -> None:
    from trpc_service.storage.models import IdempotencyKey

    repository = InMemoryPlatformAdapters(build_demo_settings()).idempotency
    key = IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id="message-001")
    trace1 = UUID("11111111-1111-4111-8111-111111111111")
    trace2 = UUID("22222222-2222-4222-8222-222222222222")
    acquired = await repository.claim(key, "a" * 64, trace1, FIXED_UTC)
    assert acquired.disposition.value == "acquired"
    processing = await repository.claim(key, "a" * 64, trace2, FIXED_UTC)
    assert processing.disposition.value == "processing" and processing.original_trace_id == trace1
    assert (await repository.claim(key, "b" * 64, trace2, FIXED_UTC)).disposition.value == "conflict"
    await repository.mark_pre_start_failed(key, acquired.owner_token, "agent_unavailable", FIXED_UTC)
    reclaimed = await repository.claim(key, "a" * 64, trace2, FIXED_UTC)
    assert reclaimed.disposition.value == "acquired" and reclaimed.attempt == 2
    with pytest.raises(ConditionalWriteFailed):
        await repository.mark_running(key, reclaimed.owner_token, trace1, FIXED_UTC)

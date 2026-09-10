from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from trpc_service.channels.contracts import DeliveryAction
from trpc_service.storage.models import ExecutionResult, ExecutionStatus, IdempotencyKey
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository


def _result(trace_id, session_id="sess_" + "a" * 64) -> ExecutionResult:
    now = datetime.now(timezone.utc)
    return ExecutionResult(status=ExecutionStatus.SUCCEEDED, response_text="stored:ALPHA",
                           original_trace_id=trace_id, platform_session_id=session_id,
                           started_at=now, finished_at=now, agent_event_count=2,
                           final_response_count=1, delivery_action=DeliveryAction.DELIVER)


@pytest.mark.shared_backend
async def test_atomic_claim_processing_completion_conflict_and_trace_fields(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    first = RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id="node-a")
    second = RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id="node-b")
    key = IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id="same-id")
    trace = uuid4()
    claim = await first.claim(key, "a" * 64, trace, datetime.now(timezone.utc))
    assert claim.disposition.value == "acquired"
    processing = await second.claim(key, "a" * 64, uuid4(), datetime.now(timezone.utc))
    assert processing.disposition.value == "processing" and processing.original_trace_id == trace
    await first.mark_running(key, claim.owner_token, trace, datetime.now(timezone.utc))
    await first.complete(key, claim.owner_token, _result(trace), datetime.now(timezone.utc))
    cached = await second.claim(key, "a" * 64, uuid4(), datetime.now(timezone.utc))
    assert cached.disposition.value == "completed" and cached.result.response_text == "stored:ALPHA"
    conflict = await second.claim(key, "b" * 64, uuid4(), datetime.now(timezone.utc))
    assert conflict.disposition.value == "conflict"
    record = await second.get(key)
    assert record.generation == 1 and record.first_claim_trace_id == trace and record.execution_trace_id == trace
    await redis.aclose()

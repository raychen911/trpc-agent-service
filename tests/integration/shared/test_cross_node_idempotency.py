from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from trpc_service.channels.contracts import DeliveryAction
from trpc_service.storage.models import ExecutionResult, ExecutionStatus, IdempotencyKey
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository


@pytest.mark.shared_backend
async def test_fifty_cross_node_claim_races_have_exactly_one_owner(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    repositories = [RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id=f"node-{i}") for i in range(2)]
    sequential_key = IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id="sequential")
    sequential_trace = uuid4()
    first = await repositories[0].claim(sequential_key, "a" * 64, sequential_trace, datetime.now(timezone.utc))
    assert first.disposition.value == "acquired"
    now = datetime.now(timezone.utc)
    await repositories[0].mark_running(sequential_key, first.owner_token, sequential_trace, now)
    await repositories[0].complete(
        sequential_key, first.owner_token,
        ExecutionResult(
            status=ExecutionStatus.SUCCEEDED, response_text="stored:ALPHA",
            original_trace_id=sequential_trace,
            platform_session_id="sess_" + "a" * 64,
            started_at=now, finished_at=now, agent_event_count=2,
            final_response_count=1, delivery_action=DeliveryAction.DELIVER,
        ), now,
    )
    for _ in range(100):
        duplicate = await repositories[1].claim(sequential_key, "a" * 64, uuid4(), datetime.now(timezone.utc))
        assert duplicate.disposition.value == "completed"
        assert duplicate.result.delivery_action == DeliveryAction.DELIVER

    total_agent_calls = 0
    for index in range(50):
        key = IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id=f"race-{index}")
        traces = [uuid4(), uuid4()]
        claims = await asyncio.gather(*[
            repository.claim(key, "a" * 64, traces[position], datetime.now(timezone.utc))
            for position, repository in enumerate(repositories)
        ])
        assert sum(item.disposition.value == "acquired" for item in claims) == 1
        assert sum(item.disposition.value == "processing" for item in claims) == 1
        owner_index = next(i for i, item in enumerate(claims) if item.disposition.value == "acquired")
        owner, claim, trace = repositories[owner_index], claims[owner_index], traces[owner_index]
        await owner.mark_running(key, claim.owner_token, trace, datetime.now(timezone.utc))
        # This counter represents the only branch allowed to invoke Agent.
        total_agent_calls += 1
        finished = datetime.now(timezone.utc)
        await owner.complete(
            key, claim.owner_token,
            ExecutionResult(
                status=ExecutionStatus.SUCCEEDED, response_text="stored:ALPHA",
                original_trace_id=trace, platform_session_id="sess_" + f"{index:064x}",
                started_at=finished, finished_at=finished, agent_event_count=2,
                final_response_count=1, delivery_action=DeliveryAction.DELIVER,
            ), finished,
        )
        cached = await repositories[1 - owner_index].claim(
            key, "a" * 64, uuid4(), datetime.now(timezone.utc),
        )
        assert cached.disposition.value == "completed"
        assert cached.result.agent_event_count == 2
        assert cached.result.final_response_count == 1
        assert cached.result.delivery_action == DeliveryAction.DELIVER

    assert total_agent_calls == 50

    same_external_other_tenant = IdempotencyKey(tenant_id="tenant-beta", binding_id="binding-beta", external_message_id="race-0")
    assert (await repositories[0].claim(same_external_other_tenant, "a" * 64, uuid4(), datetime.now(timezone.utc))).disposition.value == "acquired"
    await redis.aclose()

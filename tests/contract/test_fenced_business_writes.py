from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from trpc_agent_sdk.sessions import Session
from trpc_service.storage.contracts import StaleFence
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.redis_leases import RedisSessionLeaseManager, use_session_fence
from trpc_service.storage.redis_session import FencedRedisSessionService
from trpc_service.audit.models import AuditDecision, AuditRecord, TenantScope
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresAuditRepository
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository
from trpc_service.storage.models import IdempotencyKey, ExecutionResult, ExecutionStatus
from trpc_service.channels.contracts import DeliveryAction
from trpc_service.storage.contracts import ConditionalWriteFailed
from datetime import timedelta
from uuid import uuid4


@pytest.mark.shared_backend
async def test_old_generation_cannot_update_session_after_takeover(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    manager = RedisSessionLeaseManager(redis, namespace=shared_namespace)
    service = FencedRedisSessionService(redis, tenant_id="tenant-alpha", agent_id="agent-alpha",
                                        namespace=shared_namespace, require_fence=True)
    session_id = "sess_" + "d" * 64
    first = await manager.acquire("tenant-alpha", "agent-alpha", session_id, "e" * 64,
                                  NodeIdentity(node_id="node-a"), 80, 20)
    async with use_session_fence(first.fence):
        session = await service.create_session(app_name="app", user_id="user", session_id=session_id)
    await asyncio.sleep(0.1)
    second = await manager.acquire("tenant-alpha", "agent-alpha", session_id, "f" * 64,
                                   NodeIdentity(node_id="node-b"), 200, 100)
    stale = session.model_copy(update={"state": {"value": "stale"}})
    async with use_session_fence(first.fence):
        with pytest.raises(StaleFence):
            await service.update_session(stale)
    current = session.model_copy(update={"state": {"value": "current"}})
    async with use_session_fence(second.fence):
        await service.update_session(current)
    assert (await service.get_session(app_name="app", user_id="user", session_id=session_id)).state == {"value": "current"}
    await second.release()
    await redis.aclose()


@pytest.mark.shared_backend
async def test_stale_business_audit_is_rejected_but_platform_diagnostic_is_append_only(shared_database_url: str) -> None:
    database = PostgresDatabase(shared_database_url)
    repository = PostgresAuditRepository(database, node_id="current-node", fence_validator=lambda _proof: False)
    scope = TenantScope(tenant_id="tenant-alpha")
    record = AuditRecord(audit_id=uuid4(), trace_id=uuid4(), tenant_id="tenant-alpha",
                         channel="local_http", binding_id_digest="sha256:" + "a" * 64,
                         decision=AuditDecision.SUCCEEDED, latency_ms=0, cost=Decimal("0"),
                         created_at=datetime.now(timezone.utc))
    with pytest.raises(StaleFence):
        await repository.append(scope, record, fence_proof={"generation": 1})
    diagnostic = record.model_copy(update={"audit_id": uuid4(), "decision": AuditDecision.OUTCOME_UNKNOWN})
    await repository.append_diagnostic(scope, diagnostic)
    rows = await repository.list_by_trace(scope, diagnostic.trace_id)
    assert [item.audit_id for item in rows] == [diagnostic.audit_id]
    await database.close()


@pytest.mark.shared_backend
async def test_expired_message_owner_cannot_commit_terminal_result(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    repository = RedisIdempotencyRepository(redis, namespace=shared_namespace, node_id="old", owner_lease_ms=60)
    key = IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id="late-terminal")
    now = datetime.now(timezone.utc); trace = uuid4()
    claim = await repository.claim(key, "a" * 64, trace, now)
    await repository.mark_running(key, claim.owner_token, trace, now)
    await asyncio.sleep(0.08)
    result = ExecutionResult(status=ExecutionStatus.SUCCEEDED, response_text="stored:ALPHA", original_trace_id=trace,
                             platform_session_id="sess_" + "a" * 64, started_at=now,
                             finished_at=now + timedelta(milliseconds=1), agent_event_count=2,
                             final_response_count=1, delivery_action=DeliveryAction.DELIVER)
    with pytest.raises(ConditionalWriteFailed):
        await repository.complete(key, claim.owner_token, result, datetime.now(timezone.utc))
    # Once EXECUTION_STARTED is durable and the owner lease expires, no node
    # may infer that replay is safe. The stable terminal-facing observation is
    # outcome_unknown until durable recovery evidence resolves it.
    assert (await repository.claim(key, "a" * 64, uuid4(), datetime.now(timezone.utc))).disposition.value == "outcome_unknown"
    await redis.aclose()

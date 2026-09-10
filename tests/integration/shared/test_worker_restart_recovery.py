import asyncio
from datetime import datetime, timezone
from uuid import uuid4
from decimal import Decimal
from hashlib import sha256

import pytest
from redis.asyncio import Redis

from trpc_service.recovery.reconciler import RecoveryReconciler
from trpc_service.audit.models import AuditDecision, AuditRecord, TenantScope
from trpc_service.channels.contracts import DeliveryAction
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.models import ExecutionResult, ExecutionStatus, IdempotencyKey
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresAuditRepository
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository
from trpc_service.storage.redis_session import SharedSessionBackendFactory
from trpc_service.tenant.session_identity import derive_session_identity
from trpc_service.worker.service import AgentExecutor


def test_recovery_constructor_cannot_accept_an_agent_executor() -> None:
    import inspect
    assert "agent" not in inspect.signature(RecoveryReconciler).parameters


@pytest.mark.shared_backend
async def test_twenty_worker_restarts_consult_shared_execution_evidence(
    shared_redis_url: str, shared_namespace: str,
) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    for index in range(20):
        before = RedisIdempotencyRepository(
            redis, namespace=shared_namespace, node_id="before", owner_lease_ms=30,
        )
        after = RedisIdempotencyRepository(
            redis, namespace=shared_namespace, node_id="after", owner_lease_ms=30,
        )
        key = IdempotencyKey(
            tenant_id="tenant-alpha", binding_id="binding-alpha",
            external_message_id=f"restart-{index}",
        )
        trace = uuid4()
        claim = await before.claim(key, "e" * 64, trace, datetime.now(timezone.utc))
        if index % 2:
            await before.mark_running(key, claim.owner_token, trace, datetime.now(timezone.utc))
        await asyncio.sleep(0.045)
        observed = await after.claim(key, "e" * 64, uuid4(), datetime.now(timezone.utc))
        if index % 2:
            assert observed.disposition.value == "outcome_unknown"
            assert (await after.get(key)).generation == 1
        else:
            assert observed.disposition.value == "acquired"
            assert (await after.get(key)).generation == 2
    await redis.aclose()


@pytest.mark.shared_backend
async def test_twenty_committed_session_idempotency_and_audit_samples_survive_all_workers_restarting(
    shared_redis_url: str, shared_database_url: str, shared_namespace: str,
) -> None:
    redis_before = Redis.from_url(shared_redis_url, decode_responses=True)
    database_before = PostgresDatabase(shared_database_url)
    worker_before = AgentExecutor(SharedSessionBackendFactory(redis_before, namespace=shared_namespace))
    idempotency_before = RedisIdempotencyRepository(
        redis_before, namespace=shared_namespace, node_id="before",
    )
    audit_before = PostgresAuditRepository(database_before, node_id="before")
    contexts = InMemoryPlatformAdapters(build_demo_settings())
    samples = []
    for index in range(20):
        trace = uuid4()
        context = contexts.context_for_test("binding-alpha", f"restart-user-{index}", trace)
        identity = derive_session_identity(context, "direct", f"restart-conversation-{index}")
        execution = await (await worker_before.prepare(
            context, identity, "Remember validation token ALPHA.",
        )).execute()
        key = IdempotencyKey(
            tenant_id="tenant-alpha", binding_id="binding-alpha",
            external_message_id=f"persisted-{uuid4().hex}",
        )
        claim = await idempotency_before.claim(key, "a" * 64, trace, datetime.now(timezone.utc))
        await idempotency_before.mark_running(key, claim.owner_token, trace, datetime.now(timezone.utc))
        now = datetime.now(timezone.utc)
        result = ExecutionResult(
            status=ExecutionStatus.SUCCEEDED, response_text=execution.final_text,
            original_trace_id=trace, platform_session_id=identity.platform_session_id,
            started_at=now, finished_at=now, agent_event_count=execution.event_count,
            final_response_count=execution.final_response_count,
            delivery_action=DeliveryAction.DELIVER,
        )
        await idempotency_before.complete(key, claim.owner_token, result, now)
        await audit_before.append(
            TenantScope(tenant_id="tenant-alpha"),
            AuditRecord(
                audit_id=uuid4(), trace_id=trace, first_claim_trace_id=trace,
                execution_trace_id=trace, generation=1, tenant_id="tenant-alpha",
                agent_id="agent-alpha", channel="local_http",
                binding_id_digest="sha256:" + sha256(b"binding-alpha").hexdigest(),
                session_id=identity.platform_session_id, decision=AuditDecision.SUCCEEDED,
                latency_ms=0, cost=Decimal("0"),
                external_message_digest="sha256:" + sha256(key.external_message_id.encode()).hexdigest(),
                created_at=now,
            ),
        )
        samples.append((context, identity, key, trace, result))

    await worker_before.close()
    await redis_before.aclose()
    await database_before.close()

    # Every process-local object is reconstructed over the same shared state.
    redis_after = Redis.from_url(shared_redis_url, decode_responses=True)
    database_after = PostgresDatabase(shared_database_url)
    worker_after = AgentExecutor(SharedSessionBackendFactory(redis_after, namespace=shared_namespace))
    idempotency_after = RedisIdempotencyRepository(
        redis_after, namespace=shared_namespace, node_id="after",
    )
    audit_after = PostgresAuditRepository(database_after, node_id="after")
    for context, identity, key, trace, result in samples:
        recalled = await (await worker_after.prepare(
            context, identity, "Recall the validation token.",
        )).execute()
        assert recalled.final_text == "recalled:ALPHA"
        duplicate = await idempotency_after.claim(
            key, "a" * 64, uuid4(), datetime.now(timezone.utc),
        )
        assert duplicate.disposition.value == "completed" and duplicate.result == result
        audit_rows = await audit_after.list_by_trace(TenantScope(tenant_id="tenant-alpha"), trace)
        assert len(audit_rows) == 1 and audit_rows[0].session_id == identity.platform_session_id

    await worker_after.close()
    await redis_after.aclose()
    await database_after.close()

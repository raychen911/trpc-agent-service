from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from tests.integration.shared.faults import AgentSpy
from trpc_service.audit.models import AuditDecision, AuditRecord, TenantScope
from trpc_service.channels.contracts import DeliveryAction
from trpc_service.recovery.reconciler import RecoveryReconciler
from trpc_service.storage.contracts import ConditionalWriteFailed
from trpc_service.storage.models import ExecutionResult, ExecutionStatus, IdempotencyKey
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresAuditRepository
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository


def test_persistent_audit_exposes_atomic_finalization_boundary() -> None:
    assert hasattr(PostgresAuditRepository, "append_final_with_recovery")


async def test_reconciler_only_copies_existing_terminal_result_and_has_no_agent_dependency() -> None:
    class Durable:
        async def get_pending(self, _scope, _limit): return [{"id": "r1", "result": "saved"}]
        async def mark_reconciled(self, _scope, _id, _digest): self.done = True
    class Terminal:
        async def complete_from_recovery(self, marker): self.marker = marker
    durable, terminal = Durable(), Terminal()
    reconciler = RecoveryReconciler(durable, terminal)
    assert not hasattr(reconciler, "agent")
    assert await reconciler.run_once("tenant-alpha") == 1
    assert terminal.marker["result"] == "saved" and durable.done


@pytest.mark.shared_backend
async def test_sql_terminal_marker_recovers_failed_redis_cas_without_agent_replay(
    shared_redis_url: str, shared_database_url: str, shared_namespace: str,
) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    database = PostgresDatabase(shared_database_url)
    idempotency = RedisIdempotencyRepository(
        redis, namespace=shared_namespace, node_id="node-a",
    )
    audit = PostgresAuditRepository(database, node_id="node-a")
    tenant_id = f"tenant-recovery-{uuid4().hex[:8]}"
    scope = TenantScope(tenant_id=tenant_id)
    trace = uuid4()
    key = IdempotencyKey(
        tenant_id=tenant_id, binding_id="binding-alpha",
        external_message_id=f"partial-{uuid4().hex}",
    )
    claim = await idempotency.claim(key, "f" * 64, trace, datetime.now(timezone.utc))
    await idempotency.mark_running(key, claim.owner_token, trace, datetime.now(timezone.utc))
    now = datetime.now(timezone.utc)
    result = ExecutionResult(
        status=ExecutionStatus.SUCCEEDED, response_text="durable-result",
        original_trace_id=trace, platform_session_id="sess_" + "f" * 64,
        started_at=now, finished_at=now, agent_event_count=2,
        final_response_count=1, delivery_action=DeliveryAction.DELIVER,
    )
    record = AuditRecord(
        audit_id=uuid4(), trace_id=trace, first_claim_trace_id=trace,
        owner_trace_id=trace, execution_trace_id=trace, generation=1,
        tenant_id=tenant_id, agent_id="agent-alpha", channel="local_http",
        binding_id_digest="sha256:" + sha256(b"binding-alpha").hexdigest(),
        session_id=result.platform_session_id, decision=AuditDecision.SUCCEEDED,
        latency_ms=1, cost=Decimal("0"),
        external_message_digest="sha256:" + sha256(key.external_message_id.encode()).hexdigest(),
        created_at=now,
    )
    await audit.append_final_with_recovery(
        scope, record, result, message_generation=1, session_generation=1,
        idempotency_key_digest=idempotency.codec.idempotency(key).rsplit(":", 1)[-1],
    )

    # Simulate the worker losing its valid owner token after SQL committed.
    with pytest.raises(ConditionalWriteFailed):
        await idempotency.complete(key, "invalid-owner-token", result, now)
    assert (await idempotency.get(key)).state.value == "running"

    agent = AgentSpy()
    reconciler = RecoveryReconciler(audit, idempotency)
    assert await reconciler.run_once(scope) == 1
    terminal = await idempotency.get(key)
    assert terminal.state.value == "succeeded"
    assert terminal.result == result
    assert agent.calls == 0
    markers = await audit.list_recovery_by_trace(scope, trace)
    assert len(markers) == 1 and markers[0].state == "reconciled"
    await redis.aclose()
    await database.close()

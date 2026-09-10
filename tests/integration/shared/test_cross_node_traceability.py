import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from redis.asyncio import Redis

from trpc_service.audit.models import AuditDecision, AuditRecord, TenantScope
from trpc_service.channels.contracts import DeliveryAction
from trpc_service.storage.contracts import ConditionalWriteFailed
from trpc_service.storage.models import ExecutionResult, ExecutionStatus, IdempotencyKey
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresAuditRepository
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository


def _digest(value: str) -> str:
    return "sha256:" + sha256(value.encode()).hexdigest()


def _audit(*, trace: UUID, decision: AuditDecision, state, node: str,
           external: str, diagnostic: bool = False, error: str | None = None) -> AuditRecord:
    return AuditRecord(
        audit_id=uuid4(), trace_id=trace,
        first_claim_trace_id=state.first_claim_trace_id,
        owner_trace_id=state.owner_trace_id,
        execution_trace_id=state.execution_trace_id,
        generation=state.generation, node_id=node,
        rejected_generation=1 if diagnostic else None,
        current_generation=state.generation if diagnostic else None,
        tenant_id="tenant-alpha", agent_id="agent-alpha",
        channel="local_http", binding_id_digest=_digest("binding-alpha"),
        session_id="sess_" + "d" * 64, decision=decision,
        latency_ms=1, cost=Decimal("0"), error_type=error,
        external_message_digest=_digest(external),
        created_at=datetime.now(timezone.utc),
    )


def _result(trace: UUID) -> ExecutionResult:
    now = datetime.now(timezone.utc)
    return ExecutionResult(
        status=ExecutionStatus.SUCCEEDED, response_text="shared-result",
        original_trace_id=trace, platform_session_id="sess_" + "d" * 64,
        started_at=now, finished_at=now, agent_event_count=2,
        final_response_count=1, delivery_action=DeliveryAction.DELIVER,
    )


@pytest.mark.shared_backend
async def test_success_duplicate_conflict_takeover_stale_write_and_outage_are_traceable(
    shared_redis_url: str, shared_database_url: str, shared_namespace: str,
) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    database = PostgresDatabase(shared_database_url)
    node_a = RedisIdempotencyRepository(
        redis, namespace=shared_namespace, node_id="node-a", owner_lease_ms=5_000,
    )
    node_b = RedisIdempotencyRepository(
        redis, namespace=shared_namespace, node_id="node-b", owner_lease_ms=5_000,
    )
    audit_a = PostgresAuditRepository(database, node_id="node-a")
    audit_b = PostgresAuditRepository(database, node_id="node-b")
    scope = TenantScope(tenant_id="tenant-alpha")

    # success on A, duplicate and conflict observed on B
    success_key = IdempotencyKey(
        tenant_id="tenant-alpha", binding_id="binding-alpha",
        external_message_id=f"trace-success-{uuid4().hex}",
    )
    first_trace, duplicate_trace, conflict_trace = uuid4(), uuid4(), uuid4()
    first = await node_a.claim(success_key, "a" * 64, first_trace, datetime.now(timezone.utc))
    await node_a.mark_running(success_key, first.owner_token, first_trace, datetime.now(timezone.utc))
    running = await node_a.get(success_key)
    written = []
    written.append(await audit_a.append(scope, _audit(
        trace=first_trace, decision=AuditDecision.SUCCEEDED,
        state=running, node="node-a", external=success_key.external_message_id,
    )))
    await node_a.complete(success_key, first.owner_token, _result(first_trace), datetime.now(timezone.utc))
    duplicate = await node_b.claim(success_key, "a" * 64, duplicate_trace, datetime.now(timezone.utc))
    conflict = await node_b.claim(success_key, "b" * 64, conflict_trace, datetime.now(timezone.utc))
    terminal = await node_b.get(success_key)
    assert duplicate.disposition.value == "completed"
    assert conflict.disposition.value == "conflict"
    written.append(await audit_b.append(scope, _audit(
        trace=duplicate_trace, decision=AuditDecision.DUPLICATE,
        state=terminal, node="node-b", external=success_key.external_message_id,
    )))
    written.append(await audit_b.append(scope, _audit(
        trace=conflict_trace, decision=AuditDecision.IDEMPOTENCY_CONFLICT,
        state=terminal, node="node-b", external=success_key.external_message_id,
    )))

    # A disappears pre-start; B takes over generation 2. A's stale token is rejected.
    node_a.owner_lease_ms = 35
    node_b.owner_lease_ms = 35
    takeover_key = IdempotencyKey(
        tenant_id="tenant-alpha", binding_id="binding-alpha",
        external_message_id=f"trace-takeover-{uuid4().hex}",
    )
    old_trace, new_trace = uuid4(), uuid4()
    old = await node_a.claim(takeover_key, "c" * 64, old_trace, datetime.now(timezone.utc))
    await asyncio.sleep(0.05)
    new = await node_b.claim(takeover_key, "c" * 64, new_trace, datetime.now(timezone.utc))
    await node_b.mark_running(takeover_key, new.owner_token, new_trace, datetime.now(timezone.utc))
    takeover = await node_b.get(takeover_key)
    assert takeover.generation == 2
    assert takeover.first_claim_trace_id == old_trace
    assert takeover.owner_trace_id == new_trace
    assert takeover.execution_trace_id == new_trace
    with pytest.raises(ConditionalWriteFailed):
        await node_a.mark_running(takeover_key, old.owner_token, old_trace, datetime.now(timezone.utc))
    written.append(await audit_b.append_diagnostic(scope, _audit(
        trace=new_trace, decision=AuditDecision.OUTCOME_UNKNOWN,
        state=takeover, node="node-b", external=takeover_key.external_message_id,
        diagnostic=True, error="stale_write",
    )))
    written.append(await audit_b.append_diagnostic(scope, _audit(
        trace=uuid4(), decision=AuditDecision.AUDIT_INCOMPLETE,
        state=takeover, node="node-b", external=f"outage-{uuid4().hex}",
        diagnostic=True, error="backend_unavailable",
    )))

    evidence = await audit_b.list_by_agent(scope, "agent-alpha")
    relevant = [item for item in evidence if item.external_message_digest in {
        _digest(success_key.external_message_id), _digest(takeover_key.external_message_id)
    }]
    assert {item.node_id for item in relevant} == {"node-a", "node-b"}
    assert all(item.first_claim_trace_id and item.execution_trace_id for item in relevant)
    assert any(item.audit_kind == "diagnostic" and item.rejected_generation == 1
               and item.current_generation == 2 for item in relevant)
    for sample in written:
        from_other_node = await audit_b.list_by_trace(scope, sample.trace_id)
        assert any(row.audit_id == sample.audit_id for row in from_other_node)
        by_session = await audit_b.list_by_session(scope, sample.session_id)
        assert any(row.audit_id == sample.audit_id for row in by_session)
    await redis.aclose()
    await database.close()

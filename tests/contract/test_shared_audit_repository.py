from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4

import pytest

from trpc_service.audit.models import AuditDecision, AuditRecord, TenantScope
from trpc_service.channels.contracts import DeliveryAction
from trpc_service.storage.contracts import AccessDenied
from trpc_service.storage.models import ExecutionResult, ExecutionStatus
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresAuditRepository


def _digest(value: str) -> str:
    return "sha256:" + sha256(value.encode()).hexdigest()


def _record(*, trace: UUID, audit_id: UUID | None = None,
            decision: AuditDecision = AuditDecision.SUCCEEDED,
            external: str | None = None, diagnostic: bool = False) -> AuditRecord:
    provider_message_id = external or f"audit-message-{trace.hex}"
    return AuditRecord(
        audit_id=audit_id or uuid4(), trace_id=trace,
        first_claim_trace_id=trace, owner_trace_id=trace,
        execution_trace_id=trace, generation=2,
        rejected_generation=1 if diagnostic else None,
        current_generation=2 if diagnostic else None,
        tenant_id="tenant-alpha", agent_id="agent-alpha",
        channel="local_http", binding_id_digest=_digest("binding-alpha"),
        session_id="sess_" + trace.hex + trace.hex,
        decision=decision, latency_ms=1, cost=Decimal("0"),
        error_type="stale_write" if diagnostic else None,
        external_message_digest=_digest(provider_message_id),
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.shared_backend
async def test_persistent_audit_is_immutable_and_queryable_by_all_scopes(
    shared_database_url: str,
) -> None:
    database = PostgresDatabase(shared_database_url)
    writer = PostgresAuditRepository(database, node_id="node-a")
    reader = PostgresAuditRepository(database, node_id="node-b")
    scope = TenantScope(tenant_id="tenant-alpha")
    trace = uuid4()

    stored = await writer.append(scope, _record(trace=trace))
    diagnostic = await writer.append_diagnostic(
        scope,
        _record(
            trace=trace, decision=AuditDecision.OUTCOME_UNKNOWN,
            external=f"diagnostic-message-{trace.hex}", diagnostic=True,
        ),
    )

    by_trace = await reader.list_by_trace(scope, trace)
    assert len(by_trace) == 2
    assert stored.node_id == "node-a" and stored.audit_kind == "business"
    assert diagnostic.node_id == "node-a" and diagnostic.audit_kind == "diagnostic"
    assert await reader.list_by_trace(TenantScope(tenant_id="tenant-beta"), trace) == []
    assert len(await reader.list_by_session(scope, stored.session_id)) == 2
    assert len(await reader.list_by_agent(scope, "agent-alpha")) >= 2
    assert all(item.tenant_id == "tenant-alpha" for item in await reader.list_by_tenant(scope))

    # The compatibility method may project another decision, but immutable SQL
    # evidence itself must remain unchanged.
    projected = await writer.update_final(
        scope, stored.audit_id, trace, AuditDecision.AGENT_FAILED,
    )
    assert projected.decision == AuditDecision.AGENT_FAILED
    persisted = await reader.list_by_trace(scope, trace)
    assert persisted[0].decision == AuditDecision.SUCCEEDED
    with pytest.raises(AccessDenied):
        await writer.reset()
    await database.close()


@pytest.mark.shared_backend
async def test_recovery_marker_is_queryable_by_execution_trace(
    shared_database_url: str,
) -> None:
    database = PostgresDatabase(shared_database_url)
    repository = PostgresAuditRepository(database, node_id="node-a")
    scope = TenantScope(tenant_id="tenant-alpha")
    trace = uuid4()
    now = datetime.now(timezone.utc)
    result = ExecutionResult(
        status=ExecutionStatus.SUCCEEDED, response_text="stored-result",
        original_trace_id=trace, platform_session_id="sess_" + "c" * 64,
        started_at=now, finished_at=now, agent_event_count=2,
        final_response_count=1, delivery_action=DeliveryAction.DELIVER,
    )
    marker = await repository.append_final_with_recovery(
        scope,
        _record(trace=trace, external=f"recovery-{trace.hex}"),
        result,
        message_generation=1,
        session_generation=1,
        idempotency_key_digest=sha256(trace.bytes).hexdigest(),
    )
    queried = await repository.list_recovery_by_trace(scope, trace)
    assert len(queried) == 1
    assert queried[0].recovery_id == marker["id"]
    assert queried[0].execution_result == result
    assert queried[0].state == "terminal_pending"
    await database.close()

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from tests.support import FIXED_UTC
from trpc_service.audit.models import AuditDecision, AuditRecord, PreAuthScope, TenantScope
from trpc_service.storage.contracts import AccessDenied, AuditUnavailable
from trpc_service.storage.inmemory import InMemoryAuditRepository


def _record(tenant: str | None, trace: UUID, session: str | None = None) -> AuditRecord:
    return AuditRecord(
        audit_id=uuid4(), trace_id=trace, tenant_id=tenant, channel="local_http",
        binding_id_digest="sha256:" + "a" * 64,
        user_id=("sha256:" + "b" * 64) if tenant else None,
        session_id=session, decision=AuditDecision.AUTHORIZED if tenant else AuditDecision.UNAUTHORIZED,
        latency_ms=0, created_at=FIXED_UTC,
    )


async def test_audit_scope_isolation_queries_and_failure_injection() -> None:
    repo = InMemoryAuditRepository()
    trace = UUID("11111111-1111-4111-8111-111111111111")
    alpha, beta = TenantScope(tenant_id="tenant-alpha"), TenantScope(tenant_id="tenant-beta")
    session = "sess_" + "a" * 64
    record = await repo.append(alpha, _record("tenant-alpha", trace, session))
    await repo.append(beta, _record("tenant-beta", trace))
    await repo.append(PreAuthScope(), _record(None, trace))
    assert await repo.list_by_trace(alpha, trace) == [record]
    assert await repo.list_by_session(alpha, session) == [record]
    assert await repo.list_by_tenant(alpha) == [record]
    updated = await repo.update_final(alpha, record.audit_id, trace, AuditDecision.SUCCEEDED, latency_ms=3)
    assert updated.decision == AuditDecision.SUCCEEDED and updated.latency_ms == 3
    repo.fail_update = True
    with pytest.raises(AuditUnavailable):
        await repo.update_final(alpha, record.audit_id, trace, AuditDecision.AGENT_FAILED)
    repo.fail_update = False
    with pytest.raises(AccessDenied):
        await repo.append(alpha, _record("tenant-beta", trace))
    with pytest.raises(AccessDenied):
        await repo.list_by_trace(PreAuthScope(), trace)
    with pytest.raises(AccessDenied):
        await repo.list_preauth(alpha)
    with pytest.raises(ValueError):
        await repo.update_final(alpha, record.audit_id, trace, AuditDecision.SUCCEEDED, secret="must-not-store")
    repo.fail_append = True
    with pytest.raises(AuditUnavailable):
        await repo.append(alpha, _record("tenant-alpha", trace))

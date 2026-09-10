from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from tests.support import FIXED_UTC
from trpc_service.audit.models import AuditDecision, AuditRecord, PreAuthScope, TenantScope
from trpc_service.metrics.models import MetricSnapshot
from trpc_service.tenant.models import (
    AgentApplication,
    ChannelBinding,
    ResourceStatus,
    Tenant,
    VerifiedTenantContext,
)
from trpc_service.tenant.session_identity import derive_session_identity


def _tenant(tenant_id: str = "tenant-alpha") -> Tenant:
    return Tenant(
        tenant_id=tenant_id,
        display_name="Alpha Tenant",
        status=ResourceStatus.ACTIVE,
        created_at=FIXED_UTC,
        config_version=1,
    )


def _agent(tenant_id: str = "tenant-alpha", agent_id: str = "agent-main") -> AgentApplication:
    return AgentApplication(
        tenant_id=tenant_id,
        agent_id=agent_id,
        agent_name="Main Agent",
        status="active",
        model_profile="deterministic-offline",
        instruction="Run the deterministic validation conversation.",
        config_version=1,
    )


def _binding(tenant_id: str = "tenant-alpha", agent_id: str = "agent-main") -> ChannelBinding:
    return ChannelBinding(
        binding_id="binding-alpha",
        tenant_id=tenant_id,
        agent_id=agent_id,
        channel="local_http",
        status="active",
        secret_ref="TRPC_DEMO_ALPHA_SECRET",
        signature_version="v1",
        created_at=FIXED_UTC,
    )


def _context(agent_id: str = "agent-main") -> VerifiedTenantContext:
    return VerifiedTenantContext.from_resources(
        tenant=_tenant(),
        agent=_agent(agent_id=agent_id),
        binding=_binding(agent_id=agent_id),
        external_user_id="external-user-123",
        trace_id=UUID("11111111-1111-4111-8111-111111111111"),
    )


def test_tenant_agent_binding_and_context_are_frozen_and_owner_checked() -> None:
    context = _context()
    assert context.tenant_id == "tenant-alpha"
    assert context.agent_id == "agent-main"
    with pytest.raises(ValidationError):
        context.agent_name = "changed"

    with pytest.raises(ValueError, match="ownership"):
        VerifiedTenantContext.from_resources(
            tenant=_tenant(),
            agent=_agent(tenant_id="tenant-beta"),
            binding=_binding(),
            external_user_id="external-user-123",
            trace_id=UUID("11111111-1111-4111-8111-111111111111"),
        )


@pytest.mark.parametrize("tenant_id", ["", "UPPER", "-invalid", "x" * 65])
def test_tenant_rejects_invalid_identifiers(tenant_id: str) -> None:
    with pytest.raises(ValidationError):
        _tenant(tenant_id)


def test_binding_keeps_only_secret_reference_and_requires_utc() -> None:
    binding = _binding()
    assert binding.secret_ref == "TRPC_DEMO_ALPHA_SECRET"
    assert "secret-value" not in repr(binding)
    with pytest.raises(ValidationError):
        ChannelBinding(**{**binding.model_dump(), "created_at": datetime(2026, 9, 5)})
    with pytest.raises(ValidationError):
        ChannelBinding(**{**binding.model_dump(), "secret_ref": "bad-secret-ref"})


def test_session_identity_is_stable_agent_scoped_and_pseudonymous() -> None:
    first = derive_session_identity(_context(), "direct", "external-conversation-456")
    repeat = derive_session_identity(_context(), "direct", "external-conversation-456")
    rebound = derive_session_identity(_context("agent-other"), "direct", "external-conversation-456")

    assert first == repeat
    assert first.platform_session_id.startswith("sess_")
    assert len(first.platform_session_id) == 69
    assert first.platform_session_id != rebound.platform_session_id
    serialized = first.model_dump_json()
    assert "external-user-123" not in serialized
    assert "external-conversation-456" not in serialized


def test_audit_record_is_scoped_pseudonymous_and_rejects_raw_sensitive_fields() -> None:
    scope = TenantScope.from_context(_context())
    record = AuditRecord(
        audit_id=UUID("22222222-2222-4222-8222-222222222222"),
        trace_id=UUID("11111111-1111-4111-8111-111111111111"),
        tenant_id=scope.tenant_id,
        channel="local_http",
        binding_id_digest="sha256:" + "a" * 64,
        user_id="sha256:" + "b" * 64,
        decision=AuditDecision.AUTHORIZED,
        latency_ms=1,
        cost=Decimal("0"),
        external_message_digest="sha256:" + "c" * 64,
        created_at=FIXED_UTC,
    )
    assert record.tenant_id == scope.tenant_id
    assert record.user_id != "external-user-123"

    with pytest.raises(ValidationError):
        AuditRecord(**{**record.model_dump(), "text": "raw body"})
    with pytest.raises(ValidationError):
        AuditRecord(**{**record.model_dump(), "signature": "v1=secret"})


def test_preauth_scope_has_no_tenant_and_metric_snapshot_enforces_counts() -> None:
    preauth = PreAuthScope()
    snapshot = MetricSnapshot(scope=preauth)
    assert snapshot.request_count == 0
    assert snapshot.model_metric_status == "not_applicable"
    assert "tenant_id" not in snapshot.model_dump()["scope"]

    with pytest.raises(ValidationError):
        MetricSnapshot(scope=preauth, request_count=1, error_count=2)
    with pytest.raises(ValidationError):
        MetricSnapshot(scope=TenantScope(tenant_id="tenant-alpha"), agent_latency_ms=-1)

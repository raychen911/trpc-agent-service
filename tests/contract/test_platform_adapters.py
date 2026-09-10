from __future__ import annotations

from uuid import UUID

import pytest

from trpc_service.audit.models import AuditDecision, AuditRecord, TenantScope
from trpc_service.config.settings import build_demo_settings
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.contracts import AccessDenied
from trpc_service.channels.contracts import InboundMessage, VerifiedBindingScope
from trpc_service.gateway.service import GatewayService
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder
from trpc_service.storage.contracts import PlatformAdapters
from trpc_service.storage.locks import SessionLockManager
from trpc_service.storage.session_backend import SessionBackendFactory
from trpc_service.worker.service import AgentExecutor
from tests.support import inbound_message_data
from tests.support import FIXED_UTC


async def test_active_directory_audit_and_metrics_happy_path() -> None:
    adapters = InMemoryPlatformAdapters(build_demo_settings())
    trace_id = UUID("11111111-1111-4111-8111-111111111111")
    context = adapters.context_for_test("binding-alpha", "user-001", trace_id)
    scope = TenantScope.from_context(context)
    record = AuditRecord(
        audit_id=UUID("22222222-2222-4222-8222-222222222222"),
        trace_id=trace_id,
        tenant_id=context.tenant_id,
        channel=context.channel,
        binding_id_digest="sha256:" + "a" * 64,
        user_id="sha256:" + "b" * 64,
        decision=AuditDecision.AUTHORIZED,
        latency_ms=1,
        created_at=FIXED_UTC,
    )
    await adapters.audit.append(scope, record)
    assert await adapters.audit.list_by_trace(scope, trace_id) == [record]

    metrics = InMemoryMetricsRecorder()
    metrics.record(scope, trace_id=trace_id, stage="request", outcome="success", duration_ms=2)
    assert metrics.snapshot(scope).request_count == 1

    forged = VerifiedBindingScope.model_construct(binding_id="binding-alpha", channel="local_http")
    with pytest.raises(AccessDenied):
        await adapters.resolve_active_context(forged, external_user_id="user-001", trace_id=trace_id)


async def test_gateway_accepts_a_minimal_port_compatible_adapter() -> None:
    class FakeAdapters:
        def __init__(self):
            self.inner = InMemoryPlatformAdapters(build_demo_settings())
            self.audit = self.inner.audit
            self.idempotency = self.inner.idempotency
        async def get_auth_material(self, binding_id, channel):
            return await self.inner.get_auth_material(binding_id, channel)
        async def resolve_active_context(self, scope, **kwargs):
            return await self.inner.resolve_active_context(scope, **kwargs)

    adapters = FakeAdapters()
    assert isinstance(adapters, PlatformAdapters)
    worker = AgentExecutor(SessionBackendFactory())
    gateway = GatewayService(adapters, InMemoryMetricsRecorder(), worker, SessionLockManager(), now=lambda: FIXED_UTC)
    message = InboundMessage(**inbound_message_data())
    reply = await gateway.handle_verified_message(
        VerifiedBindingScope._issue(binding_id=message.binding_id, channel=message.channel), message
    )
    assert reply.status.value == "succeeded"
    await worker.close()


@pytest.mark.parametrize("resource", ["tenant", "agent", "binding", "ownership"])
async def test_directory_rejects_disabled_or_misowned_resources_without_business_state(resource: str) -> None:
    settings = build_demo_settings()
    if resource == "tenant":
        settings = settings.model_copy(update={"tenants": (settings.tenants[0].model_copy(update={"status": "disabled"}),) + settings.tenants[1:]})
    elif resource == "agent":
        settings = settings.model_copy(update={"agents": (settings.agents[0].model_copy(update={"status": "disabled"}),) + settings.agents[1:]})
    elif resource == "binding":
        settings = settings.model_copy(update={"bindings": (settings.bindings[0].model_copy(update={"status": "disabled"}),) + settings.bindings[1:]})
    else:
        settings = settings.model_copy(update={"bindings": (settings.bindings[0].model_copy(update={"agent_id": "agent-beta"}),) + settings.bindings[1:]})
    adapters = InMemoryPlatformAdapters(settings)
    scope = VerifiedBindingScope._issue(binding_id="binding-alpha", channel="local_http")
    with pytest.raises(AccessDenied):
        await adapters.resolve_active_context(scope, external_user_id="user-001", trace_id=UUID(int=60000))
    assert adapters.idempotency._records == {}
    assert adapters.audit._tenant_records == {}

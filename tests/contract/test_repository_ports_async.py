from __future__ import annotations

from uuid import UUID

import pytest

from tests.contract.support.repository_contracts import assert_async_port
from tests.support import FIXED_UTC
from trpc_service.audit.models import TenantScope
from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.contracts import (
    AuditRepository,
    BindingAuthRegistry,
    ConfigurationUnavailable,
    IdempotencyRepository,
    RecoveryRepository,
    SessionLeaseManager,
    SharedSessionRepository,
    StateBackendUnavailable,
    TenantDirectory,
)
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.models import IdempotencyKey


def test_external_state_ports_are_async() -> None:
    assert_async_port(BindingAuthRegistry, "get_auth_material")
    assert_async_port(TenantDirectory, "resolve_active_context")
    assert_async_port(IdempotencyRepository, "claim", "get", "complete")
    assert_async_port(AuditRepository, "append", "list_by_trace")
    assert_async_port(SessionLeaseManager, "acquire")
    assert_async_port(SharedSessionRepository, "get_session", "append_event")
    assert_async_port(RecoveryRepository, "get_pending", "mark_reconciled")


async def test_inmemory_adapter_obeys_async_configuration_and_idempotency_ports() -> None:
    adapters = InMemoryPlatformAdapters(build_demo_settings())
    material = await adapters.get_auth_material("binding-alpha", Channel.LOCAL_HTTP)
    assert material.binding_id == "binding-alpha"
    scope = VerifiedBindingScope._issue(
        binding_id="binding-alpha", channel=Channel.LOCAL_HTTP
    )
    context = await adapters.resolve_active_context(
        scope, external_user_id="user-001", trace_id=UUID(int=1)
    )
    assert context.tenant_id == "tenant-alpha"
    key = IdempotencyKey(
        tenant_id=context.tenant_id,
        binding_id=context.binding_id,
        external_message_id="message-001",
    )
    claim = await adapters.idempotency.claim(
        key, "a" * 64, UUID(int=1), FIXED_UTC
    )
    assert claim.disposition.value == "acquired"
    assert await adapters.idempotency.get(key)
    assert await adapters.audit.list_by_tenant(TenantScope.from_context(context)) == []


def test_shared_backend_errors_are_stable_and_redacted() -> None:
    assert str(ConfigurationUnavailable()) == "Configuration is unavailable."
    assert str(StateBackendUnavailable()) == "Shared state is unavailable."
    assert "redis" not in str(StateBackendUnavailable()).lower()
    assert "postgres" not in str(ConfigurationUnavailable()).lower()

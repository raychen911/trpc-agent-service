"""Storage profile and tenant binding invariants."""

import pytest
from pydantic import ValidationError

from trpc_service.storage import BackendCapability
from trpc_service.storage import BackendProfile
from trpc_service.storage import BackendProfileRegistry
from trpc_service.storage import ResourceType
from trpc_service.storage import TenantBackendBinding
from trpc_service.storage import validate_tenant_bindings
from trpc_service.tenant import BackendKind
from trpc_service.tenant import SecretRef


def profile(profile_id: str, kind: BackendKind, capabilities: set[BackendCapability]) -> BackendProfile:
    return BackendProfile(
        profile_id=profile_id,
        kind=kind,
        provider="test",
        endpoint="sqlite+pysqlite:///:memory:",
        secret_ref=SecretRef(uri=f"vault://profiles/{profile_id}"),
        capabilities=capabilities,
        profile_revision=1,
    )


def binding(resource: ResourceType, profile_id: str, tenant_id: str = "tenant_acme") -> TenantBackendBinding:
    return TenantBackendBinding(
        tenant_id=tenant_id,
        agent_app_id="customer_support",
        config_version=3,
        resource_type=resource,
        profile_id=profile_id,
        profile_revision=1,
        namespace=tenant_id,
        storage_revision=4,
    )


def test_profile_rejects_capability_incompatible_with_kind() -> None:
    with pytest.raises(ValidationError, match="not supported by backend kind"):
        profile("redis_main", BackendKind.REDIS, {BackendCapability.ARTIFACT})


def test_registry_resolves_exact_revision_without_latest_fallback() -> None:
    registry = BackendProfileRegistry([
        profile("sql_main", BackendKind.SQL, {BackendCapability.SESSION}),
    ])

    assert registry.resolve("sql_main", 1).profile_revision == 1
    with pytest.raises(LookupError, match="revision 2"):
        registry.resolve("sql_main", 2)


def test_binding_namespace_and_capability_are_enforced() -> None:
    registry = BackendProfileRegistry([
        profile("redis_main", BackendKind.REDIS, {BackendCapability.SESSION}),
    ])
    wrong_namespace = binding(ResourceType.SESSION, "redis_main").model_copy(update={"namespace": "tenant_other"})
    with pytest.raises(ValueError, match="namespace"):
        validate_tenant_bindings("tenant_acme", [wrong_namespace], registry)

    wrong_capability = binding(ResourceType.MEMORY, "redis_main")
    with pytest.raises(ValueError, match="memory capability"):
        validate_tenant_bindings("tenant_acme", [wrong_capability], registry)


def test_summary_must_follow_session_profile() -> None:
    registry = BackendProfileRegistry([
        profile("sql_a", BackendKind.SQL, {BackendCapability.SESSION, BackendCapability.SUMMARY}),
        profile("sql_b", BackendKind.SQL, {BackendCapability.SUMMARY}),
    ])
    bindings = [
        binding(ResourceType.SESSION, "sql_a"),
        binding(ResourceType.SUMMARY, "sql_b"),
    ]

    with pytest.raises(ValueError, match="summary must use the session profile"):
        validate_tenant_bindings("tenant_acme", bindings, registry)


def test_in_memory_is_rejected_for_multi_worker_production() -> None:
    registry = BackendProfileRegistry([
        profile("memory_dev", BackendKind.IN_MEMORY, {BackendCapability.SESSION}),
    ])
    with pytest.raises(ValueError, match="in-memory"):
        validate_tenant_bindings(
            "tenant_acme",
            [binding(ResourceType.SESSION, "memory_dev")],
            registry,
            production=True,
            worker_count=2,
        )

"""Tenant storage resolver lifecycle and isolation."""

import pytest

from trpc_service.storage import BackendCapability
from trpc_service.storage import BackendProfile
from trpc_service.storage import BackendProfileRegistry
from trpc_service.storage import ResourceType
from trpc_service.storage import TenantBackendBinding
from trpc_service.storage import TenantStorageResolver
from trpc_service.storage import TenantBoundaryError
from trpc_service.tenant import BackendKind
from trpc_service.tenant import SecretRef


class Service:
    def __init__(self, profile_id: str) -> None:
        self.profile_id = profile_id
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def make_profile(profile_id: str) -> BackendProfile:
    return BackendProfile(
        profile_id=profile_id,
        kind=BackendKind.SQL,
        provider="sqlite",
        endpoint="sqlite+pysqlite:///:memory:",
        secret_ref=SecretRef(uri=f"vault://profiles/{profile_id}"),
        capabilities={BackendCapability.SESSION, BackendCapability.SUMMARY},
        profile_revision=1,
    )


def make_binding(tenant_id: str, resource: ResourceType, profile_id: str, storage_revision: int = 1):
    return TenantBackendBinding(
        tenant_id=tenant_id,
        agent_app_id="support_app",
        config_version=1,
        resource_type=resource,
        profile_id=profile_id,
        profile_revision=1,
        namespace=tenant_id,
        storage_revision=storage_revision,
    )


@pytest.mark.asyncio
async def test_resolver_reuses_profile_service_but_keeps_tenant_scope() -> None:
    registry = BackendProfileRegistry([make_profile("sql_shared")])
    created: list[Service] = []

    def factory(profile, resource):
        service = Service(profile.profile_id)
        created.append(service)
        return service

    resolver = TenantStorageResolver(registry, factory)
    resources = (ResourceType.SESSION, ResourceType.SUMMARY)
    acme = await resolver.resolve(
        tenant_id="tenant_acme",
        agent_app_id="support_app",
        config_version=1,
        storage_revision=1,
        bindings=[make_binding("tenant_acme", resource, "sql_shared") for resource in resources],
    )
    beta = await resolver.resolve(
        tenant_id="tenant_beta",
        agent_app_id="support_app",
        config_version=1,
        storage_revision=1,
        bindings=[make_binding("tenant_beta", resource, "sql_shared") for resource in resources],
    )

    assert acme.session.raw_service is beta.session.raw_service
    assert acme.session.raw_service is acme.summary.raw_service
    assert acme.session.namespace == "tenant_acme"
    assert beta.session.namespace == "tenant_beta"
    assert len(created) == 1

    with pytest.raises(TenantBoundaryError, match="outside"):
        await acme.session.create_session(
            app_name="tenant_beta:support_app",
            user_id="user-1",
            session_id="session-1",
        )

    await acme.release()
    assert not all(item.closed for item in created)
    await beta.release()
    await resolver.close()
    assert all(item.closed for item in created)


@pytest.mark.asyncio
async def test_versions_coexist_and_missing_revision_never_falls_back() -> None:
    registry = BackendProfileRegistry([make_profile("sql_shared")])
    resolver = TenantStorageResolver(registry, lambda profile, resource: Service(profile.profile_id))
    bindings = [
        make_binding("tenant_acme", ResourceType.SESSION, "sql_shared", storage_revision=7).model_copy(
            update={"config_version": 2})
    ]

    bundle = await resolver.resolve(
        tenant_id="tenant_acme",
        agent_app_id="support_app",
        config_version=2,
        storage_revision=7,
        bindings=bindings,
    )
    assert bundle.storage_revision == 7
    with pytest.raises(LookupError, match="storage revision"):
        await resolver.resolve(
            tenant_id="tenant_acme",
            agent_app_id="support_app",
            config_version=2,
            storage_revision=8,
            bindings=bindings,
        )
    await bundle.release()
    await resolver.close()

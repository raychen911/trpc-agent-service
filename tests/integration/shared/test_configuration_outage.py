import pytest

from trpc_service.config.cache import AuthoritativeConfigCache
from trpc_service.storage.contracts import ConfigurationUnavailable
from trpc_service.storage.contracts import AccessDenied, NotFound
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresConfigurationRepository
from trpc_service.config.settings import build_demo_settings
from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from trpc_service.storage.postgres.models import ChannelBindingRow, TenantRow
from uuid import uuid4


async def test_positive_cache_never_authorizes_when_sql_authority_is_unavailable() -> None:
    cache = AuthoritativeConfigCache()
    cache.remember_verified("binding-alpha", 1, {"tenant_id": "tenant-alpha"})
    async def unavailable():
        raise ConfigurationUnavailable()
    with pytest.raises(ConfigurationUnavailable):
        await cache.authorize("binding-alpha", unavailable)


async def test_cached_denial_may_continue_to_deny() -> None:
    cache = AuthoritativeConfigCache()
    cache.remember_denied("revoked-binding", 3)
    called = False
    async def unavailable():
        nonlocal called; called = True; raise ConfigurationUnavailable()
    with pytest.raises(PermissionError):
        await cache.authorize("revoked-binding", unavailable)
    assert called is False


@pytest.mark.shared_backend
async def test_authority_observes_binding_version_unknown_binding_and_disabled_tenant(shared_database_url: str) -> None:
    database = PostgresDatabase(shared_database_url); repository = PostgresConfigurationRepository(database)
    await repository.seed(build_demo_settings())
    with pytest.raises(NotFound):
        await repository.get_auth_material("binding-unknown", Channel.LOCAL_HTTP)
    async with AsyncSession(database.engine) as session, session.begin():
        await session.execute(update(ChannelBindingRow).where(ChannelBindingRow.binding_id == "binding-alpha").values(config_version=2))
    scope = VerifiedBindingScope._issue(binding_id="binding-alpha", channel=Channel.LOCAL_HTTP)
    assert (await repository.resolve_active_context(scope, external_user_id="user", trace_id=uuid4())).config_version == 2
    async with AsyncSession(database.engine) as session, session.begin():
        await session.execute(update(TenantRow).where(TenantRow.tenant_id == "tenant-alpha").values(status="disabled"))
    with pytest.raises(AccessDenied):
        await repository.resolve_active_context(scope, external_user_id="user", trace_id=uuid4())
    await repository.seed(build_demo_settings())
    await database.close()

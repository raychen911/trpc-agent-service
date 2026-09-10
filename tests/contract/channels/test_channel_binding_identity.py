from __future__ import annotations

from uuid import UUID

import pytest

from tests.support_channels import dual_im_settings
from trpc_service.channels.contracts import Channel
from trpc_service.channels.identity import ChannelIdentity
from trpc_service.storage.contracts import AccessDenied
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresConfigurationRepository


async def _assert_identity_contract(repository) -> None:
    settings, identities = dual_im_settings()
    for channel, identity in identities.items():
        resolved = await repository.resolve_by_channel_identity(
            identity, external_user_id="same-external-user", trace_id=UUID(int=37)
        )
        assert resolved.context.tenant_id == (
            "tenant-alpha" if channel == Channel.FEISHU else "tenant-beta"
        )
        assert resolved.context.channel == channel
        assert resolved.context.config_version == 4
        assert resolved.scope.binding_id == resolved.context.binding_id

    invalid = (
        ChannelIdentity(channel=Channel.FEISHU, provider_tenant_key="wrong-tenant", provider_app_or_bot_id="feishu-bot-alpha"),
        ChannelIdentity(channel=Channel.FEISHU, provider_tenant_key="feishu-tenant-alpha", provider_app_or_bot_id="wrong-bot"),
        ChannelIdentity(channel=Channel.WECOM, provider_tenant_key="feishu-tenant-alpha", provider_app_or_bot_id="feishu-bot-alpha"),
        object(),
    )
    errors = []
    for identity in invalid:
        with pytest.raises(AccessDenied) as captured:
            await repository.resolve_by_channel_identity(
                identity, external_user_id="same-external-user", trace_id=UUID(int=38)
            )
        errors.append(str(captured.value))
    assert errors == ["Channel binding is unavailable."] * len(invalid)


@pytest.mark.asyncio
async def test_inmemory_repository_resolves_only_exact_authenticated_composite_identity() -> None:
    settings, _ = dual_im_settings()
    await _assert_identity_contract(InMemoryPlatformAdapters(settings))


@pytest.mark.shared_backend
@pytest.mark.asyncio
async def test_postgres_repository_obeys_same_composite_identity_contract(shared_database_url: str) -> None:
    settings, _ = dual_im_settings()
    database = PostgresDatabase(shared_database_url)
    try:
        await database.initialize_schema()
        repository = PostgresConfigurationRepository(database)
        await repository.seed(settings)
        await _assert_identity_contract(repository)
    finally:
        await database.close()

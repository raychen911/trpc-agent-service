from __future__ import annotations

from uuid import uuid4

import pytest

from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.contracts import AccessDenied, NotFound
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresConfigurationRepository


@pytest.mark.shared_backend
async def test_authoritative_configuration_is_versioned_and_fail_closed(shared_database_url: str) -> None:
    database = PostgresDatabase(shared_database_url)
    repository = PostgresConfigurationRepository(database)
    await repository.seed(build_demo_settings())
    auth = await repository.get_auth_material("binding-alpha", Channel.LOCAL_HTTP)
    assert auth.secret_ref == "TRPC_DEMO_ALPHA_SECRET"
    assert auth.status.value == "active"

    scope = VerifiedBindingScope._issue(binding_id="binding-alpha", channel=Channel.LOCAL_HTTP)
    context = await repository.resolve_active_context(scope, external_user_id="user", trace_id=uuid4())
    assert (context.tenant_id, context.agent_id, context.config_version) == ("tenant-alpha", "agent-alpha", 1)

    with pytest.raises(NotFound):
        await repository.get_auth_material("unknown-binding", Channel.LOCAL_HTTP)
    with pytest.raises(AccessDenied):
        await repository.get_auth_material("binding-alpha", Channel("local_http"), expected_tenant_id="tenant-beta")
    await database.close()

"""Tenant configuration publication contract tests."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from trpc_service.storage import Database
from trpc_service.storage.models import ChannelBinding, TenantConfigRevision
from trpc_service.tenant.models import TenantSpec
from trpc_service.tenant.service import (
    RevisionConflictError,
    RevisionSequenceError,
    TenantConfigService,
    TenantNotFoundError,
    _publication_payload,
)


def _spec(*, revision: int = 1, prompt: str = "Be concise") -> TenantSpec:
    return TenantSpec.model_validate(
        {
            "tenant_id": "tenant-acme",
            "revision": revision,
            "display_name": "Acme",
            "apps": [
                {
                    "app_id": "support",
                    "revision": revision,
                    "name": "support_agent",
                    "prompt": prompt,
                    "model": {"provider": "mock", "model": "deterministic"},
                    "tools": {"allowed": ["lookup_order"]},
                }
            ],
            "channels": [
                {
                    "binding_id": "wecom-primary",
                    "app_id": "support",
                    "app_revision": revision,
                    "channel": "wecom",
                    "external_account_id": "bot-001",
                    "callback_path": "/v1/channels/wecom/cb-acme/callback",
                    "public_callback_id": "cb-acme",
                    "secret_refs": {
                        "token": "secret://env/TEST_WECOM_TOKEN",
                        "aes_key": "secret://env/TEST_WECOM_AES_KEY",
                    },
                }
            ],
        }
    )


@pytest.fixture
async def service(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'tenant.db'}")
    await database.create_schema()
    try:
        yield TenantConfigService(database.session_factory), database
    finally:
        await database.dispose()


async def test_publish_is_idempotent_and_materializes_binding(service) -> None:
    config_service, database = service
    first = await config_service.publish(_spec(), actor="admin:test")
    repeated = await config_service.publish(_spec(), actor="admin:test")

    assert first.idempotent is False
    assert repeated.idempotent is True
    assert repeated.content_hash == first.content_hash
    assert (await config_service.load_active("tenant-acme")).revision == 1
    async with database.session_factory() as session:
        binding = await session.get(ChannelBinding, "wecom-primary")
        assert binding is not None
        assert binding.tenant_id == "tenant-acme"
        assert binding.app_revision == 1


def test_publication_payload_canonicalizes_semantic_sets() -> None:
    spec = _spec()
    app = spec.apps[0].model_copy(
        update={
            "tools": spec.apps[0].tools.model_copy(
                update={
                    "allowed": frozenset({"zeta", "alpha"}),
                    "requires_approval": frozenset({"zeta"}),
                }
            )
        }
    )
    channel = spec.channels[0].model_copy(
        update={
            "identity_policy": spec.channels[0].identity_policy.model_copy(
                update={
                    "allow_principals": frozenset({"usr_z", "usr_a"}),
                    "allowed_scopes": frozenset({"private", "group"}),
                }
            )
        }
    )
    payload = _publication_payload(spec.model_copy(update={"apps": (app,), "channels": (channel,)}))

    assert payload["apps"][0]["tools"]["allowed"] == ["alpha", "zeta"]
    assert payload["channels"][0]["identity_policy"]["allow_principals"] == [
        "usr_a",
        "usr_z",
    ]
    assert payload["channels"][0]["identity_policy"]["allowed_scopes"] == [
        "group",
        "private",
    ]


async def test_revision_content_is_immutable(service) -> None:
    config_service, _ = service
    await config_service.publish(_spec(), actor="admin:test")

    with pytest.raises(RevisionConflictError):
        await config_service.publish(_spec(prompt="Changed"), actor="admin:test")


async def test_revision_sequence_and_rollback(service) -> None:
    config_service, database = service
    await config_service.publish(_spec(), actor="admin:test")
    with pytest.raises(RevisionSequenceError):
        await config_service.publish(_spec(revision=3), actor="admin:test")

    await config_service.publish(_spec(revision=2, prompt="Revision two"), actor="admin:test")
    rolled_back = await config_service.rollback("tenant-acme", target_revision=1)
    assert rolled_back.revision == 1
    assert (await config_service.load_active("tenant-acme")).revision == 1

    async with database.session_factory() as session:
        rows = (
            await session.scalars(
                select(TenantConfigRevision).where(TenantConfigRevision.tenant_id == "tenant-acme")
            )
        ).all()
    assert {row.revision for row in rows} == {1, 2}


async def test_load_revision_is_independent_of_active_rollback(service) -> None:
    config_service, _ = service
    await config_service.publish(_spec(), actor="admin:test")
    await config_service.publish(_spec(revision=2, prompt="Revision two"), actor="admin:test")
    await config_service.rollback("tenant-acme", target_revision=1)

    pinned = await config_service.load_revision("tenant-acme", 2)
    assert pinned.revision == 2
    assert pinned.apps[0].prompt == "Revision two"
    with pytest.raises(TenantNotFoundError):
        await config_service.load_revision("tenant-acme", 3)

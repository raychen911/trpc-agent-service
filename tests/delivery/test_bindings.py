"""Contract tests for tenant-scoped outbound binding reads."""

from __future__ import annotations

from pathlib import Path

import pytest

from trpc_service.delivery import DeliveryConfigurationError, SqlChannelBindingStore
from trpc_service.storage.database import Database
from trpc_service.storage.models import AgentApp, ChannelBinding, Tenant


@pytest.mark.asyncio
async def test_sql_binding_store_never_crosses_tenant_or_returns_disabled(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'delivery.db'}")
    await database.create_schema()
    try:
        async with database.session_factory() as session, session.begin():
            for suffix in ("a", "b"):
                tenant_id = f"tenant-{suffix}"
                session.add(Tenant(tenant_id=tenant_id, display_name=tenant_id))
            await session.flush()
            for suffix in ("a", "b"):
                tenant_id = f"tenant-{suffix}"
                session.add(
                    AgentApp(
                        tenant_id=tenant_id,
                        app_id="assistant",
                        revision=1,
                        status="published",
                        agent_name="Assistant",
                        prompt="safe",
                    )
                )
            await session.flush()
            for suffix in ("a", "b"):
                tenant_id = f"tenant-{suffix}"
                session.add(
                    ChannelBinding(
                        binding_id=f"binding-{suffix}",
                        tenant_id=tenant_id,
                        app_id="assistant",
                        app_revision=1,
                        config_revision=1,
                        channel_type="telegram",
                        external_account_id=f"external-{suffix}",
                        callback_path=f"/callback/{suffix}",
                        public_callback_id=f"public-{suffix}",
                        secret_refs={"bot_token": f"secret://env/TOKEN_{suffix.upper()}"},
                        status="active" if suffix == "a" else "disabled",
                    )
                )
        store = SqlChannelBindingStore(database.session_factory)
        binding = await store.load("tenant-a", "binding-a")
        assert binding.tenant_id == "tenant-a"
        assert binding.secret_refs == {"bot_token": "secret://env/TOKEN_A"}

        with pytest.raises(DeliveryConfigurationError, match="unavailable"):
            await store.load("tenant-a", "binding-b")
        with pytest.raises(DeliveryConfigurationError, match="unavailable"):
            await store.load("tenant-b", "binding-b")
        with pytest.raises(DeliveryConfigurationError, match="unavailable"):
            await store.load("", "binding-a")
        with pytest.raises(DeliveryConfigurationError, match="unavailable"):
            await store.load("tenant-a", "")
    finally:
        await database.dispose()

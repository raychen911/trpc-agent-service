"""Tenant discovery through the public route projection."""

from __future__ import annotations

from pathlib import Path

import pytest

from trpc_service.runtime.catalog import GovernedTenantCatalog, SqlActiveTenantCatalog
from trpc_service.storage import Database
from trpc_service.tenant import TenantConfigService, TenantSpec


def tenant_spec(tenant_id: str, *, enabled: bool, status: str = "active") -> TenantSpec:
    callback_id = f"callback-{tenant_id}"
    return TenantSpec.model_validate(
        {
            "tenant_id": tenant_id,
            "revision": 1,
            "display_name": tenant_id,
            "status": status,
            "apps": [
                {
                    "app_id": "assistant",
                    "revision": 1,
                    "name": "assistant_agent",
                    "prompt": "Answer from trusted context.",
                    "model": {"provider": "openai", "model": "gpt-4.1-mini"},
                }
            ],
            "channels": [
                {
                    "binding_id": f"binding-{tenant_id}",
                    "app_id": "assistant",
                    "app_revision": 1,
                    "channel": "telegram",
                    "external_account_id": f"bot-{tenant_id}",
                    "callback_path": f"/v1/channels/telegram/{callback_id}/callback",
                    "public_callback_id": callback_id,
                    "secret_refs": {
                        "webhook_secret": "secret://env/WEBHOOK_SECRET",
                        "bot_token": "secret://env/BOT_TOKEN",
                    },
                    "enabled": enabled,
                }
            ],
        }
    )


@pytest.mark.asyncio
async def test_catalog_lists_only_distinct_active_route_tenants(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'catalog.db'}")
    await database.create_schema()
    service = TenantConfigService(database.session_factory)
    try:
        await service.publish(tenant_spec("tenant-b", enabled=True), actor="test")
        await service.publish(tenant_spec("tenant-a", enabled=True), actor="test")
        await service.publish(tenant_spec("tenant-disabled", enabled=False), actor="test")

        catalog = SqlActiveTenantCatalog(database.session_factory)
        assert await catalog.list_active_tenant_ids() == ("tenant-a", "tenant-b")
    finally:
        await database.dispose()


@pytest.mark.asyncio
async def test_governed_catalog_applies_current_tenant_suspension(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'governed.db'}")
    await database.create_schema()
    service = TenantConfigService(database.session_factory)
    try:
        await service.publish(tenant_spec("tenant-live", enabled=True), actor="test")
        await service.publish(
            tenant_spec("tenant-suspended", enabled=True, status="suspended"),
            actor="test",
        )

        routes = SqlActiveTenantCatalog(database.session_factory)
        catalog = GovernedTenantCatalog(routes, service)

        assert await routes.list_active_tenant_ids() == (
            "tenant-live",
            "tenant-suspended",
        )
        assert await catalog.list_active_tenant_ids() == ("tenant-live",)
    finally:
        await database.dispose()

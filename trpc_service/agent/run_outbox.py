"""Standalone transactional Outbox relay process."""

from __future__ import annotations

import asyncio
import os
import socket
from typing import Optional

os.environ.setdefault("OTEL_SERVICE_NAME", "trpc-agent-outbox")

from trpc_service.channels import ChannelDeliveryTransport
from trpc_service.config import SecretResolver
from trpc_service.config import ServiceSettings
from trpc_service.config import resolve_secret
from trpc_service.log import install_redacting_log_filter
from trpc_service.messaging import OutboxRelay
from trpc_service.messaging import SqlMessageStore
from trpc_service.metrics import configure_telemetry
from trpc_service.metrics import shutdown_telemetry
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import build_tenant_config_manager
from trpc_service.tenant import load_tenants
from trpc_service.web.gateway import ChannelRegistry


def build_outbox_relay(
    *,
    manager: Optional[TenantConfigManager] = None,
    settings: Optional[ServiceSettings] = None,
) -> OutboxRelay:
    """Assemble only the dependencies needed for outbound delivery."""
    install_redacting_log_filter()
    configure_telemetry("trpc-agent-outbox")
    settings = settings or ServiceSettings.from_env()
    resolver = SecretResolver(file_root=settings.secret_file_root)
    mysql_url = resolve_secret(settings.mysql_url, resolver=resolver)
    redis_url = resolve_secret(settings.redis_url, resolver=resolver) or None
    if not mysql_url:
        raise ValueError("outbox requires TRPC_SERVICE_MYSQL_URL")
    owns_manager = manager is None
    manager = manager or build_tenant_config_manager(
        mysql_url=mysql_url,
        redis_url=redis_url,
        encryption_key=resolve_secret(settings.tenant_config_encryption_key, resolver=resolver) or None,
    )
    if settings.tenants_config:
        for tenant in load_tenants(settings.tenants_config):
            if manager.get(tenant.tenant_id) is None:
                manager.register(tenant, reason="bootstrap from TRPC_SERVICE_TENANTS_CONFIG")
    registry = ChannelRegistry(secret_resolver=resolver)
    manager.subscribe(lambda tenant_id, _tenant: registry.invalidate(tenant_id))
    owner = os.environ.get("TRPC_SERVICE_NODE_ID") or socket.gethostname()
    relay = OutboxRelay(
        store=SqlMessageStore(mysql_url),
        transport=ChannelDeliveryTransport(manager=manager, registry=registry),
        owner=f"outbox-{owner}",
        max_attempts=settings.outbox_max_attempts,
        owned_resources=[registry, manager if owns_manager else None],
    )
    return relay


async def run() -> None:
    """Run the relay until it is cancelled."""
    settings = ServiceSettings.from_env()
    relay = build_outbox_relay(settings=settings)
    try:
        await relay.run(settings.outbox_poll_interval_seconds)
    finally:
        await relay.close()
        shutdown_telemetry()


def main() -> None:
    """Synchronous console-script entry point."""
    asyncio.run(run())


if __name__ == "__main__":
    main()

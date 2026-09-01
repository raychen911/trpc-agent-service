"""Tenant-scoped SQL reader for outbound channel bindings."""

from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.delivery.contracts import (
    DeliveryBinding,
    DeliveryConfigurationError,
)
from trpc_service.storage.models import ChannelBinding


class SqlChannelBindingStore:
    """Read delivery configuration inside the same PostgreSQL RLS convention.

    No ORM row escapes the transaction.  The returned value contains secret
    *references* only; resolving their plaintext is a separate trust boundary.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        bind = session_factory.kw.get("bind")
        if bind is None:
            raise ValueError("session_factory must be bound to an engine")
        self._session_factory = session_factory
        self._dialect = bind.dialect.name

    async def load(self, tenant_id: str, binding_id: str) -> DeliveryBinding:
        """Load one live binding, always constrained by tenant and binding ids."""

        if not tenant_id or len(tenant_id) > 64:
            raise DeliveryConfigurationError("delivery binding is unavailable")
        if not binding_id or len(binding_id) > 64:
            raise DeliveryConfigurationError("delivery binding is unavailable")

        async with self._session_factory() as database, database.begin():
            if self._dialect == "postgresql":
                await database.execute(
                    text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
                    {"tenant_id": tenant_id},
                )
            binding = await database.scalar(
                select(ChannelBinding).where(
                    ChannelBinding.tenant_id == tenant_id,
                    ChannelBinding.binding_id == binding_id,
                )
            )
            if binding is None or binding.status != "active":
                raise DeliveryConfigurationError("delivery binding is unavailable")
            return DeliveryBinding(
                tenant_id=binding.tenant_id,
                binding_id=binding.binding_id,
                channel_type=binding.channel_type,
                status=binding.status,
                secret_refs=dict(binding.secret_refs),
            )

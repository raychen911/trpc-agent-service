"""Safe discovery of tenants that currently have an active ingress route."""

from __future__ import annotations

import logging
import re
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.storage.models import ChannelIngressRoute
from trpc_service.tenant.models import TenantSpec

_TENANT_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
LOGGER = logging.getLogger(__name__)


class TenantCatalog(Protocol):
    """List tenant identifiers without exposing tenant configuration or secrets."""

    async def list_active_tenant_ids(self) -> tuple[str, ...]:
        """Return a sorted, duplicate-free active snapshot."""


class SqlActiveTenantCatalog:
    """Discover tenants through the deliberately public ingress-route projection.

    PostgreSQL tenant tables use FORCE RLS, so an unscoped Worker must not bypass
    them to enumerate tenants. ``channel_ingress_route`` contains only opaque routing
    identifiers and is intentionally outside RLS for pre-tenant webhook routing.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        bind = session_factory.kw.get("bind")
        if bind is None:
            raise ValueError("session_factory must be bound to an engine")
        self._session_factory = session_factory

    async def list_active_tenant_ids(self) -> tuple[str, ...]:
        """Read a fresh active-route snapshot with deterministic ordering."""

        statement = (
            select(ChannelIngressRoute.tenant_id)
            .where(ChannelIngressRoute.status == "active")
            .distinct()
            .order_by(ChannelIngressRoute.tenant_id)
        )
        async with self._session_factory() as session:
            tenant_ids = tuple((await session.scalars(statement)).all())
        if any(_TENANT_ID.fullmatch(tenant_id) is None for tenant_id in tenant_ids):
            raise RuntimeError("tenant catalog returned an invalid identifier")
        return tenant_ids


class ActiveTenantSpecLoader(Protocol):
    """Read the current tenant-wide emergency-governance state."""

    async def load_active(self, tenant_id: str) -> TenantSpec:
        """Return the active immutable configuration for one scoped tenant."""


class GovernedTenantCatalog:
    """Filter public route discovery through current tenant suspension state.

    Inbox messages pin an immutable historical revision for deterministic execution,
    but a *current* tenant suspension is an emergency gate and must still prevent new
    claims and sends.  Loading status separately per tenant respects PostgreSQL RLS.
    """

    def __init__(self, routes: TenantCatalog, specs: ActiveTenantSpecLoader) -> None:
        self._routes = routes
        self._specs = specs

    async def list_active_tenant_ids(self) -> tuple[str, ...]:
        """Return only route tenants whose current specification is active."""

        active: list[str] = []
        for tenant_id in await self._routes.list_active_tenant_ids():
            try:
                spec = await self._specs.load_active(tenant_id)
            except Exception as error:
                LOGGER.warning(
                    "tenant_governance_lookup_failed",
                    extra={"error_type": type(error).__name__},
                )
                continue
            if spec.tenant_id != tenant_id:
                LOGGER.warning("tenant_governance_identity_mismatch")
                continue
            if spec.status == "active":
                active.append(tenant_id)
        return tuple(active)

"""Tenant-scoped database context.

The application never treats a tenant identifier as an optional query filter.
Every repository operation takes :class:`TenantContext`; PostgreSQL additionally
enforces the same boundary with ``SET LOCAL app.tenant_id`` and RLS.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported only by SQL deployments
    from sqlalchemy.ext.asyncio import AsyncSession


class TenantContextError(PermissionError):
    """Raised when an operation is attempted without a valid tenant scope."""


@dataclass(frozen=True, slots=True)
class TenantContext:
    """The tenant and caller identity bound to one unit of work.

    ``actor_id`` is deliberately metadata rather than an authorization decision.
    Authorization belongs in the platform filters; repositories only use this
    object to make accidental cross-tenant access impossible to express.
    """

    tenant_id: str
    actor_id: str = "system"
    request_id: str | None = None
    trace_id: str | None = None

    def __post_init__(self) -> None:
        if not self.tenant_id or not self.tenant_id.strip():
            raise TenantContextError("tenant_id is required")
        if self.tenant_id != self.tenant_id.strip():
            raise TenantContextError("tenant_id must not contain surrounding whitespace")
        if "\x00" in self.tenant_id:
            raise TenantContextError("tenant_id must not contain NUL")


_current_tenant: ContextVar[TenantContext | None] = ContextVar(
    "trpc_service_current_tenant", default=None
)


def current_tenant() -> TenantContext:
    """Return the tenant bound to the current coroutine or fail closed."""

    context = _current_tenant.get()
    if context is None:
        raise TenantContextError("a TenantContext is required")
    return context


def bind_tenant(context: TenantContext) -> Token[TenantContext | None]:
    """Bind a context for non-database helpers; always reset the returned token."""

    return _current_tenant.set(context)


def reset_tenant(token: Token[TenantContext | None]) -> None:
    _current_tenant.reset(token)


def require_tenant(context: TenantContext | None) -> TenantContext:
    if context is None:
        raise TenantContextError("a TenantContext is required")
    return context


@asynccontextmanager
async def tenant_transaction(
    session: AsyncSession, context: TenantContext
) -> AsyncIterator[AsyncSession]:
    """Open a SQL transaction with a transaction-local PostgreSQL tenant GUC.

    ``set_config(..., true)`` is intentionally used instead of ``SET``.  It is
    reset automatically by both COMMIT and ROLLBACK, so a pooled connection
    cannot leak one request's tenant into the next request.  The contextvar is
    similarly reset even if the caller raises.
    """

    # SQLAlchemy is kept local so the in-memory demonstration backend can run
    # without importing database drivers.
    from sqlalchemy import text

    require_tenant(context)
    token = bind_tenant(context)
    try:
        async with session.begin():
            # PostgreSQL accepts this parameterized form while rejecting a
            # session-level SET.  SQLite tests harmlessly skip the server GUC.
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                await session.execute(
                    text("SELECT pg_catalog.set_config('app.tenant_id', :tenant_id, true)"),
                    {"tenant_id": context.tenant_id},
                )
            yield session
    finally:
        reset_tenant(token)


async def assert_no_tenant_context(session: AsyncSession) -> None:
    """Fail if a checked-out PostgreSQL connection has a leaked tenant GUC.

    This defensive check belongs at pool checkout as well as in tests.  A
    transaction-local GUC should always be empty outside ``tenant_transaction``.
    """

    from sqlalchemy import text

    if session.bind is None or session.bind.dialect.name != "postgresql":
        return
    value = await session.scalar(text("SELECT current_setting('app.tenant_id', true)"))
    if value:
        raise TenantContextError("pooled database connection retained tenant context")

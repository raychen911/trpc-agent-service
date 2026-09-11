"""Async SQLAlchemy engine setup with tenant-context pool hygiene."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from .context import TenantContext, tenant_transaction


def create_async_engine_and_sessionmaker(
    database_url: str,
    *,
    echo: bool = False,
    pool_size: int = 10,
    max_overflow: int = 10,
) -> tuple[Any, Any]:
    """Create the platform's SQL engine and async session factory.

    PostgreSQL is the production backend.  SQLite URLs remain useful for small
    local adapter tests, but never provide the PostgreSQL RLS guarantee.
    """

    from sqlalchemy import event
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    options: dict[str, Any] = {"echo": echo, "pool_pre_ping": True}
    if not database_url.startswith("sqlite"):
        options.update(pool_size=pool_size, max_overflow=max_overflow)
    engine = create_async_engine(database_url, **options)

    if engine.dialect.name == "postgresql":
        # Roll back any abandoned transaction on checkout.  SET LOCAL is reset
        # by this rollback, which gives pooled connections a clean tenant scope.
        @event.listens_for(engine.sync_engine, "checkout")
        def _reset_checkout(dbapi_connection: Any, _connection_record: Any, _proxy: Any) -> None:
            try:
                dbapi_connection.rollback()
            except Exception as exc:  # pragma: no cover - driver-specific path
                raise RuntimeError("unable to reset pooled database connection") from exc

    return engine, async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@asynccontextmanager
async def open_tenant_session(session_factory: Any, context: TenantContext) -> AsyncIterator[Any]:
    """Yield an async session inside the only supported tenant transaction path."""

    async with session_factory() as session:
        async with tenant_transaction(session, context):
            yield session

"""Async SQLAlchemy engine lifecycle."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from trpc_service.storage.models import Base


@event.listens_for(Engine, "connect")
def _configure_sqlite_connection(dbapi_connection: object, connection_record: object) -> None:
    """Enable the safest practical SQLite settings for local contract tests.

    SQLite remains a single-host development backend. These pragmas improve local
    durability, but do not make it equivalent to PostgreSQL for multi-worker use.
    """

    del connection_record
    module_name = dbapi_connection.__class__.__module__
    if not module_name.startswith("sqlite3") and "aiosqlite" not in module_name:
        return
    cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.execute("PRAGMA synchronous=FULL")
    cursor.close()


class Database:
    """Own the engine and session factory; no global mutable session is shared."""

    def __init__(self, url: str, *, echo: bool = False) -> None:
        connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
        self.engine: AsyncEngine = create_async_engine(
            url,
            echo=echo,
            pool_pre_ping=True,
            connect_args=connect_args,
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            autoflush=False,
        )

    async def create_schema(self) -> None:
        """Create tables for local development.

        Production uses Alembic; this method exists for clean-room demos and tests.
        """

        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def drop_schema(self) -> None:
        """Drop all tables in an isolated test database only."""

        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)

    async def dispose(self) -> None:
        """Close pooled connections."""

        await self.engine.dispose()

    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield one unit-of-work session."""

        async with self.session_factory() as session:
            yield session

    @asynccontextmanager
    async def tenant_transaction(self, tenant_id: str) -> AsyncIterator[AsyncSession]:
        """Open a transaction scoped to one tenant, including PostgreSQL RLS."""

        if not tenant_id or len(tenant_id) > 64:
            raise ValueError("tenant_id must be a non-empty value of at most 64 characters")
        async with self.session_factory() as session, session.begin():
            if self.engine.dialect.name == "postgresql":
                await session.execute(
                    text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
                    {"tenant_id": tenant_id},
                )
            session.info["tenant_id"] = tenant_id
            yield session

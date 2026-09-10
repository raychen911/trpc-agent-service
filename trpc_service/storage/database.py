"""Asynchronous SQLAlchemy database lifecycle."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from trpc_service.storage.models import Base


class Database:
    """Owns the async engine and transaction-scoped sessions."""

    def __init__(self, database_url: str) -> None:
        url = make_url(database_url)
        if url.drivername not in {"sqlite+aiosqlite", "postgresql+asyncpg"}:
            raise ValueError("database URL must use sqlite+aiosqlite or postgresql+asyncpg")

        engine_options: dict[str, object] = {"pool_pre_ping": True}
        self.is_sqlite = url.drivername == "sqlite+aiosqlite"
        if self.is_sqlite:
            raw_path = database_url.removeprefix("sqlite+aiosqlite:///")
            if raw_path == ":memory:":
                engine_options.update(
                    poolclass=StaticPool,
                    connect_args={"check_same_thread": False},
                )
            else:
                Path(raw_path).parent.mkdir(parents=True, exist_ok=True)

        self.engine: AsyncEngine = create_async_engine(database_url, **engine_options)
        self.database_url = database_url
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            autoflush=False,
        )

        if self.is_sqlite:

            @event.listens_for(self.engine.sync_engine, "connect")
            def enable_sqlite_foreign_keys(dbapi_connection: object, _: object) -> None:
                cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            if not self.is_sqlite:
                await connection.execute(text("SELECT pg_advisory_xact_lock(73492841)"))
            await connection.run_sync(Base.metadata.create_all)
            if not self.is_sqlite:
                return
            columns = await connection.execute(text("PRAGMA table_info(channel_binding)"))
            if "app_id" not in {row[1] for row in columns}:
                await connection.execute(
                    text(
                        "ALTER TABLE channel_binding ADD COLUMN app_id VARCHAR(64) "
                        "NOT NULL DEFAULT 'assistant'"
                    )
                )
            tenant_columns = await connection.execute(text("PRAGMA table_info(tenant)"))
            if "storage_config" not in {row[1] for row in tenant_columns}:
                await connection.execute(
                    text("ALTER TABLE tenant ADD COLUMN storage_config JSON NOT NULL DEFAULT '{}'")
                )
            columns = await connection.execute(text("PRAGMA table_info(channel_binding)"))
            if "connection_mode" not in {row[1] for row in columns}:
                await connection.execute(
                    text(
                        "ALTER TABLE channel_binding ADD COLUMN connection_mode VARCHAR(32) "
                        "NOT NULL DEFAULT 'webhook'"
                    )
                )

    async def dispose(self) -> None:
        await self.engine.dispose()

    async def ping(self) -> bool:
        try:
            async with self.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise


__all__ = ["Database"]

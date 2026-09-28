"""Async database engine, session factory, and FastAPI dependency helpers."""

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from trpc_service.config import Settings


def build_engine(settings: Settings) -> AsyncEngine:
    """Create an async engine and verify pooled connections before reuse."""

    return configured_engine(settings.resolved_database_url, settings)


def configured_engine(url: URL, settings: Settings) -> AsyncEngine:
    if url.get_backend_name() == "sqlite":
        return create_async_engine(url, pool_pre_ping=True)
    return create_async_engine(
        url,
        pool_pre_ping=True,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout,
        connect_args={"command_timeout": 30},
    )


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create sessions that retain loaded state after a successful commit."""

    return async_sessionmaker(engine, expire_on_commit=False)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Yield one request-scoped session and always close it afterwards."""

    session_factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with session_factory() as session:
        yield session

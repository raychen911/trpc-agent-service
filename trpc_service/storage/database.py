"""Database engine lifecycle: URL parsing, pool creation, readiness check."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping

from sqlalchemy import text
from sqlalchemy.exc import ArgumentError
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine as _sa_create_async_engine

_REQUIRED_DRIVER = "postgresql+asyncpg"


class DatabaseConfigurationError(ValueError):
    """Raised when database URL or pool settings are invalid."""


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """Non-secret database connection settings parsed from environment."""

    url: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    connect_timeout: int = 10

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> DatabaseSettings:
        import os

        values = os.environ if environ is None else environ
        raw_url = values.get("TRPC_DATABASE_URL", "").strip()
        if not raw_url:
            raise DatabaseConfigurationError("TRPC_DATABASE_URL is required")

        try:
            parsed = make_url(raw_url)
        except (ArgumentError, ValueError):
            # SQLAlchemy parser messages are not a stable public boundary and
            # may include a DSN supplied by the caller.  Never retain them in
            # the exception chain that application logging can render.
            raise DatabaseConfigurationError("TRPC_DATABASE_URL is not a valid database URL") from None

        if parsed.drivername != _REQUIRED_DRIVER:
            raise DatabaseConfigurationError(f"TRPC_DATABASE_URL must use driver '{_REQUIRED_DRIVER}'")

        if not parsed.host:
            raise DatabaseConfigurationError("TRPC_DATABASE_URL must include a hostname")

        if not parsed.database:
            raise DatabaseConfigurationError("TRPC_DATABASE_URL must include a database name")

        pool_size = _parse_positive_int(values, "TRPC_DATABASE_POOL_SIZE", default=5)
        max_overflow = _parse_int(values, "TRPC_DATABASE_MAX_OVERFLOW", default=10)
        pool_timeout = _parse_positive_int(values, "TRPC_DATABASE_POOL_TIMEOUT", default=30)
        connect_timeout = _parse_positive_int(values, "TRPC_DATABASE_CONNECT_TIMEOUT", default=10)

        return cls(
            url=raw_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            connect_timeout=connect_timeout,
        )


def create_database_engine(settings: DatabaseSettings) -> AsyncEngine:
    """Create an async engine from validated settings."""
    return _sa_create_async_engine(
        settings.url,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_timeout=settings.pool_timeout,
        connect_args={"timeout": settings.connect_timeout},
    )


async def check_database_readiness(engine: AsyncEngine) -> None:
    """Execute ``SELECT 1`` to verify the database is reachable.

    Raises the underlying driver exception on failure.
    """
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


_PASSWORD_RE = re.compile(r"(://[^:]+:)[^@]+(@)")


def mask_database_url(url: str) -> str:
    """Return a copy of *url* with the password replaced by ``***``."""
    return _PASSWORD_RE.sub(r"\1***\2", url)


def _parse_positive_int(
    environ: Mapping[str, str],
    key: str,
    *,
    default: int,
) -> int:
    raw = environ.get(key, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise DatabaseConfigurationError(f"{key} must be an integer") from None
    if value <= 0:
        raise DatabaseConfigurationError(f"{key} must be positive")
    return value


def _parse_int(
    environ: Mapping[str, str],
    key: str,
    *,
    default: int,
) -> int:
    raw = environ.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise DatabaseConfigurationError(f"{key} must be an integer") from None


__all__ = [
    "DatabaseConfigurationError",
    "DatabaseSettings",
    "check_database_readiness",
    "create_database_engine",
    "mask_database_url",
]

"""Agent state backends for shared Session and Memory services (Redis, SQL)."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Protocol
from urllib.parse import urlsplit

from trpc_agent_sdk.memory import (
    BaseMemoryService,
    MemoryServiceConfig,
    RedisMemoryService,
    SqlMemoryService,
)
from trpc_agent_sdk.sessions import (
    BaseSessionService,
    RedisSessionService,
    SessionServiceConfig,
    SqlSessionService,
)
from trpc_agent_sdk.types import Ttl

from trpc_service.storage.database import DatabaseConfigurationError, DatabaseSettings

logger = logging.getLogger(__name__)

# SQL default semantics (SDK forces store_historical_events=True when no
# session_config is given); kept explicit so the TTL override cannot silently
# turn event persistence off.
_SQL_STORE_HISTORICAL_EVENTS = True


class StateBackendConfigurationError(Exception):
    """Raised when state backend configuration is invalid."""


class AgentStateBackend(Protocol):
    """Protocol for agent state backends providing Session and Memory services."""

    @property
    def session_service(self) -> object:
        """Return the session service instance."""
        ...

    @property
    def memory_service(self) -> object:
        """Return the memory service instance."""
        ...

    def check_ready(self) -> None:
        """Check if the backend is ready to serve requests.

        Raises StateBackendConfigurationError if not ready.
        """
        ...

    async def close(self) -> None:
        """Close the backend and release resources.

        Idempotent and safe to call multiple times.
        """
        ...


class RedisStateBackend:
    """Redis-backed state backend using SDK native services."""

    def __init__(
        self,
        redis_url: str,
        session_service: RedisSessionService,
        memory_service: RedisMemoryService,
        session_ttl: int,
        memory_ttl: int,
    ) -> None:
        self._redis_url = redis_url
        self._session_service = session_service
        self._memory_service = memory_service
        self._session_ttl = session_ttl
        self._memory_ttl = memory_ttl
        self._closed = False

    @property
    def redis_url(self) -> str:
        """Return the Redis URL (for testing only, not logged)."""
        return self._redis_url

    @property
    def session_service(self) -> RedisSessionService:
        """Return the session service instance."""
        return self._session_service

    @property
    def memory_service(self) -> RedisMemoryService:
        """Return the memory service instance."""
        return self._memory_service

    @property
    def session_ttl(self) -> int:
        """Return the session TTL in seconds."""
        return self._session_ttl

    @property
    def memory_ttl(self) -> int:
        """Return the memory TTL in seconds."""
        return self._memory_ttl

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> RedisStateBackend:
        """Create a RedisStateBackend from environment variables.

        Args:
            environ: Environment variables mapping. Defaults to os.environ.

        Returns:
            RedisStateBackend instance.

        Raises:
            StateBackendConfigurationError: If configuration is invalid.
        """
        import os

        if environ is None:
            environ = os.environ

        # Validate Redis URL
        redis_url = environ.get("TRPC_REDIS_URL", "").strip()
        if not redis_url:
            raise StateBackendConfigurationError("TRPC_REDIS_URL is required but not set")

        parsed = urlsplit(redis_url)
        if parsed.scheme not in ("redis", "rediss"):
            raise StateBackendConfigurationError(
                f"TRPC_REDIS_URL scheme must be redis or rediss, got {parsed.scheme!r}")
        if not parsed.hostname:
            raise StateBackendConfigurationError("TRPC_REDIS_URL must include a hostname")

        # Validate and parse session TTL
        session_ttl_str = environ.get("TRPC_REDIS_SESSION_TTL_SECONDS", "").strip()
        if session_ttl_str:
            try:
                session_ttl = int(session_ttl_str)
                if session_ttl <= 0:
                    raise ValueError("must be positive")
            except ValueError as e:
                raise StateBackendConfigurationError(
                    f"TRPC_REDIS_SESSION_TTL_SECONDS must be a positive integer, got {session_ttl_str!r}: {e}"
                ) from None
        else:
            session_ttl = 604800  # 7 days default

        # Validate and parse memory TTL
        memory_ttl_str = environ.get("TRPC_REDIS_MEMORY_TTL_SECONDS", "").strip()
        if memory_ttl_str:
            try:
                memory_ttl = int(memory_ttl_str)
                if memory_ttl <= 0:
                    raise ValueError("must be positive")
            except ValueError as e:
                raise StateBackendConfigurationError(
                    f"TRPC_REDIS_MEMORY_TTL_SECONDS must be a positive integer, got {memory_ttl_str!r}: {e}") from None
        else:
            memory_ttl = 2592000  # 30 days default

        # Create SDK services.  store_historical_events mirrors the SQL
        # backend (R1B): a Session Summary must never replace raw active
        # events without the SDK first moving them into
        # Session.historical_events, and the Redis service only persists that
        # field when the flag is on (see durable-summary acceptance in R1B).
        session_config = SessionServiceConfig(
            ttl=Ttl(enable=True, ttl_seconds=session_ttl),
            store_historical_events=True,
        )
        session_service = RedisSessionService(
            db_url=redis_url,
            session_config=session_config,
        )

        memory_config = MemoryServiceConfig(
            enabled=True,
            ttl=Ttl(enable=True, ttl_seconds=memory_ttl),
        )
        memory_service = RedisMemoryService(
            db_url=redis_url,
            enabled=True,
            memory_service_config=memory_config,
        )

        return cls(
            redis_url=redis_url,
            session_service=session_service,
            memory_service=memory_service,
            session_ttl=session_ttl,
            memory_ttl=memory_ttl,
        )

    def check_ready(self) -> None:
        """Check if the backend is ready to serve requests.

        Raises StateBackendConfigurationError if not ready.
        """
        if self._closed:
            raise StateBackendConfigurationError("Backend is closed")

        try:
            import redis
            r = redis.from_url(self._redis_url, socket_timeout=5.0, socket_connect_timeout=5.0)
            r.ping()
            r.close()
        except Exception:
            # Sanitize error message to avoid leaking sensitive info
            raise StateBackendConfigurationError("Redis is not ready") from None

    async def close(self) -> None:
        """Close the backend and release resources.

        Idempotent and safe to call multiple times.
        """
        if self._closed:
            return

        self._closed = True

        # Close services (ignore errors during shutdown)
        try:
            await self._session_service.close()
        except Exception:
            logger.warning("Failed to close session service", exc_info=True)

        try:
            await self._memory_service.close()
        except Exception:
            logger.warning("Failed to close memory service", exc_info=True)


class _Sdk1116GetSessionFix(SqlSessionService):
    """Narrow product-side fix for an upstream SDK defect (R1A, controller-approved).

    Upstream fact (SDK <= 1.1.20, reproduced by the controller against the
    installed editable SDK): ``SqlSessionService.get_session`` lets its
    ``_get_session`` helper write ``update_time = func.now()`` and commit
    without materializing the server-generated value, so the very next
    attribute read on the same expired ORM row triggers a lazy IO from outside
    a greenlet and raises ``MissingGreenlet`` under ``is_async=True`` +
    PostgreSQL.  Sibling methods (``create_session``/``append_event``/
    ``update_session``) all end their commit with
    ``await self._sql_storage.refresh(...)``; ``get_session`` is simply
    missing that call — an SDK oversight, not a usage error.

    This subclass overrides ONLY ``_get_session`` and adds the same
    refresh-after-commit the siblings already do.  Combined with the public
    ``expire_on_commit=False`` kwarg (the SDK's own documented SqlStorage
    option), no other attribute in the ``get_session`` flow is left expired
    by the app/user-state commits that follow.  No get_session body is copied,
    no tables or pools are touched; the single reused internal is the exact
    public ``refresh`` call pattern the same SDK file already performs.
    """

    async def _get_session(self, sql_session, app_name: str, user_id: str, session_id: str):
        storage_session = await super()._get_session(sql_session, app_name, user_id, session_id)
        if storage_session is not None:
            # Materialize the func.now() write committed by the parent before
            # any downstream attribute access (state/historical_events).
            await self._sql_storage.refresh(sql_session, storage_session)
        return storage_session


def _parse_sql_ttl(environ: Mapping[str, str], key: str, *, default: int) -> int:
    """Same TTL policy as the Redis backend: unset/blank -> default, >0."""
    raw = environ.get(key, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        if value <= 0:
            raise ValueError("must be positive")
    except ValueError:
        raise StateBackendConfigurationError(f"{key} must be a positive integer, got {raw!r}") from None
    return value


class SqlStateBackend:
    """SQL (PostgreSQL)-backed state backend using SDK native services (R1A).

    Reuses the SDK public ``SqlSessionService``/``SqlMemoryService`` in async
    mode.  Like the Redis backend, engine pools are owned here and released
    exactly once by ``close()``.  ``check_ready()`` deliberately performs only
    closed/config checks (sync, no network): real SQL reachability is covered
    by the Worker lifespan repository check and fail-closed request paths.
    Every configuration failure maps onto a fixed, sanitized
    ``StateBackendConfigurationError`` — no DSN or upstream text ever escapes.
    """

    _FIXED_URL_ERROR = "TRPC_DATABASE_URL configuration is invalid"
    _FIXED_CONSTRUCTION_ERROR = "SQL state backend services could not be constructed"

    def __init__(
        self,
        db_url: str,
        session_service: BaseSessionService,
        memory_service: BaseMemoryService,
        session_ttl: int,
        memory_ttl: int,
    ) -> None:
        self._db_url = db_url
        self._session_service = session_service
        self._memory_service = memory_service
        self._session_ttl = session_ttl
        self._memory_ttl = memory_ttl
        self._closed = False

    @property
    def db_url(self) -> str:
        """Return the database URL (for testing only, never logged)."""
        return self._db_url

    @property
    def session_service(self) -> BaseSessionService:
        """Return the session service instance."""
        return self._session_service

    @property
    def memory_service(self) -> BaseMemoryService:
        """Return the memory service instance."""
        return self._memory_service

    @property
    def session_ttl(self) -> int:
        """Return the session TTL in seconds."""
        return self._session_ttl

    @property
    def memory_ttl(self) -> int:
        """Return the memory TTL in seconds."""
        return self._memory_ttl

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> SqlStateBackend:
        """Create a SqlStateBackend from environment variables.

        Reads ``TRPC_DATABASE_URL`` (must be ``postgresql+asyncpg``, validated
        through ``DatabaseSettings.from_env``), plus optional
        ``TRPC_SQL_SESSION_TTL_SECONDS`` / ``TRPC_SQL_MEMORY_TTL_SECONDS``
        (same defaults and policy as the Redis backend).

        Raises:
            StateBackendConfigurationError: If configuration is invalid; the
                message is always fixed text — never the URL or an upstream
                exception string.
        """
        import os

        if environ is None:
            environ = os.environ

        try:
            settings = DatabaseSettings.from_env(environ)
        except DatabaseConfigurationError:
            raise StateBackendConfigurationError(cls._FIXED_URL_ERROR) from None

        session_ttl = _parse_sql_ttl(environ, "TRPC_SQL_SESSION_TTL_SECONDS", default=604800)
        memory_ttl = _parse_sql_ttl(environ, "TRPC_SQL_MEMORY_TTL_SECONDS", default=2592000)

        try:
            # expire_on_commit=False is the SDK's public SqlStorage option;
            # with the _get_session refresh above it closes the asyncpg
            # MissingGreenlet read-path defect (see _Sdk1116GetSessionFix).
            session_service = _Sdk1116GetSessionFix(
                db_url=settings.url,
                is_async=True,
                expire_on_commit=False,
                session_config=SessionServiceConfig(
                    ttl=Ttl(enable=True, ttl_seconds=session_ttl),
                    store_historical_events=_SQL_STORE_HISTORICAL_EVENTS,
                ),
            )
        except Exception:
            raise StateBackendConfigurationError(cls._FIXED_CONSTRUCTION_ERROR) from None

        try:
            memory_service = SqlMemoryService(
                db_url=settings.url,
                is_async=True,
                enabled=True,
                memory_service_config=MemoryServiceConfig(
                    enabled=True,
                    ttl=Ttl(enable=True, ttl_seconds=memory_ttl),
                ),
            )
        except Exception:
            # Partial-construction rollback: release the already-built session
            # service best-effort; never let its error text escape either.
            try:
                import asyncio

                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(session_service.close())
                finally:
                    loop.close()
            except Exception:
                logger.warning("Failed to roll back SQL session service after memory service failure", exc_info=True)
            raise StateBackendConfigurationError(cls._FIXED_CONSTRUCTION_ERROR) from None

        return cls(
            db_url=settings.url,
            session_service=session_service,
            memory_service=memory_service,
            session_ttl=session_ttl,
            memory_ttl=memory_ttl,
        )

    def check_ready(self) -> None:
        """Check that the backend is configured and open (no network probe).

        Raises StateBackendConfigurationError if closed.
        """
        if self._closed:
            raise StateBackendConfigurationError("Backend is closed")

    async def close(self) -> None:
        """Close the backend and release resources.

        Idempotent and safe to call multiple times.
        """
        if self._closed:
            return

        self._closed = True

        # Close services (ignore errors during shutdown)
        try:
            await self._session_service.close()
        except Exception:
            logger.warning("Failed to close SQL session service", exc_info=True)

        try:
            await self._memory_service.close()
        except Exception:
            logger.warning("Failed to close SQL memory service", exc_info=True)


__all__ = [
    "AgentStateBackend",
    "RedisStateBackend",
    "SqlStateBackend",
    "StateBackendConfigurationError",
]

"""Per-tenant state backend selection for one Worker (Stage R1A).

A Worker owns EXACTLY one Redis and one SQL state backend for its lifetime;
each tenant's versioned ``TenantBackendProfile.state_backend`` picks one.
Selection is exact — there is no fallback to the other backend, so a broken
chosen backend fails the request closed instead of silently changing where
session data lives.  The Redis execution coordinator stays mandatory for
both choices (worker wiring unchanged), so same-Session execution remains
serialized across Workers regardless of the selected backend.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

from trpc_service.storage.state_backend import (
    AgentStateBackend,
    RedisStateBackend,
    SqlStateBackend,
    StateBackendConfigurationError,
)
from trpc_service.config.tenant import TenantBackendProfile

logger = logging.getLogger(__name__)


class TenantStateBackendResolver:
    """Owns the Worker's Redis and SQL backends and selects per tenant."""

    def __init__(
        self,
        redis_backend: RedisStateBackend,
        sql_backend: SqlStateBackend,
    ) -> None:
        self._redis_backend = redis_backend
        self._sql_backend = sql_backend
        self._closed = False

    @property
    def redis_backend(self) -> RedisStateBackend:
        """Return the owned Redis state backend."""
        return self._redis_backend

    @property
    def sql_backend(self) -> SqlStateBackend:
        """Return the owned SQL state backend."""
        return self._sql_backend

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> TenantStateBackendResolver:
        """Construct exactly one Redis and one SQL backend from the environment.

        Both must configure successfully; a Worker that cannot build either
        backend cannot honor every tenant profile, so startup fails closed.
        """
        return cls(
            redis_backend=RedisStateBackend.from_env(environ),
            sql_backend=SqlStateBackend.from_env(environ),
        )

    def resolve(self, profile: TenantBackendProfile) -> AgentStateBackend:
        """Return the already-owned backend the profile selects — never the
        other one, even if the selected backend is closed or unhealthy."""
        if profile.state_backend == "redis":
            return self._redis_backend
        if profile.state_backend == "sql":
            return self._sql_backend
        # The strict TenantBackendProfile model makes this unreachable; fail
        # closed instead of guessing if a foreign object is passed in.
        raise StateBackendConfigurationError("unknown state_backend in tenant profile")

    def check_ready(self) -> None:
        """Synchronous readiness gate, same as the pre-R1A Worker behavior:
        ping the shared Redis backend (it backs the mandatory coordinator and
        Redis-state tenants).  SQL reachability is verified by the repository
        readiness checks in the Worker lifespan and fails closed per request.
        """
        self._redis_backend.check_ready()

    async def close(self) -> None:
        """Close both owned backends exactly once.  Idempotent.

        Runtime retirement never closes backend services through the Runner
        (``close_session_service_on_close=False``), so this Worker-level close
        is the single owner of both backends' pools.
        """
        if self._closed:
            return
        self._closed = True

        try:
            await self._redis_backend.close()
        except Exception:
            logger.warning("Failed to close Redis state backend", exc_info=True)

        try:
            await self._sql_backend.close()
        except Exception:
            logger.warning("Failed to close SQL state backend", exc_info=True)


__all__ = ["TenantStateBackendResolver"]

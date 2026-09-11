"""R1A tests: SqlStateBackend — SDK SQL services, async mode, sanitized config.

The backend must reuse the SDK PUBLIC SqlSessionService/SqlMemoryService with
``is_async=True`` (memory also ``enabled=True``), keep the SDK SQL default of
``store_historical_events=True``, mirror the Redis TTL policy, and map every
malformed/unavailable configuration onto fixed
``StateBackendConfigurationError`` messages with ``from None`` — no DSN, URL,
or upstream exception text may escape.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import patch

import pytest

from trpc_agent_sdk.memory import BaseMemoryService, SqlMemoryService
from trpc_agent_sdk.sessions import BaseSessionService, SqlSessionService

from trpc_service.storage.state_backend import (
    SqlStateBackend,
    StateBackendConfigurationError,
)

VALID_URL = "postgresql+asyncpg://user:secret@db.internal:5432/trpc"


def _env(**overrides) -> dict[str, str]:
    env = {"TRPC_DATABASE_URL": VALID_URL}
    env.update(overrides)
    return env


class _FakeService:
    """Async-closeable double standing in for an SDK service."""

    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


@pytest.fixture()
def fake_services():
    """Patch the SDK SQL service classes with Mocks returning doubles.

    The session class is the R1A product-side subclass (fixes the upstream
    asyncpg get_session defect) — patching it proves from_env constructs the
    SUBCLASS, not a raw SqlSessionService.
    """
    with patch("trpc_service.storage.state_backend._Sdk1116GetSessionFix") as session_cls, \
            patch("trpc_service.storage.state_backend.SqlMemoryService") as memory_cls:
        session_cls.return_value = _FakeService()
        memory_cls.return_value = _FakeService()
        yield session_cls, memory_cls


class TestSdk1116GetSessionFix:
    """The narrow subclass: refresh-after-commit, nothing else."""

    def test_is_public_sql_session_subclass(self):
        from trpc_service.storage.state_backend import _Sdk1116GetSessionFix
        assert issubclass(_Sdk1116GetSessionFix, SqlSessionService)

    @pytest.mark.asyncio
    async def test_get_session_override_refreshes_hit(self):
        from trpc_service.storage.state_backend import _Sdk1116GetSessionFix

        obj = object()
        refreshed = []

        class _Storage:

            async def refresh(self, db, data):
                refreshed.append((db, data))

        svc = object.__new__(_Sdk1116GetSessionFix)
        svc._sql_storage = _Storage()
        with patch.object(SqlSessionService, "_get_session", return_value=obj):
            result = await _Sdk1116GetSessionFix._get_session(svc, "SESSION", "app", "user", "s1")
        assert result is obj
        assert refreshed == [("SESSION", obj)]

    @pytest.mark.asyncio
    async def test_get_session_override_skips_miss(self):
        from trpc_service.storage.state_backend import _Sdk1116GetSessionFix

        refreshed = []

        class _Storage:

            async def refresh(self, db, data):
                refreshed.append(data)

        svc = object.__new__(_Sdk1116GetSessionFix)
        svc._sql_storage = _Storage()
        with patch.object(SqlSessionService, "_get_session", return_value=None):
            result = await _Sdk1116GetSessionFix._get_session(svc, "SESSION", "app", "user", "s1")
        assert result is None
        assert refreshed == []

    @pytest.mark.asyncio
    async def test_from_env_constructs_fix_subclass_service(self):
        """Real construction against a lazy (never-connected) URL: proves the
        session service IS the fix subclass and the public options are set."""
        backend = SqlStateBackend.from_env(_env())
        try:
            from trpc_service.storage.state_backend import _Sdk1116GetSessionFix
            assert isinstance(backend.session_service, _Sdk1116GetSessionFix)
            assert isinstance(backend.session_service, SqlSessionService)
            assert isinstance(backend.memory_service, SqlMemoryService)
            assert backend.session_ttl == 604800
            assert backend.memory_ttl == 2592000
        finally:
            await backend.close()

    def test_redis_backend_untouched(self):
        """The fix must not leak into the Redis path."""
        from trpc_service.storage.state_backend import RedisStateBackend
        assert issubclass(RedisStateBackend, object)
        assert RedisStateBackend.__mro__[1] is object
        import inspect
        src = inspect.getsource(RedisStateBackend)
        assert "_Sdk1116GetSessionFix" not in src


class TestSqlStateBackendFromEnvConfig:

    @pytest.mark.parametrize(
        "env",
        [
            {},
            {
                "TRPC_DATABASE_URL": ""
            },
            {
                "TRPC_DATABASE_URL": "   "
            },
            {
                "TRPC_DATABASE_URL": "not a url at all"
            },
            {
                "TRPC_DATABASE_URL": "postgresql://user:secret@h/db"
            },
            {
                "TRPC_DATABASE_URL": "sqlite+aiosqlite:///local.db"
            },
            {
                "TRPC_DATABASE_URL": "mysql+asyncmy://user:secret@h/db"
            },
            {
                "TRPC_DATABASE_URL": "postgresql+asyncpg:///no-host-db"
            },
            {
                "TRPC_DATABASE_URL": "postgresql+asyncpg://user:secret@h"
            },
        ],
    )
    def test_malformed_config_raises_fixed_error(self, env):
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            SqlStateBackend.from_env(env)
        msg = str(exc_info.value)
        # Fixed message only: no URL fragment, no driver, no host, no user,
        # no password, no port can escape.
        assert "secret" not in msg
        assert "user" not in msg
        assert "db.internal" not in msg
        assert "5432" not in msg
        assert "postgresql" not in msg
        assert "sqlite" not in msg
        assert "mysql" not in msg

    def test_missing_error_has_no_cause_chain(self):
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            SqlStateBackend.from_env({})
        # `from None`: no cause and the original context is suppressed, so a
        # traceback can never render the upstream DatabaseConfigurationError.
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__suppress_context__ is True

    @pytest.mark.parametrize(
        "key",
        ["TRPC_SQL_SESSION_TTL_SECONDS", "TRPC_SQL_MEMORY_TTL_SECONDS"],
    )
    @pytest.mark.parametrize("bad", ["abc", "0", "-5", "1.5"])
    def test_ttl_must_be_positive_int(self, key, bad, fake_services):
        with pytest.raises(StateBackendConfigurationError) as exc_info:
            SqlStateBackend.from_env(_env(**{key: bad}))
        assert key in str(exc_info.value)

    def test_valid_config_constructs_backend(self, fake_services):
        backend = SqlStateBackend.from_env(_env())
        try:
            assert isinstance(backend.session_service, _FakeService)
            assert isinstance(backend.memory_service, _FakeService)
        finally:
            asyncio.run(backend.close())

    def test_sdk_sql_services_used_with_async_mode(self, fake_services):
        session_cls, memory_cls = fake_services
        backend = SqlStateBackend.from_env(_env())
        kwargs = session_cls.call_args.kwargs
        assert kwargs["db_url"] == VALID_URL
        assert kwargs["is_async"] is True
        # R1A adapter: public SqlStorage option that keeps the get_session
        # flow free of post-commit expiry (see _Sdk1116GetSessionFix).
        assert kwargs["expire_on_commit"] is False
        memory_kwargs = memory_cls.call_args.kwargs
        assert memory_kwargs["db_url"] == VALID_URL
        assert memory_kwargs["is_async"] is True
        assert memory_kwargs["enabled"] is True
        assert backend.session_service is session_cls.return_value
        assert backend.memory_service is memory_cls.return_value

    def test_ttl_defaults_match_redis_backend(self, fake_services):
        backend = SqlStateBackend.from_env(_env())
        assert backend.session_ttl == 604800
        assert backend.memory_ttl == 2592000

    def test_ttl_env_overrides_flow_into_sdk_configs(self, fake_services):
        session_cls, memory_cls = fake_services
        backend = SqlStateBackend.from_env(
            _env(
                TRPC_SQL_SESSION_TTL_SECONDS="3600",
                TRPC_SQL_MEMORY_TTL_SECONDS="7200",
            ))
        session_config = session_cls.call_args.kwargs["session_config"]
        assert session_config.ttl.enable is True
        assert session_config.ttl.ttl_seconds == 3600
        memory_config = memory_cls.call_args.kwargs["memory_service_config"]
        assert memory_config.enabled is True
        assert memory_config.ttl.enable is True
        assert memory_config.ttl.ttl_seconds == 7200
        assert backend.session_ttl == 3600
        assert backend.memory_ttl == 7200

    def test_store_historical_events_stays_true(self, fake_services):
        """SQL default semantics: explicit TTL config must not silently turn
        the SDK SQL default (store_historical_events=True) off."""
        session_cls, _ = fake_services
        SqlStateBackend.from_env(_env())
        session_config = session_cls.call_args.kwargs["session_config"]
        assert session_config.store_historical_events is True

    def test_service_construction_failure_is_sanitized(self):
        with patch(
                "trpc_service.storage.state_backend.SqlSessionService",
                side_effect=RuntimeError(f"cannot connect {VALID_URL}"),
        ):
            with pytest.raises(StateBackendConfigurationError) as exc_info:
                SqlStateBackend.from_env(_env())
        msg = str(exc_info.value)
        assert "secret" not in msg
        assert "postgresql" not in msg
        assert "cannot connect" not in msg
        assert exc_info.value.__cause__ is None

    def test_properties_expose_sdk_base_types(self, fake_services):
        backend = SqlStateBackend.from_env(_env())
        # The real SDK classes subclass the public base services.
        assert issubclass(SqlSessionService, BaseSessionService)
        assert issubclass(SqlMemoryService, BaseMemoryService)
        assert isinstance(backend.session_service, BaseSessionService) or callable(backend.session_service.close)
        assert isinstance(backend.memory_service, BaseMemoryService) or callable(backend.memory_service.close)
        asyncio.run(backend.close())


class TestSqlStateBackendLifecycle:

    def test_satisfies_agent_state_backend_protocol(self):
        assert hasattr(SqlStateBackend, "session_service")
        assert hasattr(SqlStateBackend, "memory_service")
        assert hasattr(SqlStateBackend, "check_ready")
        assert hasattr(SqlStateBackend, "close")

    def test_check_ready_is_sync_config_only(self, fake_services):
        """No real SQL reachability at construction time: Worker lifespan
        (repository) and the request path cover it. check_ready must not
        touch the services or the network."""
        backend = SqlStateBackend.from_env(_env())
        backend.check_ready()  # must not raise against fake services

    def test_check_ready_after_close_raises(self, fake_services):
        backend = SqlStateBackend.from_env(_env())
        asyncio.run(backend.close())
        with pytest.raises(StateBackendConfigurationError):
            backend.check_ready()

    def test_close_is_idempotent_and_calls_each_service_once(self, fake_services):
        backend = SqlStateBackend.from_env(_env())
        session = backend.session_service
        memory = backend.memory_service
        asyncio.run(backend.close())
        asyncio.run(backend.close())
        asyncio.run(backend.close())
        assert session.close_calls == 1
        assert memory.close_calls == 1

    def test_close_swallows_service_errors_with_warning(self, fake_services, caplog):
        backend = SqlStateBackend.from_env(_env())

        class Boom(_FakeService):

            async def close(self) -> None:
                raise RuntimeError("close failed")

        backend._session_service = Boom()
        with caplog.at_level(logging.WARNING, logger="trpc_service.storage.state_backend"):
            asyncio.run(backend.close())
        # memory service (constructed second) still closed despite the failure
        assert backend.memory_service.close_calls == 1
        # Redis-style: fixed warning message only, the close never raises
        assert any(r.levelname == "WARNING" for r in caplog.records)
        assert not any("Traceback" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_close_is_async(self, fake_services):
        backend = SqlStateBackend.from_env(_env())
        result = backend.close()
        assert asyncio.iscoroutine(result)
        await result

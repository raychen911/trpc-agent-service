"""R1A tests: TenantStateBackendResolver — exact selection, ownership, no fallback.

Each Worker owns EXACTLY one Redis backend and one SQL backend; selection is
driven purely by the tenant profile and must never fall back to the other
backend.  The Redis execution coordinator stays mandatory for both choices
(worker wiring covered separately), and close() releases each owned backend
exactly once, idempotently.
"""

from __future__ import annotations

import asyncio
from unittest.mock import Mock, patch

import pytest

from trpc_service.storage.backend_resolver import TenantStateBackendResolver
from trpc_service.storage.state_backend import (
    AgentStateBackend,
    RedisStateBackend,
    SqlStateBackend,
    StateBackendConfigurationError,
)
from trpc_service.config.tenant import TenantBackendProfile


def _profile(state_backend: str) -> TenantBackendProfile:
    return TenantBackendProfile(
        state_backend=state_backend,
        artifact_backend="s3",
        knowledge_backend="sql",
        audit_backend="sql",
    )


class FakeBackend:
    """Minimal AgentStateBackend double with close accounting."""

    def __init__(self, name: str, *, fail_close: bool = False) -> None:
        self.name = name
        self.fail_close = fail_close
        self.close_calls = 0
        self.check_ready_calls = 0
        self.session_service = object()
        self.memory_service = object()

    def check_ready(self) -> None:
        self.check_ready_calls += 1

    async def close(self) -> None:
        self.close_calls += 1
        if self.fail_close:
            raise RuntimeError("backend close failed")


def _resolver(redis: FakeBackend, sql: FakeBackend) -> TenantStateBackendResolver:
    return TenantStateBackendResolver(redis_backend=redis, sql_backend=sql)


class TestFromEnv:

    def test_constructs_exactly_one_redis_and_one_sql_backend(self):
        redis = FakeBackend("redis")
        sql = FakeBackend("sql")
        with patch.object(RedisStateBackend, "from_env", return_value=redis) as redis_from_env, \
                patch.object(SqlStateBackend, "from_env", return_value=sql) as sql_from_env:
            env = {"TRPC_REDIS_URL": "redis://h:6379", "TRPC_DATABASE_URL": "postgresql+asyncpg://u@h/d"}
            resolver = TenantStateBackendResolver.from_env(env)
        assert redis_from_env.call_count == 1
        assert sql_from_env.call_count == 1
        assert redis_from_env.call_args.args[0] is env or redis_from_env.call_args.kwargs.get("environ") is env
        assert sql_from_env.call_args.args[0] is env or sql_from_env.call_args.kwargs.get("environ") is env
        assert resolver.resolve(_profile("redis")) is redis
        assert resolver.resolve(_profile("sql")) is sql

    def test_sql_config_failure_propagates(self):
        redis = FakeBackend("redis")
        with patch.object(RedisStateBackend, "from_env", return_value=redis), \
                patch.object(SqlStateBackend, "from_env",
                             side_effect=StateBackendConfigurationError("TRPC_DATABASE_URL configuration is invalid")):
            with pytest.raises(StateBackendConfigurationError):
                TenantStateBackendResolver.from_env({"TRPC_REDIS_URL": "redis://h:6379"})


class TestResolve:

    @pytest.mark.parametrize(
        "kind, expected",
        [("redis", "redis"), ("sql", "sql")],
    )
    def test_selection_is_exact(self, kind, expected):
        redis = FakeBackend("redis")
        sql = FakeBackend("sql")
        resolver = _resolver(redis, sql)
        chosen = resolver.resolve(_profile(kind))
        assert chosen.name == expected
        # the chosen object is THE owned instance, not a copy or a rebind
        assert chosen is (redis if kind == "redis" else sql)

    def test_never_falls_back_to_the_other_backend(self):
        """Even when the other backend is closed or unhealthy, selection is
        the profile's own backend only — fail-closed, no silent swap."""
        redis = FakeBackend("redis")
        sql = FakeBackend("sql")
        resolver = _resolver(redis, sql)
        asyncio.run(sql.close())  # SQL owner released (e.g. shutdown race)
        assert resolver.resolve(_profile("sql")) is sql
        assert resolver.resolve(_profile("redis")) is redis

    def test_resolve_result_satisfies_protocol_shape(self):
        resolver = _resolver(FakeBackend("redis"), FakeBackend("sql"))
        for kind in ("redis", "sql"):
            backend: AgentStateBackend = resolver.resolve(_profile(kind))
            assert hasattr(backend, "session_service")
            assert hasattr(backend, "memory_service")
            assert callable(backend.check_ready)
            assert asyncio.iscoroutinefunction(backend.close)


class TestCheckReady:

    def test_pings_redis_only(self):
        redis = FakeBackend("redis")
        sql = FakeBackend("sql")
        resolver = _resolver(redis, sql)
        resolver.check_ready()
        assert redis.check_ready_calls == 1
        # SQL reachability is covered by the repository check_ready in the
        # Worker lifespan and by fail-closed request paths — not here.
        assert sql.check_ready_calls == 0

    def test_redis_failure_propagates(self):
        redis = FakeBackend("redis")
        redis.check_ready = Mock(side_effect=StateBackendConfigurationError("Redis is not ready"))
        resolver = _resolver(redis, FakeBackend("sql"))
        with pytest.raises(StateBackendConfigurationError):
            resolver.check_ready()


class TestCloseOwnership:

    def test_closes_each_backend_exactly_once(self):
        redis = FakeBackend("redis")
        sql = FakeBackend("sql")
        resolver = _resolver(redis, sql)
        asyncio.run(resolver.close())
        assert redis.close_calls == 1
        assert sql.close_calls == 1

    def test_close_is_idempotent(self):
        redis = FakeBackend("redis")
        sql = FakeBackend("sql")
        resolver = _resolver(redis, sql)
        asyncio.run(resolver.close())
        asyncio.run(resolver.close())
        asyncio.run(resolver.close())
        assert redis.close_calls == 1
        assert sql.close_calls == 1

    def test_first_backend_failure_still_closes_second(self):
        redis = FakeBackend("redis", fail_close=True)
        sql = FakeBackend("sql")
        resolver = _resolver(redis, sql)
        # must not raise; both backends get their single close
        asyncio.run(resolver.close())
        assert redis.close_calls == 1
        assert sql.close_calls == 1

    def test_sql_backend_failure_still_swallowed(self):
        redis = FakeBackend("redis")
        sql = FakeBackend("sql", fail_close=True)
        resolver = _resolver(redis, sql)
        asyncio.run(resolver.close())
        assert redis.close_calls == 1
        assert sql.close_calls == 1

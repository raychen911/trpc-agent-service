"""Tests for Redis Session Execution Coordinator (Stage 3C)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any

import pytest

from trpc_service.agent.execution_coordinator import (
    SessionExecutionIdentity, )


def _identity(**overrides: Any) -> SessionExecutionIdentity:
    defaults = {
        "tenant_id": "tenant_default",
        "app_id": "app_demo",
        "config_version": 1,
        "sdk_user_id": "user_default",
        "session_id": "sess-1",
    }
    defaults.update(overrides)
    return SessionExecutionIdentity(**defaults)


# ---------------------------------------------------------------------------
# SessionExecutionIdentity
# ---------------------------------------------------------------------------


class TestSessionExecutionIdentity:

    def test_frozen(self) -> None:
        ident = _identity()
        with pytest.raises(AttributeError):
            ident.tenant_id = "other"  # type: ignore[misc]

    def test_digest_is_sha256(self) -> None:
        ident = _identity()
        canonical = json.dumps(
            [ident.tenant_id, ident.app_id, ident.config_version, ident.sdk_user_id, ident.session_id],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        expected = hashlib.sha256(canonical.encode()).hexdigest()
        assert ident.digest == expected

    def test_different_identity_different_digest(self) -> None:
        i1 = _identity(session_id="a")
        i2 = _identity(session_id="b")
        assert i1.digest != i2.digest

    def test_same_identity_same_digest(self) -> None:
        i1 = _identity()
        i2 = _identity()
        assert i1.digest == i2.digest


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------


class TestCoordinatorConfig:

    def test_valid_config(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://127.0.0.1:6379",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
        )
        assert coord.wait_seconds == 5.0
        assert coord.lease_seconds == 30.0
        assert coord.renew_seconds == 10.0

    def test_zero_wait_raises(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        with pytest.raises(ValueError):
            RedisSessionExecutionCoordinator(
                redis_url="redis://127.0.0.1:6379",
                wait_seconds=0,
                lease_seconds=30.0,
                renew_seconds=10.0,
            )

    def test_negative_lease_raises(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        with pytest.raises(ValueError):
            RedisSessionExecutionCoordinator(
                redis_url="redis://127.0.0.1:6379",
                wait_seconds=5.0,
                lease_seconds=-1,
                renew_seconds=10.0,
            )

    def test_renew_must_be_less_than_half_lease(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        with pytest.raises(ValueError, match="renew"):
            RedisSessionExecutionCoordinator(
                redis_url="redis://127.0.0.1:6379",
                wait_seconds=5.0,
                lease_seconds=30.0,
                renew_seconds=15.0,
            )

    def test_nan_wait_raises(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        with pytest.raises(ValueError):
            RedisSessionExecutionCoordinator(
                redis_url="redis://127.0.0.1:6379",
                wait_seconds=float("nan"),
                lease_seconds=30.0,
                renew_seconds=10.0,
            )

    def test_inf_lease_raises(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        with pytest.raises(ValueError):
            RedisSessionExecutionCoordinator(
                redis_url="redis://127.0.0.1:6379",
                wait_seconds=5.0,
                lease_seconds=float("inf"),
                renew_seconds=10.0,
            )

    def test_from_env_valid(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        coord = RedisSessionExecutionCoordinator.from_env({
            "TRPC_REDIS_URL": "redis://127.0.0.1:6379",
            "TRPC_SESSION_LOCK_WAIT_SECONDS": "3",
            "TRPC_SESSION_LOCK_LEASE_SECONDS": "20",
            "TRPC_SESSION_LOCK_RENEW_SECONDS": "8",
        })
        assert coord.wait_seconds == 3.0
        assert coord.lease_seconds == 20.0
        assert coord.renew_seconds == 8.0

    def test_from_env_missing_redis_raises(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        with pytest.raises(ValueError, match="TRPC_REDIS_URL"):
            RedisSessionExecutionCoordinator.from_env({})

    def test_from_env_defaults(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        coord = RedisSessionExecutionCoordinator.from_env({
            "TRPC_REDIS_URL": "redis://127.0.0.1:6379",
        })
        assert coord.wait_seconds == 5.0
        assert coord.lease_seconds == 30.0
        assert coord.renew_seconds == 10.0

    def test_from_env_nan_raises(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        with pytest.raises(ValueError):
            RedisSessionExecutionCoordinator.from_env({
                "TRPC_REDIS_URL": "redis://127.0.0.1:6379",
                "TRPC_SESSION_LOCK_WAIT_SECONDS": "nan",
            })

    def test_from_env_inf_raises(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        with pytest.raises(ValueError):
            RedisSessionExecutionCoordinator.from_env({
                "TRPC_REDIS_URL": "redis://127.0.0.1:6379",
                "TRPC_SESSION_LOCK_LEASE_SECONDS": "inf",
            })


# ---------------------------------------------------------------------------
# Lock acquire/release with fake Redis (simulates real redis-py API)
# ---------------------------------------------------------------------------

_LUA_CAS_RENEW = """
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    return 0
end
"""

_LUA_CAS_DELETE = """
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0
end
"""


class _FakeRedis:
    """Fake Redis that simulates real redis-py async API with eval()."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[str, float]] = {}
        self._closed = False
        self.eval_calls: list[tuple[str, int, tuple]] = []

    async def set(self, key: str, value: str, nx: bool = False, px: int = 0) -> bool:
        if self._closed:
            raise ConnectionError("closed")
        self._expire_if_needed(key)
        if nx and key in self._store:
            return False
        expiry_time = time.monotonic() + px / 1000.0 if px > 0 else 0
        self._store[key] = (value, expiry_time)
        return True

    async def eval(self, script: str, numkeys: int, *args: str) -> int:
        if self._closed:
            raise ConnectionError("closed")
        keys = args[:numkeys]
        argv = args[numkeys:]
        self.eval_calls.append(("PEXPIRE" if "PEXPIRE" in script else "DEL", numkeys, args))

        if "PEXPIRE" in script and "DEL" not in script:
            key = keys[0]
            token = argv[0]
            new_px_ms = int(argv[1])
            self._expire_if_needed(key)
            if key in self._store and self._store[key][0] == token:
                self._store[key] = (token, time.monotonic() + new_px_ms / 1000.0)
                return 1
            return 0

        if "DEL" in script:
            key = keys[0]
            token = argv[0]
            self._expire_if_needed(key)
            if key in self._store and self._store[key][0] == token:
                del self._store[key]
                return 1
            return 0

        raise ValueError(f"unsupported script: {script[:50]}")

    async def aclose(self) -> None:
        self._closed = True

    def _expire_if_needed(self, key: str) -> None:
        if key in self._store:
            _, expiry = self._store[key]
            if expiry > 0 and time.monotonic() > expiry:
                del self._store[key]


class TestCoordinatorWithFakeRedis:

    @pytest.mark.asyncio
    async def test_acquire_and_release(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        identity = _identity()
        async with coord.acquire(identity) as lease:
            assert lease is not None
            lease.ensure_valid()
        await coord.close()

    @pytest.mark.asyncio
    async def test_lock_key_deleted_on_release(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        identity = _identity()
        async with coord.acquire(identity):
            assert len(fake._store) > 0
        assert len(fake._store) == 0
        await coord.close()

    @pytest.mark.asyncio
    async def test_wrong_token_cannot_delete(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        identity = _identity()
        async with coord.acquire(identity):
            key = list(fake._store.keys())[0]
            result = await fake.eval(_LUA_CAS_DELETE, 1, key, "wrong-token")
            assert result == 0
            assert key in fake._store
        await coord.close()

    @pytest.mark.asyncio
    async def test_wrong_token_cannot_renew(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        identity = _identity()
        async with coord.acquire(identity):
            key = list(fake._store.keys())[0]
            result = await fake.eval(_LUA_CAS_RENEW, 1, key, "wrong-token", "30000")
            assert result == 0
        await coord.close()

    @pytest.mark.asyncio
    async def test_second_acquire_waits_and_succeeds(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=0.3,
            renew_seconds=0.1,
            _redis=fake,
        )
        identity = _identity()

        order: list[str] = []

        async def holder():
            async with coord.acquire(identity):
                order.append("acquired-1")
                await asyncio.sleep(0.2)
                order.append("releasing-1")

        async def waiter():
            await asyncio.sleep(0.05)
            async with coord.acquire(identity):
                order.append("acquired-2")

        await asyncio.gather(holder(), waiter())
        assert order == ["acquired-1", "releasing-1", "acquired-2"]
        await coord.close()

    @pytest.mark.asyncio
    async def test_acquire_timeout_raises_busy(self) -> None:
        from trpc_service.agent.execution_coordinator import (
            RedisSessionExecutionCoordinator,
            SessionBusyError,
        )

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=0.3,
            lease_seconds=10.0,
            renew_seconds=3.0,
            _redis=fake,
        )
        identity = _identity()

        async with coord.acquire(identity):
            with pytest.raises(SessionBusyError):
                async with coord.acquire(identity):
                    pass
        await coord.close()

    @pytest.mark.asyncio
    async def test_different_sessions_independent(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        i1 = _identity(session_id="a")
        i2 = _identity(session_id="b")

        async with coord.acquire(i1):
            async with coord.acquire(i2):
                pass
        await coord.close()

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        await coord.close()
        await coord.close()

    @pytest.mark.asyncio
    async def test_token_is_unpredictable(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        tokens: list[str] = []

        for i in range(5):
            identity = _identity(session_id=f"sess-{i}")
            async with coord.acquire(identity) as lease:
                tokens.append(lease._token)
        assert len(set(tokens)) == 5
        await coord.close()

    @pytest.mark.asyncio
    async def test_renewal_uses_eval_not_cas_renew(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=0.3,
            renew_seconds=0.1,
            _redis=fake,
        )
        identity = _identity()
        async with coord.acquire(identity):
            await asyncio.sleep(0.25)
        eval_scripts = [call[0] for call in fake.eval_calls]
        assert any("PEXPIRE" in s for s in eval_scripts), "renewal should use EVAL with PEXPIRE"
        await coord.close()

    @pytest.mark.asyncio
    async def test_release_uses_eval_not_cas_delete(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        identity = _identity()
        async with coord.acquire(identity):
            pass
        eval_scripts = [call[0] for call in fake.eval_calls]
        assert any("DEL" in s for s in eval_scripts), "release must use EVAL with DEL"
        await coord.close()

    @pytest.mark.asyncio
    async def test_close_with_injected_redis_does_not_close_it(self) -> None:
        from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator

        fake = _FakeRedis()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        await coord.close()
        assert not fake._closed, "injected redis should not be closed by coordinator"


class _FakeRedisCasReturnsZero(_FakeRedis):
    """Fake Redis where CAS delete always returns 0 (ownership lost)."""

    async def eval(self, script: str, numkeys: int, *args: str) -> int:
        if "DEL" in script and "PEXPIRE" not in script:
            self.eval_calls.append(("DEL", numkeys, args))
            return 0
        return await super().eval(script, numkeys, *args)


class _FakeRedisCasThrows(_FakeRedis):
    """Fake Redis where CAS delete raises ConnectionError."""

    async def eval(self, script: str, numkeys: int, *args: str) -> int:
        if "DEL" in script and "PEXPIRE" not in script:
            self.eval_calls.append(("DEL", numkeys, args))
            raise ConnectionError("redis connection lost")
        return await super().eval(script, numkeys, *args)


class TestLockReleaseFailureRaisesLost:
    """Regression: CAS delete failure must raise SessionExecutionLostError."""

    @pytest.mark.asyncio
    async def test_cas_returns_zero_raises_lost(self) -> None:
        from trpc_service.agent.execution_coordinator import (
            RedisSessionExecutionCoordinator,
            SessionExecutionLostError,
        )

        fake = _FakeRedisCasReturnsZero()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        identity = _identity()
        with pytest.raises(SessionExecutionLostError):
            async with coord.acquire(identity):
                pass
        await coord.close()

    @pytest.mark.asyncio
    async def test_cas_throws_exception_raises_lost(self) -> None:
        from trpc_service.agent.execution_coordinator import (
            RedisSessionExecutionCoordinator,
            SessionExecutionLostError,
        )

        fake = _FakeRedisCasThrows()
        coord = RedisSessionExecutionCoordinator(
            redis_url="redis://fake",
            wait_seconds=5.0,
            lease_seconds=30.0,
            renew_seconds=10.0,
            _redis=fake,
        )
        identity = _identity()
        with pytest.raises(SessionExecutionLostError):
            async with coord.acquire(identity):
                pass
        await coord.close()

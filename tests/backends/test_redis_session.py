"""Strict fake tests for Redis Lua CAS without requiring a live Redis server."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import pytest

from trpc_service.backends import (
    REDIS_SESSION_CAS_LUA,
    BackendConflictError,
    BackendCorruptionError,
    BackendRole,
    RedisSessionProjectionBackend,
    SessionProjection,
    WatermarkRegressionError,
    WriteDisposition,
)


class StrictFakeRedis:
    """Implements exactly the Hash/Lua surface and asserts the production ABI."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttl_ms: dict[str, int] = {}
        self.eval_calls = 0
        self._lock = asyncio.Lock()

    async def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str | int,
    ) -> Any:
        assert script == REDIS_SESSION_CAS_LUA
        assert numkeys == 1
        assert len(keys_and_args) == 7
        self.eval_calls += 1
        key = str(keys_and_args[0])
        expected = int(keys_and_args[1])
        version = int(keys_and_args[2])
        watermark = int(keys_and_args[3])
        payload_hash = str(keys_and_args[4])
        payload_json = str(keys_and_args[5])
        ttl_ms = int(keys_and_args[6])
        async with self._lock:
            current = self.hashes.get(key)
            if current is None:
                if expected != -1:
                    return [0, b"-1", -1]
                self._store(key, version, watermark, payload_hash, payload_json, ttl_ms)
                return [1, str(version).encode(), watermark]
            current_version = int(current["version"])
            current_watermark = int(current["watermark"])
            if current_version != expected:
                return [0, str(current_version).encode(), current_watermark]
            if version < current_version or watermark < current_watermark:
                return [-1, current_version, current_watermark]
            if version == current_version:
                if (
                    watermark == current_watermark
                    and payload_hash == current["payload_hash"]
                    and payload_json == current["payload_json"]
                ):
                    self.ttl_ms[key] = ttl_ms
                    return [2, current_version, current_watermark]
                return [-2, current_version, current_watermark]
            self._store(key, version, watermark, payload_hash, payload_json, ttl_ms)
            return [1, version, watermark]

    async def hgetall(self, name: str) -> Mapping[Any, Any]:
        current = self.hashes.get(name)
        if current is None:
            return {}
        return {field.encode("utf-8"): value.encode("utf-8") for field, value in current.items()}

    def _store(
        self,
        key: str,
        version: int,
        watermark: int,
        payload_hash: str,
        payload_json: str,
        ttl_ms: int,
    ) -> None:
        self.hashes[key] = {
            "version": str(version),
            "watermark": str(watermark),
            "payload_hash": payload_hash,
            "payload_json": payload_json,
        }
        self.ttl_ms[key] = ttl_ms


def projection(
    *,
    version: int = 0,
    watermark: int = 0,
    state: dict[str, object] | None = None,
) -> SessionProjection:
    return SessionProjection(
        tenant_id="tenant-a",
        session_id="session:with unsafe redis characters",
        version=version,
        committed_through=watermark,
        state=state or {"turn": version},
    )


@pytest.mark.asyncio
async def test_redis_lua_cas_key_ttl_and_integrity_round_trip() -> None:
    client = StrictFakeRedis()
    backend = RedisSessionProjectionBackend(
        client,
        namespace="agent-projection",
        ttl=timedelta(hours=2),
    )
    assert backend.consistency.role is BackendRole.PROJECTION
    assert "canonical Events stay in SQL" in backend.consistency.notes
    key = backend.key_for("tenant-a", "session:with unsafe redis characters")
    assert "tenant-a" not in key
    assert "session:with" not in key
    assert key != backend.key_for("tenant-b", "session:with unsafe redis characters")

    initial = projection()
    first = await backend.compare_and_set_session(initial, expected_version=None)
    assert first.disposition is WriteDisposition.APPLIED
    assert client.ttl_ms[key] == 7_200_000
    assert await backend.get_session(initial.tenant_id, initial.session_id) == initial
    repeated = await backend.compare_and_set_session(initial, expected_version=0)
    assert repeated.disposition is WriteDisposition.UNCHANGED
    assert client.eval_calls == 2


@pytest.mark.asyncio
async def test_redis_cas_has_one_winner_and_rejects_regression_or_conflict() -> None:
    client = StrictFakeRedis()
    backend = RedisSessionProjectionBackend(client)
    await backend.compare_and_set_session(projection(), expected_version=None)
    create_only_conflict = await backend.compare_and_set_session(
        projection(version=1, watermark=1),
        expected_version=None,
    )
    assert create_only_conflict.disposition is WriteDisposition.CONFLICT
    candidates = (
        projection(version=1, watermark=1, state={"winner": "one"}),
        projection(version=1, watermark=1, state={"winner": "two"}),
    )
    outcomes = await asyncio.gather(
        *(
            backend.compare_and_set_session(candidate, expected_version=0)
            for candidate in candidates
        )
    )
    assert [result.disposition for result in outcomes].count(WriteDisposition.APPLIED) == 1
    assert [result.disposition for result in outcomes].count(WriteDisposition.CONFLICT) == 1
    current = await backend.get_session("tenant-a", projection().session_id)
    assert current is not None

    with pytest.raises(BackendConflictError):
        await backend.compare_and_set_session(
            SessionProjection(
                tenant_id=current.tenant_id,
                session_id=current.session_id,
                version=current.version,
                committed_through=current.committed_through,
                state={"same-version": "different-content"},
            ),
            expected_version=current.version,
        )
    with pytest.raises(WatermarkRegressionError):
        await backend.compare_and_set_session(
            projection(version=0, watermark=0),
            expected_version=current.version,
        )


@pytest.mark.asyncio
async def test_redis_missing_and_corrupt_projection_fail_safely() -> None:
    client = StrictFakeRedis()
    backend = RedisSessionProjectionBackend(client)
    assert await backend.get_session("tenant-a", "missing") is None
    stored = projection()
    await backend.compare_and_set_session(stored, expected_version=None)
    key = backend.key_for(stored.tenant_id, stored.session_id)
    client.hashes[key]["payload_hash"] = "0" * 64
    with pytest.raises(BackendCorruptionError, match="hash mismatch"):
        await backend.get_session(stored.tenant_id, stored.session_id)
    del client.hashes[key]["payload_hash"]
    with pytest.raises(BackendCorruptionError, match="missing fields"):
        await backend.get_session(stored.tenant_id, stored.session_id)
    client.hashes[key] = {
        "version": "not-an-int",
        "watermark": "0",
        "payload_hash": "0" * 64,
        "payload_json": "{}",
    }
    with pytest.raises(BackendCorruptionError, match="malformed"):
        await backend.get_session(stored.tenant_id, stored.session_id)
    client.hashes[key] = {
        "version": "0",
        "watermark": "0",
        "payload_hash": "0" * 64,
        "payload_json": "[]",
    }
    with pytest.raises(BackendCorruptionError, match="not an object"):
        await backend.get_session(stored.tenant_id, stored.session_id)
    client.hashes[key] = {
        "version": "0",
        "watermark": "1",
        "payload_hash": "0" * 64,
        "payload_json": "{}",
    }
    with pytest.raises(BackendCorruptionError, match="watermark"):
        await backend.get_session(stored.tenant_id, stored.session_id)


def test_redis_constructor_and_projection_validation_are_fail_closed() -> None:
    client = StrictFakeRedis()
    with pytest.raises(ValueError, match="namespace"):
        RedisSessionProjectionBackend(client, namespace="Unsafe Namespace")
    with pytest.raises(ValueError, match="TTL"):
        RedisSessionProjectionBackend(client, ttl=timedelta(milliseconds=999))
    backend = RedisSessionProjectionBackend(client)
    with pytest.raises(ValueError, match="committed_through"):
        asyncio.run(
            backend.compare_and_set_session(
                projection(version=1, watermark=2),
                expected_version=None,
            )
        )
    with pytest.raises(ValueError, match="expected_version"):
        asyncio.run(
            backend.compare_and_set_session(
                projection(),
                expected_version=-1,
            )
        )


class ForcedResponseRedis(StrictFakeRedis):
    def __init__(self, response: Any) -> None:
        super().__init__()
        self._response = response

    async def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str | int,
    ) -> Any:
        assert script == REDIS_SESSION_CAS_LUA
        assert numkeys == 1
        assert len(keys_and_args) == 7
        return self._response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "message"),
    [
        ([99, 0, 0], "unknown status"),
        ([1, 0], "malformed response"),
        ([1, "not-int", 0], "non-integer metadata"),
    ],
)
async def test_redis_protocol_response_is_strictly_validated(
    response: Any,
    message: str,
) -> None:
    backend = RedisSessionProjectionBackend(ForcedResponseRedis(response))
    with pytest.raises(BackendCorruptionError, match=message):
        await backend.compare_and_set_session(projection(), expected_version=None)

"""Redis implementation of the rebuildable Session projection only."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Protocol

from trpc_service.backends.contracts import (
    BackendConflictError,
    BackendCorruptionError,
    BackendRole,
    ConsistencyMetadata,
    DurabilityClass,
    ReadVisibility,
    SessionProjection,
    WatermarkRegressionError,
    WriteDisposition,
    WriteResult,
    canonical_json_hash,
    validate_nonempty,
    validate_tenant_id,
)

_NAMESPACE = re.compile(r"\A[a-z][a-z0-9-]{0,31}\Z")

REDIS_SESSION_CAS_LUA = """
local key = KEYS[1]
local expected = tonumber(ARGV[1])
local new_version = tonumber(ARGV[2])
local new_watermark = tonumber(ARGV[3])
local new_hash = ARGV[4]
local new_json = ARGV[5]
local ttl_ms = tonumber(ARGV[6])

if redis.call('EXISTS', key) == 0 then
    if expected ~= -1 then
        return {0, -1, -1}
    end
    redis.call(
        'HSET', key,
        'version', new_version,
        'watermark', new_watermark,
        'payload_hash', new_hash,
        'payload_json', new_json
    )
    redis.call('PEXPIRE', key, ttl_ms)
    return {1, new_version, new_watermark}
end

local current_version = tonumber(redis.call('HGET', key, 'version'))
local current_watermark = tonumber(redis.call('HGET', key, 'watermark'))
if current_version ~= expected then
    return {0, current_version, current_watermark}
end
if new_version < current_version or new_watermark < current_watermark then
    return {-1, current_version, current_watermark}
end
if new_version == current_version then
    local current_hash = redis.call('HGET', key, 'payload_hash')
    local current_json = redis.call('HGET', key, 'payload_json')
    if new_watermark == current_watermark
        and new_hash == current_hash
        and new_json == current_json then
        redis.call('PEXPIRE', key, ttl_ms)
        return {2, current_version, current_watermark}
    end
    return {-2, current_version, current_watermark}
end

redis.call(
    'HSET', key,
    'version', new_version,
    'watermark', new_watermark,
    'payload_hash', new_hash,
    'payload_json', new_json
)
redis.call('PEXPIRE', key, ttl_ms)
return {1, new_version, new_watermark}
""".strip()


class AsyncRedisSessionClient(Protocol):
    """Small injected surface used from ``redis.asyncio.Redis``."""

    async def eval(
        self,
        script: str,
        numkeys: int,
        *keys_and_args: str | int,
    ) -> Any: ...

    async def hgetall(self, name: str) -> Mapping[Any, Any]: ...


class RedisSessionProjectionBackend:
    """CAS Redis Hash projection; SQL Events remain the canonical authority."""

    def __init__(
        self,
        client: AsyncRedisSessionClient,
        *,
        namespace: str = "trpc-agent",
        ttl: timedelta = timedelta(days=7),
    ) -> None:
        if _NAMESPACE.fullmatch(namespace) is None:
            raise ValueError("Redis namespace must be a safe lowercase logical name")
        ttl_ms = int(ttl.total_seconds() * 1_000)
        if not 1_000 <= ttl_ms <= 31_536_000_000:
            raise ValueError("Redis projection TTL must be between one second and 365 days")
        self._client = client
        self._namespace = namespace
        self._ttl_ms = ttl_ms
        self._consistency = ConsistencyMetadata(
            backend_id=f"redis:{namespace}:session-projection",
            role=BackendRole.PROJECTION,
            visibility=ReadVisibility.READ_AFTER_WRITE,
            durability=DurabilityClass.REMOTE_VOLATILE,
            supports_cas=True,
            supports_monotonic_watermark=True,
            notes="Rebuildable Session projection only; canonical Events stay in SQL.",
        )

    @property
    def consistency(self) -> ConsistencyMetadata:
        return self._consistency

    def key_for(self, tenant_id: str, session_id: str) -> str:
        """Return a uniform key without exposing raw tenant or session identifiers."""

        validate_tenant_id(tenant_id)
        validate_nonempty(session_id, "session_id", max_length=128)
        tenant_digest = hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()
        session_digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
        return f"{self._namespace}:v1:session:tenant-{tenant_digest}:id-{session_digest}"

    async def get_session(
        self,
        tenant_id: str,
        session_id: str,
    ) -> SessionProjection | None:
        key = self.key_for(tenant_id, session_id)
        raw = await self._client.hgetall(key)
        if not raw:
            return None
        values = {_decode(key_part): _decode(value) for key_part, value in raw.items()}
        required = {"version", "watermark", "payload_hash", "payload_json"}
        if not required.issubset(values):
            raise BackendCorruptionError("Redis Session projection is missing fields")
        try:
            version = int(values["version"])
            watermark = int(values["watermark"])
            state = json.loads(values["payload_json"])
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise BackendCorruptionError("Redis Session projection is malformed") from error
        if not isinstance(state, dict):
            raise BackendCorruptionError("Redis Session projection state is not an object")
        if not 0 <= watermark <= version:
            raise BackendCorruptionError("Redis Session watermark is invalid")
        if canonical_json_hash(state) != values["payload_hash"]:
            raise BackendCorruptionError("Redis Session projection hash mismatch")
        return SessionProjection(
            tenant_id=tenant_id,
            session_id=session_id,
            version=version,
            committed_through=watermark,
            state=state,
        )

    async def compare_and_set_session(
        self,
        projection: SessionProjection,
        *,
        expected_version: int | None,
    ) -> WriteResult:
        self._validate_projection(projection, expected_version)
        payload_json = json.dumps(
            projection.state,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        response = await self._client.eval(
            REDIS_SESSION_CAS_LUA,
            1,
            self.key_for(projection.tenant_id, projection.session_id),
            -1 if expected_version is None else expected_version,
            projection.version,
            projection.committed_through,
            payload_hash,
            payload_json,
            self._ttl_ms,
        )
        code, current_version, current_watermark = _parse_lua_response(response)
        if code == 1:
            disposition = WriteDisposition.APPLIED
        elif code == 2:
            disposition = WriteDisposition.UNCHANGED
        elif code == 0:
            disposition = WriteDisposition.CONFLICT
        elif code == -1:
            raise WatermarkRegressionError("Redis Session projection cannot move backwards")
        elif code == -2:
            raise BackendConflictError("Redis Session projection version has conflicting content")
        else:
            raise BackendCorruptionError(f"Redis CAS returned unknown status {code}")
        return WriteResult(
            disposition=disposition,
            current_version=None if current_version == -1 else current_version,
            current_watermark=None if current_watermark == -1 else current_watermark,
        )

    @staticmethod
    def _validate_projection(
        projection: SessionProjection,
        expected_version: int | None,
    ) -> None:
        validate_tenant_id(projection.tenant_id)
        validate_nonempty(projection.session_id, "session_id", max_length=128)
        if projection.version < 0:
            raise ValueError("Session projection version must not be negative")
        if not 0 <= projection.committed_through <= projection.version:
            raise ValueError("committed_through must be between zero and version")
        if expected_version is not None and expected_version < 0:
            raise ValueError("expected_version must be non-negative or None")


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    raise BackendCorruptionError("Redis returned a non-string Hash field")


def _parse_lua_response(response: Any) -> tuple[int, int, int]:
    if not isinstance(response, (list, tuple)) or len(response) != 3:
        raise BackendCorruptionError("Redis CAS returned a malformed response")
    try:
        return tuple(int(_decode_number(item)) for item in response)  # type: ignore[return-value]
    except (TypeError, ValueError) as error:
        raise BackendCorruptionError("Redis CAS returned non-integer metadata") from error


def _decode_number(value: Any) -> str | int:
    if isinstance(value, bytes):
        return value.decode("ascii")
    if isinstance(value, (str, int)):
        return value
    raise TypeError("not a Redis integer")

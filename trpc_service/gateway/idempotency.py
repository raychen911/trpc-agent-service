# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Idempotency records used before queueing inbound messages."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Protocol

from redis.asyncio import Redis
from redis.asyncio import from_url

from trpc_service._compat import StrEnum


class IdempotencyState(StrEnum):
    PROCESSING = "processing"
    COMPLETED = "completed"


class IdempotencyConflictError(RuntimeError):
    """Raised when one key is reused with a different payload."""


class AdmissionInDoubtError(RuntimeError):
    """A legacy Redis owner has no durable request: require reconciliation."""


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    request_id: str
    state: IdempotencyState
    payload_hash: str = ""


class IdempotencyStore(Protocol):

    async def reserve(self,
                      key: str,
                      request_id: str,
                      *,
                      ttl_seconds: int,
                      payload_hash: str = "") -> IdempotencyRecord:
        """Atomically reserve ``key`` or return its existing record."""

    async def complete(self, key: str, request_id: str, *, ttl_seconds: int) -> None:
        """Mark a reservation complete only when its owner matches."""

    async def abandon(self, key: str, request_id: str) -> None:
        """Release a failed reservation only when its owner matches."""


class InMemoryIdempotencyStore:
    """Deterministic process-local implementation for development and tests."""

    def __init__(self) -> None:
        self._records: dict[str, tuple[IdempotencyRecord, float]] = {}
        self._lock = asyncio.Lock()

    async def reserve(self,
                      key: str,
                      request_id: str,
                      *,
                      ttl_seconds: int = 86400,
                      payload_hash: str = "") -> IdempotencyRecord:
        async with self._lock:
            now = time.monotonic()
            existing = self._records.get(key)
            if existing and existing[1] > now:
                if payload_hash and existing[0].payload_hash and payload_hash != existing[0].payload_hash:
                    raise IdempotencyConflictError("idempotency key was reused with a different payload")
                return existing[0]
            record = IdempotencyRecord(request_id=request_id,
                                       state=IdempotencyState.PROCESSING,
                                       payload_hash=payload_hash)
            self._records[key] = (record, now + ttl_seconds)
            return record

    async def complete(self, key: str, request_id: str, *, ttl_seconds: int = 86400) -> None:
        async with self._lock:
            existing = self._records.get(key)
            if existing and existing[0].request_id == request_id:
                self._records[key] = (
                    IdempotencyRecord(request_id=request_id,
                                      state=IdempotencyState.COMPLETED,
                                      payload_hash=existing[0].payload_hash),
                    time.monotonic() + ttl_seconds,
                )

    async def abandon(self, key: str, request_id: str) -> None:
        async with self._lock:
            existing = self._records.get(key)
            if existing and existing[0].request_id == request_id:
                self._records.pop(key, None)


_COMPLETE_SCRIPT = """
local value = redis.call('get', KEYS[1])
if value and string.sub(value, 1, string.len(ARGV[1]) + 12) == ARGV[1] .. ':processing:' then
  local payload_hash = string.sub(value, string.len(ARGV[1]) + 13)
  redis.call('set', KEYS[1], ARGV[1] .. ':completed:' .. payload_hash, 'EX', ARGV[2])
  return 1
end
return 0
"""

_ABANDON_SCRIPT = """
local value = redis.call('get', KEYS[1])
if value and string.sub(value, 1, string.len(ARGV[1]) + 12) == ARGV[1] .. ':processing:' then
  return redis.call('del', KEYS[1])
end
return 0
"""


class RedisIdempotencyStore:
    """Redis implementation shared by all Gateway replicas."""

    def __init__(self,
                 redis_url: str,
                 *,
                 prefix: str = "trpc-service:idempotency",
                 client: Redis | None = None) -> None:
        self._redis = client or from_url(redis_url, decode_responses=True)
        self._owns_client = client is None
        self._prefix = prefix.rstrip(":")

    def _key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def lookup(self, key):
        value = await self._redis.get(self._key(key))
        if not value:
            return None
        pieces = value.split(":", 2)
        return IdempotencyRecord(pieces[0], IdempotencyState(pieces[1]), pieces[2] if len(pieces) > 2 else "")

    async def reserve(self,
                      key: str,
                      request_id: str,
                      *,
                      ttl_seconds: int = 86400,
                      payload_hash: str = "") -> IdempotencyRecord:
        redis_key = self._key(key)
        value = f"{request_id}:processing:{payload_hash}"
        if await self._redis.set(redis_key, value, nx=True, ex=ttl_seconds):
            return IdempotencyRecord(request_id=request_id,
                                     state=IdempotencyState.PROCESSING,
                                     payload_hash=payload_hash)
        existing = await self._redis.get(redis_key)
        if not existing:
            return await self.reserve(key, request_id, ttl_seconds=ttl_seconds, payload_hash=payload_hash)
        try:
            owner, state, stored_hash = existing.split(":", 2)
        except ValueError:
            owner, _, state = existing.rpartition(":")
            stored_hash = ""
        if payload_hash and stored_hash and payload_hash != stored_hash:
            raise IdempotencyConflictError("idempotency key was reused with a different payload")
        return IdempotencyRecord(request_id=owner, state=IdempotencyState(state), payload_hash=stored_hash)

    async def complete(self, key: str, request_id: str, *, ttl_seconds: int = 86400) -> None:
        await self._redis.eval(_COMPLETE_SCRIPT, 1, self._key(key), request_id, ttl_seconds)

    async def abandon(self, key: str, request_id: str) -> None:
        await self._redis.eval(_ABANDON_SCRIPT, 1, self._key(key), request_id)

    async def close(self) -> None:
        if self._owns_client:
            await self._redis.aclose()

    async def ping(self) -> bool:
        return bool(await self._redis.ping())

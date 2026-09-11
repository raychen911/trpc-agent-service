"""Tenant-scoped Redis Session, Memory, and Summary backend."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from ..tenant import SessionEvent, SessionSnapshot
from .models import FencingConflict, LeaseBusy, SessionLease
from .projection import Summary

_ENSURE_SESSION = """
redis.call('HSETNX', KEYS[1], 'app_id', ARGV[1])
redis.call('HSETNX', KEYS[1], 'user_id', ARGV[2])
redis.call('HSETNX', KEYS[1], 'state', '{}')
redis.call('HSETNX', KEYS[1], 'version', '0')
redis.call('HSETNX', KEYS[1], 'fencing_epoch', '0')
redis.call('HSETNX', KEYS[1], 'updated_at', ARGV[3])
return 1
"""

_ACQUIRE_LEASE = """
local now_parts = redis.call('TIME')
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
local owner = redis.call('HGET', KEYS[1], 'lease_owner')
local expires = tonumber(redis.call('HGET', KEYS[1], 'lease_expires_ms') or '0')
if owner and owner ~= '' and expires > now_ms then return {} end
local epoch = redis.call('HINCRBY', KEYS[1], 'fencing_epoch', 1)
local version = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
local next_expiry = now_ms + tonumber(ARGV[2])
redis.call('HSET', KEYS[1], 'lease_owner', ARGV[1], 'lease_expires_ms', next_expiry)
return {version, epoch, next_expiry}
"""

_RENEW_LEASE = """
if redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[1] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'fencing_epoch') or '-1') ~= tonumber(ARGV[2]) then return 0 end
local now_parts = redis.call('TIME')
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
redis.call('HSET', KEYS[1], 'lease_expires_ms', now_ms + tonumber(ARGV[3]))
return 1
"""

_RELEASE_LEASE = """
if redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[1] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'fencing_epoch') or '-1') ~= tonumber(ARGV[2]) then return 0 end
redis.call('HDEL', KEYS[1], 'lease_owner', 'lease_expires_ms')
return 1
"""

_APPEND_TURN = """
local now_parts = redis.call('TIME')
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)
if redis.call('HGET', KEYS[1], 'lease_owner') ~= ARGV[2] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'fencing_epoch') or '-1') ~= tonumber(ARGV[3]) then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'version') or '-1') ~= tonumber(ARGV[1]) then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_expires_ms') or '0') <= now_ms then return 0 end
redis.call('RPUSH', KEYS[2], ARGV[4], ARGV[5])
redis.call('HSET', KEYS[1], 'state', ARGV[6], 'version', ARGV[7], 'updated_at', ARGV[8])
redis.call('SET', KEYS[3], ARGV[9], 'EX', ARGV[10])
redis.call('HDEL', KEYS[1], 'lease_owner', 'lease_expires_ms')
return 1
"""

_ADD_MEMORY = """
if ARGV[1] ~= '' and redis.call('SADD', KEYS[2], ARGV[1]) == 0 then return 0 end
redis.call('RPUSH', KEYS[1], ARGV[2])
redis.call('LTRIM', KEYS[1], -tonumber(ARGV[3]), -1)
return 1
"""

_PUT_SUMMARY = """
local current = tonumber(redis.call('HGET', KEYS[1], 'source_version') or '-1')
if current > tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'source_version', ARGV[1], 'content', ARGV[2])
return 1
"""


def _text(value: str | bytes | int) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _mapping(value: dict[Any, Any]) -> dict[str, str]:
    return {_text(key): _text(item) for key, item in value.items()}


class RedisStateStore:
    """A selectable Redis state backend with lease and fencing semantics."""

    def __init__(
        self,
        redis_url: str,
        *,
        client: Any | None = None,
        lease_seconds: int = 60,
        memory_limit: int = 1000,
        prefix: str = "trpc:state:v1",
        prepared_ttl_seconds: int = 30 * 24 * 3600,
        projection_sink: Any | None = None,
    ) -> None:
        if lease_seconds < 1 or memory_limit < 1 or prepared_ttl_seconds < 1:
            raise ValueError("Redis state lease and memory limits must be positive")
        if client is None:
            from redis.asyncio import Redis

            client = Redis.from_url(redis_url, decode_responses=True)
        self.client = client
        self.lease_seconds = lease_seconds
        self.memory_limit = memory_limit
        self.prepared_ttl_seconds = prepared_ttl_seconds
        self.projection_sink = projection_sink
        self.prefix = prefix.rstrip(":")

    def _key(self, tenant_id: str, kind: str, identifier: str) -> str:
        digest = hashlib.sha256(identifier.encode()).hexdigest()
        return f"{self.prefix}:{tenant_id}:{kind}:{digest}"

    def _session_key(self, tenant_id: str, session_id: str) -> str:
        return self._key(tenant_id, "session", session_id)

    def _events_key(self, tenant_id: str, session_id: str) -> str:
        return self._key(tenant_id, "events", session_id)

    async def get_or_create(
        self, tenant_id: str, session_id: str, app_id: str, user_id: str
    ) -> SessionSnapshot:
        key = self._session_key(tenant_id, session_id)
        now = datetime.now(timezone.utc).isoformat()
        await self.client.eval(_ENSURE_SESSION, 1, key, app_id, user_id, now)
        value = _mapping(await self.client.hgetall(key))
        return SessionSnapshot(
            tenant_id,
            session_id,
            value.get("app_id", app_id),
            value.get("user_id", user_id),
            json.loads(value.get("state", "{}")),
            int(value.get("version", "0")),
            datetime.fromisoformat(value.get("updated_at", now).replace("Z", "+00:00")),
        )

    @asynccontextmanager
    async def lock(self, tenant_id: str, session_id: str) -> AsyncIterator[SessionLease]:
        key = self._session_key(tenant_id, session_id)
        owner_id = uuid4().hex
        values = await self.client.eval(_ACQUIRE_LEASE, 1, key, owner_id, self.lease_seconds * 1000)
        if not values:
            raise LeaseBusy("session lease is held by another worker")
        lease = SessionLease(
            tenant_id,
            session_id,
            owner_id,
            int(values[1]),
            int(values[0]),
            datetime.fromtimestamp(int(values[2]) / 1000, timezone.utc),
        )
        stop = asyncio.Event()
        heartbeat = asyncio.create_task(self._renew_lease(key, lease, stop))
        try:
            yield lease
        finally:
            stop.set()
            await heartbeat
            await self.client.eval(_RELEASE_LEASE, 1, key, owner_id, lease.epoch)

    async def _renew_lease(self, key: str, lease: SessionLease, stop: asyncio.Event) -> None:
        interval = max(0.1, self.lease_seconds / 3)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                renewed = await self.client.eval(
                    _RENEW_LEASE,
                    1,
                    key,
                    lease.owner_id,
                    lease.epoch,
                    self.lease_seconds * 1000,
                )
                if not renewed:
                    return

    async def append_turn(
        self,
        snapshot: SessionSnapshot,
        user_text: str,
        assistant_text: str,
        trace_id: str,
        lease: SessionLease | None = None,
        inbox_key: str | None = None,
        config_version: int | None = None,
    ) -> list[SessionEvent]:
        if lease is None:
            raise FencingConflict("Redis session writes require a lease")
        start = snapshot.version + 1
        now = datetime.now(timezone.utc)
        events = [
            SessionEvent(
                snapshot.tenant_id,
                snapshot.session_id,
                start,
                "user_message",
                {"text": user_text},
                trace_id,
                now,
                uuid4().hex,
            ),
            SessionEvent(
                snapshot.tenant_id,
                snapshot.session_id,
                start + 1,
                "assistant_message",
                {"text": assistant_text},
                trace_id,
                now,
                uuid4().hex,
            ),
        ]
        state = {**snapshot.state, "last_user_text": user_text, "last_assistant_text": assistant_text}
        prepared = {
            "status": "prepared",
            "text": assistant_text,
            "session_id": snapshot.session_id,
            "source_version": start + 1,
            "config_version": config_version,
            "trace_id": trace_id,
        }
        committed = await self.client.eval(
            _APPEND_TURN,
            3,
            self._session_key(snapshot.tenant_id, snapshot.session_id),
            self._events_key(snapshot.tenant_id, snapshot.session_id),
            self._key(snapshot.tenant_id, "prepared", inbox_key or snapshot.session_id),
            lease.expected_version,
            lease.owner_id,
            lease.epoch,
            json.dumps(events[0].__dict__, ensure_ascii=False, separators=(",", ":"), default=str),
            json.dumps(events[1].__dict__, ensure_ascii=False, separators=(",", ":"), default=str),
            json.dumps(state, ensure_ascii=False, separators=(",", ":")),
            start + 1,
            now.isoformat(),
            json.dumps(prepared, ensure_ascii=False, separators=(",", ":")),
            self.prepared_ttl_seconds,
        )
        if not committed:
            raise FencingConflict("Redis session version or fencing epoch changed")
        snapshot.version = start + 1
        snapshot.state = state
        snapshot.updated_at = now
        await self._ensure_projection(snapshot.tenant_id, prepared)
        return events

    async def prepared_result(self, tenant_id: str, inbox_key: str) -> dict[str, Any] | None:
        value = await self.client.get(self._key(tenant_id, "prepared", inbox_key))
        if not value:
            return None
        prepared = json.loads(_text(value))
        await self._ensure_projection(tenant_id, prepared)
        return prepared

    async def _ensure_projection(self, tenant_id: str, prepared: dict[str, Any]) -> None:
        if self.projection_sink is None or prepared.get("config_version") is None:
            return
        await self.projection_sink.enqueue_session_projection(
            tenant_id,
            str(prepared["session_id"]),
            int(prepared["source_version"]),
            int(prepared["config_version"]),
            str(prepared.get("trace_id", "")),
        )

    async def events(self, tenant_id: str, session_id: str) -> list[SessionEvent]:
        values = await self.client.lrange(self._events_key(tenant_id, session_id), 0, -1)
        events = []
        for value in values:
            payload = json.loads(_text(value))
            payload["created_at"] = datetime.fromisoformat(str(payload["created_at"]).replace("Z", "+00:00"))
            events.append(SessionEvent(**payload))
        return events

    async def add(self, tenant_id: str, user_id: str, value: str, source_id: str | None = None) -> None:
        await self.client.eval(
            _ADD_MEMORY,
            2,
            self._key(tenant_id, "memory", user_id),
            self._key(tenant_id, "memory-sources", user_id),
            source_id or "",
            value,
            self.memory_limit,
        )

    async def list(self, tenant_id: str, user_id: str) -> list[str]:
        values = await self.client.lrange(self._key(tenant_id, "memory", user_id), -100, -1)
        return [_text(value) for value in values]

    async def put_summary(self, summary: Summary) -> bool:
        updated = await self.client.eval(
            _PUT_SUMMARY,
            1,
            self._key(summary.tenant_id, "summary", summary.session_id),
            summary.source_version,
            summary.content,
        )
        return bool(updated)

    async def get_summary(self, tenant_id: str, session_id: str) -> Summary | None:
        value = _mapping(await self.client.hgetall(self._key(tenant_id, "summary", session_id)))
        if not value:
            return None
        return Summary(tenant_id, session_id, int(value["source_version"]), value["content"])

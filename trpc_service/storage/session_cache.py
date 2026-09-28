"""Best-effort Redis cache for recent durable Session snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import hashlib
import json
import logging
from typing import Protocol, cast

from redis.asyncio import Redis

from trpc_service.storage.types import SessionEvent, SessionSnapshot
from trpc_service.tenant.context import TenantContext

logger = logging.getLogger(__name__)

_MAX_CACHE_PAYLOAD_BYTES = 1024 * 1024


class RedisSessionCacheClient(Protocol):
    """Small Redis surface needed by the snapshot cache."""

    async def get(self, name: str) -> bytes | str | None:
        """Read one cached value."""

        ...

    async def set(self, name: str, value: str, *, ex: int) -> object:
        """Store one expiring cached value."""

        ...

    async def delete(self, *names: str) -> int:
        """Delete stale or invalid cached values."""

        ...

    async def aclose(self) -> None:
        """Close client resources."""

        ...


class SessionSnapshotCache(Protocol):
    """Optional short-lived read model in front of a durable SessionStore."""

    async def get(
        self,
        context: TenantContext,
        session_id: str,
        *,
        expected_version: int,
    ) -> SessionSnapshot | None:
        """Return only a snapshot matching the version claimed in SQL."""

        ...

    async def put(self, context: TenantContext, snapshot: SessionSnapshot) -> None:
        """Publish a snapshot after its durable transaction commits."""

        ...

    async def close(self) -> None:
        """Close cache resources."""

        ...


class RedisSessionSnapshotCache(SessionSnapshotCache):
    """Cache recent events while PostgreSQL remains the authoritative journal."""

    def __init__(
        self,
        client: RedisSessionCacheClient,
        *,
        ttl_seconds: int,
        max_events: int,
        key_prefix: str = "trpc:session-cache",
    ) -> None:
        if ttl_seconds < 1 or max_events < 1:
            raise ValueError("Session cache TTL and event limit must be positive")
        self._client = client
        self._ttl_seconds = ttl_seconds
        self._max_events = max_events
        self._key_prefix = key_prefix.rstrip(":")

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        ttl_seconds: int,
        max_events: int,
    ) -> "RedisSessionSnapshotCache":
        """Create a bounded async Redis client without exposing its URL in logs."""

        client = Redis.from_url(
            url,
            decode_responses=False,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        return cls(
            cast(RedisSessionCacheClient, client),
            ttl_seconds=ttl_seconds,
            max_events=max_events,
        )

    def _key(self, context: TenantContext, session_id: str) -> str:
        digest = hashlib.sha256(session_id.encode()).hexdigest()
        return (f"{self._key_prefix}:{context.tenant_id}:"
                f"{context.agent_app_id}:{digest}")

    async def get(
        self,
        context: TenantContext,
        session_id: str,
        *,
        expected_version: int,
    ) -> SessionSnapshot | None:
        """Fail open to the durable store on misses, stale data or Redis errors."""

        key = self._key(context, session_id)
        try:
            raw = await self._client.get(key)
            if raw is None:
                return None
            encoded = raw.encode() if isinstance(raw, str) else raw
            if len(encoded) > _MAX_CACHE_PAYLOAD_BYTES:
                await self._client.delete(key)
                return None
            snapshot = self._decode(encoded, session_id)
            if snapshot.version != expected_version:
                # SQL issued the claimed version inside the execution lease.
                # Never use a cache entry from an earlier or impossible future turn.
                await self._client.delete(key)
                return None
            return snapshot
        except Exception as error:
            logger.warning(
                "Redis Session cache read failed error_type=%s",
                type(error).__name__,
            )
            return None

    async def put(self, context: TenantContext, snapshot: SessionSnapshot) -> None:
        """Write through after SQL commit; cache failure cannot roll facts back."""

        key = self._key(context, snapshot.session_id)
        try:
            encoded = self._encode(snapshot)
            if len(encoded.encode()) > _MAX_CACHE_PAYLOAD_BYTES:
                await self._client.delete(key)
                return
            await self._client.set(key, encoded, ex=self._ttl_seconds)
        except Exception as error:
            logger.warning(
                "Redis Session cache write failed error_type=%s",
                type(error).__name__,
            )

    def _encode(self, snapshot: SessionSnapshot) -> str:
        events = snapshot.events[-self._max_events:]
        return json.dumps(
            {
                "session_id":
                snapshot.session_id,
                "version":
                snapshot.version,
                "state":
                dict(snapshot.state),
                "events": [{
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "occurred_at": event.occurred_at.isoformat(),
                    "payload": dict(event.payload),
                } for event in events],
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _decode(raw: bytes, expected_session_id: str) -> SessionSnapshot:
        value = json.loads(raw)
        if not isinstance(value, Mapping):
            raise ValueError("cached Session snapshot must be an object")
        session_id = value.get("session_id")
        version = value.get("version")
        state = value.get("state")
        raw_events = value.get("events")
        if session_id != expected_session_id or isinstance(version,
                                                           bool) or not isinstance(version, int):
            raise ValueError("cached Session identity or version is invalid")
        if not isinstance(state, Mapping) or not isinstance(raw_events, list):
            raise ValueError("cached Session state or events are invalid")
        events: list[SessionEvent] = []
        for item in raw_events:
            if not isinstance(item, Mapping):
                raise ValueError("cached Session event must be an object")
            event_id = item.get("event_id")
            event_type = item.get("event_type")
            occurred_at = item.get("occurred_at")
            payload = item.get("payload")
            if (not isinstance(event_id, str) or not isinstance(event_type, str)
                    or not isinstance(occurred_at, str) or not isinstance(payload, Mapping)):
                raise ValueError("cached Session event is invalid")
            events.append(
                SessionEvent(
                    event_id=event_id,
                    event_type=event_type,
                    occurred_at=datetime.fromisoformat(occurred_at),
                    payload={
                        str(key): value
                        for key, value in payload.items()
                    },
                ))
        return SessionSnapshot(
            session_id=session_id,
            version=version,
            events=tuple(events),
            state={
                str(key): item
                for key, item in state.items()
            },
        )

    async def close(self) -> None:
        """Release Redis connections owned by this process."""

        await self._client.aclose()

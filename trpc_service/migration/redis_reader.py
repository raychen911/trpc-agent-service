"""Version-checked Redis reader for the tRPC-Agent 1.1.19 storage layout."""

from __future__ import annotations

import json
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from redis.asyncio import Redis, from_url
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import Session

from .snapshots import MemorySnapshot, SessionSnapshot


class SdkStorageCompatibilityError(RuntimeError):
    """The installed SDK does not match the inspected persistence contract."""


class SdkRedisSnapshotReader:
    """Read tenant-scoped SDK records with SCAN rather than blocking KEYS."""

    SUPPORTED_SDK_VERSION = "1.1.19"

    def __init__(self, redis_url: str, *, client: Redis | None = None) -> None:
        self._redis = client or from_url(redis_url, decode_responses=True)
        self._owns_client = client is None

    @classmethod
    def validate_sdk_version(cls) -> None:
        try:
            installed = version("trpc-agent-py")
        except PackageNotFoundError as error:
            raise SdkStorageCompatibilityError("trpc-agent-py is not installed") from error
        if installed != cls.SUPPORTED_SDK_VERSION:
            raise SdkStorageCompatibilityError(
                f"Redis migration supports trpc-agent-py {cls.SUPPORTED_SDK_VERSION}, installed {installed}")

    async def ping(self) -> bool:
        return bool(await self._redis.ping())

    async def scan_sessions(self, app_names: list[str], cursor: dict[str, Any] | None,
                            limit: int) -> tuple[list[SessionSnapshot], dict[str, Any], bool]:
        state = dict(cursor or {})
        app_index = int(state.get("app_index", 0))
        redis_cursor = int(state.get("redis_cursor", 0))
        snapshots: list[SessionSnapshot] = []
        while app_index < len(app_names) and len(snapshots) < limit:
            app_name = app_names[app_index]
            redis_cursor, keys = await self._redis.scan(redis_cursor,
                                                        match=f"session:{app_name}:*",
                                                        count=max(10, limit * 2))
            for key in keys:
                raw = await self._redis.get(key)
                if raw is None:
                    continue
                ttl_ms = int(await self._redis.pttl(key))
                if ttl_ms == -2:
                    continue
                session = Session.model_validate_json(raw)
                app_state_key = f"app_state:{session.app_name}"
                user_state_key = f"user_state:{session.app_name}:{session.user_id}"
                app_state = await self._redis.hgetall(app_state_key)
                user_state = await self._redis.hgetall(user_state_key)
                app_ttl_ms = int(await self._redis.pttl(app_state_key))
                user_ttl_ms = int(await self._redis.pttl(user_state_key))
                snapshots.append(
                    SessionSnapshot(
                        app_name=session.app_name,
                        user_id=session.user_id,
                        session_id=session.id,
                        session_state=session.state,
                        app_state=app_state or {},
                        user_state=user_state or {},
                        events=session.events,
                        historical_events=session.historical_events,
                        conversation_count=session.conversation_count,
                        last_update_time=session.last_update_time,
                        remaining_ttl_seconds=(-1 if ttl_ms < 0 else max(0, ttl_ms // 1000)),
                        app_state_remaining_ttl_seconds=(-1 if app_ttl_ms < 0 else max(0, app_ttl_ms // 1000)),
                        user_state_remaining_ttl_seconds=(-1 if user_ttl_ms < 0 else max(0, user_ttl_ms // 1000)),
                        source_updated_at=session.last_update_time).seal())
            if redis_cursor == 0:
                app_index += 1
                redis_cursor = 0
        next_cursor = {"app_index": app_index, "redis_cursor": redis_cursor}
        return snapshots, next_cursor, app_index >= len(app_names)

    async def scan_memories(self, app_names: list[str], cursor: dict[str, Any] | None,
                            limit: int) -> tuple[list[MemorySnapshot], dict[str, Any], bool]:
        state = dict(cursor or {})
        app_index = int(state.get("app_index", 0))
        redis_cursor = int(state.get("redis_cursor", 0))
        snapshots: list[MemorySnapshot] = []
        while app_index < len(app_names) and len(snapshots) < limit:
            app_name = app_names[app_index]
            redis_cursor, keys = await self._redis.scan(redis_cursor,
                                                        match=f"memory:{app_name}/*:*",
                                                        count=max(10, limit * 2))
            for key in keys:
                values = await self._redis.lrange(key, 0, -1)
                if not values:
                    continue
                ttl_ms = int(await self._redis.pttl(key))
                if ttl_ms == -2:
                    continue
                prefix = "memory:"
                value = key[len(prefix):] if key.startswith(prefix) else key
                try:
                    # Platform session identifiers start with ``t:`` and contain
                    # colons themselves. Splitting at the final colon would turn
                    # the id into only its hash suffix.
                    save_key, session_tail = value.rsplit(":t:", 1)
                    session_id = f"t:{session_tail}"
                    events = [Event.model_validate_json(item) for item in values]
                except (ValueError, json.JSONDecodeError) as error:
                    raise SdkStorageCompatibilityError(f"invalid SDK Memory record: {key}") from error
                snapshots.append(
                    MemorySnapshot(save_key=save_key,
                                   session_id=session_id,
                                   events=events,
                                   remaining_ttl_seconds=(-1 if ttl_ms < 0 else max(0, ttl_ms // 1000)),
                                   source_updated_at=max((event.timestamp for event in events), default=0.0)).seal())
            if redis_cursor == 0:
                app_index += 1
                redis_cursor = 0
        next_cursor = {"app_index": app_index, "redis_cursor": redis_cursor}
        return snapshots, next_cursor, app_index >= len(app_names)

    async def close(self) -> None:
        if self._owns_client:
            await self._redis.aclose()

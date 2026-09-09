"""Read-only PostgreSQL exporter for the tRPC-Agent 1.1.19 SQL layout."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from sqlalchemy import and_, or_, select
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.memory._sql_memory_service import MemStorageEvent
from trpc_agent_sdk.sessions._sql_session_service import (
    SessionStorageEvent,
    StorageAppState,
    StorageSession,
    StorageUserState,
)

from trpc_service.config import BackendType, StoragePolicy
from trpc_service.storage import StorageProviderFactory

from .redis_reader import SdkStorageCompatibilityError
from .snapshots import MemorySnapshot, SessionSnapshot


class SdkPostgresSnapshotReader:
    """Export SDK records without invoking reads that refresh their TTL."""

    SUPPORTED_SDK_VERSION = "1.1.19"

    def __init__(self,
                 sql_url: str,
                 *,
                 pool: Any,
                 session_ttl_seconds: int,
                 memory_ttl_seconds: int,
                 storage_factory: StorageProviderFactory | None = None,
                 bundle: Any | None = None) -> None:
        if not sql_url.startswith("postgresql"):
            raise ValueError("PostgreSQL source migration does not support SQLite")
        policy = StoragePolicy(session=BackendType.SQL,
                               memory=BackendType.SQL,
                               sql_url=sql_url,
                               session_ttl_seconds=session_ttl_seconds,
                               memory_ttl_seconds=memory_ttl_seconds)
        self._owns_bundle = bundle is None
        self._bundle = bundle or (storage_factory or StorageProviderFactory()).create(policy)
        self._pool = pool
        self._session_ttl = session_ttl_seconds
        self._memory_ttl = memory_ttl_seconds

    @classmethod
    def validate_sdk_version(cls) -> None:
        try:
            installed = version("trpc-agent-py")
        except PackageNotFoundError as error:
            raise SdkStorageCompatibilityError("trpc-agent-py is not installed") from error
        if installed != cls.SUPPORTED_SDK_VERSION:
            raise SdkStorageCompatibilityError(
                f"PostgreSQL migration supports trpc-agent-py {cls.SUPPORTED_SDK_VERSION}, installed {installed}")

    async def validate_schema(self) -> None:
        session = getattr(self._bundle.session_service, "_delegate", self._bundle.session_service)
        await session._sql_storage.create_sql_engine()
        await self._bundle.memory_service._sql_storage.create_sql_engine()

        async def columns(table: str) -> set[str]:
            rows = await self._pool.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=current_schema() AND table_name=$1", table)
            return {row["column_name"] for row in rows}

        required = {
            "sessions":
            {"app_name", "user_id", "id", "state", "historical_events", "conversation_count", "update_time"},
            "events": {
                "id", "app_name", "user_id", "session_id", "actions", "timestamp", "content", "request_id",
                "usage_metadata"
            },
            "mem_events": {"id", "save_key", "session_id", "actions", "timestamp", "content"},
            "app_states": {"app_name", "state", "update_time"},
            "user_states": {"app_name", "user_id", "state", "update_time"},
        }
        for table, expected in required.items():
            if not expected <= await columns(table):
                raise SdkStorageCompatibilityError(f"incompatible SDK PostgreSQL table: {table}")

    @staticmethod
    def _timestamp(value: datetime | None) -> float:
        if value is None:
            return 0.0
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()

    @staticmethod
    def _remaining(updated: datetime | None, ttl: int) -> int:
        if ttl <= 0:
            return -1
        return max(0, int(SdkPostgresSnapshotReader._timestamp(updated) + ttl - time.time()))

    async def _session_snapshot(self, app_name: str, user_id: str, session_id: str) -> SessionSnapshot | None:
        service = getattr(self._bundle.session_service, "_delegate", self._bundle.session_service)
        async with service._sql_storage.create_db_session() as conn:
            stored = conn.get(StorageSession, (app_name, user_id, session_id))
            if stored is None or self._remaining(stored.update_time, self._session_ttl) == 0:
                return None
            event_rows = conn.scalars(
                select(SessionStorageEvent).where(
                    SessionStorageEvent.app_name == app_name,
                    SessionStorageEvent.user_id == user_id,
                    SessionStorageEvent.session_id == session_id,
                ).order_by(SessionStorageEvent.timestamp, SessionStorageEvent.id)).all()
            app_state = conn.get(StorageAppState, (app_name, ))
            user_state = conn.get(StorageUserState, (app_name, user_id))
            app_remaining = self._remaining(app_state.update_time, self._session_ttl) if app_state else -1
            user_remaining = self._remaining(user_state.update_time, self._session_ttl) if user_state else -1
            historical = [Event.model_validate(item) for item in (stored.historical_events or [])]
            return SessionSnapshot(
                app_name=app_name,
                user_id=user_id,
                session_id=session_id,
                session_state=dict(stored.state or {}),
                app_state=(dict(app_state.state or {}) if app_state and app_remaining != 0 else {}),
                user_state=(dict(user_state.state or {}) if user_state and user_remaining != 0 else {}),
                events=[row.to_event() for row in event_rows],
                historical_events=historical,
                conversation_count=stored.conversation_count or 0,
                last_update_time=self._timestamp(stored.update_time),
                remaining_ttl_seconds=self._remaining(stored.update_time, self._session_ttl),
                app_state_remaining_ttl_seconds=app_remaining,
                user_state_remaining_ttl_seconds=user_remaining,
                source_updated_at=self._timestamp(stored.update_time),
            ).seal()

    async def read_session(self, app_name: str, user_id: str, session_id: str) -> SessionSnapshot | None:
        return await self._session_snapshot(app_name, user_id, session_id)

    async def scan_sessions(self, app_names: list[str], cursor: dict[str, Any] | None,
                            limit: int) -> tuple[list[SessionSnapshot], dict[str, Any], bool]:
        state = dict(cursor or {})
        app_index = int(state.get("app_index", 0))
        last_user = str(state.get("last_user_id", ""))
        last_session = str(state.get("last_session_id", ""))
        snapshots: list[SessionSnapshot] = []
        service = getattr(self._bundle.session_service, "_delegate", self._bundle.session_service)
        while app_index < len(app_names) and len(snapshots) < limit:
            app_name = app_names[app_index]
            requested = limit - len(snapshots)
            async with service._sql_storage.create_db_session() as conn:
                statement = select(StorageSession).where(StorageSession.app_name == app_name)
                if last_user or last_session:
                    statement = statement.where(
                        or_(StorageSession.user_id > last_user,
                            and_(StorageSession.user_id == last_user, StorageSession.id > last_session)))
                rows = conn.scalars(statement.order_by(StorageSession.user_id,
                                                       StorageSession.id).limit(requested)).all()
            for row in rows:
                last_user, last_session = row.user_id, row.id
                snapshot = await self._session_snapshot(app_name, row.user_id, row.id)
                if snapshot is not None:
                    snapshots.append(snapshot)
            if len(rows) < requested:
                app_index += 1
                last_user = last_session = ""
        next_cursor = {"app_index": app_index, "last_user_id": last_user, "last_session_id": last_session}
        return snapshots, next_cursor, app_index >= len(app_names)

    async def read_memory(self, save_key: str, session_id: str) -> MemorySnapshot | None:
        async with self._bundle.memory_service._sql_storage.create_db_session() as conn:
            rows = conn.scalars(
                select(MemStorageEvent).where(MemStorageEvent.save_key == save_key,
                                              MemStorageEvent.session_id == session_id).order_by(
                                                  MemStorageEvent.timestamp, MemStorageEvent.id)).all()
            active = [row for row in rows if self._remaining(row.timestamp, self._memory_ttl) != 0]
            if not active:
                return None
            ttl = max(self._remaining(row.timestamp, self._memory_ttl) for row in active)
            return MemorySnapshot(save_key=save_key,
                                  session_id=session_id,
                                  events=[row.to_event() for row in active],
                                  remaining_ttl_seconds=ttl,
                                  source_updated_at=max(self._timestamp(row.timestamp) for row in active)).seal()

    async def scan_memories(self, app_names: list[str], cursor: dict[str, Any] | None,
                            limit: int) -> tuple[list[MemorySnapshot], dict[str, Any], bool]:
        state = dict(cursor or {})
        app_index = int(state.get("app_index", 0))
        last_save_key = str(state.get("last_save_key", ""))
        last_session = str(state.get("last_session_id", ""))
        snapshots: list[MemorySnapshot] = []
        while app_index < len(app_names) and len(snapshots) < limit:
            app_name = app_names[app_index]
            requested = limit - len(snapshots)
            async with self._bundle.memory_service._sql_storage.create_db_session() as conn:
                statement = select(MemStorageEvent.save_key,
                                   MemStorageEvent.session_id).where(MemStorageEvent.save_key.like(f"{app_name}/%"))
                if last_save_key or last_session:
                    statement = statement.where(
                        or_(MemStorageEvent.save_key > last_save_key,
                            and_(MemStorageEvent.save_key == last_save_key, MemStorageEvent.session_id > last_session)))
                rows = conn.execute(statement.distinct().order_by(MemStorageEvent.save_key,
                                                                  MemStorageEvent.session_id).limit(requested)).all()
            for save_key, session_id in rows:
                last_save_key, last_session = save_key, session_id
                snapshot = await self.read_memory(save_key, session_id)
                if snapshot is not None:
                    snapshots.append(snapshot)
            if len(rows) < requested:
                app_index += 1
                last_save_key = last_session = ""
        next_cursor = {"app_index": app_index, "last_save_key": last_save_key, "last_session_id": last_session}
        return snapshots, next_cursor, app_index >= len(app_names)

    async def count_resources(self, app_names: list[str]) -> int:
        sessions = await self._pool.fetchval(
            "SELECT count(*) FROM sessions WHERE app_name=ANY($1::text[]) "
            "AND update_time + $2*interval '1 second' > clock_timestamp()", app_names, float(self._session_ttl))
        memories = await self._pool.fetchval(
            "SELECT count(DISTINCT (save_key,session_id)) FROM mem_events "
            "WHERE split_part(save_key,'/',1)=ANY($1::text[]) "
            "AND timestamp + $2*interval '1 second' > clock_timestamp()", app_names, float(self._memory_ttl))
        return int(sessions or 0) + int(memories or 0)

    async def close(self) -> None:
        if not self._owns_bundle:
            return
        await self._bundle.session_service.close()
        await self._bundle.memory_service.close()
        for guard in self._bundle.write_guards.values():
            await guard.close()

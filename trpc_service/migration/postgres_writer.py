"""PostgreSQL importer using SDK services for their native serialization."""

from __future__ import annotations

from typing import Any

from trpc_agent_sdk.sessions import Session
from trpc_agent_sdk.types import State

from trpc_service.config import BackendType, StoragePolicy
from trpc_service.storage import StorageProviderFactory
from trpc_service.storage.fencing import database_identity, hold_storage_guards, require_lease

from .postgres_reader import SdkPostgresSnapshotReader
from .snapshots import MemorySnapshot, SessionSnapshot


class SdkPostgresSnapshotWriter:
    """Idempotently replace one SDK Session/Memory record in PostgreSQL."""

    REQUIRED_SESSION_COLUMNS = {
        "app_name", "user_id", "id", "state", "historical_events", "conversation_count", "update_time"
    }
    REQUIRED_EVENT_COLUMNS = {
        "id", "app_name", "user_id", "session_id", "actions", "timestamp", "content", "request_id", "usage_metadata"
    }

    def __init__(self,
                 sql_url: str,
                 *,
                 pool: Any,
                 session_ttl_seconds: int,
                 memory_ttl_seconds: int,
                 storage_factory: StorageProviderFactory | None = None) -> None:
        policy = StoragePolicy(session=BackendType.SQL,
                               memory=BackendType.SQL,
                               sql_url=sql_url,
                               session_ttl_seconds=session_ttl_seconds,
                               memory_ttl_seconds=memory_ttl_seconds)
        self._bundle = (storage_factory or StorageProviderFactory()).create(policy)
        self._pool = pool
        self._identity = database_identity(sql_url)
        # SQLAlchemy returns PostgreSQL TIMESTAMP WITHOUT TIME ZONE values as
        # naive datetimes. The SDK public get_session() converts those with the
        # host's local timezone, which can incorrectly expire a still-valid UTC
        # row on Windows. Migration verification must be read-only anyway, so
        # share this bundle with the UTC-normalizing snapshot reader.
        self._reader = SdkPostgresSnapshotReader(
            sql_url,
            pool=pool,
            session_ttl_seconds=session_ttl_seconds,
            memory_ttl_seconds=memory_ttl_seconds,
            bundle=self._bundle,
        )

    async def validate_schema(self) -> None:
        # SQL SDK tables are created lazily on the first storage operation.
        # A migration validates the target before its first write, so explicitly
        # initialize both SDK engines first. This calls the installed SDK's
        # existing schema initialization and does not duplicate its DDL here.
        session_service = getattr(self._bundle.session_service, "_delegate", self._bundle.session_service)
        await session_service._sql_storage.create_sql_engine()
        await self._bundle.memory_service._sql_storage.create_sql_engine()

        async def columns(table: str) -> set[str]:
            rows = await self._pool.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema=current_schema() AND table_name=$1", table)
            return {row["column_name"] for row in rows}

        session_columns = await columns("sessions")
        event_columns = await columns("events")
        memory_columns = await columns("mem_events")
        if not self.REQUIRED_SESSION_COLUMNS <= session_columns:
            raise RuntimeError("incompatible SDK PostgreSQL sessions table")
        if not self.REQUIRED_EVENT_COLUMNS <= event_columns:
            raise RuntimeError("incompatible SDK PostgreSQL events table")
        if not {"id", "save_key", "session_id", "timestamp", "content"} <= memory_columns:
            raise RuntimeError("incompatible SDK PostgreSQL mem_events table")

    @staticmethod
    def _merged_state(snapshot: SessionSnapshot) -> dict[str, Any]:
        state = dict(snapshot.session_state)
        state.update({State.APP_PREFIX + key: value for key, value in snapshot.app_state.items()})
        state.update({State.USER_PREFIX + key: value for key, value in snapshot.user_state.items()})
        return state

    async def write_session(self, snapshot: SessionSnapshot, lock_key: str) -> None:
        async with hold_storage_guards(self._bundle.write_guards, lock_key):
            service = self._bundle.session_service
            # create_session() is an idempotent create-or-update in the SDK and
            # does not apply the timezone-sensitive public TTL read first.
            await service.create_session(app_name=snapshot.app_name,
                                         user_id=snapshot.user_id,
                                         session_id=snapshot.session_id,
                                         state=self._merged_state(snapshot))
            session = Session(id=snapshot.session_id,
                              app_name=snapshot.app_name,
                              user_id=snapshot.user_id,
                              state=dict(snapshot.session_state),
                              events=[event.model_copy(deep=True) for event in snapshot.events],
                              historical_events=[event.model_copy(deep=True) for event in snapshot.historical_events],
                              conversation_count=snapshot.conversation_count,
                              last_update_time=snapshot.last_update_time,
                              save_key=f"{snapshot.app_name}/{snapshot.user_id}")
            await service.update_session(session)
            # SDK public writes preserve Event timestamps but set the session
            # row's update time to now. Restore it while holding and verifying
            # the same native PostgreSQL fence used by the SDK write.
            lease = require_lease(self._identity)
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        "SELECT token,epoch,expires_at>clock_timestamp() AS valid "
                        "FROM platform_execution_lease WHERE lease_key=$1 FOR UPDATE", lease.key)
                    if (not row or row["token"] != lease.token or row["epoch"] != lease.epoch or not row["valid"]):
                        lease.lost.set()
                        raise RuntimeError("migration target fence rejected timestamp update")
                    await conn.execute(
                        "UPDATE sessions SET update_time=to_timestamp($4) "
                        "WHERE app_name=$1 AND user_id=$2 AND id=$3", snapshot.app_name, snapshot.user_id,
                        snapshot.session_id, snapshot.last_update_time)

    async def write_memory(self, snapshot: MemorySnapshot, lock_key: str) -> None:
        app_name, user_id = snapshot.save_key.split("/", 1)
        session = Session(id=snapshot.session_id,
                          app_name=app_name,
                          user_id=user_id,
                          state={},
                          events=[event.model_copy(deep=True) for event in snapshot.events],
                          save_key=snapshot.save_key)
        async with hold_storage_guards(self._bundle.write_guards, lock_key):
            await self._bundle.memory_service.store_session(session)

    async def read_session(self,
                           app_name: str,
                           user_id: str,
                           session_id: str,
                           lock_key: str = "") -> SessionSnapshot | None:
        key = lock_key or f"migration-read:{app_name}:{user_id}:{session_id}"
        async with hold_storage_guards(self._bundle.write_guards, key):
            return await self._reader.read_session(app_name, user_id, session_id)

    async def memory_event_ids(self, save_key: str, session_id: str) -> list[str]:
        rows = await self._pool.fetch(
            "SELECT id FROM mem_events WHERE save_key=$1 AND session_id=$2 ORDER BY timestamp,id", save_key, session_id)
        return [row["id"] for row in rows]

    async def count_resources(self, app_names: list[str]) -> int:
        sessions = await self._pool.fetchval("SELECT count(*) FROM sessions WHERE app_name=ANY($1::text[])", app_names)
        memories = await self._pool.fetchval(
            "SELECT count(DISTINCT (save_key,session_id)) FROM mem_events "
            "WHERE split_part(save_key,'/',1)=ANY($1::text[])", app_names)
        return int(sessions or 0) + int(memories or 0)

    async def close(self) -> None:
        await self._bundle.session_service.close()
        await self._bundle.memory_service.close()
        for guard in self._bundle.write_guards.values():
            await guard.close()

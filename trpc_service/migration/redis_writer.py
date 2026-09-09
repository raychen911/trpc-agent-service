"""Redis importer using the SDK services and platform-native fencing."""

from __future__ import annotations

import json
from typing import Any

from trpc_agent_sdk.sessions import Session

from trpc_service.config import BackendType, StoragePolicy
from trpc_service.storage import StorageProviderFactory
from trpc_service.storage.fencing import hold_storage_guards

from .redis_reader import SdkRedisSnapshotReader
from .snapshots import MemorySnapshot, SessionSnapshot
from .routing import _resource_key
from trpc_service.storage.keys import session_execution_key


class SdkRedisSnapshotWriter:
    """Idempotently replace one SDK Session or Memory resource in Redis."""

    def __init__(self,
                 redis_url: str,
                 *,
                 session_ttl_seconds: int,
                 memory_ttl_seconds: int,
                 storage_factory: StorageProviderFactory | None = None) -> None:
        policy = StoragePolicy(session=BackendType.REDIS,
                               memory=BackendType.REDIS,
                               redis_url=redis_url,
                               session_ttl_seconds=session_ttl_seconds,
                               memory_ttl_seconds=memory_ttl_seconds)
        self._bundle = (storage_factory or StorageProviderFactory()).create(policy)
        self._reader = SdkRedisSnapshotReader(redis_url)

    async def ping(self) -> bool:
        return await self._reader.ping()

    @staticmethod
    async def _apply_ttl(storage: Any, conn: Any, key: str, ttl: int) -> None:
        if ttl > 0:
            # FencedRedisStorage owns the Lua ownership check. Calling its
            # internal primitive here preserves the source's remaining TTL.
            await storage._write(conn, "expire", (key, ttl))

    async def write_session(self, snapshot: SessionSnapshot, lock_key: str) -> None:
        if snapshot.remaining_ttl_seconds == 0:
            return
        service = getattr(self._bundle.session_service, "_delegate", self._bundle.session_service)
        async with hold_storage_guards(self._bundle.write_guards, lock_key):
            current = await self.read_session(snapshot.app_name, snapshot.user_id, snapshot.session_id)
            if current is not None and current.source_updated_at > snapshot.source_updated_at:
                return
            session = Session(id=snapshot.session_id,
                              app_name=snapshot.app_name,
                              user_id=snapshot.user_id,
                              state=dict(snapshot.session_state),
                              events=[event.model_copy(deep=True) for event in snapshot.events],
                              historical_events=[event.model_copy(deep=True) for event in snapshot.historical_events],
                              conversation_count=snapshot.conversation_count,
                              last_update_time=snapshot.last_update_time,
                              save_key=f"{snapshot.app_name}/{snapshot.user_id}")
            storage = service._redis_storage
            async with storage.create_db_session() as conn:
                # Avoid RedisSessionService.create_session here: its default
                # connection returns hash keys as bytes, and recreating another
                # Session for the same app/user would mix bytes with str keys.
                if snapshot.app_state:
                    args = [f"app_state:{snapshot.app_name}"]
                    for key, value in snapshot.app_state.items():
                        args.extend([key, value])
                    await storage._write(conn, "hset", tuple(args))
                if snapshot.user_state:
                    args = [f"user_state:{snapshot.app_name}:{snapshot.user_id}"]
                    for key, value in snapshot.user_state.items():
                        args.extend([key, value])
                    await storage._write(conn, "hset", tuple(args))
                await service._set_session(conn, session)
                await self._apply_ttl(storage, conn,
                                      f"session:{snapshot.app_name}:{snapshot.user_id}:{snapshot.session_id}",
                                      snapshot.remaining_ttl_seconds)
                if snapshot.app_state:
                    await self._apply_ttl(storage, conn, f"app_state:{snapshot.app_name}",
                                          snapshot.app_state_remaining_ttl_seconds)
                if snapshot.user_state:
                    await self._apply_ttl(storage, conn, f"user_state:{snapshot.app_name}:{snapshot.user_id}",
                                          snapshot.user_state_remaining_ttl_seconds)

    async def write_memory(self, snapshot: MemorySnapshot, lock_key: str) -> None:
        if snapshot.remaining_ttl_seconds == 0:
            return
        app_name, user_id = snapshot.save_key.split("/", 1)
        session = Session(id=snapshot.session_id,
                          app_name=app_name,
                          user_id=user_id,
                          state={},
                          events=[event.model_copy(deep=True) for event in snapshot.events],
                          save_key=snapshot.save_key)
        async with hold_storage_guards(self._bundle.write_guards, lock_key):
            current = await self._read_memory(snapshot.save_key, snapshot.session_id)
            if current is not None and current.source_updated_at > snapshot.source_updated_at:
                return
            await self._bundle.memory_service.store_session(session)
            storage = self._bundle.memory_service._redis_storage
            async with storage.create_db_session() as conn:
                await self._apply_ttl(storage, conn, f"memory:{snapshot.save_key}:{snapshot.session_id}",
                                      snapshot.remaining_ttl_seconds)

    async def read_session(self, app_name: str, user_id: str, session_id: str) -> SessionSnapshot | None:
        snapshots, _, _ = await self._reader.scan_sessions([app_name], {}, 100000)
        return next((item for item in snapshots if item.user_id == user_id and item.session_id == session_id), None)

    async def memory_event_ids(self, save_key: str, session_id: str) -> list[str]:
        snapshot = await self._read_memory(save_key, session_id)
        return [event.id for event in snapshot.events] if snapshot else []

    async def _read_memory(self, save_key: str, session_id: str) -> MemorySnapshot | None:
        snapshots, _, _ = await self._reader.scan_memories([save_key.split("/", 1)[0]], {}, 100000)
        return next((item for item in snapshots if item.save_key == save_key and item.session_id == session_id), None)

    async def count_resources(self, app_names: list[str]) -> int:
        sessions, cursor, complete = await self._reader.scan_sessions(app_names, {}, 1000)
        while not complete:
            batch, cursor, complete = await self._reader.scan_sessions(app_names, cursor, 1000)
            sessions.extend(batch)
        memories, cursor, complete = await self._reader.scan_memories(app_names, {}, 1000)
        while not complete:
            batch, cursor, complete = await self._reader.scan_memories(app_names, cursor, 1000)
            memories.extend(batch)
        return len(sessions) + len(memories)

    async def quarantine_target_extras(self, job_id: str, tenant_id: str, app_names: list[str],
                                       expected: set[tuple[str, str]], control: Any) -> int:
        """Back up and remove only target records absent from the SQL source."""
        sessions, cursor, complete = await self._reader.scan_sessions(app_names, {}, 1000)
        while not complete:
            batch, cursor, complete = await self._reader.scan_sessions(app_names, cursor, 1000)
            sessions.extend(batch)
        memories, cursor, complete = await self._reader.scan_memories(app_names, {}, 1000)
        while not complete:
            batch, cursor, complete = await self._reader.scan_memories(app_names, cursor, 1000)
            memories.extend(batch)
        candidates = []
        for item in sessions:
            resource = _resource_key(item.app_name, item.user_id, item.session_id)
            if ("session", resource) not in expected:
                redis_key = f"session:{item.app_name}:{item.user_id}:{item.session_id}"
                candidates.append(("session", resource, item.app_name, item.session_id, redis_key))
        for item in memories:
            resource = _resource_key(item.save_key, item.session_id)
            if ("memory", resource) not in expected:
                app_name = item.save_key.split("/", 1)[0]
                redis_key = f"memory:{item.save_key}:{item.session_id}"
                candidates.append(("memory", resource, app_name, item.session_id, redis_key))
        removed = 0
        client = self._reader._redis
        for kind, resource, app_name, session_id, redis_key in candidates:
            prefix = f"tenant:{tenant_id}:app:"
            if not app_name.startswith(prefix):
                raise ValueError("refusing to quarantine a Redis key outside the tenant namespace")
            app_id = app_name[len(prefix):]
            lock_key = session_execution_key(tenant_id, app_id, session_id)
            async with hold_storage_guards(self._bundle.write_guards, lock_key):
                redis_type = await client.type(redis_key)
                if redis_type == "none":
                    continue
                if redis_type == "string":
                    payload = await client.get(redis_key)
                elif redis_type == "list":
                    payload = await client.lrange(redis_key, 0, -1)
                elif redis_type == "hash":
                    payload = await client.hgetall(redis_key)
                else:
                    raise RuntimeError(f"unsupported Redis target value type: {redis_type}")
                ttl_ms = int(await client.pttl(redis_key))
                await control.backup_redis_target(job_id, kind, resource, redis_key, redis_type, payload, ttl_ms)
                storage = (getattr(self._bundle.session_service, "_delegate",
                                   self._bundle.session_service)._redis_storage
                           if kind == "session" else self._bundle.memory_service._redis_storage)
                async with storage.create_db_session() as conn:
                    await storage.delete(conn, redis_key)
                removed += 1
        return removed

    async def restore_target_backups(self, job_id: str, control: Any) -> int:
        """Restore quarantined Redis keys while the tenant admission gate is closed."""
        restored = 0
        client = self._reader._redis
        for row in await control.target_backups(job_id):
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            key, kind = row["redis_key"], row["redis_type"]
            if kind == "string":
                await client.set(key, payload)
            elif kind == "list":
                if payload:
                    await client.rpush(key, *payload)
            elif kind == "hash":
                if payload:
                    await client.hset(key, mapping=payload)
            ttl_ms = int(row["ttl_milliseconds"])
            if ttl_ms > 0:
                await client.pexpire(key, ttl_ms)
            await control.mark_backup_restored(job_id, key)
            restored += 1
        return restored

    async def close(self) -> None:
        await self._reader.close()
        await self._bundle.session_service.close()
        await self._bundle.memory_service.close()
        for guard in self._bundle.write_guards.values():
            await guard.close()

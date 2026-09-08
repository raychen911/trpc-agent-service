# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""Real Redis/MySQL tenant data migration adapters.

The adapter deliberately uses the upstream session and memory services for
serialization and writes.  This keeps migrated rows compatible with the SDK
instead of inventing a second storage schema.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from typing import Optional

import redis.asyncio as async_redis
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import distinct
from sqlalchemy import select
from sqlalchemy.orm import Session as OrmSession

from trpc_agent_sdk.memory import RedisMemoryService
from trpc_agent_sdk.memory import SqlMemoryService
from trpc_agent_sdk.memory import MemStorageData
from trpc_agent_sdk.memory import MemStorageEvent
from trpc_agent_sdk.sessions import RedisSessionService
from trpc_agent_sdk.sessions import Session
from trpc_agent_sdk.sessions import SqlSessionService
from trpc_agent_sdk.sessions import StorageSession
from trpc_agent_sdk.sessions import extract_state_delta

from trpc_service.tenant import Tenant
from trpc_service.tenant._persistence import mysql_sync_url
from ._migration import MigrationBackend
from ._migration import MigrationReport
from ._migration import StorageRecord


def _digest(records: list[StorageRecord]) -> str:
    joined = "".join(sorted(record.checksum() for record in records)).encode()
    return hashlib.sha256(joined).hexdigest()


def _session_payload(session: Session) -> dict:
    """Canonical payload excluding backend-maintained timestamps."""

    def canonical_event(event) -> dict:
        payload = event.model_dump(mode="json", exclude_none=True)
        # SQL materialises the nullable set as an empty set while Redis keeps
        # None. They are semantically identical, so checksum the canonical []
        # representation on both sides.
        payload["long_running_tool_ids"] = sorted(event.long_running_tool_ids or [])
        return payload

    return {
        "app_name": session.app_name,
        "user_id": session.user_id,
        "id": session.id,
        "state": session.state,
        "events": [canonical_event(event) for event in session.events],
        "historical_events": [canonical_event(event) for event in session.historical_events],
        "conversation_count": session.conversation_count,
    }


def _payload_session(payload: dict) -> Session:
    return Session.model_validate({
        **payload,
        "last_update_time": 0.0,
        "save_key": f"{payload['app_name']}/{payload['user_id']}",
    })


class TenantBackendMigrationAdapter(MigrationBackend):
    """Scan and upsert tenant Session/Memory records in Redis or MySQL."""

    def __init__(self, tenant: Tenant, backend: str) -> None:
        if backend not in {"redis", "mysql"}:
            raise ValueError(f"unsupported migration backend: {backend}")
        self.tenant_id = tenant.tenant_id
        self.backend_name = backend
        self._redis = None
        self._sql_engine = None
        if backend == "redis":
            redis_url = (tenant.storage_config.redis_url.get_secret_value() if tenant.storage_config.redis_url else "")
            if not redis_url:
                raise ValueError("Redis URL is not configured for tenant migration")
            self._session_service = RedisSessionService(db_url=redis_url)
            self._memory_service = RedisMemoryService(db_url=redis_url, enabled=True)
            self._redis = async_redis.from_url(redis_url, decode_responses=True)
        else:
            mysql_url = (tenant.storage_config.mysql_url.get_secret_value() if tenant.storage_config.mysql_url else "")
            if not mysql_url:
                raise ValueError("MySQL URL is not configured for tenant migration")
            sync_url = mysql_sync_url(mysql_url)
            self._session_service = SqlSessionService(db_url=sync_url, is_async=False)
            self._memory_service = SqlMemoryService(db_url=sync_url, is_async=False, enabled=True)
            self._sql_engine = create_engine(sync_url, pool_pre_ping=True)
            MemStorageData.metadata.create_all(self._sql_engine)

    async def scan(self, tenant_id: str, kind: str, cursor: Optional[str],
                   limit: int) -> tuple[list[StorageRecord], Optional[str]]:
        if tenant_id != self.tenant_id:
            raise ValueError("migration tenant scope mismatch")
        if kind == "session":
            records = await self._scan_sessions()
        elif kind == "memory":
            records = await self._scan_memories()
        else:
            raise ValueError(f"unsupported migration kind: {kind}")
        offset = int(cursor or 0)
        batch = records[offset:offset + limit]
        next_offset = offset + len(batch)
        return batch, str(next_offset) if next_offset < len(records) else None

    async def upsert(self, record: StorageRecord) -> None:
        if record.tenant_id != self.tenant_id:
            raise ValueError("migration record is outside tenant scope")
        session = _payload_session(record.payload)
        if record.kind == "session":
            await self._upsert_session(session)
        elif record.kind == "memory":
            await self._replace_memory(session)
        else:
            raise ValueError(f"unsupported migration kind: {record.kind}")

    async def _discover_app_names(self) -> list[str]:
        prefix = f"{self.tenant_id}:"
        if self.backend_name == "redis":
            names = set()
            async for key in self._redis.scan_iter(match=f"session:{self.tenant_id}:*"):
                raw = await self._redis.get(key)
                if raw:
                    names.add(Session.model_validate_json(raw).app_name)
            return sorted(names)

        def query() -> list[str]:
            with OrmSession(self._sql_engine) as db:
                rows = db.execute(
                    select(distinct(StorageSession.app_name)).where(
                        StorageSession.app_name.like(f"{prefix}%"))).scalars().all()
                return sorted(rows)

        return await asyncio.to_thread(query)

    async def _scan_sessions(self) -> list[StorageRecord]:
        records = []
        for app_name in await self._discover_app_names():
            listed = await self._session_service.list_sessions(app_name=app_name)
            for item in listed.sessions:
                session = await self._session_service.get_session(app_name=app_name,
                                                                  user_id=item.user_id,
                                                                  session_id=item.id)
                if session is None:
                    continue
                payload = _session_payload(session)
                records.append(
                    StorageRecord(
                        tenant_id=self.tenant_id,
                        kind="session",
                        record_id=f"{app_name}/{session.user_id}/{session.id}",
                        payload=payload,
                    ))
        return sorted(records, key=lambda record: record.record_id)

    async def _scan_memories(self) -> list[StorageRecord]:
        if self.backend_name == "redis":
            records = []
            async for key in self._redis.scan_iter(match=f"memory:{self.tenant_id}:*"):
                values = await self._redis.lrange(key, 0, -1)
                if not values:
                    continue
                scoped, session_id = key.removeprefix("memory:").rsplit(":", 1)
                app_name, user_id = scoped.rsplit("/", 1)
                session = Session(
                    app_name=app_name,
                    user_id=user_id,
                    id=session_id,
                    events=[self._event_from_json(value) for value in values],
                    save_key=scoped,
                )
                records.append(
                    StorageRecord(
                        tenant_id=self.tenant_id,
                        kind="memory",
                        record_id=f"{scoped}/{session_id}",
                        payload=_session_payload(session),
                    ))
            return sorted(records, key=lambda record: record.record_id)
        return await asyncio.to_thread(self._scan_sql_memories)

    @staticmethod
    def _event_from_json(value: str):
        from trpc_agent_sdk.events import Event

        return Event.model_validate_json(value)

    def _scan_sql_memories(self) -> list[StorageRecord]:
        grouped = defaultdict(list)
        with OrmSession(self._sql_engine) as db:
            rows = db.execute(select(MemStorageEvent).where(
                MemStorageEvent.save_key.like(f"{self.tenant_id}:%"))).scalars().all()
            for row in rows:
                grouped[(row.save_key, row.session_id)].append(row.to_event())
        records = []
        for (save_key, session_id), events in grouped.items():
            app_name, user_id = save_key.rsplit("/", 1)
            events.sort(key=lambda event: (event.timestamp, event.id))
            session = Session(
                app_name=app_name,
                user_id=user_id,
                id=session_id,
                events=events,
                save_key=save_key,
            )
            records.append(
                StorageRecord(
                    tenant_id=self.tenant_id,
                    kind="memory",
                    record_id=f"{save_key}/{session_id}",
                    payload=_session_payload(session),
                ))
        return sorted(records, key=lambda record: record.record_id)

    async def _upsert_session(self, source: Session) -> None:
        existing = await self._session_service.get_session(app_name=source.app_name,
                                                           user_id=source.user_id,
                                                           session_id=source.id)
        if existing is None:
            existing = await self._session_service.create_session(
                app_name=source.app_name,
                user_id=source.user_id,
                session_id=source.id,
                state=source.state,
            )
        # create_session already separated app/user-prefixed state. Preserve
        # only the session-scoped view when replacing the session row.
        existing.state = extract_state_delta(source.state).session_state
        existing.events = source.events
        existing.historical_events = source.historical_events
        existing.conversation_count = source.conversation_count
        await self._session_service.update_session(existing)

    async def _replace_memory(self, session: Session) -> None:
        if self.backend_name == "mysql":

            def remove_existing() -> None:
                with OrmSession(self._sql_engine) as db:
                    db.execute(
                        delete(MemStorageEvent).where(
                            MemStorageEvent.save_key == session.save_key,
                            MemStorageEvent.session_id == session.id,
                        ))
                    db.commit()

            await asyncio.to_thread(remove_existing)
        await self._memory_service.store_session(session)

    async def close(self) -> None:
        await self._session_service.close()
        await self._memory_service.close()
        if self._redis is not None:
            await self._redis.aclose()
        if self._sql_engine is not None:
            self._sql_engine.dispose()


class TenantDataMigrator:
    """Merge-copy source records and verify every copied ID on the target."""

    def __init__(self, source: MigrationBackend, target: MigrationBackend, batch_size: int = 200) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.source = source
        self.target = target
        self.batch_size = batch_size

    async def _all(self, backend: MigrationBackend, tenant_id: str, kind: str) -> list[StorageRecord]:
        records = []
        cursor = None
        while True:
            batch, cursor = await backend.scan(tenant_id, kind, cursor, self.batch_size)
            records.extend(batch)
            if cursor is None:
                return records

    async def migrate(self, tenant_id: str, kinds: list[str]) -> MigrationReport:
        copied = {}
        source_checksums = {}
        target_checksums = {}
        verified = True
        for kind in kinds:
            source_records = await self._all(self.source, tenant_id, kind)
            for record in source_records:
                await self.target.upsert(record)
            # Re-scan the source after the copy. A concurrent source write must
            # make verification fail instead of being silently omitted during
            # the cutover window.
            latest_source = await self._all(self.source, tenant_id, kind)
            target_records = await self._all(self.target, tenant_id, kind)
            source_ids = {record.record_id for record in latest_source}
            matched_target = [record for record in target_records if record.record_id in source_ids]
            copied[kind] = len(source_records)
            source_checksums[kind] = _digest(latest_source)
            target_checksums[kind] = _digest(matched_target)
            verified = verified and source_checksums[kind] == target_checksums[kind]
        return MigrationReport(
            tenant_id=tenant_id,
            copied_by_kind=copied,
            source_checksums=source_checksums,
            target_checksums=target_checksums,
            verified=verified,
        )

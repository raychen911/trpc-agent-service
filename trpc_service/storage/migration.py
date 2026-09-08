import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.storage.artifacts import LocalArtifactStore, MinioArtifactStore
from trpc_service.storage.contracts import VectorDocument, VectorStore
from trpc_service.storage.models import AgentSession, Memory
from trpc_service.storage.vector import SemanticStore


@dataclass(frozen=True, slots=True)
class MigrationReport:
    scanned: int = 0
    migrated: int = 0
    skipped: int = 0
    failed: int = 0


class RedisToSqlSessionMigrator:
    def __init__(
        self,
        redis: Redis,
        sql_factory: sessionmaker[Session],
        key_prefix: str = "trpc",
    ) -> None:
        self._redis = redis
        self._factory = sql_factory
        self._pattern = f"{key_prefix.rstrip(':')}:hot:*"

    async def run(self, *, overwrite: bool = False) -> MigrationReport:
        scanned = migrated = skipped = failed = 0
        async for key in self._redis.scan_iter(match=self._pattern, count=500):
            scanned += 1
            try:
                raw = await self._redis.get(key)
                if raw is None:
                    skipped += 1
                    continue
                payload = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                changed = await asyncio.to_thread(self._write, payload, overwrite)
                migrated += int(changed)
                skipped += int(not changed)
            except Exception:
                failed += 1
        return MigrationReport(scanned, migrated, skipped, failed)

    def _write(self, payload: dict[str, Any], overwrite: bool) -> bool:
        with self._factory.begin() as session:
            row = session.scalar(
                select(AgentSession).where(
                    AgentSession.tenant_id == payload["tenant_id"],
                    AgentSession.agent_app_id == payload["agent_app_id"],
                    AgentSession.session_key == payload["session_id"],
                )
            )
            source_version = int(payload["version"])
            if row is not None and (not overwrite or row.version > source_version):
                return False
            if row is None:
                row = AgentSession(
                    tenant_id=payload["tenant_id"],
                    agent_app_id=payload["agent_app_id"],
                    user_id=payload["user_id"],
                    session_key=payload["session_id"],
                )
                session.add(row)
            row.state = dict(payload["state"])
            row.version = source_version
            return True


class LocalToMinioArtifactMigrator:
    def __init__(
        self,
        local_root: str | Path,
        destination: MinioArtifactStore,
    ) -> None:
        self._root = Path(local_root).resolve()
        self._source = LocalArtifactStore(self._root)
        self._destination = destination

    async def run(self) -> MigrationReport:
        metadata_files = list(self._root.rglob("*.metadata.json"))
        migrated = failed = 0
        for metadata_path in metadata_files:
            try:
                payload = json.loads(metadata_path.read_text(encoding="utf-8"))
                artifact = await self._source.get(
                    str(payload["tenant_id"]), str(payload["object_key"])
                )
                stored = await self._destination.put(
                    artifact.metadata.tenant_id,
                    artifact.metadata.object_key,
                    artifact.content,
                    artifact.metadata.mime_type,
                    artifact.metadata.metadata,
                )
                if stored.checksum != artifact.metadata.checksum:
                    raise OSError("destination checksum mismatch")
                migrated += 1
            except Exception:
                failed += 1
        return MigrationReport(len(metadata_files), migrated, 0, failed)


class SqlMemoryToVectorMigrator:
    """Rebuild a remote vector index from durable SQL Memory facts."""

    def __init__(
        self,
        sql_factory: sessionmaker[Session],
        destination: VectorStore,
        batch_size: int = 200,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self._factory = sql_factory
        self._destination = destination
        self._batch_size = batch_size

    async def run(self, *, tenant_id: str | None = None) -> MigrationReport:
        scanned = migrated = failed = 0
        offset = 0
        while True:
            rows = await asyncio.to_thread(self._read_batch, offset, tenant_id)
            if not rows:
                break
            scanned += len(rows)
            documents = [
                VectorDocument(
                    id=row.memory_key,
                    namespace=SemanticStore.memory_namespace(
                        row.tenant_id, row.agent_app_id, row.user_id
                    ),
                    text=row.content,
                    metadata={
                        "memory_key": row.memory_key,
                        "topics": list(row.topics),
                        "version": row.version,
                    },
                )
                for row in rows
            ]
            try:
                await self._destination.upsert(documents)
                migrated += len(documents)
            except Exception:
                failed += len(documents)
            offset += len(rows)
        return MigrationReport(scanned, migrated, 0, failed)

    def _read_batch(self, offset: int, tenant_id: str | None) -> list[Memory]:
        with self._factory() as session:
            statement = select(Memory).order_by(Memory.id).offset(offset).limit(self._batch_size)
            if tenant_id is not None:
                statement = statement.where(Memory.tenant_id == tenant_id)
            rows = list(session.scalars(statement))
            for row in rows:
                session.expunge(row)
            return rows

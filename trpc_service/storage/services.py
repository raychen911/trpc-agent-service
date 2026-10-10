"""Tenant-selected storage services and PostgreSQL-backed implementations.

The runtime repository remains the authority for session turns and fencing.
The smaller services in this module expose the other tenant-scoped records so
that callers do not need to know which backend a storage profile selected.
Every PostgreSQL operation sets ``app.tenant_id`` in the transaction before
touching a tenant table; this is required by the RLS policies in the initial
schema migration.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID, uuid4

import asyncpg
from pydantic import ValidationError
from trpc_agent_sdk.sessions import BaseSessionService

from trpc_service.storage.models import SessionSnapshot, SummarySnapshot
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import (
    ArtifactStore,
    AuditStore,
    KnowledgeStore,
    MemoryStore,
    RuntimeRepository,
    SessionStore,
    SummaryStore,
)
from trpc_service.tenant.models import (
    ProductionStorageSelection,
    StorageSelection,
    TenantConfig,
    TenantContext,
)

JsonObject = dict[str, object]
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TenantDataServices:
    """Immutable service bundle selected for one tenant configuration."""

    session: SessionStore
    memory: MemoryStore
    summary: SummaryStore
    artifact: ArtifactStore
    knowledge: KnowledgeStore
    audit: AuditStore


class TenantServiceFactory(Protocol):
    async def for_context(
        self, context: TenantContext, config: TenantConfig
    ) -> TenantDataServices: ...


@dataclass(frozen=True, slots=True)
class RegisteredTenantServiceBundle:
    """One deployment-built bundle bound to an immutable storage selection."""

    selection: StorageSelection
    services: TenantDataServices


class TenantStorageProfileRegistry:
    """Resolve only explicitly registered tenant/profile service bundles.

    The registry stores constructed services, never connection strings or
    secret references.  A registration is exact to one tenant and to the full
    pinned ``StorageSelection`` so a later configuration revision cannot reuse
    a bundle under the same profile id with different backend semantics.
    """

    def __init__(
        self,
        profiles: Mapping[tuple[str, str], RegisteredTenantServiceBundle],
    ) -> None:
        validated: dict[tuple[str, str], RegisteredTenantServiceBundle] = {}
        for key, registration in profiles.items():
            if (
                not isinstance(key, tuple)
                or len(key) != 2
                or not all(isinstance(value, str) and value.strip() for value in key)
            ):
                raise ValueError("tenant storage profile registry key is invalid")
            tenant_id, profile_id = key
            if registration.selection.profile_id != profile_id:
                raise ValueError("registered storage profile id does not match its registry key")
            validated[(tenant_id, profile_id)] = registration
        self._profiles = validated

    def resolve(self, context: TenantContext, config: TenantConfig) -> TenantDataServices | None:
        if context.tenant_id != config.tenant_id:
            raise ValueError("storage configuration belongs to another tenant")
        registration = self._profiles.get((context.tenant_id, config.storage.profile_id))
        if registration is None:
            return None
        if registration.selection.model_dump() != config.storage.model_dump():
            raise LookupError(
                "registered tenant storage profile does not match the pinned configuration"
            )
        return registration.services


class ProfileServiceFactory(TenantStorageProfileRegistry):
    """Backward-compatible exact tenant/profile factory.

    Global profile-name fallback was intentionally removed: every registration
    includes the tenant id and the complete immutable storage selection.
    """

    async def for_context(self, context: TenantContext, config: TenantConfig) -> TenantDataServices:
        registered = self.resolve(context, config)
        if registered is None:
            raise LookupError("tenant storage profile is unavailable")
        return registered


class CompositeTenantServiceFactory:
    """Route an exact registered profile or use the production default factory."""

    def __init__(
        self,
        registry: TenantStorageProfileRegistry,
        default_factory: TenantServiceFactory,
    ) -> None:
        self._registry = registry
        self._default_factory = default_factory

    async def for_context(self, context: TenantContext, config: TenantConfig) -> TenantDataServices:
        registered = self._registry.resolve(context, config)
        if registered is not None:
            return registered
        return await self._default_factory.for_context(context, config)


class PostgresSessionStore:
    """Session read service backed by the authoritative runtime repository."""

    def __init__(self, repository: RuntimeRepository) -> None:
        self._repository = repository

    async def get_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        return await self._repository.get_session_snapshot(tenant_id, session_id)

    def open_turn(self, snapshot: SessionSnapshot) -> BaseSessionService:
        """Return the SDK session facade used by a fenced, buffered turn."""
        from trpc_service.agent.session import TurnBufferSessionService

        return TurnBufferSessionService(snapshot)


class _PostgresTenantStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @asynccontextmanager
    async def _transaction(self, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            yield connection


def _dump(value: Mapping[str, object]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"))


def _decode(value: object) -> JsonObject:
    if isinstance(value, str):
        decoded = json.loads(value)
    else:
        decoded = value
    if not isinstance(decoded, dict):
        raise ValueError("stored JSON value is not an object")
    return dict(decoded)


class PostgresMemoryStore(_PostgresTenantStore):
    """Authoritative memory table access; vector projection is separate."""

    async def put(
        self,
        tenant_id: str,
        principal_id: str,
        value: Mapping[str, object],
        *,
        memory_id: str | None = None,
        session_id: str | None = None,
        source_sequence: int | None = None,
    ) -> str:
        memory_uuid = UUID(memory_id) if memory_id else uuid4()
        async with self._transaction(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO memories (
                    tenant_id,memory_id,principal_id,session_id,source_sequence,memory_json
                ) VALUES ($1,$2,$3,$4,$5,$6::jsonb)
                ON CONFLICT (tenant_id,memory_id)
                DO UPDATE SET principal_id=excluded.principal_id,
                              session_id=excluded.session_id,
                              source_sequence=excluded.source_sequence,
                              memory_json=excluded.memory_json
                 WHERE (
                    memories.source_sequence IS NULL
                    AND excluded.source_sequence IS NOT NULL
                 ) OR (
                    memories.source_sequence IS NOT NULL
                    AND excluded.source_sequence IS NOT NULL
                    AND excluded.source_sequence > memories.source_sequence
                 ) OR (
                    memories.source_sequence IS NOT DISTINCT FROM excluded.source_sequence
                    AND memories.memory_json = excluded.memory_json
                 )
                """,
                tenant_id,
                memory_uuid,
                principal_id,
                session_id,
                source_sequence,
                _dump(value),
            )
        return str(memory_uuid)

    async def list_recent(
        self, tenant_id: str, principal_id: str, *, limit: int = 100
    ) -> tuple[JsonObject, ...]:
        if limit < 1 or limit > 1000:
            raise ValueError("memory limit must be between 1 and 1000")
        async with self._transaction(tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT memory_id,session_id,source_sequence,memory_json,created_at
                  FROM memories
                 WHERE tenant_id=$1 AND principal_id=$2
                 ORDER BY created_at DESC, memory_id DESC
                 LIMIT $3
                """,
                tenant_id,
                principal_id,
                limit,
            )
        return tuple(
            {
                "memory_id": str(row["memory_id"]),
                "session_id": row["session_id"],
                "source_sequence": row["source_sequence"],
                "memory": _decode(row["memory_json"]),
                "created_at": row["created_at"].isoformat()
                if isinstance(row["created_at"], datetime)
                else str(row["created_at"]),
            }
            for row in rows
        )


class PostgresSummaryStore(_PostgresTenantStore):
    """Monotonic summary records with a version and sequence CAS."""

    async def get(self, tenant_id: str, session_id: str) -> SummarySnapshot | None:
        async with self._transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT up_to_sequence,summary_json,version
                  FROM session_summaries
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                tenant_id,
                session_id,
            )
        if row is None:
            return None
        return SummarySnapshot(
            tenant_id=tenant_id,
            session_id=session_id,
            up_to_sequence=row["up_to_sequence"],
            summary=_decode(row["summary_json"]),
            version=row["version"],
        )

    async def put(
        self,
        tenant_id: str,
        session_id: str,
        *,
        up_to_sequence: int,
        summary: Mapping[str, object],
        expected_version: int | None = None,
    ) -> bool:
        if up_to_sequence < 0:
            raise ValueError("summary sequence cannot be negative")
        async with self._transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO session_summaries (
                    tenant_id,session_id,up_to_sequence,summary_json
                ) VALUES ($1,$2,$3,$4::jsonb)
                ON CONFLICT (tenant_id,session_id) DO UPDATE
                   SET up_to_sequence=excluded.up_to_sequence,
                       summary_json=excluded.summary_json,
                       version=session_summaries.version+1,
                       updated_at=now()
                   WHERE session_summaries.up_to_sequence <= excluded.up_to_sequence
                    AND (
                        session_summaries.up_to_sequence < excluded.up_to_sequence
                        OR session_summaries.summary_json = excluded.summary_json
                    )
                   AND ($5::bigint IS NULL OR session_summaries.version=$5)
                RETURNING version
                """,
                tenant_id,
                session_id,
                up_to_sequence,
                _dump(summary),
                expected_version,
            )
        return row is not None


class PostgresArtifactStore:
    """Persist artifact metadata around an S3-compatible staged object store."""

    def __init__(self, pool: asyncpg.Pool, objects: ArtifactStore) -> None:
        self._db = _PostgresTenantStore(pool)
        self._objects = objects

    async def stage(
        self, tenant_id: str, artifact_id: str, content: bytes, *, checksum: str
    ) -> str:
        staged_key: str | None = None
        existing_key: str | None = None
        try:
            async with self._db._transaction(tenant_id) as connection:
                existing = await connection.fetchrow(
                    """
                    SELECT object_key,checksum,size_bytes,status
                      FROM artifacts
                     WHERE tenant_id=$1 AND artifact_id=$2
                     FOR UPDATE
                    """,
                    tenant_id,
                    artifact_id,
                )
                if existing is not None:
                    if existing["checksum"] != checksum:
                        raise ValueError("artifact id conflicts with a different checksum")
                    existing_key = existing["object_key"]
                    if existing["status"] == "committed":
                        return str(existing_key)
                    if existing["status"] != "staged":
                        raise ValueError("artifact is not available for staging")
                staged_key = await self._objects.stage(
                    tenant_id, artifact_id, content, checksum=checksum
                )
                status = await connection.execute(
                    """
                    INSERT INTO artifacts (
                        tenant_id,artifact_id,object_key,checksum,size_bytes,status
                    ) VALUES ($1,$2,$3,$4,$5,'staged')
                    ON CONFLICT (tenant_id,artifact_id) DO UPDATE
                       SET object_key=excluded.object_key,size_bytes=excluded.size_bytes,
                           status='staged',created_at=clock_timestamp()
                     WHERE artifacts.status='staged'
                       AND artifacts.checksum=excluded.checksum
                    """,
                    tenant_id,
                    artifact_id,
                    staged_key,
                    checksum,
                    len(content),
                )
                if status != "INSERT 0 1" and status != "UPDATE 1":
                    raise RuntimeError("artifact stage CAS was lost")
            if existing_key is not None and staged_key != existing_key:
                try:
                    # Replacing a staged row leaves its previous temporary
                    # object unreferenced.  Best-effort cleanup here keeps the
                    # crash-recovery path safe while allowing a later orphan
                    # sweep to handle provider outages.
                    await self._objects.discard(existing_key)
                except Exception as error:
                    _LOGGER.warning(
                        "artifact staging orphan cleanup failed",
                        extra={"error_type": type(error).__name__},
                    )
            return staged_key
        except BaseException:
            # A provider upload can complete before the metadata transaction
            # fails.  Remove only the newly allocated key; never discard the
            # key that was already recorded for a pre-existing staged row.
            if staged_key is not None and staged_key != existing_key:
                try:
                    await self._objects.discard(staged_key)
                except Exception as error:
                    _LOGGER.warning(
                        "artifact staging cleanup failed",
                        extra={"error_type": type(error).__name__},
                    )
            raise

    async def commit(self, tenant_id: str, artifact_id: str, staged_key: str) -> str:
        async with self._db._transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT checksum,size_bytes,status,object_key FROM artifacts
                 WHERE tenant_id=$1 AND artifact_id=$2
                 FOR UPDATE
                """,
                tenant_id,
                artifact_id,
            )
            if row is None:
                raise LookupError("staged artifact metadata is unavailable")
            object_key = row.get("object_key") if hasattr(row, "get") else row["object_key"]
            if row["status"] == "committed":
                # A retry may still hold the pre-commit temporary key.  Once
                # the row is committed its object key is authoritative and
                # immutable; returning it makes the operation crash-recoverable
                # without allowing a second checksum/object to replace it.
                return str(object_key or staged_key)
            if object_key is not None and object_key != staged_key:
                raise LookupError("staged artifact key is no longer current")
            checksum = str(row["checksum"])
            # Keep the metadata row lock while finalizing the object.  If the
            # lock were released during the provider call, a concurrent stage
            # could replace the temporary key after this read and make the
            # commit race an orphaned-object/metadata mismatch.
            target = await self._objects.commit(tenant_id, artifact_id, staged_key)
            status = await connection.execute(
                """
                UPDATE artifacts SET object_key=$3,status='committed'
                 WHERE tenant_id=$1 AND artifact_id=$2 AND object_key=$4
                   AND checksum=$5 AND status='staged'
                """,
                tenant_id,
                artifact_id,
                target,
                staged_key,
                checksum,
            )
            if status != "UPDATE 1":
                current = await connection.fetchrow(
                    """
                    SELECT object_key,status,checksum
                      FROM artifacts
                     WHERE tenant_id=$1 AND artifact_id=$2
                    """,
                    tenant_id,
                    artifact_id,
                )
                if (
                    not current
                    or current["status"] != "committed"
                    or current["checksum"] != checksum
                ):
                    raise RuntimeError("artifact commit CAS was lost")
                return str(current["object_key"])
            return target

    async def discard(self, staged_key: str) -> None:
        await self._objects.discard(staged_key)

    async def discard_for_tenant(self, tenant_id: str, artifact_id: str, staged_key: str) -> None:
        await self._objects.discard(staged_key)
        async with self._db._transaction(tenant_id) as connection:
            await connection.execute(
                """
                UPDATE artifacts SET status='deleted'
                 WHERE tenant_id=$1 AND artifact_id=$2 AND object_key=$3
                """,
                tenant_id,
                artifact_id,
                staged_key,
            )


class _UnavailableArtifactStore:
    """Explicit failure when a profile omitted its S3-compatible object store."""

    async def stage(
        self, _tenant_id: str, _artifact_id: str, _content: bytes, *, checksum: str
    ) -> str:
        raise LookupError("S3 artifact object store is not configured")

    async def commit(self, _tenant_id: str, _artifact_id: str, _staged_key: str) -> str:
        raise LookupError("S3 artifact object store is not configured")

    async def discard(self, _staged_key: str) -> None:
        raise LookupError("S3 artifact object store is not configured")


class PostgresKnowledgeStore(_PostgresTenantStore):
    """Persist knowledge metadata and its pgvector projection atomically."""

    def __init__(self, pool: asyncpg.Pool, *, profile_id: str, dimension: int = 1536) -> None:
        super().__init__(pool)
        if dimension != 1536:
            raise ValueError("knowledge embedding dimension must be exactly 1536")
        self._profile_id = profile_id
        self._dimension = dimension

    async def upsert(
        self,
        tenant_id: str,
        item_id: str,
        embedding: list[float],
        metadata: Mapping[str, object],
    ) -> None:
        if len(embedding) != self._dimension:
            raise ValueError(f"embedding dimension must be {self._dimension}")
        chunk_id = str(metadata.get("chunk_id", "0"))
        source_uri = metadata.get("source_uri")
        content_checksum = metadata.get("content_checksum")
        if not isinstance(content_checksum, str) or not content_checksum:
            content_checksum = hashlib.sha256(_dump(metadata).encode()).hexdigest()
        vector = "[" + ",".join(format(value, ".9g") for value in embedding) + "]"
        async with self._transaction(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO knowledge_items (
                    tenant_id,item_id,profile_id,source_uri,content_checksum,metadata_json,
                    projection_status
                ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,'projected')
                ON CONFLICT (tenant_id,item_id) DO UPDATE
                   SET profile_id=excluded.profile_id,source_uri=excluded.source_uri,
                       content_checksum=excluded.content_checksum,
                       metadata_json=excluded.metadata_json,
                       projection_status='projected',updated_at=now()
                """,
                tenant_id,
                item_id,
                self._profile_id,
                source_uri,
                content_checksum,
                _dump(metadata),
            )
            await connection.execute(
                """
                INSERT INTO knowledge_embeddings (
                    tenant_id,item_id,chunk_id,embedding,metadata_json
                ) VALUES ($1,$2,$3,$4::vector,$5::jsonb)
                ON CONFLICT (tenant_id,item_id,chunk_id) DO UPDATE
                   SET embedding=excluded.embedding,metadata_json=excluded.metadata_json,
                       created_at=now()
                """,
                tenant_id,
                item_id,
                chunk_id,
                vector,
                _dump(metadata),
            )

    async def search(
        self,
        tenant_id: str,
        embedding: list[float],
        *,
        limit: int = 5,
    ) -> tuple[JsonObject, ...]:
        """Return nearest tenant-owned knowledge chunks.

        The tenant predicate is intentionally repeated on the embedding table
        rather than relying only on the item foreign key.  This keeps the
        method safe for both shared profiles and PostgreSQL RLS deployments.
        ``embedding`` is supplied by an injected query provider; this store
        never manufactures a vector from user text.
        """

        if len(embedding) != self._dimension:
            raise ValueError(f"embedding dimension must be {self._dimension}")
        if limit < 1 or limit > 100:
            raise ValueError("knowledge result limit must be between 1 and 100")
        vector = "[" + ",".join(format(value, ".9g") for value in embedding) + "]"
        async with self._transaction(tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT embedding.item_id,embedding.chunk_id,embedding.metadata_json,
                       1 - (embedding.embedding <=> $3::vector) AS score
                  FROM knowledge_embeddings AS embedding
                  JOIN knowledge_items AS item
                    ON item.tenant_id=embedding.tenant_id
                   AND item.item_id=embedding.item_id
                 WHERE embedding.tenant_id=$1
                   AND item.profile_id=$2
                 ORDER BY embedding.embedding <=> $3::vector
                 LIMIT $4
                """,
                tenant_id,
                self._profile_id,
                vector,
                limit,
            )
        return tuple(
            {
                "item_id": str(row["item_id"]),
                "chunk_id": str(row["chunk_id"]),
                "score": float(row["score"]),
                "metadata": _decode(row["metadata_json"]),
            }
            for row in rows
        )


class PostgresAuditStore(_PostgresTenantStore):
    """Structured PostgreSQL audit records with no message/tool content."""

    async def append(
        self,
        tenant_id: str,
        *,
        decision: str,
        trace_id: str,
        channel: str | None = None,
        user_id: str | None = None,
        session_id: str | None = None,
        agent_name: str | None = None,
        tool_name: str | None = None,
        latency_ms: int | None = None,
        error_type: str | None = None,
        cost_units: int = 0,
        config_version: int | None = None,
        policy_version: int | None = None,
        idempotency_key: str | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> str:
        if cost_units < 0:
            raise ValueError("audit cost cannot be negative")
        async with self._transaction(tenant_id) as connection:
            audit_id = await connection.fetchval(
                """
                INSERT INTO audit_logs (
                    tenant_id,channel,user_id,session_id,agent_name,tool_name,decision,
                    latency_ms,error_type,cost_units,trace_id,config_version,policy_version,
                    idempotency_key,redaction_applied,metadata_json
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,true,$15::jsonb)
                RETURNING audit_id
                """,
                tenant_id,
                channel,
                user_id,
                session_id,
                agent_name,
                tool_name,
                decision,
                latency_ms,
                error_type,
                cost_units,
                trace_id,
                config_version,
                policy_version,
                idempotency_key,
                _dump(metadata or {}),
            )
        if audit_id is None:
            raise RuntimeError("audit insert did not return an id")
        return str(audit_id)


class PostgresTenantServiceFactory:
    """Build one tenant-scoped service bundle over shared PostgreSQL.

    This factory is deliberately narrow.  It must not silently replace a
    tenant's configured backend with PostgreSQL: alternative selections are
    resolved by :class:`TenantStorageProfileRegistry` using a pre-registered
    exact tenant/profile bundle.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        repository: RuntimeRepository | None = None,
        artifact_objects: ArtifactStore | None = None,
        profile_dimensions: Mapping[str, int] | None = None,
    ) -> None:
        self._pool = pool
        self._repository = repository or PostgresRuntimeRepository(pool)
        self._artifact_objects = artifact_objects
        self._profile_dimensions = dict(profile_dimensions or {})

    async def for_context(self, context: TenantContext, config: TenantConfig) -> TenantDataServices:
        if context.tenant_id != config.tenant_id:
            raise ValueError("storage configuration belongs to another tenant")
        return self.build_bundle(config.storage)

    def build_bundle(self, selection: StorageSelection) -> TenantDataServices:
        """Build the built-in production bundle for one validated profile."""

        try:
            configured = ProductionStorageSelection.model_validate(selection.model_dump())
        except ValidationError as exc:
            fields = sorted({str(error["loc"][0]) for error in exc.errors() if error["loc"]})
            raise ValueError(
                "PostgresTenantServiceFactory cannot satisfy configured storage backends: "
                + ", ".join(fields)
            ) from exc
        profile = configured.profile_id
        dimension = self._profile_dimensions.get(profile, 1536)
        if dimension != 1536:
            raise ValueError("pgvector storage profiles must use exactly 1536 dimensions")
        if self._artifact_objects is None:
            raise LookupError(
                "configured S3 artifact backend is unavailable; inject an object store "
                "or register an exact tenant profile with TenantStorageProfileRegistry"
            )
        artifact = PostgresArtifactStore(self._pool, self._artifact_objects)
        return TenantDataServices(
            session=PostgresSessionStore(self._repository),
            memory=PostgresMemoryStore(self._pool),
            summary=PostgresSummaryStore(self._pool),
            artifact=artifact,
            knowledge=PostgresKnowledgeStore(self._pool, profile_id=profile, dimension=dimension),
            audit=PostgresAuditStore(self._pool),
        )


__all__ = [
    "CompositeTenantServiceFactory",
    "PostgresArtifactStore",
    "PostgresAuditStore",
    "PostgresKnowledgeStore",
    "PostgresMemoryStore",
    "PostgresSessionStore",
    "PostgresSummaryStore",
    "PostgresTenantServiceFactory",
    "ProfileServiceFactory",
    "RegisteredTenantServiceBundle",
    "TenantDataServices",
    "TenantServiceFactory",
    "TenantStorageProfileRegistry",
]

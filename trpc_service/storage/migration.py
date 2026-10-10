"""Resumable tenant data migration state machine.

Backend-specific readers and writers plug into this coordinator.  The state
machine deliberately advances one explicit phase at a time so operators can
observe, pause, or roll back a single tenant without changing other tenants.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class MigrationPhase(StrEnum):
    PREPARE = "prepare"
    BACKFILL = "backfill"
    SHADOW_READ = "shadow-read"
    DUAL_WRITE = "dual-write"
    CUTOVER = "cutover"
    VERIFY = "verify"
    CLEANUP = "cleanup"
    ROLLBACK = "rollback"


class MigrationSourceKind(StrEnum):
    """Supported source backends for a guarded migration scope.

    The value is metadata only.  Credentials and source payloads are supplied
    by the caller's adapters and are deliberately not part of the manifest.
    """

    REDIS = "redis"
    LOCAL_VECTOR = "local_vector"
    EXTERNAL_VECTOR = "external_vector"
    EXTERNAL_MEMORY = "external_memory"


_MIGRATION_RECORD_KINDS = frozenset({"session", "memory", "summary", "artifact", "knowledge"})
CANONICAL_REDIS_MIGRATION_KINDS = ("session", "memory")
# These limits are intentionally conservative.  They bound accidental live
# migrations before a Redis client or PostgreSQL pool is created; larger
# migrations must be split into separately reviewed scopes.
MAX_MIGRATION_BATCH_SIZE = 10_000
MAX_MIGRATION_EXPECTED_RECORDS = 1_000_000
MAX_MIGRATION_DB_POOL_SIZE = 16
MIGRATION_TARGET_PAGE_SIZE = MAX_MIGRATION_BATCH_SIZE
_TARGET_EMPTY_TABLES = (
    "inbound_messages",
    "outbound_messages",
    "delivery_attempts",
    "sessions",
    "session_turns",
    "turn_intents",
    "session_events",
    "memories",
    "session_summaries",
    "artifacts",
    "knowledge_items",
    "knowledge_embeddings",
    "outbox_events",
    "dead_letters",
    "tool_executions",
    "confirmation_challenges",
    "audit_logs",
    "session_mailboxes",
    "session_mailbox_items",
    "wecom_connection_state",
    "im_acceptance_evidence_events",
    "im_acceptance_runs",
    "migration_checkpoints",
    "migration_scope_manifests",
    "migration_leases",
)


_ORDER = (
    MigrationPhase.PREPARE,
    MigrationPhase.BACKFILL,
    MigrationPhase.SHADOW_READ,
    MigrationPhase.DUAL_WRITE,
    MigrationPhase.CUTOVER,
    MigrationPhase.VERIFY,
    MigrationPhase.CLEANUP,
)


def canonical_migration_kinds(kinds: Iterable[str]) -> tuple[str, ...]:
    """Return the stable manifest/runtime order for migration record kinds.

    Redis-to-PostgreSQL migration evidence has one canonical order.  Keeping
    that order in the shared model prevents a caller's environment-variable
    spelling from changing the runtime fingerprint while the persisted
    manifest is normalized differently.
    """

    normalized = tuple(kinds)
    if not normalized or any(not isinstance(kind, str) or not kind for kind in normalized):
        raise ValueError("migration kinds must contain at least one non-empty string")
    if len(set(normalized)) != len(normalized):
        raise ValueError("migration kinds must be unique")
    unsupported = set(normalized) - _MIGRATION_RECORD_KINDS
    if unsupported:
        raise ValueError(
            "migration manifest contains unsupported kinds: " + ", ".join(sorted(unsupported))
        )
    if set(normalized) == set(CANONICAL_REDIS_MIGRATION_KINDS):
        return CANONICAL_REDIS_MIGRATION_KINDS
    return tuple(sorted(normalized))


class MigrationRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: str
    resource_id: str
    payload: dict[str, Any]

    @property
    def checksum(self) -> str:
        canonical = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(canonical).hexdigest()


class MigrationSourceSnapshot(BaseModel):
    """Immutable identity of the source set observed before a migration.

    The snapshot is intentionally content-free.  It binds a migration to the
    exact count and rolling checksum of the source records, so a resumed run
    cannot silently continue after the source has changed.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_snapshot_id: str = Field(min_length=1, max_length=256)
    source_count: int = Field(ge=0, le=MAX_MIGRATION_EXPECTED_RECORDS)
    source_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")


class MigrationCheckpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    tenant_id: str
    migration_id: str
    phase: MigrationPhase
    cursor: str | None = None
    source_count: int = Field(default=0, ge=0, le=MAX_MIGRATION_EXPECTED_RECORDS)
    target_count: int = Field(default=0, ge=0, le=MAX_MIGRATION_EXPECTED_RECORDS)
    checksum: str = "0" * 64
    differences: tuple[str, ...] = ()
    completed: bool = False


class MigrationResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    baseline: str
    candidate: str
    case_deltas: dict[str, Any]
    gate: str
    rejection_reasons: tuple[str, ...] = ()


class MigrationSource(Protocol):
    async def fetch(
        self, tenant_id: str, *, cursor: str | None, limit: int
    ) -> tuple[tuple[MigrationRecord, ...], str | None]: ...


class SnapshotMigrationSource(MigrationSource, Protocol):
    async def snapshot(self, tenant_id: str) -> MigrationSourceSnapshot: ...


class MigrationTarget(Protocol):
    async def prepare(self, tenant_id: str) -> None: ...

    async def upsert(self, tenant_id: str, record: MigrationRecord) -> None: ...

    async def read(self, tenant_id: str, kind: str, resource_id: str) -> MigrationRecord | None: ...

    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None: ...

    async def cutover(self, tenant_id: str) -> None: ...

    async def cleanup(self, tenant_id: str) -> None: ...

    async def rollback(self, tenant_id: str) -> None: ...

    async def list_records_page(
        self,
        tenant_id: str,
        kind: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> tuple[tuple[MigrationRecord, ...], str | None]: ...


class FencedMigrationControl(Protocol):
    """Control-plane hooks that carry the database migration fence.

    A cutover or cleanup often changes state outside the target data tables.
    The production implementation must accept the same lease token and reject
    a stale owner.  The unfenced methods remain available for offline adapters
    and legacy, unguarded tests only.
    """

    async def set_dual_write_fenced(
        self, tenant_id: str, enabled: bool, lease: MigrationLease
    ) -> None: ...

    async def cutover_fenced(self, tenant_id: str, lease: MigrationLease) -> None: ...

    async def cleanup_fenced(self, tenant_id: str, lease: MigrationLease) -> None: ...

    async def rollback_fenced(self, tenant_id: str, lease: MigrationLease) -> None: ...


class MigrationControl(Protocol):
    """Deployment-owned hooks that atomically switch an immutable storage profile."""

    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None: ...

    async def cutover(self, tenant_id: str) -> None: ...

    async def cleanup(self, tenant_id: str) -> None: ...

    async def rollback(self, tenant_id: str) -> None: ...


class RedisMigrationClient(Protocol):
    """Small subset of the async Redis API used by ``RedisMigrationSource``."""

    def scan_iter(self, *, match: str, count: int = 1000) -> AsyncIterator[Any]: ...

    async def type(self, key: str) -> Any: ...

    async def get(self, key: str) -> Any: ...

    async def hget(self, key: str, field: str) -> Any: ...

    async def hgetall(self, key: str) -> dict[Any, Any]: ...


class RedisMigrationSource:
    """Read tenant-scoped Redis session projections and memory records.

    The service's rebuildable session projection is a Redis hash at
    ``trpc:projection:session:{tenant_id}:{session_id}``, with a JSON ``payload``
    field.  Legacy memory installations vary, so the memory prefix is explicit
    and both JSON strings and JSON/hash records are accepted.  Keys are
    discovered once per tenant and sorted to make the cursor resumable and
    deterministic while a migration is running.
    """

    def __init__(
        self,
        redis: RedisMigrationClient,
        *,
        session_prefix: str = "trpc:projection:session:",
        memory_prefix: str = "trpc:memory:",
        kinds: tuple[str, ...] = ("session", "memory"),
    ) -> None:
        normalized_kinds = canonical_migration_kinds(kinds)
        if any(kind not in {"session", "memory"} for kind in normalized_kinds):
            raise ValueError("Redis migration source kinds must be session and/or memory")
        self._redis = redis
        self._prefixes = {
            "session": session_prefix,
            "memory": memory_prefix,
        }
        self._kinds = normalized_kinds
        self._keys: dict[str, tuple[tuple[str, str, str], ...]] = {}

    async def snapshot(self, tenant_id: str) -> MigrationSourceSnapshot:
        """Return a fresh source snapshot and invalidate the discovery cache."""

        keys = await self._tenant_keys(tenant_id, refresh=True)
        records = await self._read_records(tenant_id, keys)
        checksum = "0" * 64
        for record in records:
            checksum = _rolling_checksum(checksum, record.checksum)
        count = len(records)
        snapshot_id = hashlib.sha256(f"{count}:{checksum}".encode("ascii")).hexdigest()
        return MigrationSourceSnapshot(
            source_snapshot_id=snapshot_id,
            source_count=count,
            source_checksum=checksum,
        )

    async def fetch(
        self, tenant_id: str, *, cursor: str | None, limit: int
    ) -> tuple[tuple[MigrationRecord, ...], str | None]:
        if limit < 1:
            raise ValueError("migration source limit must be positive")
        keys = await self._tenant_keys(tenant_id)
        if len(keys) > MAX_MIGRATION_EXPECTED_RECORDS:
            raise MigrationGuardError("migration source exceeds MAX_MIGRATION_EXPECTED_RECORDS")
        start = _cursor_position(cursor, keys)
        selected = keys[start : start + limit]
        records = await self._read_records(tenant_id, selected)
        next_position = start + len(selected)
        next_cursor = _encode_source_cursor(selected[-1]) if next_position < len(keys) else None
        return records, next_cursor

    async def _read_records(
        self, tenant_id: str, keys: tuple[tuple[str, str, str], ...]
    ) -> tuple[MigrationRecord, ...]:
        values = await self._read_values(keys)
        records: list[MigrationRecord] = []
        for (kind, resource_id, _), value in zip(keys, values, strict=True):
            records.append(
                MigrationRecord(
                    kind=kind,
                    resource_id=resource_id,
                    payload=await self._canonical_payload(kind, tenant_id, resource_id, value),
                )
            )
        return tuple(records)

    async def _read_values(self, keys: tuple[tuple[str, str, str], ...]) -> tuple[Any, ...]:
        if not keys:
            return ()
        pipelined = await self._read_values_with_pipeline(keys)
        if pipelined is not None:
            return pipelined
        values: list[Any] = []
        for _, _, key in keys:
            values.append(await self._read_value(key))
        return tuple(values)

    async def _read_values_with_pipeline(
        self, keys: tuple[tuple[str, str, str], ...]
    ) -> tuple[Any, ...] | None:
        type_commands = tuple(("type", (key,)) for _, _, key in keys)
        type_results = await self._execute_pipeline(type_commands)
        if type_results is None:
            return None
        if len(type_results) != len(keys):
            raise RuntimeError("Redis migration pipeline returned an unexpected TYPE result count")

        key_types = tuple(_text(value) for value in type_results)
        for _, key_type in zip(keys, key_types, strict=True):
            if key_type not in {"hash", "string", "none"}:
                raise ValueError(f"unsupported Redis migration key type: {key_type}")

        value_commands = tuple(
            ("hget", (key, "payload")) if key_type == "hash" else ("get", (key,))
            for (_, _, key), key_type in zip(keys, key_types, strict=True)
        )
        value_results = await self._execute_pipeline(value_commands)
        if value_results is None:
            return None
        if len(value_results) != len(keys):
            raise RuntimeError("Redis migration pipeline returned an unexpected value result count")

        values: list[Any] = [None] * len(keys)
        missing_hashes: list[tuple[int, str]] = []
        for index, (key_type, value) in enumerate(zip(key_types, value_results, strict=True)):
            if key_type == "hash" and value is None:
                missing_hashes.append((index, keys[index][2]))
            elif value is not None:
                values[index] = _json_value(value)

        if missing_hashes:
            fallback_commands = tuple(("hgetall", (key,)) for _, key in missing_hashes)
            fallback_results = await self._execute_pipeline(fallback_commands)
            if fallback_results is None:
                return None
            if len(fallback_results) != len(missing_hashes):
                raise RuntimeError(
                    "Redis migration pipeline returned an unexpected HGETALL result count"
                )
            for (index, _), raw_values in zip(missing_hashes, fallback_results, strict=True):
                values[index] = {
                    _text(field): _json_value(value) for field, value in raw_values.items()
                }
        return tuple(values)

    async def _execute_pipeline(
        self, commands: tuple[tuple[str, tuple[Any, ...]], ...]
    ) -> tuple[Any, ...] | None:
        pipeline_factory = getattr(self._redis, "pipeline", None)
        if not callable(pipeline_factory):
            return None
        try:
            pipeline = pipeline_factory(transaction=False)
        except TypeError:
            try:
                pipeline = pipeline_factory()
            except (AttributeError, NotImplementedError, TypeError):
                return None
        except (AttributeError, NotImplementedError):
            return None
        if inspect.isawaitable(pipeline):
            pipeline = await pipeline
        if pipeline is None:
            return None

        enter = getattr(pipeline, "__aenter__", None)
        exit_ = getattr(pipeline, "__aexit__", None)
        if callable(enter) and callable(exit_):
            async with pipeline as active:
                return await self._execute_pipeline_commands(active, commands)

        try:
            return await self._execute_pipeline_commands(pipeline, commands)
        finally:
            close = getattr(pipeline, "close", None)
            if callable(close):
                result = close()
                if inspect.isawaitable(result):
                    await result

    async def _execute_pipeline_commands(
        self, pipeline: Any, commands: tuple[tuple[str, tuple[Any, ...]], ...]
    ) -> tuple[Any, ...] | None:
        execute = getattr(pipeline, "execute", None)
        if not callable(execute):
            return None
        for name, args in commands:
            command = getattr(pipeline, name, None)
            if not callable(command):
                return None
            result = command(*args)
            if inspect.isawaitable(result):
                await result
        results = execute()
        if inspect.isawaitable(results):
            results = await results
        return tuple(results)

    async def _tenant_keys(
        self, tenant_id: str, *, refresh: bool = False
    ) -> tuple[tuple[str, str, str], ...]:
        cached = None if refresh else self._keys.get(tenant_id)
        if cached is not None:
            return cached
        discovered: list[tuple[str, str, str]] = []
        for kind in self._kinds:
            prefix = self._prefixes[kind]
            patterns = [
                f"{prefix}{_redis_pattern_literal(tenant_id)}:*",
            ]
            # RedisProjectionStore v2 encodes both tenant and session in the
            # key; scan it in addition to the legacy delimiter form.  The
            # decoded tenant is checked below, so a colon or glob character in
            # an ID cannot broaden the migration scope.
            if kind == "session" and prefix == "trpc:projection:session:":
                patterns.append("trpc:projection:session:v2:*")
            for pattern in patterns:
                async for raw_key in self._redis.scan_iter(match=pattern, count=1000):
                    key = _text(raw_key)
                    decoded = _decode_projection_v2_key(key, tenant_id)
                    if decoded is not None:
                        resource_id = decoded
                    else:
                        tenant_prefix = f"{prefix}{tenant_id}:"
                        if not key.startswith(tenant_prefix):
                            continue
                        resource_id = key[len(tenant_prefix) :]
                    if not resource_id:
                        continue
                    discovered.append((kind, resource_id, key))
                    if len(discovered) > MAX_MIGRATION_EXPECTED_RECORDS:
                        raise MigrationGuardError(
                            "migration source exceeds MAX_MIGRATION_EXPECTED_RECORDS"
                        )
        # A deployment can briefly retain both the legacy delimiter key and
        # the v2 delimiter-safe key for one session.  They represent one
        # logical source record; counting both would make the immutable
        # snapshot unreconcilable with PostgreSQL's unique session row.  Keep
        # the v2 key when both forms exist and use the key as a deterministic
        # tie-breaker for duplicate v2 entries.
        # Keep the manifest's canonical kind order.  Sorting by the kind name
        # would put ``memory`` before ``session`` even though Redis and the
        # target snapshot both use ``("session", "memory")``.  That leaves
        # every record individually equal while producing a different rolling
        # checksum for a mixed-kind migration.
        kind_order = {kind: index for index, kind in enumerate(self._kinds)}
        ordered = sorted(
            set(discovered),
            key=lambda item: (
                kind_order[item[0]],
                item[1],
                0
                if item[0] == "session"
                and _decode_projection_v2_key(item[2], tenant_id) is not None
                else 1,
                item[2],
            ),
        )
        unique: list[tuple[str, str, str]] = []
        seen_resources: set[tuple[str, str]] = set()
        for item in ordered:
            resource_key = (item[0], item[1])
            if resource_key in seen_resources:
                continue
            seen_resources.add(resource_key)
            unique.append(item)
        result = tuple(unique)
        self._keys[tenant_id] = result
        return result

    async def _read_value(self, key: str) -> Any:
        key_type = _text(await self._redis.type(key))
        if key_type == "hash":
            payload = await self._redis.hget(key, "payload")
            if payload is not None:
                return _json_value(payload)
            values = await self._redis.hgetall(key)
            return {_text(field): _json_value(value) for field, value in values.items()}
        if key_type in {"string", "none"}:
            value = await self._redis.get(key)
            return _json_value(value) if value is not None else None
        raise ValueError(f"unsupported Redis migration key type: {key_type}")

    @staticmethod
    async def _canonical_payload(
        kind: str, tenant_id: str, resource_id: str, value: Any
    ) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError(f"Redis {kind} record {resource_id!r} must contain a JSON object")
        if kind == "session":
            return _canonical_session_payload(tenant_id, resource_id, value)
        return _canonical_memory_payload(value)


class PostgresMigrationCheckpointStore:
    """Persist coordinator checkpoints in the production checkpoint table."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def load(self, tenant_id: str, migration_id: str) -> MigrationCheckpoint | None:
        async with self._tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT phase, batch_key, source_count, target_count, checksum,
                       differences, status, cursor
                  FROM migration_checkpoints
                 WHERE tenant_id=$1 AND migration_id=$2
                 ORDER BY updated_at DESC
                 LIMIT 1
                """,
                tenant_id,
                migration_id,
            )
        if row is None:
            return None
        differences = row["differences"]
        if isinstance(differences, str):
            differences = json.loads(differences)
        return MigrationCheckpoint(
            tenant_id=tenant_id,
            migration_id=migration_id,
            phase=MigrationPhase(row["phase"]),
            cursor=row.get("cursor") if hasattr(row, "get") else None,
            source_count=int(row["source_count"]),
            target_count=int(row["target_count"]),
            checksum=row["checksum"] or "0" * 64,
            differences=tuple(str(item) for item in (differences or [])),
            completed=row["status"] == "completed",
        )

    async def save(self, checkpoint: MigrationCheckpoint) -> None:
        async with self._tenant_transaction(checkpoint.tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO migration_checkpoints (
                    tenant_id, migration_id, phase, batch_key, source_count,
                    target_count, checksum, differences, status, cursor
                ) VALUES ($1,$2,$3,'state',$4,$5,$6,$7::jsonb,$8,$9)
                ON CONFLICT (tenant_id, migration_id, phase, batch_key)
                DO UPDATE SET source_count=EXCLUDED.source_count,
                              target_count=EXCLUDED.target_count,
                              checksum=EXCLUDED.checksum,
                              differences=EXCLUDED.differences,
                              status=EXCLUDED.status,
                              cursor=EXCLUDED.cursor,
                              updated_at=now()
                """,
                checkpoint.tenant_id,
                checkpoint.migration_id,
                checkpoint.phase.value,
                checkpoint.source_count,
                checkpoint.target_count,
                checkpoint.checksum,
                json.dumps(list(checkpoint.differences), separators=(",", ":")),
                "completed" if checkpoint.completed else "running",
                checkpoint.cursor,
            )

    @asynccontextmanager
    async def _tenant_transaction(self, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            yield connection


class PostgresMigrationTarget:
    """Write canonical Redis records into PostgreSQL authoritative tables.

    Session events from a projection do not carry the inbound/turn foreign-key
    context required by the runtime schema.  The adapter creates one
    deterministic, content-free migration turn per session, preserving event
    IDs, sequence, author, timestamp, event JSON and state deltas without
    inventing user-visible message content.  Existing tenant app/config and a
    channel binding are required when events are present.

    Dual-write/cutover hooks are intentionally local state hooks.  Deployments
    that switch an immutable storage profile should subclass or wrap this
    target and perform that control-plane operation in the hooks; the adapter
    never silently changes tenant configuration.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        control: MigrationControl | None = None,
        manifest: MigrationScopeManifest | None = None,
        lease: MigrationLease | None = None,
    ) -> None:
        self._pool = pool
        self._control = control
        self._manifest = manifest
        self._lease = lease

    def bind_migration_lease(self, lease: MigrationLease) -> None:
        """Bind the immutable fence used by every target transaction.

        The coordinator calls this for injected production targets.  Binding
        cannot silently switch to another owner while a migration is in
        flight.  Heartbeat renewal is allowed to replace only the expiry
        timestamp for the same tenant/owner/epoch fence, so long-running
        target writes and control hooks receive the current lease object.
        """

        if self._manifest is not None and (
            self._manifest.tenant_id != lease.tenant_id
            or self._manifest.migration_id != lease.migration_id
        ):
            raise MigrationManifestConflict("migration target lease is outside the manifest scope")
        if self._lease is not None and (
            self._lease.tenant_id != lease.tenant_id
            or self._lease.migration_id != lease.migration_id
            or self._lease.owner_id != lease.owner_id
            or self._lease.owner_instance != lease.owner_instance
            or self._lease.lease_epoch != lease.lease_epoch
        ):
            raise MigrationManifestConflict("migration target lease cannot change owner or epoch")
        self._lease = lease

    async def prepare(self, tenant_id: str) -> None:
        if self._manifest is not None:
            unsupported = set(self._manifest.kinds) - {"session", "memory"}
            if unsupported:
                raise ValueError(
                    "PostgreSQL migration target does not implement record kinds: "
                    + ", ".join(sorted(unsupported))
                )
        async with self._tenant_transaction(tenant_id) as connection:
            missing = await connection.fetchval(
                """
                SELECT string_agg(name, ', ' ORDER BY name)
                  FROM unnest(
                      ARRAY['sessions','session_events','memories']::text[]
                  ) AS required(name)
                 WHERE to_regclass('public.' || required.name) IS NULL
                """
            )
            if self._manifest is not None:
                await self._migration_context(connection, tenant_id, self._manifest.app_id)
        if missing:
            raise RuntimeError(f"PostgreSQL migration target is missing tables: {missing}")

    async def upsert(self, tenant_id: str, record: MigrationRecord) -> None:
        self._validate_record_scope(tenant_id, record)
        if record.kind == "session":
            await self._upsert_session(tenant_id, record)
        elif record.kind == "memory":
            await self._upsert_memory(tenant_id, record)
        else:
            raise ValueError(f"unsupported PostgreSQL migration record kind: {record.kind}")

    async def read(self, tenant_id: str, kind: str, resource_id: str) -> MigrationRecord | None:
        if self._manifest is not None and kind not in self._manifest.kinds:
            raise ValueError(f"record kind {kind!r} is outside the migration manifest")
        if kind == "session":
            return await self._read_session(tenant_id, resource_id)
        if kind == "memory":
            return await self._read_memory(tenant_id, resource_id)
        raise ValueError(f"unsupported PostgreSQL migration record kind: {kind}")

    async def list_records_page(
        self,
        tenant_id: str,
        kind: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> tuple[tuple[MigrationRecord, ...], str | None]:
        """Enumerate a complete target set with bounded keyset pagination.

        The extra row is fetched only to establish whether another page
        exists.  A caller must continue with the returned cursor; a single
        ``LIMIT`` query is never treated as a complete verification.
        """

        _validate_page_limit(limit, "migration target page size")
        if kind not in {"session", "memory"}:
            raise ValueError(f"unsupported PostgreSQL migration record kind: {kind}")
        if self._manifest is not None:
            if tenant_id != self._manifest.tenant_id:
                raise MigrationManifestConflict(
                    "target enumeration tenant does not match the migration manifest"
                )
            if kind not in self._manifest.kinds:
                raise MigrationManifestConflict(
                    f"record kind {kind!r} is outside the migration manifest"
                )
        if kind == "memory" and cursor is not None:
            try:
                UUID(cursor)
            except ValueError as error:
                raise ValueError("memory target cursor must be a UUID") from error
        async with self._tenant_transaction(tenant_id) as connection:
            if kind == "session":
                if self._manifest is None:
                    rows = await connection.fetch(
                        """
                        SELECT session_id, app_id, principal_id, state_json,
                               version, next_sequence
                          FROM sessions
                         WHERE tenant_id=$1 AND ($2::text IS NULL OR session_id>$2)
                         ORDER BY session_id LIMIT $3
                        """,
                        tenant_id,
                        cursor,
                        limit + 1,
                    )
                else:
                    rows = await connection.fetch(
                        """
                        SELECT session_id, app_id, principal_id, state_json,
                               version, next_sequence
                          FROM sessions
                         WHERE tenant_id=$1 AND app_id=$2
                           AND ($3::text IS NULL OR session_id>$3)
                         ORDER BY session_id LIMIT $4
                        """,
                        tenant_id,
                        self._manifest.app_id,
                        cursor,
                        limit + 1,
                    )
                has_more = len(rows) > limit
                page_rows = rows[:limit]
                events_by_session: dict[str, list[dict[str, Any]]] = {}
                if page_rows:
                    session_ids = [str(row["session_id"]) for row in page_rows]
                    event_rows = await connection.fetch(
                        """
                        SELECT session_id, sequence, event_id, author,
                               event_timestamp, event_json, state_delta
                          FROM session_events
                         WHERE tenant_id=$1 AND session_id=ANY($2::text[])
                         ORDER BY session_id, sequence
                        """,
                        tenant_id,
                        session_ids,
                    )
                    for event_row in event_rows:
                        session_id = str(event_row["session_id"])
                        events_by_session.setdefault(session_id, []).append(
                            {
                                "sequence": int(event_row["sequence"]),
                                "event_id": event_row["event_id"],
                                "author": event_row["author"],
                                "timestamp": float(event_row["event_timestamp"]),
                                "event": _json_object(event_row["event_json"]),
                                "state_delta": _json_object(event_row["state_delta"]),
                            }
                        )
                session_values: list[MigrationRecord] = []
                for row in page_rows:
                    session_id = str(row["session_id"])
                    if self._session_row_is_complete(row):
                        session_values.append(
                            self._session_record_from_row(
                                tenant_id,
                                row,
                                events_by_session.get(session_id, []),
                            )
                        )
                    else:
                        # Lightweight asyncpg doubles may only return the key
                        # column from the page query.  Keep those doubles
                        # compatible without affecting real full-row queries.
                        record = await self._read_session(tenant_id, session_id)
                        if record is not None:
                            session_values.append(record)
                values = tuple(session_values)
            else:
                rows = await connection.fetch(
                    """
                    SELECT memory_id, source_record_id, principal_id, session_id,
                           source_sequence, memory_json, projection_status
                      FROM memories
                     WHERE tenant_id=$1
                       AND ($2::uuid IS NULL OR memory_id>$2::uuid)
                     ORDER BY memory_id LIMIT $3
                    """,
                    tenant_id,
                    cursor,
                    limit + 1,
                )
                has_more = len(rows) > limit
                page_rows = rows[:limit]
                memory_values: list[MigrationRecord] = []
                for row in page_rows:
                    if self._memory_row_is_complete(row):
                        memory_values.append(self._memory_record_from_row(row))
                    else:
                        source_record_id = (
                            row.get("source_record_id") if hasattr(row, "get") else None
                        )
                        resource_id = str(source_record_id or row["memory_id"])
                        record = await self._read_memory(tenant_id, resource_id)
                        if record is not None:
                            memory_values.append(record)
                values = tuple(memory_values)
        if not has_more:
            return values, None
        last = page_rows[-1]
        next_cursor = str(last["session_id"] if kind == "session" else last["memory_id"])
        return values, next_cursor

    def _session_record_from_row(
        self,
        tenant_id: str,
        row: Any,
        events: list[dict[str, Any]],
    ) -> MigrationRecord:
        session_id = str(row["session_id"])
        payload = _canonical_session_payload(
            tenant_id,
            session_id,
            {
                "app_id": row["app_id"],
                "principal_id": row["principal_id"],
                "state_json": row["state_json"],
                "version": row["version"],
                "next_sequence": row["next_sequence"],
                "events": events,
            },
        )
        return MigrationRecord(kind="session", resource_id=session_id, payload=payload)

    @staticmethod
    def _session_row_is_complete(row: Any) -> bool:
        required = {"app_id", "principal_id", "state_json", "version", "next_sequence"}
        return required.issubset(row.keys() if hasattr(row, "keys") else ())

    @staticmethod
    def _memory_record_from_row(row: Any) -> MigrationRecord:
        source_record_id = row.get("source_record_id") if hasattr(row, "get") else None
        memory_id = str(row["memory_id"])
        resource_id = str(source_record_id or memory_id)
        payload = _canonical_memory_payload(
            {
                "principal_id": row["principal_id"],
                "session_id": row["session_id"],
                "source_sequence": row["source_sequence"],
                "memory_json": row["memory_json"],
                "projection_status": row["projection_status"],
            }
        )
        return MigrationRecord(kind="memory", resource_id=resource_id, payload=payload)

    @staticmethod
    def _memory_row_is_complete(row: Any) -> bool:
        required = {
            "memory_id",
            "principal_id",
            "session_id",
            "source_sequence",
            "memory_json",
            "projection_status",
        }
        return required.issubset(row.keys() if hasattr(row, "keys") else ())

    async def list_records(
        self, tenant_id: str, kind: str, *, limit: int = MIGRATION_TARGET_PAGE_SIZE
    ) -> tuple[MigrationRecord, ...]:
        """Compatibility helper that refuses silently truncated results."""

        _validate_page_limit(limit, "migration target list limit")
        values, cursor = await self.list_records_page(tenant_id, kind, cursor=None, limit=limit)
        if cursor is not None:
            raise MigrationGuardError("target enumeration requires list_records_page pagination")
        return values

    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None:
        await self._call_fenced_control("set_dual_write", tenant_id, enabled)

    async def cutover(self, tenant_id: str) -> None:
        await self._call_fenced_control("cutover", tenant_id)

    async def cleanup(self, tenant_id: str) -> None:
        await self._call_fenced_control("cleanup", tenant_id)

    async def rollback(self, tenant_id: str) -> None:
        await self._call_fenced_control("rollback", tenant_id)

    async def release_write_barrier(self, tenant_id: str) -> None:
        """Release the database barrier only after terminal control succeeds."""

        if self._lease is None:
            return
        async with self._tenant_transaction(tenant_id, require_barrier=True) as connection:
            row = await connection.fetchrow(
                """
                UPDATE migration_write_barriers
                   SET mode='released', updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND migration_id=$2 AND owner_instance=$3
                   AND lease_epoch=$4 AND mode='active'
                RETURNING tenant_id
                """,
                self._lease.tenant_id,
                self._lease.migration_id,
                self._lease.owner_instance,
                self._lease.lease_epoch,
            )
            if row is None:
                raise MigrationLeaseLost("migration write barrier is stale or missing")

    def _require_control(self) -> MigrationControl:
        if self._control is None:
            raise RuntimeError(
                "migration control hook is required for dual-write, cutover, or rollback"
            )
        return self._control

    async def _call_fenced_control(self, name: str, tenant_id: str, *args: Any) -> None:
        control = self._require_control()
        if self._lease is None:
            method = getattr(control, name)
            await method(tenant_id, *args)
            return
        fenced = getattr(control, f"{name}_fenced", None)
        if not callable(fenced):
            raise MigrationGuardError(f"production migration control must expose {name}_fenced")
        await fenced(tenant_id, *args, lease=self._lease)

    async def _upsert_session(self, tenant_id: str, record: MigrationRecord) -> None:
        payload = _canonical_session_payload(tenant_id, record.resource_id, record.payload)
        events = payload["events"]
        async with self._tenant_transaction(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO sessions (
                    tenant_id, session_id, app_id, principal_id, state_json,
                    version, next_sequence
                ) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7)
                ON CONFLICT (tenant_id, session_id)
                DO UPDATE SET app_id=EXCLUDED.app_id,
                              principal_id=EXCLUDED.principal_id,
                              state_json=EXCLUDED.state_json,
                              version=EXCLUDED.version,
                              next_sequence=EXCLUDED.next_sequence,
                              updated_at=now()
                """,
                tenant_id,
                record.resource_id,
                payload["app_id"],
                payload["principal_id"],
                json.dumps(payload["state"], ensure_ascii=False, separators=(",", ":")),
                payload["version"],
                payload["next_sequence"],
            )
            if events:
                config_version, binding = await self._migration_context(
                    connection, tenant_id, payload["app_id"]
                )
                turn_id, _inbound_id = await self._ensure_migration_turn(
                    connection,
                    tenant_id,
                    record.resource_id,
                    payload["app_id"],
                    payload["principal_id"],
                    config_version,
                    binding,
                )
                for event in events:
                    await connection.execute(
                        """
                        INSERT INTO session_events (
                            tenant_id, session_id, sequence, event_id, turn_id,
                            author, event_timestamp, event_json, state_delta
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)
                        ON CONFLICT (tenant_id, session_id, sequence)
                        DO UPDATE SET event_id=EXCLUDED.event_id,
                                      turn_id=EXCLUDED.turn_id,
                                      author=EXCLUDED.author,
                                      event_timestamp=EXCLUDED.event_timestamp,
                                      event_json=EXCLUDED.event_json,
                                      state_delta=EXCLUDED.state_delta
                        """,
                        tenant_id,
                        record.resource_id,
                        event["sequence"],
                        event["event_id"],
                        turn_id,
                        event["author"],
                        event["timestamp"],
                        json.dumps(event["event"], ensure_ascii=False, separators=(",", ":")),
                        json.dumps(event["state_delta"], ensure_ascii=False, separators=(",", ":")),
                    )

    async def _upsert_memory(self, tenant_id: str, record: MigrationRecord) -> None:
        payload = _canonical_memory_payload(record.payload)
        memory_id = _stable_uuid(tenant_id, record.resource_id)
        async with self._tenant_transaction(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO memories (
                    tenant_id, memory_id, source_record_id, principal_id, session_id,
                    source_sequence, memory_json, projection_status
                ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
                ON CONFLICT (tenant_id, memory_id)
                DO UPDATE SET source_record_id=EXCLUDED.source_record_id,
                              principal_id=EXCLUDED.principal_id,
                              session_id=EXCLUDED.session_id,
                              source_sequence=EXCLUDED.source_sequence,
                              memory_json=EXCLUDED.memory_json,
                              projection_status=EXCLUDED.projection_status
                """,
                tenant_id,
                memory_id,
                record.resource_id,
                payload["principal_id"],
                payload["session_id"],
                payload["source_sequence"],
                json.dumps(payload["memory"], ensure_ascii=False, separators=(",", ":")),
                payload["projection_status"],
            )

    async def _read_session(self, tenant_id: str, session_id: str) -> MigrationRecord | None:
        async with self._tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM sessions WHERE tenant_id=$1 AND session_id=$2",
                tenant_id,
                session_id,
            )
            if row is None:
                return None
            if self._manifest is not None and row["app_id"] != self._manifest.app_id:
                return None
            event_rows = await connection.fetch(
                """
                SELECT sequence,event_id,author,event_timestamp,event_json,state_delta
                  FROM session_events
                 WHERE tenant_id=$1 AND session_id=$2
                 ORDER BY sequence
                """,
                tenant_id,
                session_id,
            )
        payload = {
            "tenant_id": tenant_id,
            "app_id": row["app_id"],
            "session_id": session_id,
            "principal_id": row["principal_id"],
            "version": int(row["version"]),
            "next_sequence": int(row["next_sequence"]),
            "state": _json_object(row["state_json"]),
            "events": [
                {
                    "sequence": int(item["sequence"]),
                    "event_id": item["event_id"],
                    "author": item["author"],
                    "timestamp": float(item["event_timestamp"]),
                    "event": _json_object(item["event_json"]),
                    "state_delta": _json_object(item["state_delta"]),
                }
                for item in event_rows
            ],
        }
        return MigrationRecord(kind="session", resource_id=session_id, payload=payload)

    async def _read_memory(self, tenant_id: str, resource_id: str) -> MigrationRecord | None:
        memory_id = _stable_uuid(tenant_id, resource_id)
        async with self._tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                "SELECT * FROM memories WHERE tenant_id=$1 AND memory_id=$2",
                tenant_id,
                memory_id,
            )
            if row is None:
                try:
                    direct_id = UUID(resource_id)
                except ValueError:
                    direct_id = None
                if direct_id is not None and direct_id != memory_id:
                    row = await connection.fetchrow(
                        "SELECT * FROM memories WHERE tenant_id=$1 AND memory_id=$2",
                        tenant_id,
                        direct_id,
                    )
        if row is None:
            return None
        source_record_id = row.get("source_record_id") if hasattr(row, "get") else None
        effective_resource_id = str(source_record_id or resource_id)
        payload = {
            "principal_id": row["principal_id"],
            "session_id": row["session_id"],
            "source_sequence": row["source_sequence"],
            "memory": _json_value(row["memory_json"]),
            "projection_status": row["projection_status"],
        }
        return MigrationRecord(kind="memory", resource_id=effective_resource_id, payload=payload)

    async def _migration_context(
        self, connection: asyncpg.Connection, tenant_id: str, app_id: str
    ) -> tuple[int, dict[str, str]]:
        manifest = self._manifest
        if manifest is not None:
            if manifest.tenant_id != tenant_id or manifest.app_id != app_id:
                raise MigrationManifestConflict(
                    "session record app or tenant is outside the immutable migration manifest"
                )
            config_version = await connection.fetchval(
                """
                SELECT version FROM config_revisions
                 WHERE tenant_id=$1 AND app_id=$2 AND version=$3
                """,
                tenant_id,
                app_id,
                manifest.config_version,
            )
            binding = await connection.fetchrow(
                """
                SELECT b.binding_id, b.channel, b.account_id
                  FROM channel_bindings AS b
                  JOIN agent_apps AS a
                    ON a.tenant_id=b.tenant_id AND a.app_id=b.app_id
                 WHERE b.tenant_id=$1 AND b.app_id=$2 AND b.binding_id=$3
                   AND b.enabled AND b.control_version=$4
                   AND a.control_version=$5
                """,
                tenant_id,
                app_id,
                manifest.binding_id,
                manifest.binding_revision,
                manifest.app_revision,
            )
        else:
            config_version = await connection.fetchval(
                """
                SELECT version FROM config_revisions
                 WHERE tenant_id=$1 AND app_id=$2
                 ORDER BY version DESC LIMIT 1
                """,
                tenant_id,
                app_id,
            )
            binding = await connection.fetchrow(
                """
                SELECT binding_id, channel, account_id FROM channel_bindings
                 WHERE tenant_id=$1 AND app_id=$2 AND enabled
                 ORDER BY binding_id LIMIT 1
                """,
                tenant_id,
                app_id,
            )
        if config_version is None or binding is None:
            raise RuntimeError(
                "session event migration requires an existing app config revision "
                "and channel binding"
            )
        return int(config_version), {
            "binding_id": binding["binding_id"],
            "channel": binding["channel"],
            "account_id": binding["account_id"],
        }

    def _validate_record_scope(self, tenant_id: str, record: MigrationRecord) -> None:
        manifest = self._manifest
        if manifest is None:
            return
        if manifest.tenant_id != tenant_id:
            raise MigrationManifestConflict("record tenant does not match migration manifest")
        if record.kind not in manifest.kinds:
            raise MigrationManifestConflict(
                f"record kind {record.kind!r} is outside the migration manifest"
            )
        if record.kind == "session":
            payload_app = record.payload.get("app_id")
            if payload_app != manifest.app_id:
                raise MigrationManifestConflict(
                    "session record app_id does not match the migration manifest"
                )

    async def _ensure_migration_turn(
        self,
        connection: asyncpg.Connection,
        tenant_id: str,
        session_id: str,
        app_id: str,
        principal_id: str,
        config_version: int,
        binding: dict[str, str],
    ) -> tuple[UUID, UUID]:
        inbound_id = _stable_uuid(tenant_id, f"migration-inbound:{session_id}")
        turn_id = _stable_uuid(tenant_id, f"migration-turn:{session_id}")
        external_id = f"migration:{session_id}"
        await connection.execute(
            """
            INSERT INTO inbound_messages (
                tenant_id,inbound_id,binding_id,app_id,config_version,channel,
                account_id,external_message_id,principal_id,session_id,
                request_id,trace_id,envelope_json,status
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,'committed')
            ON CONFLICT (tenant_id, inbound_id) DO NOTHING
            """,
            tenant_id,
            inbound_id,
            binding["binding_id"],
            app_id,
            config_version,
            binding["channel"],
            binding["account_id"],
            external_id,
            principal_id,
            session_id,
            f"migration-request:{session_id}",
            f"migration-trace:{session_id}",
            json.dumps({"source": "redis-migration"}, separators=(",", ":")),
        )
        await connection.execute(
            """
            INSERT INTO session_turns (
                tenant_id,turn_id,session_id,inbound_id,config_version,
                status,fencing_token,attempt,committed_at
            ) VALUES ($1,$2,$3,$4,$5,'committed',0,1,now())
            ON CONFLICT (tenant_id, turn_id) DO UPDATE
                SET status='committed', committed_at=coalesce(session_turns.committed_at, now())
            """,
            tenant_id,
            turn_id,
            session_id,
            inbound_id,
            config_version,
        )
        return turn_id, inbound_id

    @asynccontextmanager
    async def _tenant_transaction(
        self, tenant_id: str, *, require_barrier: bool = False
    ) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            if self._lease is not None:
                await connection.execute(
                    """
                    SELECT set_config('app.migration_id', $1, true),
                           set_config('app.migration_owner_instance', $2, true),
                           set_config('app.migration_lease_epoch', $3, true)
                    """,
                    self._lease.migration_id,
                    self._lease.owner_instance,
                    str(self._lease.lease_epoch),
                )
                fence = await connection.fetchrow(
                    """
                    UPDATE migration_leases AS l
                       SET updated_at=clock_timestamp()
                      FROM migration_write_barriers AS b
                     WHERE l.tenant_id=$1 AND l.migration_id=$2
                       AND l.owner_id=$3 AND l.owner_instance=$4
                       AND l.lease_epoch=$5 AND l.expires_at>clock_timestamp()
                       AND b.tenant_id=l.tenant_id AND b.migration_id=l.migration_id
                       AND b.owner_instance=l.owner_instance
                       AND b.lease_epoch=l.lease_epoch AND b.mode='active'
                    RETURNING l.tenant_id
                    """,
                    self._lease.tenant_id,
                    self._lease.migration_id,
                    self._lease.owner_id,
                    self._lease.owner_instance,
                    self._lease.lease_epoch,
                )
                if fence is None:
                    raise MigrationLeaseLost("migration lease or write barrier is stale or missing")
            elif require_barrier:
                raise MigrationLeaseLost("migration write barrier requires a bound lease")
            yield connection


class MigrationCheckpointStore(Protocol):
    async def load(self, tenant_id: str, migration_id: str) -> MigrationCheckpoint | None: ...

    async def save(self, checkpoint: MigrationCheckpoint) -> None: ...


class InMemoryMigrationCheckpointStore:
    def __init__(self) -> None:
        self._values: dict[tuple[str, str], MigrationCheckpoint] = {}

    async def load(self, tenant_id: str, migration_id: str) -> MigrationCheckpoint | None:
        return self._values.get((tenant_id, migration_id))

    async def save(self, checkpoint: MigrationCheckpoint) -> None:
        self._values[(checkpoint.tenant_id, checkpoint.migration_id)] = checkpoint


class MigrationCoordinator:
    def __init__(
        self,
        source: MigrationSource,
        target: MigrationTarget,
        checkpoints: MigrationCheckpointStore,
        *,
        batch_size: int = 500,
        guard: PostgresMigrationGuard | None = None,
        lease: MigrationLease | None = None,
        manifest: MigrationScopeManifest | None = None,
        lease_for: timedelta = timedelta(minutes=1),
        heartbeat_interval: timedelta | None = None,
    ) -> None:
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size < 1
            or batch_size > MAX_MIGRATION_BATCH_SIZE
        ):
            raise ValueError(
                "migration batch size must be a positive integer no greater than "
                f"{MAX_MIGRATION_BATCH_SIZE}"
            )
        self._source = source
        self._target = target
        self._checkpoints = checkpoints
        self._batch_size = batch_size
        self._guard = guard
        self._lease = lease
        self._manifest = manifest
        self._lease_for = lease_for
        if lease is not None:
            bind_lease = getattr(target, "bind_migration_lease", None)
            if callable(bind_lease):
                bind_lease(lease)
        if lease_for <= timedelta(0):
            raise ValueError("migration lease duration must be positive")
        interval = heartbeat_interval or timedelta(
            seconds=min(max(lease_for.total_seconds() / 3, 0.5), 30.0)
        )
        if interval <= timedelta(0) or interval >= lease_for:
            raise ValueError(
                "migration lease heartbeat interval must be positive and shorter than the lease"
            )
        self._heartbeat_interval = interval
        self._lease_lock = asyncio.Lock()
        self._heartbeat_stop: asyncio.Event | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._heartbeat_error: BaseException | None = None
        if (guard is None) != (lease is None):
            raise ValueError("migration guard and lease must be supplied together")
        if manifest is not None and lease is not None:
            if lease.tenant_id != manifest.tenant_id or lease.migration_id != manifest.migration_id:
                raise MigrationManifestConflict("migration lease does not match manifest scope")

    async def run(
        self, tenant_id: str, migration_id: str, phase: MigrationPhase
    ) -> MigrationResult:
        if self._guard is None:
            return await self._run_once(tenant_id, migration_id, phase)
        async with self._lease_liveness():
            return await self._run_once(tenant_id, migration_id, phase)

    async def _run_once(
        self, tenant_id: str, migration_id: str, phase: MigrationPhase
    ) -> MigrationResult:
        self._validate_scope(tenant_id, migration_id)
        await self._validate_source_snapshot(tenant_id)
        current = await self._checkpoints.load(tenant_id, migration_id)
        self._validate_transition(current, phase)
        checkpoint = MigrationCheckpoint(
            tenant_id=tenant_id,
            migration_id=migration_id,
            phase=phase,
        )
        if current:
            checkpoint = current.model_copy(
                update={"phase": phase, "cursor": None, "completed": False}
            )
            if current.phase == phase and not current.completed:
                checkpoint = current

        if phase == MigrationPhase.PREPARE:
            await self._assert_lease()
            await self._target.prepare(tenant_id)
            checkpoint = checkpoint.model_copy(update={"completed": True})
        elif phase == MigrationPhase.BACKFILL:
            checkpoint = await self._copy_all(checkpoint)
        elif phase == MigrationPhase.SHADOW_READ:
            checkpoint = await self._compare_all(checkpoint, reject_differences=False)
        elif phase == MigrationPhase.DUAL_WRITE:
            await self._assert_lease()
            await self._target.set_dual_write(tenant_id, True)
            checkpoint = checkpoint.model_copy(update={"completed": True})
        elif phase == MigrationPhase.CUTOVER:
            await self._assert_lease()
            await self._target.cutover(tenant_id)
            checkpoint = checkpoint.model_copy(update={"completed": True})
        elif phase == MigrationPhase.VERIFY:
            checkpoint = await self._compare_all(checkpoint, reject_differences=True)
        elif phase == MigrationPhase.CLEANUP:
            await self._assert_lease()
            await self._target.cleanup(tenant_id)
            await self._assert_lease()
            await self._target.set_dual_write(tenant_id, False)
            checkpoint = checkpoint.model_copy(update={"completed": True})
        else:
            await self._assert_lease()
            await self._target.rollback(tenant_id)
            await self._assert_lease()
            await self._target.set_dual_write(tenant_id, False)
            checkpoint = checkpoint.model_copy(update={"completed": True})

        await self._save_checkpoint(checkpoint)
        await self._validate_source_snapshot(tenant_id)
        target_snapshot = await self._target_snapshot(tenant_id)
        observed_target_count = (
            target_snapshot[0] if target_snapshot is not None else checkpoint.target_count
        )
        observed_target_checksum = (
            target_snapshot[1] if target_snapshot is not None else checkpoint.checksum
        )
        reasons = tuple(f"record differs: {item}" for item in checkpoint.differences)
        return MigrationResult(
            baseline="source",
            candidate="target",
            case_deltas={
                "phase": phase.value,
                "source_count": checkpoint.source_count,
                "target_count": observed_target_count,
                "checksum": checkpoint.checksum,
                # Production targets expose an independent enumeration; use
                # its observed digest instead of merely echoing the source.
                "target_checksum": observed_target_checksum,
                "differences": list(checkpoint.differences),
            },
            gate="fail" if reasons else "pass",
            rejection_reasons=reasons,
        )

    async def _target_snapshot(self, tenant_id: str) -> tuple[int, str] | None:
        """Enumerate a target digest when the adapter supports it.

        Lightweight/offline adapters may be write-only.  The production
        PostgreSQL target implements ``list_records``; using it here makes
        ``target_checksum`` an observed target value rather than a copy of
        the source checkpoint.
        """

        target_list = getattr(self._target, "list_records", None)
        target_page = getattr(self._target, "list_records_page", None)
        if not callable(target_list) and not callable(target_page):
            return None
        kinds = self._manifest.kinds if self._manifest is not None else ("session", "memory")
        records: list[MigrationRecord] = []
        for kind in kinds:
            try:
                values = await self._enumerate_target_kind(
                    tenant_id, kind, target_list=target_list, target_page=target_page
                )
            except ValueError:
                # A deliberately write-only injected adapter may signal that
                # target enumeration is unavailable.  Production PostgreSQL
                # targets expose list_records_page and do not take this path.
                return None
            # RedisMigrationSource emits the canonical kind order from the
            # manifest and sorts resource IDs within each kind.  Preserve
            # that same order for the independently enumerated target digest;
            # a global lexical sort would put ``memory`` before ``session``
            # and make an identical mixed-kind migration fail checksum
            # validation.  Memory target pagination is keyed by its stable
            # UUID, so normalize the returned page order by source ID here.
            records.extend(sorted(values, key=lambda record: record.resource_id))
        checksum = "0" * 64
        for record in records:
            checksum = _rolling_checksum(checksum, record.checksum)
        return len(records), checksum

    async def _enumerate_target_kind(
        self,
        tenant_id: str,
        kind: str,
        *,
        target_list: Any,
        target_page: Any,
    ) -> tuple[MigrationRecord, ...]:
        """Read every target row or fail closed on an incomplete adapter."""

        values: list[MigrationRecord] = []
        cursor: str | None = None
        if callable(target_page):
            while True:
                page, next_cursor = await target_page(
                    tenant_id, kind, cursor=cursor, limit=self._batch_size
                )
                if len(page) > self._batch_size:
                    raise MigrationGuardError("target page exceeded the migration batch bound")
                values.extend(page)
                _ensure_record_count(len(values), "target enumeration")
                if next_cursor is None:
                    return tuple(values)
                if not page or next_cursor == cursor:
                    raise MigrationGuardError("target pagination did not advance")
                cursor = next_cursor
        if not callable(target_list):
            raise MigrationGuardError("target does not expose complete enumeration")
        try:
            page = await target_list(tenant_id, kind, limit=MAX_MIGRATION_EXPECTED_RECORDS)
        except TypeError:
            page = await target_list(tenant_id, kind)
        values.extend(page)
        _ensure_record_count(len(values), "target enumeration")
        if len(page) >= MAX_MIGRATION_EXPECTED_RECORDS:
            raise MigrationGuardError(
                "target enumeration is not provably complete; use keyset pagination"
            )
        return tuple(values)

    def _validate_scope(self, tenant_id: str, migration_id: str) -> None:
        manifest = self._manifest
        if manifest is None:
            return
        if manifest.tenant_id != tenant_id or manifest.migration_id != migration_id:
            raise MigrationManifestConflict("requested migration is outside the immutable manifest")

    async def _validate_source_snapshot(self, tenant_id: str) -> None:
        manifest = self._manifest
        if manifest is None:
            return
        snapshot = getattr(self._source, "snapshot", None)
        if snapshot is None:
            raise MigrationGuardError("guarded migration source must provide a source snapshot")
        observed = await snapshot(tenant_id)
        _ensure_record_count(observed.source_count, "source snapshot")
        if (
            observed.source_snapshot_id != manifest.source_snapshot_id
            or observed.source_count != manifest.source_count
            or observed.source_checksum != manifest.source_checksum
        ):
            raise MigrationManifestConflict(
                "migration source changed after the immutable manifest was created"
            )

    async def _save_checkpoint(self, checkpoint: MigrationCheckpoint) -> None:
        if self._guard is not None and self._lease is not None:
            await self._renew_lease()
        await self._checkpoints.save(checkpoint)

    async def _copy_all(self, checkpoint: MigrationCheckpoint) -> MigrationCheckpoint:
        cursor = checkpoint.cursor
        count = checkpoint.source_count
        checksum = checkpoint.checksum
        while True:
            records, next_cursor = await self._source.fetch(
                checkpoint.tenant_id, cursor=cursor, limit=self._batch_size
            )
            if len(records) > self._batch_size:
                raise MigrationGuardError("source page exceeded the migration batch bound")
            _ensure_record_count(count + len(records), "migration backfill")
            for record in records:
                await self._assert_lease()
                await self._target.upsert(checkpoint.tenant_id, record)
                checksum = _rolling_checksum(checksum, record.checksum)
                count += 1
            checkpoint = checkpoint.model_copy(
                update={
                    "cursor": next_cursor,
                    "source_count": count,
                    "target_count": count,
                    "checksum": checksum,
                    "completed": next_cursor is None,
                }
            )
            self._validate_manifest_totals(
                source_count=count,
                checksum=checksum,
                completed=next_cursor is None,
            )
            await self._save_checkpoint(checkpoint)
            if next_cursor is None:
                return checkpoint
            if not records or next_cursor == cursor:
                raise MigrationGuardError("source pagination did not advance")
            cursor = next_cursor

    async def _compare_all(
        self, checkpoint: MigrationCheckpoint, *, reject_differences: bool
    ) -> MigrationCheckpoint:
        cursor: str | None = None
        source_count = 0
        target_count = 0
        checksum = "0" * 64
        differences: list[str] = []
        target_list = getattr(self._target, "list_records", None)
        target_page = getattr(self._target, "list_records_page", None)
        target_records: dict[str, set[str]] = {}
        target_checksums: dict[str, dict[str, str]] = {}
        target_enumerated = False
        if callable(target_list) or callable(target_page):
            kinds = self._manifest.kinds if self._manifest is not None else ("session", "memory")
            for kind in kinds:
                try:
                    records = await self._enumerate_target_kind(
                        checkpoint.tenant_id,
                        kind,
                        target_list=target_list,
                        target_page=target_page,
                    )
                except ValueError:
                    target_records.clear()
                    target_checksums.clear()
                    target_count = 0
                    break
                target_records[kind] = {record.resource_id for record in records}
                target_checksums[kind] = {record.resource_id: record.checksum for record in records}
                target_count += len(records)
                _ensure_record_count(target_count, "target verification")
            else:
                target_enumerated = True
        source_resource_ids: dict[str, set[str]] = {}
        while True:
            previous_cursor = cursor
            # Comparison is read-only.  Fence each bounded source page while
            # the background heartbeat guards the whole phase; target writes
            # in ``_copy_all`` still assert the lease before every record.
            await self._assert_lease()
            records, next_cursor = await self._source.fetch(
                checkpoint.tenant_id, cursor=cursor, limit=self._batch_size
            )
            _ensure_record_count(source_count + len(records), "source verification")
            if next_cursor is not None and (not records or next_cursor == previous_cursor):
                raise MigrationGuardError("source pagination did not advance")
            for record in records:
                source_count += 1
                source_resource_ids.setdefault(record.kind, set()).add(record.resource_id)
                checksum = _rolling_checksum(checksum, record.checksum)
                if target_enumerated:
                    target_checksum = target_checksums.get(record.kind, {}).get(record.resource_id)
                    if target_checksum != record.checksum:
                        differences.append(f"{record.kind}/{record.resource_id}")
                else:
                    target = await self._target.read(
                        checkpoint.tenant_id, record.kind, record.resource_id
                    )
                    if target is not None:
                        target_count += 1
                    if target is None or target.checksum != record.checksum:
                        differences.append(f"{record.kind}/{record.resource_id}")
            if next_cursor is None:
                break
            cursor = next_cursor
        for kind, resource_ids in target_records.items():
            for resource_id in sorted(resource_ids - source_resource_ids.get(kind, set())):
                differences.append(f"target-only:{kind}/{resource_id}")
        result = checkpoint.model_copy(
            update={
                "source_count": source_count,
                "target_count": target_count,
                "checksum": checksum,
                "differences": tuple(differences),
                "completed": not differences,
            }
        )
        self._validate_manifest_totals(
            source_count=source_count,
            checksum=checksum,
            completed=not differences,
        )
        await self._save_checkpoint(result)
        if reject_differences and differences:
            raise ValueError("migration verification found differences")
        return result

    @asynccontextmanager
    async def _lease_liveness(self) -> AsyncIterator[None]:
        """Keep a guarded migration fenced during long source/target calls.

        Checkpoint-bound renewal is insufficient when one batch or a control
        hook takes longer than the lease.  A bounded background heartbeat
        renews the same owner/epoch while every target side effect also does a
        database owner/instance/epoch check.  Any heartbeat failure is kept
        and raised by the foreground operation; no work continues on a lost
        lease.
        """

        self._heartbeat_error = None
        self._heartbeat_stop = asyncio.Event()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        raised: BaseException | None = None
        try:
            await self._assert_lease()
            yield
            self._raise_heartbeat_error()
        except BaseException as error:
            raised = error
            raise
        finally:
            stop = self._heartbeat_stop
            task = self._heartbeat_task
            if stop is not None:
                stop.set()
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            self._heartbeat_stop = None
            self._heartbeat_task = None
            if raised is None:
                self._raise_heartbeat_error()

    async def _heartbeat_loop(self) -> None:
        stop = self._heartbeat_stop
        if stop is None:
            return
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), self._heartbeat_interval.total_seconds())
                return
            except TimeoutError:
                pass
            try:
                await self._renew_lease()
            except BaseException as error:
                self._heartbeat_error = error
                stop.set()
                return

    async def _renew_lease(self) -> None:
        if self._guard is None or self._lease is None:
            return
        self._raise_heartbeat_error()
        async with self._lease_lock:
            self._raise_heartbeat_error()
            renewed = await self._guard.renew(self._lease, lease_for=self._lease_for)
            self._lease = renewed
            bind_lease = getattr(self._target, "bind_migration_lease", None)
            if callable(bind_lease):
                bind_lease(renewed)

    async def _assert_lease(self) -> None:
        self._raise_heartbeat_error()
        if self._guard is None or self._lease is None:
            return
        assert_active = getattr(self._guard, "assert_active", None)
        if callable(assert_active):
            await assert_active(self._lease)
        self._raise_heartbeat_error()

    def _raise_heartbeat_error(self) -> None:
        error = self._heartbeat_error
        if error is None:
            return
        if isinstance(error, MigrationLeaseLost):
            raise error
        raise MigrationLeaseLost("migration lease heartbeat failed") from error

    def _validate_manifest_totals(
        self, *, source_count: int, checksum: str, completed: bool
    ) -> None:
        """Keep every completed phase bound to the immutable source snapshot.

        A source adapter can be cursor-correct while still returning a
        duplicate or silently dropping a record.  The manifest is the only
        stable boundary shared by resumed processes, so completed backfill and
        verification must agree with both its count and rolling checksum.
        """

        manifest = self._manifest
        if manifest is None or not completed:
            return
        if source_count != manifest.source_count:
            raise MigrationManifestConflict(
                "migration source count does not match the immutable manifest"
            )
        if checksum != manifest.source_checksum:
            raise MigrationManifestConflict(
                "migration source checksum does not match the immutable manifest"
            )

    @staticmethod
    def _validate_transition(
        current: MigrationCheckpoint | None, requested: MigrationPhase
    ) -> None:
        if requested == MigrationPhase.ROLLBACK:
            return
        if current is None:
            if requested != MigrationPhase.PREPARE:
                raise ValueError("migration must start with prepare")
            return
        if current.phase == requested and not current.completed:
            return
        if not current.completed:
            raise ValueError("current migration phase has not completed")
        current_index = _ORDER.index(current.phase)
        if current_index + 1 >= len(_ORDER) or _ORDER[current_index + 1] != requested:
            raise ValueError("migration phases must run in order")


class MigrationGuardError(RuntimeError):
    """Base error for migration-scope safety checks."""


class MigrationManifestConflict(MigrationGuardError):
    """The immutable scope for an existing migration does not match."""


class MigrationLeaseConflict(MigrationGuardError):
    """Another owner currently holds the tenant's active migration lease."""


class MigrationLeaseLost(MigrationGuardError):
    """A renew or release was attempted with a stale/expired lease."""


class MigrationTargetNotEmpty(MigrationGuardError):
    """A target tenant already contains data in one or more guarded tables."""


class MigrationScopeManifest(BaseModel):
    """Immutable, content-free identity of one tenant migration scope.

    A manifest is deliberately a small set of identifiers and checksums.  It
    never accepts arbitrary payload, credentials, URLs, or secret references.
    Once persisted, every field must compare equal before a lease can be
    acquired again.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    tenant_id: str = Field(min_length=1, max_length=256)
    migration_id: str = Field(min_length=1, max_length=256)
    source_kind: MigrationSourceKind
    kinds: tuple[str, ...] = Field(min_length=1, max_length=len(_MIGRATION_RECORD_KINDS))
    source_snapshot_id: str = Field(min_length=1, max_length=256)
    source_count: int = Field(ge=0, le=MAX_MIGRATION_EXPECTED_RECORDS)
    source_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    app_id: str = Field(min_length=1, max_length=256)
    app_revision: int = Field(default=1, ge=1)
    config_version: int = Field(ge=1)
    binding_id: str = Field(min_length=1, max_length=256)
    binding_revision: int = Field(ge=1)

    @model_validator(mode="before")
    @classmethod
    def _accept_stable_field_aliases(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if "record_kinds" in normalized:
            normalized["kinds"] = normalized.pop("record_kinds")
        if "config_revision" in normalized:
            normalized["config_version"] = normalized.pop("config_revision")
        return normalized

    @field_validator("kinds")
    @classmethod
    def _validate_kinds(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return canonical_migration_kinds(value)

    @property
    def config_revision(self) -> int:
        """Compatibility spelling for callers that call it a revision."""

        return self.config_version


class MigrationLease(BaseModel):
    """Fenced lease for a single active migration in a tenant.

    ``owner_id`` identifies the logical worker.  ``owner_instance`` is an
    unguessable, one-acquisition run nonce and is intentionally not reused by
    the guard.  This prevents a restarted process with the same configured
    worker name from being mistaken for the still-live process that owns the
    lease.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    tenant_id: str = Field(min_length=1, max_length=256)
    migration_id: str = Field(min_length=1, max_length=256)
    owner_id: str = Field(min_length=1, max_length=256)
    owner_instance: str = Field(default="legacy", min_length=1, max_length=256)
    lease_epoch: int = Field(ge=1)
    expires_at: datetime

    @model_validator(mode="before")
    @classmethod
    def _accept_fencing_alias(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if "lease_epoch" not in normalized and "fencing_token" in normalized:
            normalized["lease_epoch"] = normalized.pop("fencing_token")
        return normalized

    @property
    def fencing_token(self) -> int:
        return self.lease_epoch


class TargetEmptyPreflight(BaseModel):
    """Non-destructive target occupancy check performed under tenant RLS."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tenant_id: str
    checked_tables: tuple[str, ...]
    table_counts: dict[str, int]
    non_empty_tables: tuple[str, ...]

    @field_validator("checked_tables")
    @classmethod
    def _validate_checked_tables(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("target preflight table list must be unique")
        return value

    @field_validator("table_counts")
    @classmethod
    def _validate_table_counts(cls, value: dict[str, int]) -> dict[str, int]:
        if any(
            isinstance(count, bool) or not isinstance(count, int) or count < 0
            for count in value.values()
        ):
            raise ValueError("target preflight table counts must be non-negative integers")
        return value

    @field_validator("non_empty_tables")
    @classmethod
    def _validate_non_empty_tables(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("target preflight non-empty table list must be unique")
        return value

    @property
    def empty(self) -> bool:
        return not self.non_empty_tables


class PostgresMigrationGuard:
    """Transactional guard for migration scope, lease and target preflight.

    The guard owns no source/target data.  It only protects the coordinator's
    lifecycle.  Every operation sets ``app.tenant_id`` in the transaction,
    takes a transaction-scoped advisory lock for the tenant, and uses a
    compare-and-set predicate for lease mutations.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        max_lease_for: timedelta = timedelta(minutes=5),
    ) -> None:
        if max_lease_for <= timedelta(0) or max_lease_for > timedelta(hours=1):
            raise ValueError("migration guard max lease must be between 0 and 1 hour")
        self._pool = pool
        self._max_lease_for = max_lease_for

    async def acquire(
        self,
        manifest: MigrationScopeManifest,
        owner_id: str,
        *,
        lease_for: timedelta = timedelta(minutes=1),
    ) -> MigrationLease:
        self._validate_lease_for(lease_for)
        _validate_identifier(owner_id, "migration lease owner")
        owner_instance = _new_owner_instance()
        async with self._tenant_transaction(manifest.tenant_id) as connection:
            await self._lock_tenant(connection, manifest.tenant_id)
            await self._ensure_manifest(connection, manifest)
            lease = await self._acquire_locked(
                connection, manifest, owner_id, owner_instance, lease_for
            )
            await self._ensure_write_barrier(connection, lease, create=False)
            return lease

    async def acquire_with_target_preflight(
        self,
        manifest: MigrationScopeManifest,
        owner_id: str,
        *,
        lease_for: timedelta = timedelta(minutes=1),
    ) -> tuple[MigrationLease, TargetEmptyPreflight]:
        """Atomically verify an empty target and acquire the fenced lease.

        The target occupancy check, immutable manifest creation, and lease CAS
        all run after the same transaction-scoped tenant advisory lock is
        acquired.  Callers must use this method for a new migration instead
        of composing ``preflight_target_empty`` and ``acquire`` separately;
        otherwise a concurrent writer could fill the target between the two
        calls.
        """

        self._validate_lease_for(lease_for)
        _validate_identifier(owner_id, "migration lease owner")
        owner_instance = _new_owner_instance()
        async with self._tenant_transaction(manifest.tenant_id) as connection:
            await self._lock_tenant(connection, manifest.tenant_id)
            # Check before inserting the manifest/lease rows because those
            # rows are themselves part of the guarded target scope.
            preflight = await self._target_empty_preflight(connection, manifest.tenant_id)
            await self._ensure_manifest(connection, manifest)
            lease = await self._acquire_locked(
                connection, manifest, owner_id, owner_instance, lease_for
            )
            await self._ensure_write_barrier(connection, lease, create=True)
            return lease, preflight

    async def _acquire_locked(
        self,
        connection: asyncpg.Connection,
        manifest: MigrationScopeManifest,
        owner_id: str,
        owner_instance: str,
        lease_for: timedelta,
    ) -> MigrationLease:
        current = await connection.fetchrow(
            """
            SELECT tenant_id,migration_id,owner_id,owner_instance,lease_epoch,expires_at
              FROM migration_leases
             WHERE tenant_id=$1 AND expires_at > now()
             ORDER BY expires_at DESC
             FOR UPDATE
            """,
            manifest.tenant_id,
        )
        if current is not None:
            # An active lease is never reacquired, even by the same logical
            # owner.  A second acquire is a duplicate process attempt; only
            # the heartbeat/renew path may extend an existing lease.  This is
            # the key distinction between a live run and a restart/resume.
            raise MigrationLeaseConflict(
                f"tenant {manifest.tenant_id!r} already has an active migration lease"
            )

        row = await connection.fetchrow(
            """
            INSERT INTO migration_leases (
                tenant_id,migration_id,owner_id,owner_instance,lease_epoch,expires_at
            ) VALUES ($1,$2,$3,$4,1,now() + ($5 * interval '1 second'))
            ON CONFLICT (tenant_id,migration_id)
            DO UPDATE SET owner_id=EXCLUDED.owner_id,
                          owner_instance=EXCLUDED.owner_instance,
                          lease_epoch=migration_leases.lease_epoch+1,
                          expires_at=EXCLUDED.expires_at,
                          updated_at=now()
             WHERE migration_leases.expires_at <= now()
            RETURNING tenant_id,migration_id,owner_id,owner_instance,lease_epoch,expires_at
            """,
            manifest.tenant_id,
            manifest.migration_id,
            owner_id,
            owner_instance,
            lease_for.total_seconds(),
        )
        if row is None:
            raise MigrationLeaseConflict("migration lease CAS did not win")
        return _lease_from_row(row)

    async def _ensure_write_barrier(
        self,
        connection: asyncpg.Connection,
        lease: MigrationLease,
        *,
        create: bool,
    ) -> None:
        """Create or validate the persistent target write barrier.

        New migrations create the barrier only after the target-empty query in
        the same transaction.  Resumed migrations must find the existing
        barrier; silently creating one would reopen the preflight race.
        """

        if create:
            row = await connection.fetchrow(
                """
                INSERT INTO migration_write_barriers (
                    tenant_id,migration_id,owner_instance,lease_epoch,mode
                ) VALUES ($1,$2,$3,$4,'active')
                ON CONFLICT (tenant_id) DO UPDATE
                    SET migration_id=EXCLUDED.migration_id,
                        owner_instance=EXCLUDED.owner_instance,
                        lease_epoch=EXCLUDED.lease_epoch,
                        mode='active', updated_at=now()
                RETURNING tenant_id
                """,
                lease.tenant_id,
                lease.migration_id,
                lease.owner_instance,
                lease.lease_epoch,
            )
        else:
            row = await connection.fetchrow(
                """
                UPDATE migration_write_barriers
                   SET migration_id=$2, owner_instance=$3, lease_epoch=$4,
                       mode='active', updated_at=now()
                 WHERE tenant_id=$1
                RETURNING tenant_id
                """,
                lease.tenant_id,
                lease.migration_id,
                lease.owner_instance,
                lease.lease_epoch,
            )
        if row is None:
            raise MigrationLeaseLost("migration write barrier is missing")

    async def renew(
        self,
        lease: MigrationLease,
        *,
        lease_for: timedelta = timedelta(minutes=1),
    ) -> MigrationLease:
        self._validate_lease_for(lease_for)
        async with self._tenant_transaction(lease.tenant_id) as connection:
            await self._lock_tenant(connection, lease.tenant_id)
            row = await connection.fetchrow(
                """
                UPDATE migration_leases AS l
                   SET expires_at=GREATEST(
                       l.expires_at + interval '1 microsecond',
                       now() + ($6 * interval '1 second')
                   ), updated_at=now()
                  FROM migration_write_barriers AS b
                 WHERE l.tenant_id=$1 AND l.migration_id=$2 AND l.owner_id=$3
                   AND l.owner_instance=$4 AND l.lease_epoch=$5 AND l.expires_at > now()
                   AND b.tenant_id=l.tenant_id AND b.migration_id=l.migration_id
                   AND b.owner_instance=l.owner_instance AND b.lease_epoch=l.lease_epoch
                   AND b.mode='active'
                RETURNING l.tenant_id,l.migration_id,l.owner_id,l.owner_instance,
                          l.lease_epoch,l.expires_at
                """,
                lease.tenant_id,
                lease.migration_id,
                lease.owner_id,
                lease.owner_instance,
                lease.lease_epoch,
                lease_for.total_seconds(),
            )
            if row is None:
                raise MigrationLeaseLost("migration lease is stale or expired")
            return _lease_from_row(row)

    async def release(self, lease: MigrationLease) -> bool:
        async with self._tenant_transaction(lease.tenant_id) as connection:
            await self._lock_tenant(connection, lease.tenant_id)
            barrier = await connection.fetchrow(
                """
                UPDATE migration_write_barriers
                   SET mode='released', updated_at=now()
                 WHERE tenant_id=$1 AND migration_id=$2 AND owner_instance=$3
                   AND lease_epoch=$4 AND mode='active'
                RETURNING tenant_id
                """,
                lease.tenant_id,
                lease.migration_id,
                lease.owner_instance,
                lease.lease_epoch,
            )
            if barrier is None:
                raise MigrationLeaseLost("migration write barrier is stale or missing")
            row = await connection.fetchrow(
                """
                UPDATE migration_leases
                   SET expires_at=now(), updated_at=now()
                 WHERE tenant_id=$1 AND migration_id=$2 AND owner_id=$3
                   AND owner_instance=$4 AND lease_epoch=$5 AND expires_at > now()
                RETURNING tenant_id
                """,
                lease.tenant_id,
                lease.migration_id,
                lease.owner_id,
                lease.owner_instance,
                lease.lease_epoch,
            )
            if row is None:
                raise MigrationLeaseLost("migration lease is stale or expired")
            return True

    async def assert_active(self, lease: MigrationLease) -> None:
        """Verify owner instance and fencing epoch immediately before a side effect."""

        async with self._tenant_transaction(lease.tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT tenant_id
                 FROM migration_leases
                 WHERE tenant_id=$1 AND migration_id=$2 AND owner_id=$3
                   AND owner_instance=$4 AND lease_epoch=$5 AND expires_at > now()
                   AND EXISTS (
                       SELECT 1 FROM migration_write_barriers
                        WHERE tenant_id=migration_leases.tenant_id
                          AND migration_id=migration_leases.migration_id
                          AND owner_instance=migration_leases.owner_instance
                          AND lease_epoch=migration_leases.lease_epoch
                          AND mode='active'
                   )
                """,
                lease.tenant_id,
                lease.migration_id,
                lease.owner_id,
                lease.owner_instance,
                lease.lease_epoch,
            )
        if row is None:
            raise MigrationLeaseLost("migration lease is stale or expired")

    async def load_manifest(
        self, tenant_id: str, migration_id: str
    ) -> MigrationScopeManifest | None:
        _validate_identifier(tenant_id, "tenant id")
        _validate_identifier(migration_id, "migration id")
        async with self._tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT tenant_id,migration_id,source_kind,kinds,source_snapshot_id,
                       source_count,source_checksum,app_id,app_revision,
                       config_version,binding_id,binding_revision
                  FROM migration_scope_manifests
                 WHERE tenant_id=$1 AND migration_id=$2
                """,
                tenant_id,
                migration_id,
            )
        return None if row is None else _manifest_from_row(row)

    async def preflight_target_empty(self, tenant_id: str) -> TargetEmptyPreflight:
        _validate_identifier(tenant_id, "tenant id")
        async with self._tenant_transaction(tenant_id) as connection:
            await self._lock_tenant(connection, tenant_id)
            return await self._target_empty_preflight(connection, tenant_id)

    async def _target_empty_preflight(
        self, connection: asyncpg.Connection, tenant_id: str
    ) -> TargetEmptyPreflight:
        query = """
            SELECT table_name,row_count
              FROM (
                SELECT 'inbound_messages'::text AS table_name,
                       count(*)::bigint AS row_count
                  FROM public.inbound_messages WHERE tenant_id=$1
                UNION ALL
                SELECT 'outbound_messages'::text,count(*)::bigint
                  FROM public.outbound_messages WHERE tenant_id=$1
                UNION ALL
                SELECT 'delivery_attempts'::text,count(*)::bigint
                  FROM public.delivery_attempts WHERE tenant_id=$1
                UNION ALL
                SELECT 'sessions'::text,count(*)::bigint
                  FROM public.sessions WHERE tenant_id=$1
                UNION ALL
                SELECT 'session_turns'::text,count(*)::bigint
                  FROM public.session_turns WHERE tenant_id=$1
                UNION ALL
                SELECT 'turn_intents'::text,count(*)::bigint
                  FROM public.turn_intents WHERE tenant_id=$1
                UNION ALL
                SELECT 'session_events'::text,count(*)::bigint
                  FROM public.session_events WHERE tenant_id=$1
                UNION ALL
                SELECT 'memories'::text,count(*)::bigint
                  FROM public.memories WHERE tenant_id=$1
                UNION ALL
                SELECT 'session_summaries'::text,count(*)::bigint
                  FROM public.session_summaries WHERE tenant_id=$1
                UNION ALL
                SELECT 'artifacts'::text,count(*)::bigint
                  FROM public.artifacts WHERE tenant_id=$1
                UNION ALL
                SELECT 'knowledge_items'::text,count(*)::bigint
                  FROM public.knowledge_items WHERE tenant_id=$1
                UNION ALL
                SELECT 'knowledge_embeddings'::text,count(*)::bigint
                  FROM public.knowledge_embeddings WHERE tenant_id=$1
                UNION ALL
                SELECT 'outbox_events'::text,count(*)::bigint
                  FROM public.outbox_events WHERE tenant_id=$1
                UNION ALL
                SELECT 'dead_letters'::text,count(*)::bigint
                  FROM public.dead_letters WHERE tenant_id=$1
                UNION ALL
                SELECT 'tool_executions'::text,count(*)::bigint
                  FROM public.tool_executions WHERE tenant_id=$1
                UNION ALL
                SELECT 'confirmation_challenges'::text,count(*)::bigint
                  FROM public.confirmation_challenges WHERE tenant_id=$1
                UNION ALL
                SELECT 'audit_logs'::text,count(*)::bigint
                  FROM public.audit_logs WHERE tenant_id=$1
                UNION ALL
                SELECT 'session_mailboxes'::text,count(*)::bigint
                  FROM public.session_mailboxes WHERE tenant_id=$1
                UNION ALL
                SELECT 'session_mailbox_items'::text,count(*)::bigint
                  FROM public.session_mailbox_items WHERE tenant_id=$1
                UNION ALL
                SELECT protected.table_name,protected.row_count
                  FROM public.migration_protected_target_counts($1) AS protected
                UNION ALL
                SELECT 'migration_checkpoints'::text,count(*)::bigint
                  FROM public.migration_checkpoints WHERE tenant_id=$1
                UNION ALL
                SELECT 'migration_scope_manifests'::text,count(*)::bigint
                  FROM public.migration_scope_manifests WHERE tenant_id=$1
                UNION ALL
                SELECT 'migration_leases'::text,count(*)::bigint
                  FROM public.migration_leases WHERE tenant_id=$1
              ) AS target_counts
             ORDER BY table_name
        """
        rows = await connection.fetch(query, tenant_id)
        count_rows = [
            (str(_row_value(row, "table_name")), int(_row_value(row, "row_count"))) for row in rows
        ]
        table_names = [table_name for table_name, _row_count in count_rows]
        expected_tables = set(_TARGET_EMPTY_TABLES)
        observed_tables = set(table_names)
        missing = expected_tables - observed_tables
        unexpected = observed_tables - expected_tables
        duplicates = {table_name for table_name in table_names if table_names.count(table_name) > 1}
        if missing or unexpected or duplicates:
            details = []
            if missing:
                details.append("missing=" + ",".join(sorted(missing)))
            if unexpected:
                details.append("unexpected=" + ",".join(sorted(unexpected)))
            if duplicates:
                details.append("duplicates=" + ",".join(sorted(duplicates)))
            raise MigrationGuardError(
                "target empty preflight did not return the exact guarded table set: "
                + "; ".join(details)
            )
        counts = dict(count_rows)
        result = TargetEmptyPreflight(
            tenant_id=tenant_id,
            checked_tables=_TARGET_EMPTY_TABLES,
            table_counts={table: counts[table] for table in _TARGET_EMPTY_TABLES},
            non_empty_tables=tuple(table for table in _TARGET_EMPTY_TABLES if counts[table] > 0),
        )
        if not result.empty:
            raise MigrationTargetNotEmpty(
                "target tenant is not empty: " + ", ".join(result.non_empty_tables)
            )
        return result

    async def target_empty_preflight(self, tenant_id: str) -> TargetEmptyPreflight:
        """Alias with the noun-first spelling used by migration operators."""

        return await self.preflight_target_empty(tenant_id)

    async def _ensure_manifest(
        self, connection: asyncpg.Connection, manifest: MigrationScopeManifest
    ) -> None:
        await connection.execute(
            """
            INSERT INTO migration_scope_manifests (
                tenant_id,migration_id,source_kind,kinds,source_snapshot_id,
                source_count,source_checksum,app_id,app_revision,config_version,
                binding_id,binding_revision
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (tenant_id,migration_id) DO NOTHING
            """,
            manifest.tenant_id,
            manifest.migration_id,
            manifest.source_kind.value,
            list(manifest.kinds),
            manifest.source_snapshot_id,
            manifest.source_count,
            manifest.source_checksum,
            manifest.app_id,
            manifest.app_revision,
            manifest.config_version,
            manifest.binding_id,
            manifest.binding_revision,
        )
        row = await connection.fetchrow(
            """
            SELECT tenant_id,migration_id,source_kind,kinds,source_snapshot_id,
                   source_count,source_checksum,app_id,app_revision,
                   config_version,binding_id,binding_revision
              FROM migration_scope_manifests
             WHERE tenant_id=$1 AND migration_id=$2
            """,
            manifest.tenant_id,
            manifest.migration_id,
        )
        if row is None:
            raise MigrationGuardError("migration manifest was not persisted")
        stored = _manifest_from_row(row)
        if stored != manifest:
            raise MigrationManifestConflict(
                "migration manifest is immutable and does not match the stored scope"
            )

    async def _lock_tenant(self, connection: asyncpg.Connection, tenant_id: str) -> None:
        await connection.fetchval(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            tenant_id,
        )

    @asynccontextmanager
    async def _tenant_transaction(self, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            yield connection

    def _validate_lease_for(self, lease_for: timedelta) -> None:
        if lease_for <= timedelta(0) or lease_for > self._max_lease_for:
            raise ValueError("migration lease duration exceeds the configured safety bound")


def _validate_page_limit(value: int, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 1
        or value > MAX_MIGRATION_BATCH_SIZE
    ):
        raise ValueError(f"{label} must be between 1 and {MAX_MIGRATION_BATCH_SIZE}")


def _ensure_record_count(value: int, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > MAX_MIGRATION_EXPECTED_RECORDS
    ):
        raise MigrationGuardError(f"{label} exceeds MAX_MIGRATION_EXPECTED_RECORDS")


def _validate_identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError(f"{label} must be 1-256 characters")


def _new_owner_instance() -> str:
    """Return a fresh per-acquisition nonce that is never derived from owner_id."""

    return uuid4().hex


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row[key]
    return row[key]


def _manifest_from_row(row: Any) -> MigrationScopeManifest:
    raw_kinds = _row_value(row, "kinds")
    if isinstance(raw_kinds, str):
        raw_kinds = json.loads(raw_kinds)
    return MigrationScopeManifest(
        tenant_id=str(_row_value(row, "tenant_id")),
        migration_id=str(_row_value(row, "migration_id")),
        source_kind=str(_row_value(row, "source_kind")),
        kinds=tuple(str(item) for item in raw_kinds),
        source_snapshot_id=str(_row_value(row, "source_snapshot_id")),
        source_count=int(_row_value(row, "source_count")),
        source_checksum=str(_row_value(row, "source_checksum")),
        app_id=str(_row_value(row, "app_id")),
        app_revision=int(_row_value(row, "app_revision")),
        config_version=int(_row_value(row, "config_version")),
        binding_id=str(_row_value(row, "binding_id")),
        binding_revision=int(_row_value(row, "binding_revision")),
    )


def _lease_from_row(row: Any) -> MigrationLease:
    expires_at = _row_value(row, "expires_at")
    if not isinstance(expires_at, datetime):
        expires_at = datetime.fromisoformat(str(expires_at))
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return MigrationLease(
        tenant_id=str(_row_value(row, "tenant_id")),
        migration_id=str(_row_value(row, "migration_id")),
        owner_id=str(_row_value(row, "owner_id")),
        owner_instance=str(row.get("owner_instance", "legacy"))
        if isinstance(row, dict)
        else str(_row_value(row, "owner_instance")),
        lease_epoch=int(_row_value(row, "lease_epoch")),
        expires_at=expires_at,
    )


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _redis_pattern_literal(value: str) -> str:
    """Escape Redis glob metacharacters in a tenant-owned key prefix."""

    return value.replace("\\", "\\\\").replace("*", "\\*").replace("?", "\\?").replace("[", "\\[")


def _decode_projection_v2_key(key: str, tenant_id: str) -> str | None:
    prefix = "trpc:projection:session:v2:"
    if not key.startswith(prefix):
        return None
    encoded = key[len(prefix) :]
    tenant_part, separator, session_part = encoded.partition(".")
    if not separator or not tenant_part or not session_part:
        return None
    try:
        tenant_raw = base64.urlsafe_b64decode(tenant_part + "=" * (-len(tenant_part) % 4))
        session_raw = base64.urlsafe_b64decode(session_part + "=" * (-len(session_part) % 4))
        decoded_tenant = tenant_raw.decode("utf-8")
        decoded_session = session_raw.decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None
    if decoded_tenant != tenant_id:
        return None
    return decoded_session


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _json_object(value: Any) -> dict[str, Any]:
    value = _json_value(value)
    if not isinstance(value, dict):
        raise ValueError("PostgreSQL migration JSON column is not an object")
    return value


def _parse_cursor(cursor: str | None, *, upper_bound: int) -> int:
    if cursor is None:
        return 0
    try:
        value = int(cursor)
    except (TypeError, ValueError) as exc:
        raise ValueError("migration cursor must be a non-negative integer") from exc
    if value < 0 or value > upper_bound:
        raise ValueError("migration cursor is outside the source snapshot")
    return value


def _cursor_position(cursor: str | None, keys: tuple[tuple[str, str, str], ...]) -> int:
    if cursor is None:
        return 0
    if cursor.isdecimal():
        return _parse_cursor(cursor, upper_bound=len(keys))
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        value = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("migration cursor is invalid") from exc
    if (
        not isinstance(value, list)
        or len(value) != 3
        or not all(isinstance(item, str) for item in value)
    ):
        raise ValueError("migration cursor is invalid")
    cursor_key = (value[0], value[1], value[2])
    # ``keys`` is grouped in the manifest's custom kind order and sorted by
    # resource/key within each group.  A normal tuple bisect would silently
    # assume lexical kind order, so derive the small kind-rank map and bisect
    # with the same comparison without allocating a transformed million-row
    # list for every page resume.
    kind_order: dict[str, int] = {}
    for item in keys:
        kind_order.setdefault(item[0], len(kind_order))
    if value[0] not in kind_order:
        raise ValueError("migration cursor is outside the source snapshot")

    def sort_key(item: tuple[str, str, str]) -> tuple[int, str, str]:
        return (kind_order[item[0]], item[1], item[2])

    cursor_sort_key = sort_key(cursor_key)
    left, right = 0, len(keys)
    while left < right:
        middle = (left + right) // 2
        if sort_key(keys[middle]) <= cursor_sort_key:
            left = middle + 1
        else:
            right = middle
    if left == 0 or keys[left - 1] != cursor_key:
        # Resuming at an insertion point could skip records or replay a
        # different scope, so an unknown cursor fails closed instead of
        # guessing a position.
        raise ValueError("migration cursor is outside the source snapshot")
    return left


def _encode_source_cursor(value: tuple[str, str, str]) -> str:
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _canonical_session_payload(
    tenant_id: str, resource_id: str, value: dict[str, Any]
) -> dict[str, Any]:
    app_id = value.get("app_id") or value.get("app_name")
    principal_id = value.get("principal_id") or value.get("user_id")
    if not isinstance(app_id, str) or not app_id:
        raise ValueError(f"session {resource_id!r} has no app_id/app_name")
    if not isinstance(principal_id, str) or not principal_id:
        raise ValueError(f"session {resource_id!r} has no principal_id/user_id")
    state = _json_value(value.get("state", value.get("state_json", {})))
    if not isinstance(state, dict):
        raise ValueError(f"session {resource_id!r} state must be a JSON object")
    raw_events = _json_value(value.get("events", value.get("session_events", [])))
    if raw_events is None:
        raw_events = []
    if not isinstance(raw_events, list):
        raise ValueError(f"session {resource_id!r} events must be a JSON array")
    events: list[dict[str, Any]] = []
    for index, raw_event in enumerate(raw_events, start=1):
        if not isinstance(raw_event, dict):
            raise ValueError(f"session {resource_id!r} contains a non-object event")
        sequence = int(raw_event.get("sequence", index))
        if sequence < 1:
            raise ValueError(f"session {resource_id!r} contains an invalid event sequence")
        event_json = _json_value(raw_event.get("event", raw_event.get("event_json", {})))
        if not isinstance(event_json, dict):
            raise ValueError(f"session {resource_id!r} event {sequence} must be an object")
        state_delta = _json_value(raw_event.get("state_delta", {}))
        if not isinstance(state_delta, dict):
            raise ValueError(f"session {resource_id!r} event {sequence} state_delta must be object")
        timestamp = raw_event.get("timestamp", raw_event.get("event_timestamp", 0.0))
        if timestamp is None:
            timestamp = 0.0
        events.append(
            {
                "sequence": sequence,
                "event_id": str(
                    raw_event.get(
                        "event_id", raw_event.get("id", f"migration-event:{resource_id}:{sequence}")
                    )
                ),
                "author": str(raw_event.get("author", "migration")),
                "timestamp": float(timestamp),
                "event": event_json,
                "state_delta": state_delta,
            }
        )
    events.sort(key=lambda item: item["sequence"])
    max_sequence = max((item["sequence"] for item in events), default=0)
    next_sequence = int(value.get("next_sequence", max_sequence + 1))
    if next_sequence < max_sequence + 1:
        raise ValueError(f"session {resource_id!r} next_sequence precedes its events")
    return {
        "tenant_id": tenant_id,
        "app_id": app_id,
        "session_id": resource_id,
        "principal_id": principal_id,
        "version": int(value.get("version", 0)),
        "next_sequence": next_sequence,
        "state": state,
        "events": events,
    }


def _canonical_memory_payload(value: dict[str, Any]) -> dict[str, Any]:
    principal_id = value.get("principal_id") or value.get("user_id")
    if not isinstance(principal_id, str) or not principal_id:
        raise ValueError("memory record has no principal_id/user_id")
    memory = value.get("memory", value.get("memory_json", value.get("value", value)))
    memory = _json_value(memory)
    source_sequence = value.get("source_sequence")
    if source_sequence is not None:
        source_sequence = int(source_sequence)
    return {
        "principal_id": str(principal_id),
        "session_id": value.get("session_id"),
        "source_sequence": source_sequence,
        "memory": memory,
        "projection_status": str(value.get("projection_status", "pending")),
    }


def _stable_uuid(tenant_id: str, value: str) -> UUID:
    return uuid5(uuid5(NAMESPACE_URL, tenant_id), value)


def _rolling_checksum(previous: str, record_checksum: str) -> str:
    return hashlib.sha256(bytes.fromhex(previous) + bytes.fromhex(record_checksum)).hexdigest()


__all__ = [
    "CANONICAL_REDIS_MIGRATION_KINDS",
    "MAX_MIGRATION_BATCH_SIZE",
    "MAX_MIGRATION_EXPECTED_RECORDS",
    "FencedMigrationControl",
    "InMemoryMigrationCheckpointStore",
    "MigrationCheckpoint",
    "MigrationCheckpointStore",
    "MigrationControl",
    "MigrationCoordinator",
    "MigrationGuardError",
    "MigrationLease",
    "MigrationLeaseConflict",
    "MigrationLeaseLost",
    "MigrationManifestConflict",
    "MigrationPhase",
    "MigrationRecord",
    "MigrationResult",
    "MigrationScopeManifest",
    "MigrationSource",
    "MigrationSourceKind",
    "MigrationSourceSnapshot",
    "MigrationTarget",
    "MigrationTargetNotEmpty",
    "PostgresMigrationCheckpointStore",
    "PostgresMigrationGuard",
    "PostgresMigrationTarget",
    "RedisMigrationSource",
    "TargetEmptyPreflight",
    "canonical_migration_kinds",
]

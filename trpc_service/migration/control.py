"""Durable route, item and admission state for storage migrations."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from trpc_service._compat import StrEnum


class StorageRouteMode(StrEnum):
    SOURCE_ONLY = "source_only"
    DUAL_WRITE = "dual_write"
    SHADOW_READ = "shadow_read"
    TARGET_PRIMARY_MIRROR = "target_primary_mirror"
    TARGET_ONLY = "target_only"


class StorageMigrationRoute(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    route_version: int
    job_id: str = ""
    mode: StorageRouteMode
    source_backend: str
    target_backend: str
    config_version: int
    shadow_sample_rate: float = 0.1
    admission_paused: bool = False
    active: bool = True
    created_at: datetime | None = None


class MigrationItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    resource_kind: str
    resource_key: str
    source_hash: str = ""
    target_hash: str = ""
    state: str
    attempts: int = 0
    last_error: str = ""


class PostgresMigrationControlStore:
    """PostgreSQL control plane shared by Gateway, Worker and Admin."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def active_route(self, tenant_id: str) -> StorageMigrationRoute | None:
        row = await self._pool.fetchrow("SELECT * FROM storage_migration_route WHERE tenant_id=$1 AND active",
                                        tenant_id)
        if row is None:
            return None
        return StorageMigrationRoute(tenant_id=row["tenant_id"],
                                     route_version=row["route_version"],
                                     job_id=row["job_id"] or "",
                                     mode=row["mode"],
                                     source_backend=row["source_backend"],
                                     target_backend=row["target_backend"],
                                     config_version=row["config_version"],
                                     shadow_sample_rate=row["shadow_sample_rate"],
                                     admission_paused=row["admission_paused"],
                                     active=row["active"],
                                     created_at=row["created_at"])

    async def get_route(self, tenant_id: str, route_version: int) -> StorageMigrationRoute | None:
        row = await self._pool.fetchrow("SELECT * FROM storage_migration_route WHERE tenant_id=$1 AND route_version=$2",
                                        tenant_id, route_version)
        if row is None:
            return None
        return StorageMigrationRoute(tenant_id=row["tenant_id"],
                                     route_version=row["route_version"],
                                     job_id=row["job_id"] or "",
                                     mode=row["mode"],
                                     source_backend=row["source_backend"],
                                     target_backend=row["target_backend"],
                                     config_version=row["config_version"],
                                     shadow_sample_rate=row["shadow_sample_rate"],
                                     admission_paused=row["admission_paused"],
                                     active=row["active"],
                                     created_at=row["created_at"])

    async def publish_route(self, route: StorageMigrationRoute) -> StorageMigrationRoute:
        """Publish an immutable generation and atomically move the active pointer."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                                   f"storage-route:{route.tenant_id}")
                await conn.execute("UPDATE storage_migration_route SET active=FALSE WHERE tenant_id=$1 AND active",
                                   route.tenant_id)
                await conn.execute(
                    "INSERT INTO storage_migration_route "
                    "(tenant_id,route_version,job_id,mode,source_backend,target_backend,config_version,"
                    "shadow_sample_rate,admission_paused,active) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE)",
                    route.tenant_id, route.route_version, route.job_id or None, route.mode.value, route.source_backend,
                    route.target_backend, route.config_version, route.shadow_sample_rate, route.admission_paused)
        return route.model_copy(deep=True)

    async def next_route_version(self, tenant_id: str) -> int:
        value = await self._pool.fetchval(
            "SELECT COALESCE(MAX(route_version),0)+1 FROM storage_migration_route WHERE tenant_id=$1", tenant_id)
        return int(value)

    async def set_admission_paused(self, tenant_id: str, paused: bool) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", f"storage-route:{tenant_id}")
                status = await conn.execute(
                    "UPDATE storage_migration_route SET admission_paused=$2 WHERE tenant_id=$1 AND active", tenant_id,
                    paused)
        if status != "UPDATE 1":
            raise RuntimeError("active storage migration route is missing")

    async def active_request_count(self, tenant_id: str) -> int:
        return int(await self._pool.fetchval(
            "SELECT count(*) FROM request_execution WHERE tenant_id=$1 "
            "AND state IN ('reserved','queued','running')", tenant_id))

    async def upsert_item(self, item: MigrationItem) -> None:
        await self._pool.execute(
            "INSERT INTO migration_item (job_id,resource_kind,resource_key,source_hash,target_hash,state,"
            "attempts,last_error) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) "
            "ON CONFLICT (job_id,resource_kind,resource_key) DO UPDATE SET "
            "source_hash=EXCLUDED.source_hash,target_hash=EXCLUDED.target_hash,state=EXCLUDED.state,"
            "attempts=migration_item.attempts+1,last_error=EXCLUDED.last_error,updated_at=now()", item.job_id,
            item.resource_kind, item.resource_key, item.source_hash or None, item.target_hash or None, item.state,
            item.attempts, item.last_error or None)

    async def list_items(self, job_id: str, *, state: str = "", limit: int = 200) -> list[MigrationItem]:
        if state:
            rows = await self._pool.fetch(
                "SELECT * FROM migration_item WHERE job_id=$1 AND state=$2 "
                "ORDER BY resource_kind,resource_key LIMIT $3", job_id, state, limit)
        else:
            rows = await self._pool.fetch(
                "SELECT * FROM migration_item WHERE job_id=$1 ORDER BY resource_kind,resource_key LIMIT $2", job_id,
                limit)
        return [
            MigrationItem(job_id=row["job_id"],
                          resource_kind=row["resource_kind"],
                          resource_key=row["resource_key"],
                          source_hash=row["source_hash"] or "",
                          target_hash=row["target_hash"] or "",
                          state=row["state"],
                          attempts=row["attempts"],
                          last_error=row["last_error"] or "") for row in rows
        ]

    async def mark_dirty(self, job_id: str, resource_kind: str, resource_key: str, error: Exception) -> None:
        await self._pool.execute(
            "INSERT INTO migration_dirty_key (job_id,resource_kind,resource_key,last_error,attempts) "
            "VALUES ($1,$2,$3,$4,1) ON CONFLICT (job_id,resource_kind,resource_key) DO UPDATE SET "
            "last_error=EXCLUDED.last_error,attempts=migration_dirty_key.attempts+1,updated_at=now()", job_id,
            resource_kind, resource_key, (str(error) or type(error).__name__)[:128])

    async def clear_dirty(self, job_id: str, resource_kind: str, resource_key: str) -> None:
        await self._pool.execute(
            "DELETE FROM migration_dirty_key WHERE job_id=$1 AND resource_kind=$2 AND resource_key=$3", job_id,
            resource_kind, resource_key)

    async def dirty_count(self, job_id: str) -> int:
        return int(await self._pool.fetchval("SELECT count(*) FROM migration_dirty_key WHERE job_id=$1", job_id))

    async def mismatch_count(self, job_id: str) -> int:
        return int(await self._pool.fetchval(
            "SELECT count(*) FROM migration_item WHERE job_id=$1 AND state IN ('dirty','failed')", job_id))

    async def resource_keys(self, job_id: str) -> set[tuple[str, str]]:
        rows = await self._pool.fetch("SELECT resource_kind,resource_key FROM migration_item WHERE job_id=$1", job_id)
        return {(row["resource_kind"], row["resource_key"]) for row in rows}

    async def backup_redis_target(self, job_id: str, resource_kind: str, resource_key: str, redis_key: str,
                                  redis_type: str, payload: Any, ttl_ms: int) -> None:
        import json
        await self._pool.execute(
            "INSERT INTO migration_target_backup "
            "(job_id,resource_kind,resource_key,redis_key,redis_type,payload,ttl_milliseconds) "
            "VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7) ON CONFLICT (job_id,redis_key) DO NOTHING", job_id, resource_kind,
            resource_key, redis_key, redis_type, json.dumps(payload, ensure_ascii=False), ttl_ms)

    async def target_backups(self, job_id: str, *, unrestored_only: bool = True) -> list[dict[str, Any]]:
        condition = "AND NOT restored" if unrestored_only else ""
        rows = await self._pool.fetch(
            "SELECT * FROM migration_target_backup WHERE job_id=$1 " + condition + " ORDER BY redis_key", job_id)
        return [dict(row) for row in rows]

    async def mark_backup_restored(self, job_id: str, redis_key: str) -> None:
        await self._pool.execute("UPDATE migration_target_backup SET restored=TRUE WHERE job_id=$1 AND redis_key=$2",
                                 job_id, redis_key)

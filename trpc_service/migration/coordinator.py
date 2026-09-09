"""Explicit migration phases, checkpoints, verification and rollback."""

from __future__ import annotations

import asyncio
import uuid
import json
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any
from typing import Protocol
from datetime import datetime

from pydantic import BaseModel
from pydantic import Field

from trpc_service._compat import StrEnum


class MigrationPhase(StrEnum):
    PREPARING = "preparing"
    DUAL_WRITE = "dual_write"
    BACKFILLING = "backfilling"
    VERIFYING = "verifying"
    SHADOW_READ = "shadow_read"
    CUTOVER = "cutover"
    COMPLETED = "completed"
    ROLLED_BACK = "rolled_back"


_NEXT = {
    MigrationPhase.PREPARING: MigrationPhase.DUAL_WRITE,
    MigrationPhase.DUAL_WRITE: MigrationPhase.BACKFILLING,
    MigrationPhase.BACKFILLING: MigrationPhase.VERIFYING,
    MigrationPhase.VERIFYING: MigrationPhase.SHADOW_READ,
    MigrationPhase.SHADOW_READ: MigrationPhase.CUTOVER,
    MigrationPhase.CUTOVER: MigrationPhase.COMPLETED,
}


class MigrationJob(BaseModel):
    job_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    tenant_id: str
    resource_type: str
    source_backend: str
    target_backend: str
    phase: MigrationPhase = MigrationPhase.PREPARING
    checkpoint: dict[str, Any] = Field(default_factory=dict)
    source_count: int = 0
    target_count: int = 0
    mismatch_count: int = 0
    last_error: str = ""
    revision: int = 0
    source_config_version: int = 0
    target_config_version: int = 0
    route_version: int = 0
    batch_size: int = Field(default=100, ge=1, le=1000)
    shadow_sample_rate: float = Field(default=0.1, ge=0, le=1)
    rollback_window_seconds: int = Field(default=3600, ge=0)
    rollback_deadline: datetime | None = None
    lease_owner: str = ""
    lease_until: datetime | None = None


class MigrationStore(Protocol):

    async def save(self, job: MigrationJob) -> MigrationJob:
        ...

    async def get(self, job_id: str) -> MigrationJob:
        ...


class InMemoryMigrationStore:

    def __init__(self) -> None:
        self._jobs: dict[str, MigrationJob] = {}
        self._lock = asyncio.Lock()
        self._claims: set[str] = set()

    async def save(self, job: MigrationJob) -> MigrationJob:
        async with self._lock:
            self._jobs[job.job_id] = job.model_copy(deep=True)
        return job.model_copy(deep=True)

    async def get(self, job_id: str) -> MigrationJob:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"migration job not found: {job_id}")
            return job.model_copy(deep=True)

    async def claim(self, job_id: str, owner: str, lease_seconds: int = 60) -> MigrationJob | None:
        del lease_seconds
        async with self._lock:
            if job_id in self._claims:
                return None
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"migration job not found: {job_id}")
            self._claims.add(job_id)
            job = job.model_copy(update={"lease_owner": owner})
            self._jobs[job_id] = job
            return job.model_copy(deep=True)

    async def release(self, job_id: str, owner: str) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job and job.lease_owner == owner:
                self._jobs[job_id] = job.model_copy(update={"lease_owner": "", "lease_until": None})
            self._claims.discard(job_id)


class PostgresMigrationStore:
    """Durable migration_job adapter for restart-safe phase/checkpoint storage."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def save(self, job: MigrationJob) -> MigrationJob:
        await self._pool.execute(
            """
            INSERT INTO migration_job
                (job_id,tenant_id,resource_type,source_backend,target_backend,
                 phase,checkpoint,source_count,target_count,mismatch_count,
                 updated_at,last_error,revision,source_config_version,target_config_version,
                 route_version,batch_size,shadow_sample_rate,rollback_window_seconds,
                 rollback_deadline,lease_owner,lease_until)
            VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7::jsonb,$8,$9,$10,now(),$11,
                    $12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
            ON CONFLICT (job_id) DO UPDATE SET
                phase=EXCLUDED.phase,checkpoint=EXCLUDED.checkpoint,
                source_count=EXCLUDED.source_count,target_count=EXCLUDED.target_count,
                mismatch_count=EXCLUDED.mismatch_count,updated_at=now(),
                last_error=EXCLUDED.last_error,revision=EXCLUDED.revision,
                source_config_version=EXCLUDED.source_config_version,
                target_config_version=EXCLUDED.target_config_version,
                route_version=EXCLUDED.route_version,batch_size=EXCLUDED.batch_size,
                shadow_sample_rate=EXCLUDED.shadow_sample_rate,
                rollback_window_seconds=EXCLUDED.rollback_window_seconds,
                rollback_deadline=EXCLUDED.rollback_deadline,
                lease_owner=EXCLUDED.lease_owner,lease_until=EXCLUDED.lease_until,
                completed_at=CASE WHEN EXCLUDED.phase='completed' THEN now()
                                  ELSE migration_job.completed_at END
            """, job.job_id, job.tenant_id, job.resource_type, json.dumps({"type": job.source_backend}),
            json.dumps({"type": job.target_backend}), job.phase.value, json.dumps(job.checkpoint), job.source_count,
            job.target_count, job.mismatch_count, job.last_error or None, job.revision, job.source_config_version,
            job.target_config_version, job.route_version, job.batch_size, job.shadow_sample_rate,
            job.rollback_window_seconds, job.rollback_deadline, job.lease_owner or None, job.lease_until)
        return job.model_copy(deep=True)

    async def get(self, job_id: str) -> MigrationJob:
        row = await self._pool.fetchrow("SELECT * FROM migration_job WHERE job_id=$1", job_id)
        if row is None:
            raise KeyError(f"migration job not found: {job_id}")
        source = row["source_backend"]
        target = row["target_backend"]
        checkpoint = row["checkpoint"]
        if isinstance(source, str):
            source = json.loads(source)
        if isinstance(target, str):
            target = json.loads(target)
        if isinstance(checkpoint, str):
            checkpoint = json.loads(checkpoint)
        return MigrationJob(job_id=row["job_id"],
                            tenant_id=row["tenant_id"],
                            resource_type=row["resource_type"],
                            source_backend=source["type"],
                            target_backend=target["type"],
                            phase=row["phase"],
                            checkpoint=checkpoint,
                            source_count=row["source_count"],
                            target_count=row["target_count"],
                            mismatch_count=row["mismatch_count"],
                            last_error=row["last_error"] or "",
                            revision=row.get("revision") or 0,
                            source_config_version=row.get("source_config_version") or 0,
                            target_config_version=row.get("target_config_version") or 0,
                            route_version=row.get("route_version") or 0,
                            batch_size=row.get("batch_size", 100),
                            shadow_sample_rate=row.get("shadow_sample_rate", 0.1),
                            rollback_window_seconds=row.get("rollback_window_seconds", 3600),
                            rollback_deadline=row.get("rollback_deadline"),
                            lease_owner=row.get("lease_owner") or "",
                            lease_until=row.get("lease_until"))

    async def claim(self, job_id: str, owner: str, lease_seconds: int = 60) -> MigrationJob | None:
        row = await self._pool.fetchrow(
            "UPDATE migration_job SET lease_owner=$2,lease_until=clock_timestamp()+$3*interval '1 second' "
            "WHERE job_id=$1 AND (lease_until IS NULL OR lease_until<=clock_timestamp() OR lease_owner=$2) "
            "RETURNING *", job_id, owner, float(lease_seconds))
        return await self.get(job_id) if row else None

    async def release(self, job_id: str, owner: str) -> None:
        await self._pool.execute(
            "UPDATE migration_job SET lease_owner=NULL,lease_until=NULL WHERE job_id=$1 AND lease_owner=$2", job_id,
            owner)

    async def renew(self, job_id: str, owner: str, lease_seconds: int = 60) -> bool:
        status = await self._pool.execute(
            "UPDATE migration_job SET lease_until=clock_timestamp()+$3*interval '1 second' "
            "WHERE job_id=$1 AND lease_owner=$2 AND lease_until>clock_timestamp()", job_id, owner, float(lease_seconds))
        return status == "UPDATE 1"


MigrationStep = Callable[[MigrationJob], Awaitable[MigrationJob]]


class MigrationCoordinator:
    """Advance one durable phase at a time so an interrupted job can resume."""

    def __init__(self, store: MigrationStore, steps: dict[MigrationPhase, MigrationStep] | None = None) -> None:
        self._store = store
        self._steps = steps or {}

    async def create(self,
                     tenant_id: str,
                     resource_type: str,
                     source_backend: str,
                     target_backend: str,
                     *,
                     batch_size: int = 100,
                     shadow_sample_rate: float = 0.1,
                     rollback_window_seconds: int = 3600) -> MigrationJob:
        if source_backend == target_backend:
            raise ValueError("migration source and target must differ")
        return await self._store.save(
            MigrationJob(
                tenant_id=tenant_id,
                resource_type=resource_type,
                source_backend=source_backend,
                target_backend=target_backend,
                batch_size=batch_size,
                shadow_sample_rate=shadow_sample_rate,
                rollback_window_seconds=rollback_window_seconds,
            ))

    async def advance(self, job_id: str) -> MigrationJob:
        owner = uuid.uuid4().hex
        claim = getattr(self._store, "claim", None)
        release = getattr(self._store, "release", None)
        renew = getattr(self._store, "renew", None)
        job = await claim(job_id, owner) if claim else await self._store.get(job_id)
        if job is None:
            raise RuntimeError("migration job is already being advanced")
        renewal = None
        if renew:

            async def renew_loop() -> None:
                while True:
                    await asyncio.sleep(20)
                    if not await renew(job_id, owner):
                        raise RuntimeError("migration job lease was lost")

            renewal = asyncio.create_task(renew_loop())
        try:
            if job.phase in {MigrationPhase.COMPLETED, MigrationPhase.ROLLED_BACK}:
                return job
            step = self._steps.get(job.phase)
            if step is None:
                job.last_error = f"provider_not_implemented:{job.phase.value}"
                await self._store.save(job)
                raise NotImplementedError(f"no migration provider for phase: {job.phase.value}")
            try:
                operation = asyncio.create_task(step(job))
                if renewal:
                    done, _ = await asyncio.wait({operation, renewal}, return_when=asyncio.FIRST_COMPLETED)
                    if renewal in done:
                        operation.cancel()
                        await asyncio.gather(operation, return_exceptions=True)
                        await renewal
                job = await operation
            except Exception as error:
                job.last_error = f"{type(error).__name__}:{error}"
                await self._store.save(job)
                raise
            if job.phase == MigrationPhase.VERIFYING and job.mismatch_count:
                job.last_error = "verification_mismatch"
                await self._store.save(job)
                raise RuntimeError("migration verification has mismatches")
            phase_complete = bool(job.checkpoint.pop("_phase_complete", True))
            if phase_complete:
                job.phase = _NEXT[job.phase]
            job.revision += 1
            job.last_error = ""
            return await self._store.save(job)
        finally:
            if renewal:
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
            if release:
                await release(job_id, owner)

    async def rollback(self, job_id: str) -> MigrationJob:
        owner = uuid.uuid4().hex
        claim = getattr(self._store, "claim", None)
        release = getattr(self._store, "release", None)
        renew = getattr(self._store, "renew", None)
        job = await claim(job_id, owner) if claim else await self._store.get(job_id)
        if job is None:
            raise RuntimeError("migration job is already being advanced")
        renewal = None
        if renew:

            async def renew_loop() -> None:
                while True:
                    await asyncio.sleep(20)
                    if not await renew(job_id, owner):
                        raise RuntimeError("migration job lease was lost")

            renewal = asyncio.create_task(renew_loop())
        try:
            step = self._steps.get(MigrationPhase.ROLLED_BACK)
            if step is None:
                raise NotImplementedError("no migration rollback provider")
            job.phase = MigrationPhase.ROLLED_BACK
            job.revision += 1
            operation = asyncio.create_task(step(job))
            if renewal:
                done, _ = await asyncio.wait({operation, renewal}, return_when=asyncio.FIRST_COMPLETED)
                if renewal in done:
                    operation.cancel()
                    await asyncio.gather(operation, return_exceptions=True)
                    await renewal
            return await self._store.save(await operation)
        finally:
            if renewal:
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
            if release:
                await release(job_id, owner)

    async def get(self, job_id: str) -> MigrationJob:
        return await self._store.get(job_id)

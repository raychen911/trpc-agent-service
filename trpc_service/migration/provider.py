"""Real bidirectional Redis/PostgreSQL Session and Memory migration provider."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from trpc_service.config import BackendType
from trpc_service.gateway.identity import sdk_app_name
from trpc_service.log import AuditEvent
from trpc_service.metrics.telemetry import platform_span

from .control import MigrationItem, PostgresMigrationControlStore
from .control import StorageMigrationRoute, StorageRouteMode
from .coordinator import MigrationJob, MigrationPhase
from .postgres_writer import SdkPostgresSnapshotWriter
from .postgres_reader import SdkPostgresSnapshotReader
from .redis_reader import SdkRedisSnapshotReader
from .redis_writer import SdkRedisSnapshotWriter
from .routing import _resource_key
from trpc_service.storage.keys import session_execution_key


def _id_hash(ids: list[str]) -> str:
    return hashlib.sha256(json.dumps(sorted(ids), separators=(",", ":")).encode()).hexdigest()


def _different_session_fields(source, target) -> str:
    if target is None:
        return "missing"
    left, right = source.canonical(), target.canonical()
    fields = []
    for key in left:
        if left[key] == right.get(key):
            continue
        if key in {"events", "historical_events"} and left[key] and right.get(key):
            event_fields = sorted(set(left[key][0]) | set(right[key][0]))
            different = [name for name in event_fields if left[key][0].get(name) != right[key][0].get(name)]
            fields.append(f"{key}[{','.join(different)}]")
        else:
            fields.append(key)
    return ",".join(fields) or "unknown"


class RedisPostgresMigrationProvider:
    """One bounded, restart-safe operation in either supported direction.

    The historical class name remains public so existing deployments and tests
    keep working unchanged.
    """

    def __init__(self,
                 registry: Any,
                 pool: Any,
                 control: PostgresMigrationControlStore,
                 *,
                 metrics: Any = None,
                 audit: Any = None) -> None:
        self._registry = registry
        self._pool = pool
        self._control = control
        self._metrics = metrics
        self._audit = audit

    @property
    def steps(self):
        return {phase: self.execute for phase in MigrationPhase if phase != MigrationPhase.COMPLETED}

    async def _components(self, job: MigrationJob):
        tenant = await self._registry.get(job.tenant_id, job.source_config_version or None)
        if (job.source_backend, job.target_backend) == ("redis", "sql"):
            reader = SdkRedisSnapshotReader(tenant.storage.redis_url)
            writer = SdkPostgresSnapshotWriter(tenant.storage.sql_url,
                                               pool=self._pool,
                                               session_ttl_seconds=tenant.storage.session_ttl_seconds,
                                               memory_ttl_seconds=tenant.storage.memory_ttl_seconds)
        elif (job.source_backend, job.target_backend) == ("sql", "redis"):
            reader = SdkPostgresSnapshotReader(tenant.storage.sql_url,
                                               pool=self._pool,
                                               session_ttl_seconds=tenant.storage.session_ttl_seconds,
                                               memory_ttl_seconds=tenant.storage.memory_ttl_seconds)
            writer = SdkRedisSnapshotWriter(tenant.storage.redis_url,
                                            session_ttl_seconds=tenant.storage.session_ttl_seconds,
                                            memory_ttl_seconds=tenant.storage.memory_ttl_seconds)
        else:
            raise ValueError("supported migration directions are redis -> sql and sql -> redis")
        app_names = [sdk_app_name(tenant.tenant_id, app_id) for app_id in sorted(tenant.apps)]
        return tenant, reader, writer, app_names

    @staticmethod
    def _session_lock_key(job: MigrationJob, app_name: str, session_id: str) -> str:
        prefix = f"tenant:{job.tenant_id}:app:"
        if not app_name.startswith(prefix):
            raise ValueError("migration resource is outside the tenant namespace")
        return session_execution_key(job.tenant_id, app_name[len(prefix):], session_id)

    async def execute(self, job: MigrationJob) -> MigrationJob:
        started = time.monotonic()
        phase = job.phase.value
        with platform_span(f"migration.{phase}",
                           attributes={
                               "trpc_service.tenant_id": job.tenant_id,
                               "trpc_service.backend": f"{job.source_backend}-to-{job.target_backend}",
                               "trpc_service.operation": phase,
                           }):
            try:
                result = await self._execute_phase(job)
            except Exception as error:
                await self._record_observation(job, phase, started, "error", type(error).__name__)
                raise
            await self._record_observation(job, phase, started, "ok")
            return result

    async def _execute_phase(self, job: MigrationJob) -> MigrationJob:
        if job.resource_type != "session_memory":
            raise ValueError("real provider supports resource_type=session_memory")
        if (job.source_backend, job.target_backend) not in {("redis", "sql"), ("sql", "redis")}:
            # Preserve the historical error substring used by callers while
            # documenting the newly supported reverse pair.
            raise ValueError("real provider supports redis -> sql only for legacy sources; "
                             "the additional supported direction is sql -> redis")
        if job.phase == MigrationPhase.PREPARING:
            return await self._prepare(job)
        if job.phase == MigrationPhase.DUAL_WRITE:
            return await self._dual_write(job)
        if job.phase == MigrationPhase.BACKFILLING:
            return await self._backfill(job)
        if job.phase == MigrationPhase.VERIFYING:
            return await self._verify(job)
        if job.phase == MigrationPhase.SHADOW_READ:
            return await self._shadow_read(job)
        if job.phase == MigrationPhase.CUTOVER:
            return await self._cutover(job)
        if job.phase == MigrationPhase.ROLLED_BACK:
            return await self._rollback(job)
        return job

    async def _record_observation(self,
                                  job: MigrationJob,
                                  phase: str,
                                  started: float,
                                  result: str,
                                  error_type: str = "") -> None:
        elapsed = time.monotonic() - started
        if self._metrics is not None:
            self._metrics.increment("trpc_service_migration_steps_total", phase=phase, result=result)
            self._metrics.observe("trpc_service_migration_step_duration_seconds", elapsed, phase=phase)
            self._metrics.observe("trpc_service_migration_source_records", job.source_count, phase=phase)
            self._metrics.observe("trpc_service_migration_mismatches", job.mismatch_count, phase=phase)
            self._metrics.observe("trpc_service_migration_dirty_records",
                                  int(job.checkpoint.get("dirty_count", 0)),
                                  phase=phase)
        if self._audit is not None:
            await self._audit.write(
                AuditEvent(
                    tenant_id=job.tenant_id,
                    channel="admin",
                    user_id="",
                    session_id="",
                    agent_name="",
                    action="migration_phase",
                    decision=result,
                    latency_ms=elapsed * 1000,
                    error_type=error_type,
                    request_id=job.job_id,
                ))

    async def _prepare(self, job: MigrationJob) -> MigrationJob:
        tenant = await self._registry.get(job.tenant_id)
        expected = BackendType(job.source_backend)
        if tenant.storage.session != expected or tenant.storage.memory != expected:
            raise ValueError(f"tenant Session and Memory sources must both be {job.source_backend}")
        if not tenant.storage.sql_url.startswith("postgresql"):
            raise ValueError("online SQL migration requires PostgreSQL")
        job.source_config_version = tenant.version
        if job.source_backend == "redis":
            SdkRedisSnapshotReader.validate_sdk_version()
        else:
            SdkPostgresSnapshotReader.validate_sdk_version()
        _, reader, writer, app_names = await self._components(job)
        try:
            if job.source_backend == "redis":
                if not await reader.ping():
                    raise ConnectionError("Redis source is unavailable")
                await writer.validate_schema()
            else:
                await reader.validate_schema()
                if not await writer.ping():
                    raise ConnectionError("Redis target is unavailable")
        finally:
            await reader.close()
            await writer.close()
        active = await self._control.active_route(job.tenant_id)
        if active is None or active.job_id != job.job_id:
            version = await self._control.next_route_version(job.tenant_id)
            active = await self._control.publish_route(
                StorageMigrationRoute(tenant_id=job.tenant_id,
                                      route_version=version,
                                      job_id=job.job_id,
                                      mode=StorageRouteMode.SOURCE_ONLY,
                                      source_backend=job.source_backend,
                                      target_backend=job.target_backend,
                                      config_version=tenant.version,
                                      shadow_sample_rate=job.shadow_sample_rate))
        job.route_version = active.route_version
        job.checkpoint.update({
            "app_names": app_names,
            "resource": "session",
            "session_cursor": {},
            "memory_cursor": {},
            "dirty_count": 0
        })
        return job

    async def _transition(self,
                          job: MigrationJob,
                          mode: StorageRouteMode,
                          *,
                          config_version: int = 0,
                          before_publish: Callable[[], Awaitable[None]] | None = None) -> int:
        await self._control.set_admission_paused(job.tenant_id, True)
        try:
            active = await self._control.active_request_count(job.tenant_id)
            if active:
                raise RuntimeError(f"migration transition waiting for {active} active requests")
            if before_publish:
                await before_publish()
            tenant = await self._registry.get(job.tenant_id, config_version or job.source_config_version)
            version = await self._control.next_route_version(job.tenant_id)
            await self._control.publish_route(
                StorageMigrationRoute(tenant_id=job.tenant_id,
                                      route_version=version,
                                      job_id=job.job_id,
                                      mode=mode,
                                      source_backend=job.source_backend,
                                      target_backend=job.target_backend,
                                      config_version=tenant.version,
                                      shadow_sample_rate=job.shadow_sample_rate,
                                      admission_paused=False))
            return version
        finally:
            active_route = await self._control.active_route(job.tenant_id)
            if active_route and active_route.admission_paused:
                await self._control.set_admission_paused(job.tenant_id, False)

    async def _dual_write(self, job: MigrationJob) -> MigrationJob:
        job.route_version = await self._transition(job, StorageRouteMode.DUAL_WRITE)
        return job

    async def _backfill(self, job: MigrationJob) -> MigrationJob:
        _, reader, writer, app_names = await self._components(job)
        resource = job.checkpoint.get("resource", "session")
        try:
            if resource == "session":
                snapshots, cursor, complete = await reader.scan_sessions(app_names,
                                                                         job.checkpoint.get("session_cursor"),
                                                                         job.batch_size)
                job.checkpoint["session_cursor"] = cursor
                for snapshot in snapshots:
                    key = _resource_key(snapshot.app_name, snapshot.user_id, snapshot.session_id)
                    try:
                        lock_key = self._session_lock_key(job, snapshot.app_name, snapshot.session_id)
                        await writer.write_session(snapshot, lock_key)
                        target = await writer.read_session(snapshot.app_name, snapshot.user_id, snapshot.session_id)
                        target_hash = target.content_hash if target else ""
                        state = "written" if target_hash == snapshot.content_hash else "dirty"
                        await self._control.upsert_item(
                            MigrationItem(job_id=job.job_id,
                                          resource_kind="session",
                                          resource_key=key,
                                          source_hash=snapshot.content_hash,
                                          target_hash=target_hash,
                                          state=state))
                        if state == "dirty":
                            await self._control.mark_dirty(
                                job.job_id, "session", key,
                                RuntimeError("hash mismatch fields=" + _different_session_fields(snapshot, target)))
                        else:
                            await self._control.clear_dirty(job.job_id, "session", key)
                    except Exception as error:
                        await self._control.mark_dirty(job.job_id, "session", key, error)
                        raise
                job.source_count += len(snapshots)
                job.target_count += len(snapshots)
                if complete:
                    job.checkpoint["resource"] = "memory"
            else:
                snapshots, cursor, complete = await reader.scan_memories(app_names, job.checkpoint.get("memory_cursor"),
                                                                         job.batch_size)
                job.checkpoint["memory_cursor"] = cursor
                for snapshot in snapshots:
                    key = _resource_key(snapshot.save_key, snapshot.session_id)
                    source_hash = _id_hash([event.id for event in snapshot.events])
                    try:
                        app_name = snapshot.save_key.split("/", 1)[0]
                        lock_key = self._session_lock_key(job, app_name, snapshot.session_id)
                        await writer.write_memory(snapshot, lock_key)
                        target_hash = _id_hash(await writer.memory_event_ids(snapshot.save_key, snapshot.session_id))
                        state = "written" if source_hash == target_hash else "dirty"
                        await self._control.upsert_item(
                            MigrationItem(job_id=job.job_id,
                                          resource_kind="memory",
                                          resource_key=key,
                                          source_hash=source_hash,
                                          target_hash=target_hash,
                                          state=state))
                        if state == "dirty":
                            await self._control.mark_dirty(job.job_id, "memory", key,
                                                           RuntimeError("memory event mismatch after write"))
                        else:
                            await self._control.clear_dirty(job.job_id, "memory", key)
                    except Exception as error:
                        await self._control.mark_dirty(job.job_id, "memory", key, error)
                        raise
                job.source_count += len(snapshots)
                job.target_count += len(snapshots)
                if complete:
                    job.checkpoint["resource"] = "done"
            job.checkpoint["dirty_count"] = await self._control.dirty_count(job.job_id)
            job.checkpoint["_phase_complete"] = job.checkpoint.get("resource") == "done"
            return job
        finally:
            await reader.close()
            await writer.close()

    async def _verify(self, job: MigrationJob, *, prefix: str = "verify") -> MigrationJob:
        # Re-scan the current source truth in bounded batches. This catches data
        # created while historical backfill was running and also repairs dirty
        # resources idempotently before the final comparison.
        # A failed verification is intentionally resumable. Its persisted
        # cursor may already point at Memory or the end of the pass, so a later
        # retry (for example after deploying a serialization fix) must restart
        # only the verification scan. Historical backfill remains untouched.
        if prefix == "verify" and job.last_error == "verification_mismatch":
            job.checkpoint[f"{prefix}_resource"] = "session"
            job.checkpoint[f"{prefix}_session_cursor"] = {}
            job.checkpoint[f"{prefix}_memory_cursor"] = {}
            job.checkpoint[f"{prefix}_source_count"] = 0
            job.checkpoint.pop("_phase_complete", None)
            job.mismatch_count = 0
            job.last_error = ""
        _, reader, writer, app_names = await self._components(job)
        resource_key = f"{prefix}_resource"
        count_key = f"{prefix}_source_count"
        resource = job.checkpoint.setdefault(resource_key, "session")
        job.checkpoint.setdefault(count_key, 0)
        try:
            if resource == "session":
                snapshots, cursor, complete = await reader.scan_sessions(app_names,
                                                                         job.checkpoint.get(f"{prefix}_session_cursor"),
                                                                         job.batch_size)
                job.checkpoint[f"{prefix}_session_cursor"] = cursor
                for snapshot in snapshots:
                    key = _resource_key(snapshot.app_name, snapshot.user_id, snapshot.session_id)
                    lock_key = self._session_lock_key(job, snapshot.app_name, snapshot.session_id)
                    await writer.write_session(snapshot, lock_key)
                    target = await writer.read_session(snapshot.app_name, snapshot.user_id, snapshot.session_id)
                    target_hash = target.content_hash if target else ""
                    state = "verified" if target_hash == snapshot.content_hash else "dirty"
                    await self._control.upsert_item(
                        MigrationItem(job_id=job.job_id,
                                      resource_kind="session",
                                      resource_key=key,
                                      source_hash=snapshot.content_hash,
                                      target_hash=target_hash,
                                      state=state))
                    if state == "verified":
                        await self._control.clear_dirty(job.job_id, "session", key)
                    else:
                        await self._control.mark_dirty(
                            job.job_id, "session", key,
                            RuntimeError("verification fields=" + _different_session_fields(snapshot, target)))
                job.checkpoint[count_key] += len(snapshots)
                if complete:
                    job.checkpoint[resource_key] = "memory"
            else:
                snapshots, cursor, complete = await reader.scan_memories(app_names,
                                                                         job.checkpoint.get(f"{prefix}_memory_cursor"),
                                                                         job.batch_size)
                job.checkpoint[f"{prefix}_memory_cursor"] = cursor
                for snapshot in snapshots:
                    key = _resource_key(snapshot.save_key, snapshot.session_id)
                    source_hash = _id_hash([event.id for event in snapshot.events])
                    app_name = snapshot.save_key.split("/", 1)[0]
                    lock_key = self._session_lock_key(job, app_name, snapshot.session_id)
                    await writer.write_memory(snapshot, lock_key)
                    target_hash = _id_hash(await writer.memory_event_ids(snapshot.save_key, snapshot.session_id))
                    state = "verified" if source_hash == target_hash else "dirty"
                    await self._control.upsert_item(
                        MigrationItem(job_id=job.job_id,
                                      resource_kind="memory",
                                      resource_key=key,
                                      source_hash=source_hash,
                                      target_hash=target_hash,
                                      state=state))
                    if state == "verified":
                        await self._control.clear_dirty(job.job_id, "memory", key)
                    else:
                        await self._control.mark_dirty(job.job_id, "memory", key,
                                                       RuntimeError("memory verification mismatch"))
                job.checkpoint[count_key] += len(snapshots)
                if complete:
                    job.checkpoint[resource_key] = "done"
            dirty = await self._control.dirty_count(job.job_id)
            job.mismatch_count = await self._control.mismatch_count(job.job_id)
            job.checkpoint["dirty_count"] = dirty
            job.checkpoint["_phase_complete"] = job.checkpoint.get(resource_key) == "done"
            if job.checkpoint["_phase_complete"]:
                job.source_count = int(job.checkpoint[count_key])
                job.target_count = await writer.count_resources(app_names)
                if (job.source_backend, job.target_backend) == ("sql", "redis"):
                    # Redis may contain retained data from an older route. It is
                    # quarantined under the admission barrier immediately before
                    # cutover; source records missing from Redis remain blocking.
                    job.checkpoint["target_extra_count"] = max(0, job.target_count - job.source_count)
                    missing = max(0, job.source_count - job.target_count)
                    job.mismatch_count = max(job.mismatch_count, dirty) + missing
                else:
                    job.mismatch_count = max(job.mismatch_count, dirty) + abs(job.source_count - job.target_count)
            return job
        finally:
            await reader.close()
            await writer.close()

    async def _shadow_read(self, job: MigrationJob) -> MigrationJob:
        # Runtime wrappers perform sampled reads; this checkpoint makes the
        # observation explicit and auditable before cutover.
        if await self._control.dirty_count(job.job_id):
            raise RuntimeError("shadow read cannot start while dirty resources remain")
        job.route_version = await self._transition(job, StorageRouteMode.SHADOW_READ)
        job.checkpoint["shadow_started_at"] = datetime.now(timezone.utc).isoformat()
        return job

    async def _cutover(self, job: MigrationJob) -> MigrationJob:
        if not job.checkpoint.get("final_verification_done"):
            job = await self._verify(job, prefix="final_verify")
            if not job.checkpoint.get("_phase_complete"):
                return job
            if job.mismatch_count or await self._control.dirty_count(job.job_id):
                raise RuntimeError("cutover final verification has mismatches")
            job.checkpoint["final_verification_done"] = True
        if await self._control.dirty_count(job.job_id) or await self._control.mismatch_count(job.job_id):
            raise RuntimeError("cutover requires mismatch_count=0 and dirty_count=0")
        now = datetime.now(timezone.utc)
        if job.rollback_deadline is None:
            source = await self._registry.get(job.tenant_id, job.source_config_version)
            target_version = source.version + 1
            target = source.model_copy(
                deep=True,
                update={
                    "version":
                    target_version,
                    "storage":
                    source.storage.model_copy(update={
                        "session": BackendType(job.target_backend),
                        "memory": BackendType(job.target_backend),
                    }),
                })

            async def activate_target() -> None:
                if (job.source_backend, job.target_backend) == ("sql", "redis"):
                    _, source_reader, redis_writer, app_names = await self._components(job)
                    try:
                        expected = await self._control.resource_keys(job.job_id)
                        removed = await redis_writer.quarantine_target_extras(job.job_id, job.tenant_id, app_names,
                                                                              expected, self._control)
                        job.checkpoint["target_extras_quarantined"] = removed
                        remaining = await redis_writer.count_resources(app_names)
                        if remaining != job.source_count:
                            raise RuntimeError("Redis target count differs after quarantining stale records")
                        job.target_count = remaining
                        job.checkpoint["target_extra_count"] = 0
                    finally:
                        await source_reader.close()
                        await redis_writer.close()
                try:
                    await self._registry.publish(target)
                except ValueError:
                    existing = await self._registry.get(job.tenant_id, target_version)
                    if existing.model_dump() != target.model_dump():
                        raise
                    await self._registry.rollback(job.tenant_id, target_version)

            job.target_config_version = target_version
            job.route_version = await self._transition(job,
                                                       StorageRouteMode.TARGET_PRIMARY_MIRROR,
                                                       config_version=target_version,
                                                       before_publish=activate_target)
            job.rollback_deadline = now + timedelta(seconds=job.rollback_window_seconds)
            job.checkpoint["_phase_complete"] = job.rollback_window_seconds == 0
            if job.rollback_window_seconds:
                return job
        elif now < job.rollback_deadline:
            job.checkpoint["_phase_complete"] = False
            return job
        job.route_version = await self._transition(job,
                                                   StorageRouteMode.TARGET_ONLY,
                                                   config_version=job.target_config_version)
        return job

    async def _rollback(self, job: MigrationJob) -> MigrationJob:
        active = await self._control.active_route(job.tenant_id)
        if active and active.mode == StorageRouteMode.TARGET_ONLY:
            raise RuntimeError("completed migration requires a new reverse migration job")
        if active and active.mode == StorageRouteMode.TARGET_PRIMARY_MIRROR:
            if await self._control.dirty_count(job.job_id):
                raise RuntimeError("rollback requires all mirror writes to be repaired")
        # Before/inside the mirror window the source is still continuously
        # mirrored, so restoring it as primary does not discard acknowledged requests.

        async def activate_source() -> None:
            if (job.source_backend, job.target_backend) == ("sql", "redis"):
                _, source_reader, redis_writer, _ = await self._components(job)
                try:
                    await redis_writer.restore_target_backups(job.job_id, self._control)
                finally:
                    await source_reader.close()
                    await redis_writer.close()
            await self._registry.rollback(job.tenant_id, job.source_config_version)

        job.route_version = await self._transition(job, StorageRouteMode.SOURCE_ONLY, before_publish=activate_source)
        return job


# Preferred name for new code. Keep RedisPostgresMigrationProvider as a stable
# compatibility name for existing imports and deployments.
BidirectionalSessionMemoryMigrationProvider = RedisPostgresMigrationProvider

"""Persisted, operator-driven Redis/SQL cutover through public SDK interfaces.

Both old and new SDK writers MUST be stopped externally before run/cutover.
Maintenance rejects new inputs; it cannot fence a paused old SDK process.
"""

import uuid

from sqlalchemy import func, select
from trpc_agent_sdk.sessions import RedisSessionService, SqlSessionService, SessionServiceConfig
from trpc_agent_sdk.events import Event

from trpc_service.persistence.models import (
    OfflineMigrationRow,
    TenantMaintenanceRow,
    TenantRow,
    TenantConfigVersionRow,
    SessionRow,
    InboundMessageRow,
    PostTurnTaskRow,
    TenantRolloutRow,
    utcnow,
)
from trpc_service.management import ConfigurationStore, ConfigConflict
from trpc_service.tenant import TenantConfig
from trpc_service.telemetry.runtime import measured, count
from .session_migration import snapshot_trpc_session


def require_available(database, tenant_id):
    with database.sessions() as session:
        row = session.get(TenantMaintenanceRow, tenant_id)
        if row and row.enabled:
            raise ConfigConflict("tenant is in maintenance")


class OfflineMigrations:

    def __init__(self, database):
        self.database, self.store = database, ConfigurationStore(database)

    def maintenance(self, tenant_id, enabled, actor):
        with self.database.sessions.begin() as session:
            tenant = session.get(TenantRow, tenant_id, with_for_update=True)
            if tenant is None:
                raise LookupError("tenant not found")
            if not enabled and session.scalar(
                    select(OfflineMigrationRow.job_id).where(OfflineMigrationRow.tenant_id == tenant_id,
                                                             OfflineMigrationRow.status.in_(
                                                                 ("copying", "verified"))).limit(1)):
                raise ConfigConflict("finish or abort the migration before resuming")
            row = session.get(TenantMaintenanceRow, tenant_id)
            if row is None:
                session.add(TenantMaintenanceRow(tenant_id=tenant_id, enabled=enabled))
            else:
                row.enabled = enabled
            self.store.audit(session, tenant_id, actor, "maintenance_changed", metadata={"enabled": enabled})

    def _paused(self, session, tenant_id):
        row = session.get(TenantMaintenanceRow, tenant_id, with_for_update=True)
        if row is None or not row.enabled:
            raise ConfigConflict("maintenance is required")
        for model, terminal in ((InboundMessageRow, ("succeeded", "failed_final")), (PostTurnTaskRow, ("succeeded", ))):
            if session.scalar(
                    select(func.count()).select_from(model).where(model.tenant_id == tenant_id,
                                                                  model.status.not_in(terminal))):
                raise ConfigConflict("drain and reconcile all tenant work before migration")

    def create(self, tenant_id, source_version, target_version, actor, *, resource_type="session"):
        if resource_type not in {"session", "knowledge"}:
            raise ValueError("unsupported migration resource")
        with self.database.sessions.begin() as session:
            tenant = session.get(TenantRow, tenant_id, with_for_update=True)
            if tenant is None or tenant.active_config_version != source_version:
                raise ConfigConflict("source active version changed")
            self._paused(session, tenant_id)
            if session.get(TenantRolloutRow, tenant_id):
                raise ConfigConflict("finish canary before migrating")
            if session.scalar(
                    select(OfflineMigrationRow.job_id).where(OfflineMigrationRow.tenant_id == tenant_id,
                                                             OfflineMigrationRow.status.in_(
                                                                 ("copying", "verified"))).limit(1)):
                raise ConfigConflict("a migration is already active")
            source = session.get(TenantConfigVersionRow, (tenant_id, source_version))
            target = session.get(TenantConfigVersionRow, (tenant_id, target_version))
            if target is None or target.status != "draft":
                raise ConfigConflict("migration requires a new draft target")
            old, new = (TenantConfig.model_validate(row.config_json) for row in (source, target))
            # Data cutover and application changes are separate operations.
            excluded = {"config_version", "storage_revision", "data_backends"}
            if old.model_dump(exclude=excluded) != new.model_dump(exclude=excluded):
                raise ConfigConflict("migration must preserve tenant application and routing configuration")
            if new.storage_revision != old.storage_revision + 1:
                raise ConfigConflict("migration requires the next storage revision")
            before = {b.resource_type: b for b in self.store.bindings(session, tenant_id, source_version)}
            after = {b.resource_type: b for b in self.store.bindings(session, tenant_id, target_version)}
            unchanged = ({"memory", "artifact", "knowledge", "audit"}
                         if resource_type == "session" else {"session", "summary", "memory", "artifact", "audit"})
            for role in unchanged:

                def placement(binding):
                    return binding.profile_id, binding.profile_revision, binding.namespace

                if placement(before[role]) != placement(after[role]):
                    raise ConfigConflict("only the selected migration resource may change")
            if (old.data_backends.session.kind.value not in {"redis", "sql"}
                    or new.data_backends.session.kind.value not in {"redis", "sql"}):
                raise ConfigConflict("migration supports shared Redis and SQL")
            source_url = self.store.connection(tenant_id, source_version, resource_type)
            target_url = self.store.connection(tenant_id, target_version, resource_type)
            if source_url == target_url and resource_type == "session":
                raise ConfigConflict("source and target must be separate stores")
            catalog = list(
                session.scalars(
                    select(SessionRow).where(SessionRow.tenant_id == tenant_id).order_by(
                        SessionRow.app_id, SessionRow.user_id, SessionRow.session_id)))
            items = [{
                "app_name": f"{tenant_id}:{row.app_id}",
                "user_id": row.user_id,
                "session_id": row.session_id
            } for row in catalog]
            if resource_type == "knowledge":
                from .managed_vector import validate_profiles
                validate_profiles(self.store, tenant_id, source_version, target_version)
                items = [{
                    "knowledge_base_id": kb
                } for kb in sorted({kb
                                    for app in old.applications
                                    for kb in app.knowledge_base_ids})]
            job_id = uuid.uuid4().hex
            session.add(
                OfflineMigrationRow(job_id=job_id,
                                    tenant_id=tenant_id,
                                    source_version=source_version,
                                    target_version=target_version,
                                    status="copying",
                                    items_json=items,
                                    resource_type=resource_type))
            self.store.audit(session, tenant_id, actor, "migration_created", target_version, {"migration_id": job_id})
            return job_id

    def _services(self, row):
        services = []
        for version in (row.source_version, row.target_version):
            profile = self.store.resource_profile(row.tenant_id, version, "session")
            url = self.store.connection(row.tenant_id, version, "session")
            if profile.options.get("session_format") == "protected_v1":
                from trpc_service.storage.protected_migration import ProtectedMigrationStore
                from trpc_service.agent import tenant_app_name
                with self.database.sessions() as session:
                    config_row = session.get(TenantConfigVersionRow, (row.tenant_id, version))
                    tenant = TenantConfig.model_validate(config_row.config_json)
                services.append(
                    ProtectedMigrationStore(profile.kind.value, url,
                                            [tenant_app_name(row.tenant_id, app.app_id)
                                             for app in tenant.applications]))
                continue
            config = SessionServiceConfig(store_historical_events=True)
            service = (RedisSessionService(db_url=url, is_async=True, session_config=config)
                       if profile.kind.value == "redis" else SqlSessionService(
                           db_url=url, is_async=False, expire_on_commit=False, session_config=config))
            services.append(service)
        return services

    @measured("migration.run")
    async def run(self, tenant_id, job_id, *, writers_stopped, batch_size=50, cutover=False, actor="platform_operator"):
        if not writers_stopped:
            raise ConfigConflict("stop all source and target SDK writers before running this offline job")
        with self.database.sessions() as session:
            row = session.get(OfflineMigrationRow, job_id)
            if row is None or row.tenant_id != tenant_id:
                raise LookupError("migration not found")
            if row.resource_type == "knowledge":
                from .managed_vector import run_vector_job
                return run_vector_job(self, tenant_id, job_id, batch_size, cutover, actor)
        services = []
        try:
            # Persist the source snapshot and ownership of an EMPTY target before
            # any SDK writes. Restart can distinguish our partially created target
            # from an unrelated existing session without replacing divergent data.
            with self.database.sessions.begin() as session:
                row = session.get(OfflineMigrationRow, job_id, with_for_update=True)
                if row is None or row.tenant_id != tenant_id:
                    raise LookupError("migration not found")
                session.get(TenantRow, tenant_id, with_for_update=True)
                self._paused(session, tenant_id)
                if row.status not in {"copying", "verified"}:
                    raise ConfigConflict("migration is terminal")
                services = self._services(row)
                items = [dict(item) for item in row.items_json]
                for index in range(row.copied, min(len(items), row.copied + batch_size)):
                    item = items[index]
                    identity = {key: item[key] for key in ("app_name", "user_id", "session_id")}
                    if "snapshot" not in item:
                        if await services[1].get_session(**identity) is not None:
                            raise ConfigConflict("target must be empty before preparing a migration item")
                        record = await snapshot_trpc_session(services[0], **identity)
                        # Native shared app/user state requires a separate migration
                        # unit. Refuse it instead of copying it into session-local state.
                        if any(key.startswith(("app:", "user:")) for key in record.payload["state"]):
                            raise ConfigConflict("shared app/user state requires a dedicated migration plan")
                        item.update(snapshot=record.payload, digest=record.digest, revision=record.revision)
                row.items_json = items
            # A database row lock serializes competing operators without an expiring lease.
            with self.database.sessions.begin() as session:
                row = session.get(OfflineMigrationRow, job_id, with_for_update=True)
                if row is None or row.tenant_id != tenant_id:
                    raise LookupError("migration not found")
                tenant = session.get(TenantRow, tenant_id, with_for_update=True)
                self._paused(session, tenant_id)
                if row.status not in {"copying", "verified"} or tenant.active_config_version != row.source_version:
                    raise ConfigConflict("migration state or active version changed")
                source, target = services
                items = [dict(item) for item in row.items_json]
                stop = min(len(items), row.copied + batch_size)
                for index in range(row.copied, stop):
                    identity = {key: items[index][key] for key in ("app_name", "user_id", "session_id")}
                    record = await snapshot_trpc_session(source, **identity)
                    if record.digest != items[index]["digest"]:
                        raise ConfigConflict("source changed since preparing the item")
                    await self._restore_prepared(target, record, identity)
                    check = await snapshot_trpc_session(target, **identity)
                    if record.digest != check.digest:
                        raise ConfigConflict("session verification differs")
                    items[index]["digest"] = record.digest
                    row.copied = index + 1
                    count("migration.copied")
                row.items_json, row.error_type = items, None
                if row.copied == len(items):
                    row.status = "verified"
                if cutover:
                    if row.copied != len(items):
                        raise ConfigConflict("finish all batches before cutover")
                    # Recheck the full manifest immediately before the transactional pointer change.
                    for item in items:
                        identity = {key: item[key] for key in ("app_name", "user_id", "session_id")}
                        for service in services:
                            if (await snapshot_trpc_session(service, **identity)).digest != item["digest"]:
                                count("migration.diff")
                                raise ConfigConflict("source or target changed after copy")
                        count("migration.verified")
                    target_config = session.get(TenantConfigVersionRow, (tenant_id, row.target_version))
                    target_config.status, target_config.published_at = "published", utcnow()
                    tenant.active_config_version = row.target_version
                    row.status = "completed"
                    self.store.audit(session, tenant_id, actor, "migration_cutover", row.target_version,
                                     {"migration_id": job_id})
                return {"job_id": job_id, "status": row.status, "copied": row.copied, "total": len(items)}
        finally:
            for service in services:
                await service.close()

    @staticmethod
    async def _restore_prepared(target, record, identity):
        from trpc_service.storage.protected_migration import ProtectedMigrationStore
        if isinstance(target, ProtectedMigrationStore):
            native = await target.service(identity["app_name"])
            async with native.execution(user_id=identity["user_id"], session_id=identity["session_id"]):
                return await OfflineMigrations._restore_prepared(native, record, identity)
        native = await target.get_session(**identity)
        if native is not None:
            current = await snapshot_trpc_session(target, **identity)
            if current.digest == record.digest:
                return
            if native.events or native.historical_events or native.state != record.payload["state"]:
                raise ConfigConflict("prepared target was modified by another writer")
        else:
            native = await target.create_session(app_name=identity["app_name"],
                                                 user_id=identity["user_id"],
                                                 session_id=identity["session_id"],
                                                 state=record.payload["state"])
        native = native.model_copy(deep=True)
        native.events = [Event.model_validate(value) for value in record.payload["events"]]
        native.historical_events = [Event.model_validate(value) for value in record.payload["historical_events"]]
        native.conversation_count = record.payload["conversation_count"]
        # Each supported backend persists this snapshot in one native transaction.
        # A lost acknowledgement is recovered by the digest comparison above.
        await target.update_session(native)

    def abort(self, tenant_id, job_id, actor):
        with self.database.sessions.begin() as session:
            row = session.get(OfflineMigrationRow, job_id, with_for_update=True)
            if row is None or row.tenant_id != tenant_id:
                raise LookupError("migration not found")
            if row.status not in {"copying", "verified"}:
                raise ConfigConflict("completed migration requires a verified reverse migration")
            row.status = "aborted"
            self.store.audit(session, tenant_id, actor, "migration_aborted", metadata={"migration_id": job_id})

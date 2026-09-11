"""Explicit schema boundary: old identity encodings are never silently upgraded."""

from sqlalchemy import inspect, select, func, update
from .models import ServiceSchemaRow, Base

SCHEMA_VERSION = 5
ACTION_TABLES = {"pending_actions", "action_jobs", "action_notices"}
BUDGET_TABLES = {"model_prices", "budget_policies", "budget_accounts", "model_attempts"}
NEW_TABLES = {"service_schema", "im_accounts", "im_memberships", "im_event_receipts", "im_delivery_attempts"
              } | BUDGET_TABLES | ACTION_TABLES


def initialize_schema(database):
    names = database.table_names()
    if names:
        check_version(database)
        database.check_schema()
        return
    Base.metadata.create_all(database.engine)
    with database.sessions.begin() as session:
        session.add(ServiceSchemaRow(component="platform", version=SCHEMA_VERSION))


def check_version(database):
    if "service_schema" not in inspect(database.engine).get_table_names():
        raise RuntimeError("unversioned schema: initialize an empty database or run an explicit offline upgrade")
    with database.sessions() as session:
        row = session.get(ServiceSchemaRow, "platform")
        if row is None or row.version != SCHEMA_VERSION:
            raise RuntimeError("database schema version does not match this service")


def upgrade_phase_two(database, *, offline: bool, backup_id: str):
    """Add platform tables, retain all historical data and suspend old bindings.

    The operator must stop all writers and verify a backup first. There is no claim
    that a schema tool can verify external Redis/object-store writers are stopped.
    """
    import re
    import uuid
    from sqlalchemy.orm import Session
    from .models import (TenantRow, ChannelBindingRow, InboundMessageRow, OutboxMessageRow, PostTurnTaskRow,
                         ToolInvocationRow, SessionLeaseRow, AuditLogRow, TenantRolloutRow)
    from trpc_service.channels.persistence import IMAccounts, database_now
    from trpc_service.tenant import TenantConfig
    from .models import TenantConfigVersionRow

    if not offline or not re.fullmatch(r"[A-Za-z0-9_.-]{3,128}", backup_id):
        raise ValueError("offline acknowledgement and a non-secret backup identifier are required")
    names = database.table_names()
    if "service_schema" in names:
        with database.sessions() as session:
            row = session.get(ServiceSchemaRow, "platform")
            previous = row.version if row else None
        if previous == 3:
            return upgrade_budget_schema(database, backup_id=backup_id)
        if previous == 4:
            return upgrade_budget_schema(database, backup_id=backup_id, source_version=4)
        database.check_schema()
        return {"version": SCHEMA_VERSION, "changed": False}
    inspector = inspect(database.engine)
    for table in Base.metadata.sorted_tables:
        if table.name in NEW_TABLES:
            continue
        if table.name not in names or set(
                table.columns.keys()) != {column["name"]
                                          for column in inspector.get_columns(table.name)}:
            raise RuntimeError("source is not the supported phase-two schema")
    if names & NEW_TABLES:
        raise RuntimeError("partial upgrade requires restoring the verified backup")
    with database.engine.begin() as connection:
        # PostgreSQL DDL and these checks share a transaction. SQLite is serialized
        # by Database's explicit BEGIN IMMEDIATE for development migration tests.
        with Session(bind=connection) as session:
            for model, terminal in ((InboundMessageRow, ("succeeded", "failed_final")), (OutboxMessageRow,
                                                                                         ("delivered", "dead_letter")),
                                    (PostTurnTaskRow, ("succeeded", "failed")), (ToolInvocationRow,
                                                                                 ("succeeded", "failed", "rejected"))):
                if session.scalar(select(func.count()).select_from(model).where(model.status.not_in(terminal))):
                    raise RuntimeError("drain or reconcile all in-flight work before schema upgrade")
            if session.scalar(
                    select(func.count()).select_from(SessionLeaseRow).where(
                        SessionLeaseRow.expires_at > database_now(session))):
                raise RuntimeError("active session lease prevents schema upgrade")
            Base.metadata.create_all(connection, tables=[Base.metadata.tables[name] for name in sorted(NEW_TABLES)])
            tenants = list(session.scalars(select(TenantRow).with_for_update()))
            for tenant in tenants:
                snapshot = session.get(TenantConfigVersionRow, (tenant.tenant_id, tenant.active_config_version))
                if snapshot:
                    for binding in TenantConfig.model_validate(snapshot.config_json).channel_bindings:
                        IMAccounts.register_in_session(session, binding)
                tenant.status = "suspended"
                session.add(
                    AuditLogRow(audit_id=uuid.uuid4().hex,
                                tenant_id=tenant.tenant_id,
                                user_id="offline-schema-upgrade",
                                decision="schema_upgraded",
                                redacted=True,
                                metadata_json={
                                    "schema_version": SCHEMA_VERSION,
                                    "backup_id": backup_id,
                                    "identity_policy": "new_scopes_after_explicit_publish"
                                }))
            session.execute(update(ChannelBindingRow).values(enabled=False))
            session.query(TenantRolloutRow).delete(synchronize_session=False)
            session.add(ServiceSchemaRow(component="platform", version=SCHEMA_VERSION))
            session.flush()
    return {"version": SCHEMA_VERSION, "changed": True, "suspended_tenants": len(tenants)}


def upgrade_budget_schema(database, *, backup_id, source_version=3):
    """Called only by the offline/backup-validated upgrade entrypoint."""
    import uuid
    from sqlalchemy.orm import Session
    from .models import IMAccountRow, AuditLogRow, SessionLeaseRow, ModelAttemptRow
    from trpc_service.channels.persistence import database_now
    names = database.table_names()
    added = ACTION_TABLES | (BUDGET_TABLES if source_version == 3 else set())
    if names & added:
        raise RuntimeError("partial budget upgrade requires restoring the verified backup")
    inspector = inspect(database.engine)
    for table in Base.metadata.sorted_tables:
        if table.name in added:
            continue
        if table.name not in names or set(
                table.columns.keys()) != {column["name"]
                                          for column in inspector.get_columns(table.name)}:
            raise RuntimeError("source is not the supported prior schema")
    with database.engine.begin() as connection:
        with Session(bind=connection) as session:
            if session.scalar(
                    select(func.count()).select_from(IMAccountRow).where(
                        IMAccountRow.lease_expires_at > database_now(session))):
                raise RuntimeError("stop channel owners before schema upgrade")
            if session.scalar(select(func.count()).select_from(SessionLeaseRow).where(
                    SessionLeaseRow.expires_at > database_now(session))):
                raise RuntimeError("stop session writers before schema upgrade")
            if source_version == 4 and session.scalar(select(func.count()).select_from(ModelAttemptRow).where(
                    ModelAttemptRow.status.in_(("reserved", "in_flight")))):
                raise RuntimeError("finish or reconcile active model attempts before schema upgrade")
            Base.metadata.create_all(connection, tables=[Base.metadata.tables[name] for name in sorted(added)])
            session.get(ServiceSchemaRow, "platform", with_for_update=True).version = SCHEMA_VERSION
            session.add(
                AuditLogRow(audit_id=uuid.uuid4().hex,
                            tenant_id="platform",
                            user_id="offline-schema-upgrade",
                            decision="action_schema_upgraded" if source_version == 4 else "budget_schema_upgraded",
                            redacted=True,
                            metadata_json={
                                "backup_id": backup_id,
                                "schema_version": SCHEMA_VERSION
                            }))
            session.flush()
    return {"version": SCHEMA_VERSION, "changed": True, "suspended_tenants": 0}

"""Async SQL adapter for PostgreSQL, MySQL-compatible URLs, and SQLite."""

from __future__ import annotations

import asyncio
import hashlib
import math
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

from sqlalchemy import and_, delete, event, func, insert, inspect, or_, select, text, tuple_, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from tenant_agent.models import (
    ArtifactRecord,
    AuditRecord,
    ConfigVersion,
    KnowledgeRecord,
    MemoryRecord,
    OutboundMessage,
    ProcessingReceipt,
    ReceiptStatus,
    SessionEvent,
    SessionSnapshot,
    SummaryRecord,
    TenantConfig,
    UsageDelta,
)
from tenant_agent.storage import schema
from tenant_agent.storage.base import (
    ConcurrentWriteError,
    OutboxItem,
    ReceiptClaim,
    SessionLeaseTimeout,
    UsageReservationResult,
    UsageSnapshot,
    same_artifact_payload,
)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class SqlPlane:
    """Tenant-predicated SQL implementation of all persistence ports."""

    backend_name = "sql"
    resource_tables: ClassVar[dict[str, set[str]]] = {
        "session": {"sessions", "session_events", "session_leases"},
        "memory": {"memories"},
        "summary": {"summaries"},
        "artifact": {"artifacts"},
        "knowledge": {"knowledge_chunks"},
        "audit": {"audit_logs"},
    }

    def __init__(
        self,
        url: str,
        *,
        echo: bool = False,
        pool_size: int = 10,
        create_schema: bool = True,
    ) -> None:
        kwargs: dict[str, Any] = {"echo": echo, "pool_pre_ping": True}
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"timeout": 30.0}
        else:
            kwargs.update(pool_size=pool_size, max_overflow=pool_size)
        self.engine: AsyncEngine = create_async_engine(url, **kwargs)
        self.create_schema = create_schema
        if self.engine.dialect.name == "sqlite":
            event.listen(self.engine.sync_engine, "connect", self._enable_sqlite_foreign_keys)

    @staticmethod
    def _enable_sqlite_foreign_keys(dbapi_connection: Any, connection_record: Any) -> None:
        del connection_record
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async def initialize(self) -> None:
        if self.engine.dialect.name == "sqlite":
            # Set journal mode once during initialization, before request traffic.
            # WAL allows readers alongside a writer; FULL synchronous durability
            # remains SQLite's default. The busy timeout bounds writer waiting.
            async with self.engine.connect() as connection:
                await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        if self.create_schema:
            async with self.engine.begin() as connection:
                await connection.run_sync(schema.metadata.create_all)
        else:
            async with self.engine.connect() as connection:
                await connection.execute(select(1))

    async def close(self) -> None:
        await self.engine.dispose()

    async def healthcheck(self) -> bool:
        async with self.engine.connect() as connection:
            await connection.execute(select(schema.tenants.c.tenant_id).limit(1))
        return True

    async def healthcheck_resource(self, resource: str) -> bool:
        required = self.resource_tables.get(resource)
        if required is None:
            raise ValueError("unknown SQL resource health check")
        async with self.engine.connect() as connection:
            present = await connection.run_sync(
                lambda sync_connection: {
                    table for table in required if inspect(sync_connection).has_table(table)
                }
            )
        if present != required:
            raise RuntimeError(f"{resource} SQL schema is not provisioned")
        return True

    async def assert_runtime_role_unprivileged(self) -> None:
        if self.engine.dialect.name != "postgresql":
            return
        async with self.engine.connect() as connection:
            row = (
                await connection.execute(
                    text("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user")
                )
            ).one()
            owned_tables = {
                str(owned[0])
                for owned in (
                    await connection.execute(
                        text(
                            "SELECT c.relname FROM pg_class c "
                            "JOIN pg_namespace n ON n.oid = c.relnamespace "
                            "WHERE c.relkind IN ('r', 'p') "
                            "AND n.nspname = ANY(current_schemas(false)) "
                            "AND c.relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)"
                        )
                    )
                ).all()
            }
        owns_platform_schema = bool(owned_tables & set(schema.metadata.tables))
        if bool(row[0]) or bool(row[1]) or owns_platform_schema:
            raise RuntimeError("production runtime database role must be non-owner and non-bypass")

    async def _insert_ignore(self, connection: AsyncConnection, table: Any, values: dict[str, Any]) -> int:
        dialect = self.engine.dialect.name
        statement: Any
        if dialect == "postgresql":
            statement = postgresql_insert(table).values(**values).on_conflict_do_nothing()
        elif dialect == "sqlite":
            statement = sqlite_insert(table).values(**values).on_conflict_do_nothing()
        elif dialect in {"mysql", "mariadb"}:
            statement = mysql_insert(table).values(**values).prefix_with("IGNORE")
        else:
            statement = insert(table).values(**values)
        try:
            result = await connection.execute(statement)
            return int(result.rowcount or 0)
        except Exception:
            raise

    async def save_config_version(self, version: ConfigVersion) -> None:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            await self._insert_ignore(
                connection,
                schema.tenants,
                {
                    "tenant_id": version.tenant_id,
                    "display_name": version.config.display_name,
                    "status": version.config.status.value,
                    "active_config_revision": None,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            inserted = await self._insert_ignore(
                connection,
                schema.tenant_config_versions,
                {
                    "tenant_id": version.tenant_id,
                    "revision": version.revision,
                    "status": version.status,
                    "config_json": version.config.model_dump(mode="json"),
                    "checksum_sha256": version.checksum_sha256,
                    "created_by": version.created_by,
                    "created_at": version.created_at,
                    "activated_at": version.activated_at,
                },
            )
            if inserted:
                return
            existing = (
                (
                    await connection.execute(
                        select(schema.tenant_config_versions).where(
                            schema.tenant_config_versions.c.tenant_id == version.tenant_id,
                            schema.tenant_config_versions.c.revision == version.revision,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if existing is None or existing["checksum_sha256"] != version.checksum_sha256:
                raise ConcurrentWriteError("an immutable configuration revision already exists")

    async def activate_config(self, tenant_id: str, revision: int, activated_at: datetime) -> None:
        async with self.engine.begin() as connection:
            row = (
                (
                    await connection.execute(
                        select(schema.tenant_config_versions)
                        .where(
                            schema.tenant_config_versions.c.tenant_id == tenant_id,
                            schema.tenant_config_versions.c.revision == revision,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise KeyError(f"unknown configuration revision {tenant_id}/{revision}")
            config = TenantConfig.model_validate(row["config_json"])
            for binding in config.channels:
                conflict = (
                    await connection.execute(
                        select(schema.channel_bindings.c.tenant_id).where(
                            schema.channel_bindings.c.channel == binding.channel.value,
                            schema.channel_bindings.c.binding_id == binding.binding_id,
                            schema.channel_bindings.c.tenant_id != tenant_id,
                        )
                    )
                ).first()
                if conflict:
                    raise ConcurrentWriteError("channel binding is already assigned to another tenant")

            await connection.execute(
                update(schema.tenant_config_versions)
                .where(
                    schema.tenant_config_versions.c.tenant_id == tenant_id,
                    schema.tenant_config_versions.c.status == "active",
                )
                .values(status="superseded")
            )
            await connection.execute(
                update(schema.tenant_config_versions)
                .where(
                    schema.tenant_config_versions.c.tenant_id == tenant_id,
                    schema.tenant_config_versions.c.revision == revision,
                )
                .values(status="active", activated_at=activated_at)
            )
            await connection.execute(
                update(schema.tenants)
                .where(schema.tenants.c.tenant_id == tenant_id)
                .values(
                    display_name=config.display_name,
                    status=config.status.value,
                    active_config_revision=revision,
                    updated_at=activated_at,
                )
            )
            await connection.execute(
                delete(schema.channel_bindings).where(schema.channel_bindings.c.tenant_id == tenant_id)
            )
            await connection.execute(
                delete(schema.agent_apps).where(schema.agent_apps.c.tenant_id == tenant_id)
            )
            for app in config.apps.values():
                await connection.execute(
                    insert(schema.agent_apps).values(
                        tenant_id=tenant_id,
                        app_id=app.app_id,
                        agent_name=app.agent_name,
                        config_revision=revision,
                        config_json=app.model_dump(mode="json"),
                        enabled=app.enabled,
                        updated_at=activated_at,
                    )
                )
            for binding in config.channels:
                await connection.execute(
                    insert(schema.channel_bindings).values(
                        tenant_id=tenant_id,
                        binding_id=binding.binding_id,
                        channel=binding.channel.value,
                        app_id=binding.app_id,
                        external_account_id=binding.external_account_id,
                        config_revision=revision,
                        credential_refs_json={
                            key: reference.model_dump(mode="json")
                            for key, reference in binding.credential_refs.items()
                        },
                        settings_json=binding.settings,
                        enabled=binding.enabled,
                        updated_at=activated_at,
                    )
                )

    async def get_active_tenant(self, tenant_id: str) -> TenantConfig | None:
        statement = (
            select(schema.tenant_config_versions.c.config_json)
            .join(
                schema.tenants,
                and_(
                    schema.tenants.c.tenant_id == schema.tenant_config_versions.c.tenant_id,
                    schema.tenants.c.active_config_revision == schema.tenant_config_versions.c.revision,
                ),
            )
            .where(
                schema.tenants.c.tenant_id == tenant_id,
                schema.tenants.c.status == "active",
            )
        )
        async with self.engine.connect() as connection:
            value = (await connection.execute(statement)).scalar_one_or_none()
        return TenantConfig.model_validate(value) if value else None

    async def get_config_version(self, tenant_id: str, revision: int) -> ConfigVersion | None:
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(schema.tenant_config_versions).where(
                            schema.tenant_config_versions.c.tenant_id == tenant_id,
                            schema.tenant_config_versions.c.revision == revision,
                        )
                    )
                )
                .mappings()
                .first()
            )
        return self._config_version(row) if row else None

    async def list_config_versions(self, tenant_id: str) -> Sequence[ConfigVersion]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.tenant_config_versions)
                        .where(schema.tenant_config_versions.c.tenant_id == tenant_id)
                        .order_by(schema.tenant_config_versions.c.revision.desc())
                    )
                )
                .mappings()
                .all()
            )
        return tuple(self._config_version(row) for row in rows)

    @staticmethod
    def _config_version(row: Any) -> ConfigVersion:
        return ConfigVersion(
            tenant_id=row["tenant_id"],
            revision=row["revision"],
            config=TenantConfig.model_validate(row["config_json"]),
            status=row["status"],
            created_by=row["created_by"],
            created_at=_aware(row["created_at"]),
            activated_at=_aware(row["activated_at"]) if row["activated_at"] else None,
            checksum_sha256=row["checksum_sha256"],
        )

    async def get_tenant_by_binding(self, channel: str, binding_id: str) -> TenantConfig | None:
        async with self.engine.connect() as connection:
            tenant_id = (
                await connection.execute(
                    select(schema.channel_bindings.c.tenant_id).where(
                        schema.channel_bindings.c.channel == channel,
                        schema.channel_bindings.c.binding_id == binding_id,
                        schema.channel_bindings.c.enabled.is_(True),
                    )
                )
            ).scalar_one_or_none()
        return await self.get_active_tenant(tenant_id) if tenant_id else None

    async def list_active_tenants(self) -> Sequence[TenantConfig]:
        statement = (
            select(schema.tenant_config_versions.c.config_json)
            .join(
                schema.tenants,
                and_(
                    schema.tenants.c.tenant_id == schema.tenant_config_versions.c.tenant_id,
                    schema.tenants.c.active_config_revision == schema.tenant_config_versions.c.revision,
                ),
            )
            .where(schema.tenants.c.status == "active")
            .order_by(schema.tenants.c.tenant_id)
        )
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).scalars().all()
        return tuple(TenantConfig.model_validate(row) for row in rows)

    async def list_configured_tenants(self) -> Sequence[TenantConfig]:
        statement = (
            select(schema.tenant_config_versions.c.config_json)
            .join(
                schema.tenants,
                and_(
                    schema.tenants.c.tenant_id == schema.tenant_config_versions.c.tenant_id,
                    schema.tenants.c.active_config_revision == schema.tenant_config_versions.c.revision,
                ),
            )
            .order_by(schema.tenants.c.tenant_id)
        )
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).scalars().all()
        return tuple(TenantConfig.model_validate(row) for row in rows)

    async def prune_operational_records(
        self,
        *,
        before: datetime,
        limit: int,
    ) -> dict[str, int]:
        async with self.engine.begin() as connection:
            receipt_statement = (
                select(
                    schema.inbound_receipts.c.tenant_id,
                    schema.inbound_receipts.c.dedupe_key,
                )
                .where(
                    schema.inbound_receipts.c.status != ReceiptStatus.PROCESSING.value,
                    schema.inbound_receipts.c.updated_at < before,
                )
                .order_by(schema.inbound_receipts.c.updated_at)
                .limit(limit)
            )
            if self.engine.dialect.name in {"postgresql", "mysql"}:
                receipt_statement = receipt_statement.with_for_update(skip_locked=True)
            receipt_rows = (await connection.execute(receipt_statement)).all()
            receipts_deleted = 0
            if receipt_rows:
                result = await connection.execute(
                    delete(schema.inbound_receipts).where(
                        tuple_(
                            schema.inbound_receipts.c.tenant_id,
                            schema.inbound_receipts.c.dedupe_key,
                        ).in_(receipt_rows),
                        schema.inbound_receipts.c.status != ReceiptStatus.PROCESSING.value,
                    )
                )
                receipts_deleted = int(result.rowcount or 0)
            outbox_statement = (
                select(schema.outbox.c.outbox_id)
                .where(
                    schema.outbox.c.status == "completed",
                    schema.outbox.c.updated_at < before,
                )
                .order_by(schema.outbox.c.updated_at)
                .limit(limit)
            )
            if self.engine.dialect.name in {"postgresql", "mysql"}:
                outbox_statement = outbox_statement.with_for_update(skip_locked=True)
            outbox_ids = (await connection.execute(outbox_statement)).scalars().all()
            outbox_deleted = 0
            if outbox_ids:
                result = await connection.execute(
                    delete(schema.outbox).where(
                        schema.outbox.c.outbox_id.in_(tuple(outbox_ids)),
                        schema.outbox.c.status == "completed",
                    )
                )
                outbox_deleted = int(result.rowcount or 0)
            reservation_statement = (
                select(
                    schema.usage_reservations.c.tenant_id,
                    schema.usage_reservations.c.reservation_id,
                )
                .where(schema.usage_reservations.c.expires_at < before)
                .order_by(schema.usage_reservations.c.expires_at)
                .limit(limit)
            )
            if self.engine.dialect.name in {"postgresql", "mysql"}:
                reservation_statement = reservation_statement.with_for_update(skip_locked=True)
            reservation_rows = (await connection.execute(reservation_statement)).all()
            reservations_deleted = 0
            if reservation_rows:
                result = await connection.execute(
                    delete(schema.usage_reservations).where(
                        tuple_(
                            schema.usage_reservations.c.tenant_id,
                            schema.usage_reservations.c.reservation_id,
                        ).in_(reservation_rows),
                        schema.usage_reservations.c.expires_at < before,
                    )
                )
                reservations_deleted = int(result.rowcount or 0)
            return {
                "receipts": receipts_deleted,
                "outbox": outbox_deleted,
                "usage_reservations": reservations_deleted,
            }

    async def get_or_create_session(
        self,
        *,
        tenant_id: str,
        app_id: str,
        session_id: str,
        user_id: str,
        channel: str,
    ) -> SessionSnapshot:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            await self._insert_ignore(
                connection,
                schema.sessions,
                {
                    "tenant_id": tenant_id,
                    "session_id": session_id,
                    "app_id": app_id,
                    "user_id": user_id,
                    "channel": channel,
                    "state_json": {},
                    "revision": 0,
                    "last_event_sequence": 0,
                    "summary_version": 0,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            row = (
                (
                    await connection.execute(
                        select(schema.sessions).where(
                            schema.sessions.c.tenant_id == tenant_id,
                            schema.sessions.c.session_id == session_id,
                        )
                    )
                )
                .mappings()
                .one()
            )
        if row["app_id"] != app_id or row["user_id"] != user_id:
            raise ConcurrentWriteError("session identity is immutable")
        return self._session(row)

    async def get_session(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(schema.sessions).where(
                            schema.sessions.c.tenant_id == tenant_id,
                            schema.sessions.c.session_id == session_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
        return self._session(row) if row else None

    @staticmethod
    def _session(row: Any) -> SessionSnapshot:
        return SessionSnapshot(
            tenant_id=row["tenant_id"],
            app_id=row["app_id"],
            session_id=row["session_id"],
            user_id=row["user_id"],
            channel=row["channel"],
            state=row["state_json"] or {},
            revision=row["revision"],
            last_event_sequence=row["last_event_sequence"],
            summary_version=row["summary_version"],
            created_at=_aware(row["created_at"]),
            updated_at=_aware(row["updated_at"]),
        )

    async def append_event(
        self,
        *,
        snapshot: SessionSnapshot,
        event_id: str,
        kind: str,
        actor_id: str,
        payload: dict[str, Any],
        state_delta: dict[str, Any],
        trace_id: str,
    ) -> tuple[SessionSnapshot, SessionEvent]:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            current_row = (
                (
                    await connection.execute(
                        select(schema.sessions)
                        .where(
                            schema.sessions.c.tenant_id == snapshot.tenant_id,
                            schema.sessions.c.session_id == snapshot.session_id,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .first()
            )
            if current_row is None:
                raise KeyError("session does not exist")
            current = self._session(current_row)
            duplicate = (
                (
                    await connection.execute(
                        select(schema.session_events).where(
                            schema.session_events.c.tenant_id == snapshot.tenant_id,
                            schema.session_events.c.event_id == event_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if duplicate:
                duplicate_event = self._event(duplicate)
                if duplicate_event.session_id != snapshot.session_id:
                    raise ConcurrentWriteError("event ID is already owned by another session")
                return current, duplicate_event
            if current.revision != snapshot.revision:
                raise ConcurrentWriteError(
                    f"expected session revision {snapshot.revision}, found {current.revision}"
                )
            sequence = current.last_event_sequence + 1
            state = dict(current.state)
            state.update(state_delta)
            result = await connection.execute(
                update(schema.sessions)
                .where(
                    schema.sessions.c.tenant_id == snapshot.tenant_id,
                    schema.sessions.c.session_id == snapshot.session_id,
                    schema.sessions.c.revision == snapshot.revision,
                )
                .values(
                    state_json=state,
                    revision=snapshot.revision + 1,
                    last_event_sequence=sequence,
                    updated_at=now,
                )
            )
            if result.rowcount != 1:
                raise ConcurrentWriteError("session revision changed during append")
            await connection.execute(
                insert(schema.session_events).values(
                    tenant_id=snapshot.tenant_id,
                    event_id=event_id,
                    session_id=snapshot.session_id,
                    sequence=sequence,
                    kind=kind,
                    actor_id=actor_id,
                    payload_json=payload,
                    state_delta_json=state_delta,
                    trace_id=trace_id,
                    created_at=now,
                )
            )
            updated = current.model_copy(
                update={
                    "state": state,
                    "revision": snapshot.revision + 1,
                    "last_event_sequence": sequence,
                    "updated_at": now,
                }
            )
            session_event = SessionEvent(
                event_id=event_id,
                tenant_id=snapshot.tenant_id,
                session_id=snapshot.session_id,
                sequence=sequence,
                kind=kind,
                actor_id=actor_id,
                payload=payload,
                state_delta=state_delta,
                trace_id=trace_id,
                created_at=now,
            )
            return updated, session_event

    @staticmethod
    def _event(row: Any) -> SessionEvent:
        return SessionEvent(
            event_id=row["event_id"],
            tenant_id=row["tenant_id"],
            session_id=row["session_id"],
            sequence=row["sequence"],
            kind=row["kind"],
            actor_id=row["actor_id"],
            payload=row["payload_json"] or {},
            state_delta=row["state_delta_json"] or {},
            trace_id=row["trace_id"],
            created_at=_aware(row["created_at"]),
        )

    async def list_events(
        self,
        tenant_id: str,
        session_id: str,
        *,
        after_sequence: int = 0,
    ) -> Sequence[SessionEvent]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.session_events)
                        .where(
                            schema.session_events.c.tenant_id == tenant_id,
                            schema.session_events.c.session_id == session_id,
                            schema.session_events.c.sequence > after_sequence,
                        )
                        .order_by(schema.session_events.c.sequence)
                    )
                )
                .mappings()
                .all()
            )
        return tuple(self._event(row) for row in rows)

    async def get_event(self, tenant_id: str, session_id: str, event_id: str) -> SessionEvent | None:
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(schema.session_events).where(
                            schema.session_events.c.tenant_id == tenant_id,
                            schema.session_events.c.session_id == session_id,
                            schema.session_events.c.event_id == event_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
        return self._event(row) if row else None

    async def iter_sessions(self, tenant_id: str) -> AsyncIterator[SessionSnapshot]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.sessions)
                        .where(schema.sessions.c.tenant_id == tenant_id)
                        .order_by(schema.sessions.c.session_id)
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            yield self._session(row)

    async def put_summary(self, summary: SummaryRecord) -> None:
        async with self.engine.begin() as connection:
            committed_sequence = (
                await connection.execute(
                    select(schema.sessions.c.last_event_sequence)
                    .where(
                        schema.sessions.c.tenant_id == summary.tenant_id,
                        schema.sessions.c.session_id == summary.session_id,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if committed_sequence is not None and summary.through_event_sequence > committed_sequence:
                raise ConcurrentWriteError("summary cannot cover events that have not committed")
            current = (
                (
                    await connection.execute(
                        select(schema.summaries).where(
                            schema.summaries.c.tenant_id == summary.tenant_id,
                            schema.summaries.c.session_id == summary.session_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            values = {
                "version": summary.version,
                "through_event_sequence": summary.through_event_sequence,
                "content": summary.content,
                "created_at": summary.created_at,
            }
            if current is None:
                await connection.execute(
                    insert(schema.summaries).values(
                        tenant_id=summary.tenant_id,
                        session_id=summary.session_id,
                        **values,
                    )
                )
            elif current["version"] == summary.version:
                if (
                    current["through_event_sequence"] != summary.through_event_sequence
                    or current["content"] != summary.content
                ):
                    raise ConcurrentWriteError("summary version is immutable")
            elif current["version"] < summary.version:
                await connection.execute(
                    update(schema.summaries)
                    .where(
                        schema.summaries.c.tenant_id == summary.tenant_id,
                        schema.summaries.c.session_id == summary.session_id,
                    )
                    .values(**values)
                )
            await connection.execute(
                update(schema.sessions)
                .where(
                    schema.sessions.c.tenant_id == summary.tenant_id,
                    schema.sessions.c.session_id == summary.session_id,
                    schema.sessions.c.summary_version < summary.version,
                )
                .values(summary_version=summary.version)
            )

    async def get_summary(self, tenant_id: str, session_id: str) -> SummaryRecord | None:
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(schema.summaries).where(
                            schema.summaries.c.tenant_id == tenant_id,
                            schema.summaries.c.session_id == session_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
        return self._summary(row) if row else None

    @staticmethod
    def _summary(row: Any) -> SummaryRecord:
        return SummaryRecord(
            tenant_id=row["tenant_id"],
            session_id=row["session_id"],
            version=row["version"],
            through_event_sequence=row["through_event_sequence"],
            content=row["content"],
            created_at=_aware(row["created_at"]),
        )

    async def iter_summaries(self, tenant_id: str) -> AsyncIterator[SummaryRecord]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.summaries).where(schema.summaries.c.tenant_id == tenant_id)
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            yield self._summary(row)

    async def put_memory(self, memory: MemoryRecord) -> None:
        async with self.engine.begin() as connection:
            current = (
                (
                    await connection.execute(
                        select(schema.memories).where(
                            schema.memories.c.tenant_id == memory.tenant_id,
                            schema.memories.c.user_id == memory.user_id,
                            schema.memories.c.memory_id == memory.memory_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            values = {
                "content": memory.content,
                "metadata_json": memory.metadata,
                "revision": memory.revision,
                "updated_at": memory.updated_at,
            }
            if current is None:
                await connection.execute(
                    insert(schema.memories).values(
                        tenant_id=memory.tenant_id,
                        user_id=memory.user_id,
                        memory_id=memory.memory_id,
                        created_at=memory.created_at,
                        **values,
                    )
                )
            elif current["revision"] == memory.revision:
                if (
                    current["content"] != memory.content
                    or (current["metadata_json"] or {}) != memory.metadata
                ):
                    raise ConcurrentWriteError("memory revision is immutable")
            elif current["revision"] < memory.revision:
                await connection.execute(
                    update(schema.memories)
                    .where(
                        schema.memories.c.tenant_id == memory.tenant_id,
                        schema.memories.c.user_id == memory.user_id,
                        schema.memories.c.memory_id == memory.memory_id,
                    )
                    .values(**values)
                )

    async def search_memory(
        self, tenant_id: str, user_id: str, query: str, *, limit: int = 10
    ) -> Sequence[MemoryRecord]:
        terms = [term for term in query.casefold().split() if term]
        statement = select(schema.memories).where(
            schema.memories.c.tenant_id == tenant_id,
            schema.memories.c.user_id == user_id,
        )
        if terms:
            escaped_terms = [
                term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") for term in terms
            ]
            statement = statement.where(
                or_(*(schema.memories.c.content.ilike(f"%{term}%", escape="\\") for term in escaped_terms))
            )
        statement = statement.order_by(schema.memories.c.updated_at.desc()).limit(limit)
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
        return tuple(self._memory(row) for row in rows)

    @staticmethod
    def _memory(row: Any) -> MemoryRecord:
        return MemoryRecord(
            memory_id=row["memory_id"],
            tenant_id=row["tenant_id"],
            user_id=row["user_id"],
            content=row["content"],
            metadata=row["metadata_json"] or {},
            revision=row["revision"],
            created_at=_aware(row["created_at"]),
            updated_at=_aware(row["updated_at"]),
        )

    async def iter_memories(self, tenant_id: str) -> AsyncIterator[MemoryRecord]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.memories).where(schema.memories.c.tenant_id == tenant_id)
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            yield self._memory(row)

    async def put_artifact(self, record: ArtifactRecord, content: bytes) -> None:
        if len(content) != record.size_bytes or hashlib.sha256(content).hexdigest() != record.checksum_sha256:
            raise ValueError("artifact size or checksum does not match its content")
        values = {
            "tenant_id": record.tenant_id,
            "artifact_id": record.artifact_id,
            "session_id": record.session_id,
            "filename": record.filename,
            "content_type": record.content_type,
            "size_bytes": record.size_bytes,
            "checksum_sha256": record.checksum_sha256,
            "storage_uri": record.storage_uri,
            "version": record.version,
            "content_blob": content,
            "created_at": record.created_at,
        }
        async with self.engine.begin() as connection:
            existing = (
                (
                    await connection.execute(
                        select(schema.artifacts)
                        .where(
                            schema.artifacts.c.tenant_id == record.tenant_id,
                            schema.artifacts.c.artifact_id == record.artifact_id,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .first()
            )
            if existing is None:
                await connection.execute(insert(schema.artifacts).values(**values))
            elif existing["version"] > record.version:
                return
            elif existing["version"] == record.version:
                current_record = self._artifact(existing)
                current_content = bytes(existing["content_blob"] or b"")
                if not same_artifact_payload(current_record, current_content, record, content):
                    raise ConcurrentWriteError("artifact version is immutable")
            else:
                await connection.execute(
                    update(schema.artifacts)
                    .where(
                        schema.artifacts.c.tenant_id == record.tenant_id,
                        schema.artifacts.c.artifact_id == record.artifact_id,
                    )
                    .values(**values)
                )

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactRecord, bytes] | None:
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(schema.artifacts).where(
                            schema.artifacts.c.tenant_id == tenant_id,
                            schema.artifacts.c.artifact_id == artifact_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            return None
        record = self._artifact(row)
        content = bytes(row["content_blob"] or b"")
        if len(content) != record.size_bytes or hashlib.sha256(content).hexdigest() != record.checksum_sha256:
            raise ValueError("stored artifact failed size or checksum verification")
        return record, content

    @staticmethod
    def _artifact(row: Any) -> ArtifactRecord:
        return ArtifactRecord(
            tenant_id=row["tenant_id"],
            session_id=row["session_id"],
            artifact_id=row["artifact_id"],
            filename=row["filename"],
            content_type=row["content_type"],
            size_bytes=row["size_bytes"],
            checksum_sha256=row["checksum_sha256"],
            storage_uri=row["storage_uri"],
            version=row["version"],
            created_at=_aware(row["created_at"]),
        )

    async def iter_artifacts(self, tenant_id: str) -> AsyncIterator[ArtifactRecord]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.artifacts).where(schema.artifacts.c.tenant_id == tenant_id)
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            yield self._artifact(row)

    async def put_knowledge(self, record: KnowledgeRecord) -> None:
        values = {
            "text": record.text,
            "embedding_json": list(record.embedding),
            "metadata_json": record.metadata,
            "embedding_model": record.embedding_model,
            "updated_at": record.updated_at,
        }
        async with self.engine.begin() as connection:
            existing = (
                await connection.execute(
                    select(schema.knowledge_chunks.c.chunk_id).where(
                        schema.knowledge_chunks.c.tenant_id == record.tenant_id,
                        schema.knowledge_chunks.c.document_id == record.document_id,
                        schema.knowledge_chunks.c.chunk_id == record.chunk_id,
                    )
                )
            ).first()
            if existing:
                await connection.execute(
                    update(schema.knowledge_chunks)
                    .where(
                        schema.knowledge_chunks.c.tenant_id == record.tenant_id,
                        schema.knowledge_chunks.c.document_id == record.document_id,
                        schema.knowledge_chunks.c.chunk_id == record.chunk_id,
                    )
                    .values(**values)
                )
            else:
                await connection.execute(
                    insert(schema.knowledge_chunks).values(
                        tenant_id=record.tenant_id,
                        document_id=record.document_id,
                        chunk_id=record.chunk_id,
                        **values,
                    )
                )

    async def search_knowledge(
        self,
        tenant_id: str,
        query_embedding: Sequence[float],
        *,
        limit: int = 10,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Sequence[KnowledgeRecord]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.knowledge_chunks).where(
                            schema.knowledge_chunks.c.tenant_id == tenant_id
                        )
                    )
                )
                .mappings()
                .all()
            )
        candidates: list[tuple[float, KnowledgeRecord]] = []
        for row in rows:
            record = self._knowledge(row)
            if metadata_filter and any(record.metadata.get(k) != v for k, v in metadata_filter.items()):
                continue
            candidates.append((_cosine(query_embedding, record.embedding), record))
        candidates.sort(key=lambda item: item[0], reverse=True)
        return tuple(record for _, record in candidates[:limit])

    @staticmethod
    def _knowledge(row: Any) -> KnowledgeRecord:
        return KnowledgeRecord(
            tenant_id=row["tenant_id"],
            document_id=row["document_id"],
            chunk_id=row["chunk_id"],
            text=row["text"],
            embedding=tuple(row["embedding_json"] or ()),
            metadata=row["metadata_json"] or {},
            embedding_model=row["embedding_model"],
            updated_at=_aware(row["updated_at"]),
        )

    async def iter_knowledge(self, tenant_id: str) -> AsyncIterator[KnowledgeRecord]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.knowledge_chunks).where(
                            schema.knowledge_chunks.c.tenant_id == tenant_id
                        )
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            yield self._knowledge(row)

    async def append_audit(self, record: AuditRecord) -> None:
        async with self.engine.begin() as connection:
            await self._insert_ignore(
                connection,
                schema.audit_logs,
                {
                    "tenant_id": record.tenant_id,
                    "audit_id": record.audit_id,
                    "occurred_at": record.occurred_at,
                    "channel": record.channel,
                    "user_id": record.user_id,
                    "session_id": record.session_id,
                    "agent_name": record.agent_name,
                    "tool_name": record.tool_name,
                    "decision": record.decision,
                    "latency_ms": record.latency_ms,
                    "error_type": record.error_type,
                    "cost_usd": record.cost_usd,
                    "token_input": record.token_input,
                    "token_output": record.token_output,
                    "trace_id": record.trace_id,
                    "message_id": record.message_id,
                    "details_json": record.details,
                },
            )

    async def query_audit(
        self,
        tenant_id: str,
        *,
        limit: int = 100,
        before: datetime | None = None,
        oldest_first: bool = False,
    ) -> Sequence[AuditRecord]:
        statement = select(schema.audit_logs).where(schema.audit_logs.c.tenant_id == tenant_id)
        if before:
            statement = statement.where(schema.audit_logs.c.occurred_at < before)
        order_columns = (
            (schema.audit_logs.c.occurred_at.asc(), schema.audit_logs.c.audit_id.asc())
            if oldest_first
            else (schema.audit_logs.c.occurred_at.desc(), schema.audit_logs.c.audit_id.desc())
        )
        statement = statement.order_by(*order_columns).limit(limit)
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
        return tuple(self._audit_record(row) for row in rows)

    async def prune_audit(self, tenant_id: str, *, before: datetime, limit: int = 500) -> int:
        candidate_ids = (
            select(schema.audit_logs.c.audit_id)
            .where(
                schema.audit_logs.c.tenant_id == tenant_id,
                schema.audit_logs.c.occurred_at < before,
            )
            .order_by(schema.audit_logs.c.occurred_at.asc(), schema.audit_logs.c.audit_id.asc())
            .limit(limit)
        )
        async with self.engine.begin() as connection:
            result = await connection.execute(
                delete(schema.audit_logs).where(
                    schema.audit_logs.c.tenant_id == tenant_id,
                    schema.audit_logs.c.audit_id.in_(candidate_ids),
                )
            )
            return int(result.rowcount or 0)

    async def delete_audit_ids(self, tenant_id: str, *, audit_ids: Sequence[str]) -> int:
        if not audit_ids:
            return 0
        async with self.engine.begin() as connection:
            result = await connection.execute(
                delete(schema.audit_logs).where(
                    schema.audit_logs.c.tenant_id == tenant_id,
                    schema.audit_logs.c.audit_id.in_(tuple(audit_ids)),
                )
            )
            return int(result.rowcount or 0)

    @staticmethod
    def _audit_record(row: Any) -> AuditRecord:
        return AuditRecord(
            audit_id=row["audit_id"],
            occurred_at=_aware(row["occurred_at"]),
            tenant_id=row["tenant_id"],
            channel=row["channel"],
            user_id=row["user_id"],
            session_id=row["session_id"],
            agent_name=row["agent_name"],
            tool_name=row["tool_name"],
            decision=row["decision"],
            latency_ms=row["latency_ms"],
            error_type=row["error_type"],
            cost_usd=row["cost_usd"],
            token_input=row["token_input"],
            token_output=row["token_output"],
            trace_id=row["trace_id"],
            message_id=row["message_id"],
            details=row["details_json"] or {},
        )

    async def claim_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        lease_expires_at: datetime,
    ) -> ReceiptClaim:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            await self._insert_ignore(
                connection,
                schema.inbound_receipts,
                {
                    "tenant_id": tenant_id,
                    "dedupe_key": dedupe_key,
                    "status": ReceiptStatus.PROCESSING.value,
                    "owner": owner,
                    "lease_expires_at": lease_expires_at,
                    "response_json": [],
                    "error_type": None,
                    "updated_at": now,
                },
            )
            row = (
                (
                    await connection.execute(
                        select(schema.inbound_receipts)
                        .where(
                            schema.inbound_receipts.c.tenant_id == tenant_id,
                            schema.inbound_receipts.c.dedupe_key == dedupe_key,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            acquired = row["owner"] == owner and row["status"] == ReceiptStatus.PROCESSING.value
            expired = (
                row["status"] == ReceiptStatus.PROCESSING.value and _aware(row["lease_expires_at"]) <= now
            )
            if not acquired and (row["status"] == ReceiptStatus.FAILED.value or expired):
                await connection.execute(
                    update(schema.inbound_receipts)
                    .where(
                        schema.inbound_receipts.c.tenant_id == tenant_id,
                        schema.inbound_receipts.c.dedupe_key == dedupe_key,
                        or_(
                            schema.inbound_receipts.c.status == ReceiptStatus.FAILED.value,
                            and_(
                                schema.inbound_receipts.c.status == ReceiptStatus.PROCESSING.value,
                                schema.inbound_receipts.c.lease_expires_at <= now,
                            ),
                        ),
                    )
                    .values(
                        status=ReceiptStatus.PROCESSING.value,
                        owner=owner,
                        lease_expires_at=lease_expires_at,
                        response_json=[],
                        error_type=None,
                        updated_at=now,
                    )
                )
                row_data = dict(row)
                row_data.update(
                    status=ReceiptStatus.PROCESSING.value,
                    owner=owner,
                    lease_expires_at=lease_expires_at,
                    response_json=[],
                    error_type=None,
                    updated_at=now,
                )
                acquired = True
                return ReceiptClaim(acquired, self._receipt(row_data))
            return ReceiptClaim(acquired, self._receipt(row))

    @staticmethod
    def _receipt(row: Any) -> ProcessingReceipt:
        return ProcessingReceipt(
            tenant_id=row["tenant_id"],
            dedupe_key=row["dedupe_key"],
            status=row["status"],
            owner=row["owner"],
            lease_expires_at=_aware(row["lease_expires_at"]),
            response=tuple(OutboundMessage.model_validate(item) for item in (row["response_json"] or [])),
            error_type=row["error_type"],
            updated_at=_aware(row["updated_at"]),
        )

    async def complete_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        response: Sequence[OutboundMessage],
    ) -> None:
        if any(item.tenant_id != tenant_id for item in response):
            raise ValueError("receipt response tenant scope mismatch")
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(schema.inbound_receipts)
                .where(
                    schema.inbound_receipts.c.tenant_id == tenant_id,
                    schema.inbound_receipts.c.dedupe_key == dedupe_key,
                    schema.inbound_receipts.c.owner == owner,
                    schema.inbound_receipts.c.status == ReceiptStatus.PROCESSING.value,
                )
                .values(
                    status=ReceiptStatus.COMPLETED.value,
                    response_json=[item.model_dump(mode="json") for item in response],
                    updated_at=datetime.now(UTC),
                )
            )
            if result.rowcount != 1:
                raise ConcurrentWriteError("receipt is not owned by this worker")

    async def complete_receipt_with_outbox(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        response: Sequence[OutboundMessage],
        items: Sequence[OutboxItem],
        usage_period: str | None = None,
        usage_delta: UsageDelta | None = None,
        usage_reservation_id: str | None = None,
    ) -> None:
        if any(item.tenant_id != tenant_id for item in response) or any(
            item.tenant_id != tenant_id for item in items
        ):
            raise ValueError("receipt/outbox tenant scope mismatch")
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(schema.inbound_receipts)
                .where(
                    schema.inbound_receipts.c.tenant_id == tenant_id,
                    schema.inbound_receipts.c.dedupe_key == dedupe_key,
                    schema.inbound_receipts.c.owner == owner,
                    schema.inbound_receipts.c.status == ReceiptStatus.PROCESSING.value,
                )
                .values(
                    status=ReceiptStatus.COMPLETED.value,
                    response_json=[item.model_dump(mode="json") for item in response],
                    updated_at=now,
                )
            )
            if result.rowcount != 1:
                raise ConcurrentWriteError("receipt is not owned by this worker")
            if usage_period is not None and usage_delta is not None:
                await self._insert_ignore(
                    connection,
                    schema.tenant_usage,
                    {
                        "tenant_id": tenant_id,
                        "period": usage_period,
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "cost_usd": 0.0,
                        "updated_at": now,
                    },
                )
                await connection.execute(
                    update(schema.tenant_usage)
                    .where(
                        schema.tenant_usage.c.tenant_id == tenant_id,
                        schema.tenant_usage.c.period == usage_period,
                    )
                    .values(
                        input_tokens=(schema.tenant_usage.c.input_tokens + usage_delta.input_tokens),
                        output_tokens=(schema.tenant_usage.c.output_tokens + usage_delta.output_tokens),
                        cost_usd=schema.tenant_usage.c.cost_usd + usage_delta.cost_usd,
                        updated_at=now,
                    )
                )
            if usage_reservation_id is not None:
                await connection.execute(
                    delete(schema.usage_reservations).where(
                        schema.usage_reservations.c.tenant_id == tenant_id,
                        schema.usage_reservations.c.reservation_id == usage_reservation_id,
                    )
                )
            for item in items:
                await self._insert_ignore(
                    connection,
                    schema.outbox,
                    {
                        "outbox_id": item.outbox_id,
                        "tenant_id": item.tenant_id,
                        "kind": item.kind,
                        "payload_json": item.payload,
                        "status": item.status,
                        "attempts": item.attempts,
                        "available_at": item.available_at,
                        "owner": item.owner,
                        "last_error_type": item.last_error_type,
                        "lease_expires_at": item.lease_expires_at,
                        "created_at": now,
                        "updated_at": now,
                    },
                )

    async def fail_receipt(
        self,
        *,
        tenant_id: str,
        dedupe_key: str,
        owner: str,
        error_type: str,
        usage_reservation_id: str | None = None,
    ) -> None:
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(schema.inbound_receipts)
                .where(
                    schema.inbound_receipts.c.tenant_id == tenant_id,
                    schema.inbound_receipts.c.dedupe_key == dedupe_key,
                    schema.inbound_receipts.c.owner == owner,
                    schema.inbound_receipts.c.status == ReceiptStatus.PROCESSING.value,
                )
                .values(
                    status=ReceiptStatus.FAILED.value,
                    error_type=error_type,
                    updated_at=datetime.now(UTC),
                )
            )
            if result.rowcount != 1:
                raise ConcurrentWriteError("receipt is not owned by this worker")
            if usage_reservation_id is not None:
                await connection.execute(
                    delete(schema.usage_reservations).where(
                        schema.usage_reservations.c.tenant_id == tenant_id,
                        schema.usage_reservations.c.reservation_id == usage_reservation_id,
                    )
                )

    async def get_usage(self, tenant_id: str, period: str) -> UsageSnapshot:
        async with self.engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(schema.tenant_usage).where(
                            schema.tenant_usage.c.tenant_id == tenant_id,
                            schema.tenant_usage.c.period == period,
                        )
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            return UsageSnapshot(tenant_id, period)
        return UsageSnapshot(
            tenant_id=tenant_id,
            period=period,
            input_tokens=row["input_tokens"],
            output_tokens=row["output_tokens"],
            cost_usd=row["cost_usd"],
        )

    async def add_usage(self, tenant_id: str, period: str, delta: UsageDelta) -> UsageSnapshot:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            await self._insert_ignore(
                connection,
                schema.tenant_usage,
                {
                    "tenant_id": tenant_id,
                    "period": period,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cost_usd": 0.0,
                    "updated_at": now,
                },
            )
            await connection.execute(
                update(schema.tenant_usage)
                .where(
                    schema.tenant_usage.c.tenant_id == tenant_id,
                    schema.tenant_usage.c.period == period,
                )
                .values(
                    input_tokens=schema.tenant_usage.c.input_tokens + delta.input_tokens,
                    output_tokens=schema.tenant_usage.c.output_tokens + delta.output_tokens,
                    cost_usd=schema.tenant_usage.c.cost_usd + delta.cost_usd,
                    updated_at=now,
                )
            )
            row = (
                (
                    await connection.execute(
                        select(schema.tenant_usage).where(
                            schema.tenant_usage.c.tenant_id == tenant_id,
                            schema.tenant_usage.c.period == period,
                        )
                    )
                )
                .mappings()
                .one()
            )
        return UsageSnapshot(
            tenant_id=tenant_id,
            period=period,
            input_tokens=row["input_tokens"],
            output_tokens=row["output_tokens"],
            cost_usd=row["cost_usd"],
        )

    async def reserve_usage(
        self,
        *,
        tenant_id: str,
        reservation_id: str,
        period: str,
        reserved_tokens: int,
        reserved_cost_usd: float,
        token_limit: int,
        cost_limit_usd: float,
        expires_at: datetime,
    ) -> UsageReservationResult:
        if reserved_tokens < 0 or reserved_cost_usd < 0:
            raise ValueError("usage reservations cannot be negative")
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            await self._insert_ignore(
                connection,
                schema.tenant_usage,
                {
                    "tenant_id": tenant_id,
                    "period": period,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cost_usd": 0.0,
                    "updated_at": now,
                },
            )
            usage = (
                (
                    await connection.execute(
                        select(schema.tenant_usage)
                        .where(
                            schema.tenant_usage.c.tenant_id == tenant_id,
                            schema.tenant_usage.c.period == period,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            await connection.execute(
                delete(schema.usage_reservations).where(
                    schema.usage_reservations.c.tenant_id == tenant_id,
                    schema.usage_reservations.c.expires_at <= now,
                )
            )
            existing = (
                (
                    await connection.execute(
                        select(schema.usage_reservations).where(
                            schema.usage_reservations.c.tenant_id == tenant_id,
                            schema.usage_reservations.c.reservation_id == reservation_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if existing is not None:
                if (
                    existing["period"] != period
                    or existing["reserved_tokens"] != reserved_tokens
                    or not math.isclose(existing["reserved_cost_usd"], reserved_cost_usd)
                ):
                    raise ConcurrentWriteError("usage reservation identity is immutable")
                return UsageReservationResult(True)
            reserved = (
                await connection.execute(
                    select(
                        func.coalesce(func.sum(schema.usage_reservations.c.reserved_tokens), 0),
                        func.coalesce(func.sum(schema.usage_reservations.c.reserved_cost_usd), 0.0),
                    ).where(
                        schema.usage_reservations.c.tenant_id == tenant_id,
                        schema.usage_reservations.c.period == period,
                        schema.usage_reservations.c.expires_at > now,
                    )
                )
            ).one()
            if usage["input_tokens"] + usage["output_tokens"] + reserved[0] + reserved_tokens > token_limit:
                return UsageReservationResult(False, "monthly_token_budget")
            if usage["cost_usd"] + reserved[1] + reserved_cost_usd > cost_limit_usd:
                return UsageReservationResult(False, "monthly_cost_budget")
            await connection.execute(
                insert(schema.usage_reservations).values(
                    tenant_id=tenant_id,
                    reservation_id=reservation_id,
                    period=period,
                    reserved_tokens=reserved_tokens,
                    reserved_cost_usd=reserved_cost_usd,
                    expires_at=expires_at,
                    created_at=now,
                )
            )
            return UsageReservationResult(True)

    async def release_usage_reservation(self, tenant_id: str, reservation_id: str) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                delete(schema.usage_reservations).where(
                    schema.usage_reservations.c.tenant_id == tenant_id,
                    schema.usage_reservations.c.reservation_id == reservation_id,
                )
            )

    async def acquire_tenant_slot(
        self,
        *,
        tenant_id: str,
        owner: str,
        limit: int,
        lease_expires_at: datetime,
    ) -> bool:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            tenant = (
                await connection.execute(
                    select(schema.tenants.c.tenant_id)
                    .where(schema.tenants.c.tenant_id == tenant_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if tenant is None:
                return False
            await connection.execute(
                delete(schema.tenant_concurrency_slots).where(
                    schema.tenant_concurrency_slots.c.tenant_id == tenant_id,
                    schema.tenant_concurrency_slots.c.lease_expires_at <= now,
                )
            )
            existing = (
                await connection.execute(
                    select(schema.tenant_concurrency_slots.c.owner).where(
                        schema.tenant_concurrency_slots.c.tenant_id == tenant_id,
                        schema.tenant_concurrency_slots.c.owner == owner,
                    )
                )
            ).scalar_one_or_none()
            if existing:
                await connection.execute(
                    update(schema.tenant_concurrency_slots)
                    .where(
                        schema.tenant_concurrency_slots.c.tenant_id == tenant_id,
                        schema.tenant_concurrency_slots.c.owner == owner,
                    )
                    .values(lease_expires_at=lease_expires_at)
                )
                return True
            active = (
                await connection.execute(
                    select(func.count())
                    .select_from(schema.tenant_concurrency_slots)
                    .where(schema.tenant_concurrency_slots.c.tenant_id == tenant_id)
                )
            ).scalar_one()
            if active >= limit:
                return False
            await connection.execute(
                insert(schema.tenant_concurrency_slots).values(
                    tenant_id=tenant_id,
                    owner=owner,
                    lease_expires_at=lease_expires_at,
                )
            )
            return True

    async def release_tenant_slot(self, *, tenant_id: str, owner: str) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                delete(schema.tenant_concurrency_slots).where(
                    schema.tenant_concurrency_slots.c.tenant_id == tenant_id,
                    schema.tenant_concurrency_slots.c.owner == owner,
                )
            )

    async def enqueue_outbox(self, item: OutboxItem) -> None:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            await self._insert_ignore(
                connection,
                schema.outbox,
                {
                    "outbox_id": item.outbox_id,
                    "tenant_id": item.tenant_id,
                    "kind": item.kind,
                    "payload_json": item.payload,
                    "status": item.status,
                    "attempts": item.attempts,
                    "available_at": item.available_at,
                    "owner": item.owner,
                    "last_error_type": item.last_error_type,
                    "lease_expires_at": item.lease_expires_at,
                    "created_at": now,
                    "updated_at": now,
                },
            )

    async def claim_outbox(
        self, owner: str, *, limit: int, now: datetime, kinds: tuple[str, ...] | None = None
    ) -> Sequence[OutboxItem]:
        async with self.engine.begin() as connection:
            eligible = or_(
                and_(
                    schema.outbox.c.status.in_(("pending", "retry")),
                    schema.outbox.c.available_at <= now,
                ),
                and_(
                    schema.outbox.c.status == "processing",
                    schema.outbox.c.lease_expires_at.is_not(None),
                    schema.outbox.c.lease_expires_at <= now,
                ),
            )
            if kinds is not None:
                eligible = and_(eligible, schema.outbox.c.kind.in_(kinds))
            statement = (
                select(schema.outbox).where(eligible).order_by(schema.outbox.c.available_at).limit(limit)
            )
            if self.engine.dialect.name in {"postgresql", "mysql"}:
                statement = statement.with_for_update(skip_locked=True)
            else:
                statement = statement.with_for_update()
            rows = (await connection.execute(statement)).mappings().all()
            claimed: list[OutboxItem] = []
            for row in rows:
                attempts = row["attempts"] + 1
                result = await connection.execute(
                    update(schema.outbox)
                    .where(
                        schema.outbox.c.outbox_id == row["outbox_id"],
                        eligible,
                    )
                    .values(
                        status="processing",
                        owner=owner,
                        attempts=attempts,
                        lease_expires_at=now + timedelta(seconds=300),
                        updated_at=now,
                    )
                )
                if result.rowcount != 1:
                    continue
                mutable = dict(row)
                mutable.update(
                    status="processing",
                    owner=owner,
                    attempts=attempts,
                    lease_expires_at=now + timedelta(seconds=300),
                )
                claimed.append(self._outbox(mutable))
            return tuple(claimed)

    @staticmethod
    def _outbox(row: Any) -> OutboxItem:
        return OutboxItem(
            outbox_id=row["outbox_id"],
            tenant_id=row["tenant_id"],
            kind=row["kind"],
            payload=row["payload_json"] or {},
            status=row["status"],
            attempts=row["attempts"],
            available_at=_aware(row["available_at"]),
            owner=row["owner"],
            last_error_type=row["last_error_type"],
            lease_expires_at=_aware(row["lease_expires_at"]) if row["lease_expires_at"] else None,
        )

    async def complete_outbox(self, outbox_id: str, owner: str) -> None:
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(schema.outbox)
                .where(schema.outbox.c.outbox_id == outbox_id, schema.outbox.c.owner == owner)
                .values(status="completed", lease_expires_at=None, updated_at=datetime.now(UTC))
            )
            if result.rowcount != 1:
                raise ConcurrentWriteError("outbox item is not owned by this worker")

    async def checkpoint_outbox(
        self,
        outbox_id: str,
        owner: str,
        *,
        payload: dict[str, Any],
    ) -> None:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(schema.outbox)
                .where(
                    schema.outbox.c.outbox_id == outbox_id,
                    schema.outbox.c.owner == owner,
                    schema.outbox.c.status == "processing",
                )
                .values(
                    payload_json=payload,
                    lease_expires_at=now + timedelta(seconds=300),
                    updated_at=now,
                )
            )
            if result.rowcount != 1:
                raise ConcurrentWriteError("outbox item is not owned by this worker")

    async def retry_outbox(
        self,
        outbox_id: str,
        owner: str,
        *,
        error_type: str,
        available_at: datetime,
        terminal: bool,
    ) -> None:
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(schema.outbox)
                .where(schema.outbox.c.outbox_id == outbox_id, schema.outbox.c.owner == owner)
                .values(
                    status="dead" if terminal else "retry",
                    owner=None,
                    last_error_type=error_type,
                    available_at=available_at,
                    lease_expires_at=None,
                    updated_at=datetime.now(UTC),
                )
            )
            if result.rowcount != 1:
                raise ConcurrentWriteError("outbox item is not owned by this worker")

    async def list_dead_outbox(self, tenant_id: str, *, limit: int) -> Sequence[OutboxItem]:
        async with self.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(schema.outbox)
                        .where(
                            schema.outbox.c.tenant_id == tenant_id,
                            schema.outbox.c.status == "dead",
                        )
                        .order_by(schema.outbox.c.updated_at.desc())
                        .limit(limit)
                    )
                )
                .mappings()
                .all()
            )
        return tuple(self._outbox(row) for row in rows)

    async def requeue_dead_outbox(self, tenant_id: str, outbox_id: str) -> OutboxItem:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(schema.outbox)
                .where(
                    schema.outbox.c.outbox_id == outbox_id,
                    schema.outbox.c.tenant_id == tenant_id,
                    schema.outbox.c.status == "dead",
                )
                .values(
                    status="retry",
                    attempts=0,
                    available_at=now,
                    owner=None,
                    last_error_type=None,
                    lease_expires_at=None,
                    updated_at=now,
                )
            )
            if result.rowcount != 1:
                existing = (
                    await connection.execute(
                        select(schema.outbox.c.status).where(
                            schema.outbox.c.outbox_id == outbox_id,
                            schema.outbox.c.tenant_id == tenant_id,
                        )
                    )
                ).scalar_one_or_none()
                if existing is None:
                    raise KeyError("unknown outbox item")
                raise ConcurrentWriteError("only dead outbox items can be requeued")
            row = (
                (
                    await connection.execute(
                        select(schema.outbox).where(
                            schema.outbox.c.outbox_id == outbox_id,
                            schema.outbox.c.tenant_id == tenant_id,
                        )
                    )
                )
                .mappings()
                .one()
            )
        return self._outbox(row)

    @asynccontextmanager
    async def acquire_session(
        self,
        *,
        tenant_id: str,
        session_id: str,
        owner: str,
        wait_timeout: float,
        lease_seconds: float,
    ) -> AsyncIterator[None]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_timeout
        acquired = False
        while loop.time() < deadline:
            now = datetime.now(UTC)
            expires_at = now + timedelta(seconds=lease_seconds)
            async with self.engine.begin() as connection:
                inserted = await self._insert_ignore(
                    connection,
                    schema.session_leases,
                    {
                        "tenant_id": tenant_id,
                        "session_id": session_id,
                        "owner": owner,
                        "expires_at": expires_at,
                        "fencing_token": 1,
                    },
                )
                if inserted:
                    acquired = True
                else:
                    row = (
                        (
                            await connection.execute(
                                select(schema.session_leases)
                                .where(
                                    schema.session_leases.c.tenant_id == tenant_id,
                                    schema.session_leases.c.session_id == session_id,
                                )
                                .with_for_update()
                            )
                        )
                        .mappings()
                        .first()
                    )
                    if row and (row["owner"] == owner or _aware(row["expires_at"]) <= now):
                        await connection.execute(
                            update(schema.session_leases)
                            .where(
                                schema.session_leases.c.tenant_id == tenant_id,
                                schema.session_leases.c.session_id == session_id,
                            )
                            .values(
                                owner=owner,
                                expires_at=expires_at,
                                fencing_token=row["fencing_token"] + 1,
                            )
                        )
                        acquired = True
            if acquired:
                break
            await asyncio.sleep(0.05)
        if not acquired:
            raise SessionLeaseTimeout("timed out waiting for the SQL session lease")

        lost = asyncio.Event()

        async def renew() -> None:
            interval = max(0.05, lease_seconds / 3)
            while True:
                await asyncio.sleep(interval)
                try:
                    async with self.engine.begin() as connection:
                        result = await connection.execute(
                            update(schema.session_leases)
                            .where(
                                schema.session_leases.c.tenant_id == tenant_id,
                                schema.session_leases.c.session_id == session_id,
                                schema.session_leases.c.owner == owner,
                            )
                            .values(expires_at=datetime.now(UTC) + timedelta(seconds=lease_seconds))
                        )
                    if result.rowcount != 1:
                        lost.set()
                        return
                except Exception:
                    lost.set()
                    return

        renew_task = asyncio.create_task(renew(), name=f"renew-sql-session-lease:{session_id}")
        try:
            yield
            if lost.is_set():
                raise ConcurrentWriteError("SQL session lease was lost during execution")
        finally:
            renew_task.cancel()
            await asyncio.gather(renew_task, return_exceptions=True)
            async with self.engine.begin() as connection:
                await connection.execute(
                    delete(schema.session_leases).where(
                        schema.session_leases.c.tenant_id == tenant_id,
                        schema.session_leases.c.session_id == session_id,
                        schema.session_leases.c.owner == owner,
                    )
                )


def _cosine(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or not left:
        return float("-inf")
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)

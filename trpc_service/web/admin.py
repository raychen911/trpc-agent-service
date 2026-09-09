"""Read-only, tenant-scoped projections for the operator console.

The runtime database role must not bypass PostgreSQL RLS.  Cross-tenant dashboard
queries therefore discover only opaque tenant IDs from the public ingress route
projection, then open one explicitly scoped transaction per tenant.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.storage.models import (
    AgentRun,
    AuditLog,
    ChannelIngressRoute,
    InboxMessage,
    ProjectionJob,
    ReplyOutbox,
    Session,
    Tenant,
    TenantConfigRevision,
)


class QueueSummary(BaseModel):
    """Bounded operational counters safe for a dashboard response."""

    model_config = ConfigDict(frozen=True)

    sessions: int = Field(ge=0)
    inbox_open: int = Field(ge=0)
    runs_open: int = Field(ge=0)
    outbox_open: int = Field(ge=0)
    projections_open: int = Field(ge=0)
    audit_records: int = Field(ge=0)


class TenantConsoleSummary(BaseModel):
    """Non-secret current configuration and queue health for one tenant."""

    model_config = ConfigDict(frozen=True)

    tenant_id: str
    display_name: str
    status: str
    active_revision: int
    app_count: int = Field(ge=0)
    channel_count: int = Field(ge=0)
    channels: tuple[str, ...]
    model_routes: tuple[str, ...]
    storage_routes: tuple[str, ...]
    queue: QueueSummary


class PlatformConsoleOverview(BaseModel):
    """One refreshable snapshot for the console overview."""

    model_config = ConfigDict(frozen=True)

    generated_at: datetime
    environment: str
    database: str
    tenant_count: int = Field(ge=0)
    active_tenant_count: int = Field(ge=0)
    totals: QueueSummary
    tenants: tuple[TenantConsoleSummary, ...]


class TenantRevisionSummary(BaseModel):
    """Immutable revision metadata without the potentially sensitive specification."""

    model_config = ConfigDict(frozen=True)

    revision: int = Field(ge=1)
    status: str
    content_hash: str
    created_by: str
    created_at: datetime
    active: bool


class AuditConsoleEntry(BaseModel):
    """Content-free audit fields appropriate for operator investigation."""

    model_config = ConfigDict(frozen=True)

    audit_id: str
    created_at: datetime
    decision: str
    action: str
    resource: str
    channel: str
    agent_name: str
    tool_name: str | None
    latency_ms: int = Field(ge=0)
    error_type: str | None
    cost_micros: int = Field(ge=0)
    trace_id: str
    request_id: str


class OperatorConsoleService:
    """Build dashboard projections without weakening tenant database isolation."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        bind = session_factory.kw.get("bind")
        if bind is None:
            raise ValueError("session_factory must be bound to an engine")
        self._session_factory = session_factory
        self._database_name = bind.dialect.name

    async def overview(self, *, environment: str) -> PlatformConsoleOverview:
        tenant_ids = await self._known_tenant_ids()
        tenants: list[TenantConsoleSummary] = []
        for tenant_id in tenant_ids:
            summary = await self._tenant_summary(tenant_id)
            if summary is not None:
                tenants.append(summary)

        totals = QueueSummary(
            sessions=sum(item.queue.sessions for item in tenants),
            inbox_open=sum(item.queue.inbox_open for item in tenants),
            runs_open=sum(item.queue.runs_open for item in tenants),
            outbox_open=sum(item.queue.outbox_open for item in tenants),
            projections_open=sum(item.queue.projections_open for item in tenants),
            audit_records=sum(item.queue.audit_records for item in tenants),
        )
        return PlatformConsoleOverview(
            generated_at=datetime.now(UTC),
            environment=environment,
            database=self._database_name,
            tenant_count=len(tenants),
            active_tenant_count=sum(item.status == "active" for item in tenants),
            totals=totals,
            tenants=tuple(tenants),
        )

    async def revisions(self, tenant_id: str) -> tuple[TenantRevisionSummary, ...]:
        async with self._session_factory() as database, database.begin():
            await _set_tenant_scope(database, tenant_id)
            tenant = await database.get(Tenant, tenant_id)
            active_revision = tenant.active_config_revision if tenant is not None else None
            rows = (
                await database.scalars(
                    select(TenantConfigRevision)
                    .where(TenantConfigRevision.tenant_id == tenant_id)
                    .order_by(TenantConfigRevision.revision.desc())
                )
            ).all()
        return tuple(
            TenantRevisionSummary(
                revision=row.revision,
                status=row.status,
                content_hash=row.content_hash,
                created_by=row.created_by,
                created_at=_utc(row.created_at),
                active=row.revision == active_revision,
            )
            for row in rows
        )

    async def activity(
        self,
        tenant_id: str,
        *,
        limit: int,
    ) -> tuple[AuditConsoleEntry, ...]:
        async with self._session_factory() as database, database.begin():
            await _set_tenant_scope(database, tenant_id)
            rows = (
                await database.scalars(
                    select(AuditLog)
                    .where(AuditLog.tenant_id == tenant_id)
                    .order_by(AuditLog.created_at.desc(), AuditLog.audit_id.desc())
                    .limit(limit)
                )
            ).all()
        return tuple(
            AuditConsoleEntry(
                audit_id=row.audit_id,
                created_at=_utc(row.created_at),
                decision=row.decision,
                action=row.action,
                resource=row.resource,
                channel=row.channel,
                agent_name=row.agent_name,
                tool_name=row.tool_name,
                latency_ms=row.latency_ms,
                error_type=row.error_type,
                cost_micros=row.cost_micros,
                trace_id=row.trace_id,
                request_id=row.request_id,
            )
            for row in rows
        )

    async def _known_tenant_ids(self) -> tuple[str, ...]:
        statement = (
            select(ChannelIngressRoute.tenant_id).distinct().order_by(ChannelIngressRoute.tenant_id)
        )
        async with self._session_factory() as database:
            return tuple((await database.scalars(statement)).all())

    async def _tenant_summary(self, tenant_id: str) -> TenantConsoleSummary | None:
        async with self._session_factory() as database, database.begin():
            await _set_tenant_scope(database, tenant_id)
            tenant = await database.get(Tenant, tenant_id)
            if tenant is None or tenant.active_config_revision is None:
                return None
            revision = await database.get(
                TenantConfigRevision,
                (tenant_id, tenant.active_config_revision),
            )
            if revision is None:
                return None
            spec = revision.spec
            apps = spec.get("apps", [])
            channels = spec.get("channels", [])
            storage = spec.get("storage", {})
            queue = QueueSummary(
                sessions=await _count(database, Session, tenant_id),
                inbox_open=await _count_statuses(
                    database,
                    InboxMessage,
                    tenant_id,
                    {"received", "running", "retry_wait", "reconcile", "dead_letter"},
                ),
                runs_open=await _count_statuses(
                    database,
                    AgentRun,
                    tenant_id,
                    {"pending", "running", "retry_wait", "reconcile", "failed_final"},
                ),
                outbox_open=await _count_statuses(
                    database,
                    ReplyOutbox,
                    tenant_id,
                    {"pending", "sending", "retry_wait", "unknown", "dead_letter"},
                ),
                projections_open=await _count_statuses(
                    database,
                    ProjectionJob,
                    tenant_id,
                    {"pending", "processing", "retry_wait", "dead_letter"},
                ),
                audit_records=await _count(database, AuditLog, tenant_id),
            )

        model_routes = tuple(
            sorted(
                {
                    f"{app.get('model', {}).get('provider', 'unknown')} / "
                    f"{app.get('model', {}).get('model', 'unknown')}"
                    for app in apps
                    if isinstance(app, dict)
                }
            )
        )
        channel_names = tuple(
            sorted(
                {
                    str(channel.get("channel", "unknown"))
                    for channel in channels
                    if isinstance(channel, dict)
                }
            )
        )
        storage_routes = tuple(
            f"{name}: {value}"
            for name, value in sorted(storage.items())
            if isinstance(name, str) and isinstance(value, str)
        )
        return TenantConsoleSummary(
            tenant_id=tenant_id,
            display_name=tenant.display_name,
            status=tenant.status,
            active_revision=tenant.active_config_revision,
            app_count=len(apps),
            channel_count=len(channels),
            channels=channel_names,
            model_routes=model_routes,
            storage_routes=storage_routes,
            queue=queue,
        )


async def _set_tenant_scope(database: AsyncSession, tenant_id: str) -> None:
    if database.get_bind().dialect.name == "postgresql":
        await database.execute(
            text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
            {"tenant_id": tenant_id},
        )


async def _count(database: AsyncSession, model: type[object], tenant_id: str) -> int:
    value = await database.scalar(
        select(func.count()).select_from(model).where(model.tenant_id == tenant_id)  # type: ignore[attr-defined]
    )
    return int(value or 0)


async def _count_statuses(
    database: AsyncSession,
    model: type[object],
    tenant_id: str,
    statuses: set[str],
) -> int:
    value = await database.scalar(
        select(func.count())
        .select_from(model)
        .where(
            model.tenant_id == tenant_id,  # type: ignore[attr-defined]
            model.status.in_(statuses),  # type: ignore[attr-defined]
        )
    )
    return int(value or 0)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

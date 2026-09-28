"""SQLAlchemy implementation of atomic model budget and usage accounting."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from sqlalchemy import case, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentExecutionRequest,
    AgentRunResult,
    AgentRuntimeConfig,
)
from trpc_service.agent.usage import (
    UsageReader,
    UsageRecorder,
    UsageTotals,
    token_reservation,
)
from trpc_service.storage.runtime_orm import UsageLedgerRow


class PostgreSQLUsageRecorder(UsageRecorder, UsageReader):
    """Reserve budgets and persist actual usage once per logical request."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def record(self, context: AgentExecutionContext, result: AgentRunResult) -> None:
        """Finalize a reservation, or create a completed fact without a budget."""

        request = context.request
        usage = result.usage
        model = context.config.model
        async with self._session_factory() as session:
            row = await session.scalar(
                select(UsageLedgerRow).where(
                    UsageLedgerRow.tenant_id == request.tenant.tenant_id,
                    UsageLedgerRow.request_id == request.tenant.request_id,
                ))
            if row is not None and row.status == "completed":
                return
            if row is None:
                row = UsageLedgerRow(
                    tenant_id=request.tenant.tenant_id,
                    agent_app_id=request.tenant.agent_app_id,
                    request_id=request.tenant.request_id,
                    trace_id=request.tenant.trace_id,
                    model_provider=str(model.get("provider", "unknown")),
                    model_name=str(model.get("model_name", "unknown")),
                    input_tokens=0,
                    output_tokens=0,
                    total_tokens=0,
                    estimated_cost=Decimal("0"),
                    occurred_at=datetime.now(timezone.utc),
                )
                session.add(row)
            row.input_tokens = usage.input_tokens
            row.output_tokens = usage.output_tokens
            row.total_tokens = usage.total_tokens
            row.estimated_cost = Decimal(str(usage.estimated_cost))
            row.reserved_tokens = 0
            row.status = "completed"
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                duplicate = await session.scalar(
                    select(UsageLedgerRow.usage_id).where(
                        UsageLedgerRow.tenant_id == request.tenant.tenant_id,
                        UsageLedgerRow.request_id == request.tenant.request_id,
                        UsageLedgerRow.status == "completed",
                    ))
                if duplicate is None:
                    raise

    async def release(self, context: AgentExecutionContext) -> None:
        """Cancel only an unfinished reservation so a retry can reserve again."""

        await self.release_request(context.request)

    async def release_request(self, request: AgentExecutionRequest) -> None:
        """Cancel a reservation even when failure precedes context creation."""

        async with self._session_factory.begin() as session:
            row = await session.scalar(
                select(UsageLedgerRow).where(
                    UsageLedgerRow.tenant_id == request.tenant.tenant_id,
                    UsageLedgerRow.request_id == request.tenant.request_id,
                ).with_for_update())
            if row is not None and row.status == "reserved":
                row.status = "cancelled"
                row.reserved_tokens = 0

    async def reserve(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        *,
        since: datetime,
        daily_calls: int | None,
        daily_tokens: int | None,
    ) -> str | None:
        """Serialize one tenant check and reservation across all Worker nodes."""

        reservation = token_reservation(request, config)
        model = config.model
        async with self._session_factory.begin() as session:
            bind = session.get_bind()
            if bind.dialect.name == "postgresql":
                # The transaction-scoped tenant lock protects both aggregates
                # and insertion without holding a connection during model I/O.
                await session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:tenant, 0))"),
                    {"tenant": str(request.tenant.tenant_id)},
                )
            existing = await session.scalar(
                select(UsageLedgerRow).where(
                    UsageLedgerRow.tenant_id == request.tenant.tenant_id,
                    UsageLedgerRow.request_id == request.tenant.request_id,
                ).with_for_update())
            if existing is not None and existing.status != "cancelled":
                return None
            values = (await session.execute(
                select(
                    func.count(UsageLedgerRow.usage_id),
                    func.coalesce(
                        func.sum(
                            case(
                                (UsageLedgerRow.status
                                 == "reserved", UsageLedgerRow.reserved_tokens),
                                else_=UsageLedgerRow.total_tokens,
                            )),
                        0,
                    ),
                ).where(
                    UsageLedgerRow.tenant_id == request.tenant.tenant_id,
                    UsageLedgerRow.status.in_(("reserved", "completed")),
                    UsageLedgerRow.occurred_at >= since,
                ))).one()
            if daily_calls is not None and int(values[0]) >= daily_calls:
                return "DAILY_CALL_BUDGET_EXCEEDED"
            if daily_tokens is not None and int(values[1]) + reservation > daily_tokens:
                return "DAILY_TOKEN_BUDGET_EXCEEDED"
            if existing is None:
                session.add(
                    UsageLedgerRow(
                        tenant_id=request.tenant.tenant_id,
                        agent_app_id=request.tenant.agent_app_id,
                        request_id=request.tenant.request_id,
                        trace_id=request.tenant.trace_id,
                        model_provider=str(model.get("provider", "unknown")),
                        model_name=str(model.get("model_name", "unknown")),
                        input_tokens=0,
                        output_tokens=0,
                        total_tokens=0,
                        reserved_tokens=reservation,
                        estimated_cost=Decimal("0"),
                        status="reserved",
                        occurred_at=datetime.now(timezone.utc),
                    ))
            else:
                existing.status = "reserved"
                existing.reserved_tokens = reservation
                existing.occurred_at = datetime.now(timezone.utc)
            return None

    async def totals(self, tenant_id: UUID, *, since: datetime) -> UsageTotals:
        """Aggregate completed usage without exposing temporary reservations."""

        async with self._session_factory() as session:
            values = (await session.execute(
                select(
                    func.count(UsageLedgerRow.usage_id),
                    func.coalesce(func.sum(UsageLedgerRow.total_tokens), 0),
                ).where(
                    UsageLedgerRow.tenant_id == tenant_id,
                    UsageLedgerRow.status == "completed",
                    UsageLedgerRow.occurred_at >= since,
                ))).one()
        return UsageTotals(calls=int(values[0]), total_tokens=int(values[1]))

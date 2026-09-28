import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from tests.test_trpc_agent_runner import UnusedToolInvoker, _context
from trpc_service.agent.contracts import (
    AgentReply,
    AgentRunResult,
    AgentUsage,
)
from trpc_service.agent.observability import ObservedAgentRunner
from trpc_service.agent.ports import AgentRunner
from trpc_service.agent.usage import (
    UsageReader,
    UsageRecorder,
    UsageTotals,
    token_reservation,
)
from trpc_service.channels import MessageKind
from trpc_service.metrics import PlatformTelemetry
from trpc_service.storage.adapters.postgresql_usage import PostgreSQLUsageRecorder
from trpc_service.storage.orm import Base
from trpc_service.storage.runtime_orm import UsageLedgerRow


class UsageRunner(AgentRunner):

    async def run(self, context, tools):  # type: ignore[no-untyped-def]
        del context, tools
        return AgentRunResult(
            replies=(AgentReply(MessageKind.TEXT, "ok"), ),
            usage=AgentUsage(
                input_tokens=20,
                output_tokens=5,
                total_tokens=25,
                estimated_cost=0.01,
            ),
        )


class HangingRunner(AgentRunner):
    """Simulate a provider SDK that never returns on its own."""

    async def run(self, context, tools):  # type: ignore[no-untyped-def]
        del context, tools
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class UnpricedUsageRunner(AgentRunner):

    async def run(self, context, tools):  # type: ignore[no-untyped-def]
        del context, tools
        return AgentRunResult(
            replies=(AgentReply(MessageKind.TEXT, "ok"), ),
            usage=AgentUsage(input_tokens=100, output_tokens=50, total_tokens=150),
        )


class RecordingAudit:

    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    async def record(self, request, config, **fields):  # type: ignore[no-untyped-def]
        del request, config
        self.records.append(fields)


class BrokenReleaseRecorder(UsageRecorder):
    """Exercise the fail-open logging path when reservation cleanup itself fails."""

    async def record(self, context, result):  # type: ignore[no-untyped-def]
        del context, result

    async def release(self, context):  # type: ignore[no-untyped-def]
        del context
        raise RuntimeError("storage unavailable")


@pytest.mark.anyio
async def test_observed_runner_records_usage_metrics_and_runtime_audit() -> None:
    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    audit = RecordingAudit()
    runner = ObservedAgentRunner(UsageRunner(), telemetry, audit)  # type: ignore[arg-type]
    context = _context()

    result = await runner.run(context, UnusedToolInvoker())
    metrics = telemetry.render_prometheus().decode()

    assert result.usage.total_tokens == 25
    assert audit.records[0]["action"] == "agent.runner.execute"
    assert audit.records[0]["reason_code"] == "MODEL_COMPLETED"
    assert audit.records[0]["cost_amount"] == 0.01
    assert audit.records[0]["details"] == {
        "input_tokens": 20,
        "output_tokens": 5,
        "total_tokens": 25,
    }
    assert 'direction="input",model_provider="unknown"} 20.0' in metrics


@pytest.mark.anyio
async def test_usage_ledger_is_durable_and_idempotent_by_request(
        tmp_path) -> None:  # type: ignore[no-untyped-def]
    """A Worker retry must not bill the same logical model turn twice."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'usage.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    recorder = PostgreSQLUsageRecorder(session_factory)
    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    runner = ObservedAgentRunner(
        UsageRunner(),
        telemetry,
        RecordingAudit(),  # type: ignore[arg-type]
        usage_recorder=recorder,
    )

    context = _context()
    await runner.run(context, UnusedToolInvoker())
    await runner.run(context, UnusedToolInvoker())

    async with session_factory() as session:
        total = await session.scalar(select(func.count()).select_from(UsageLedgerRow))
        row = await session.scalar(select(UsageLedgerRow))
    assert total == 1
    assert row is not None
    assert row.request_id == "request-1"
    assert row.total_tokens == 25
    assert row.estimated_cost == Decimal("0.01")
    assert row.status == "completed"
    await engine.dispose()


@pytest.mark.anyio
async def test_usage_budget_is_reserved_before_model_execution(tmp_path) -> None:
    """One outstanding turn consumes the call budget across Worker requests."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'reservation.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    recorder = PostgreSQLUsageRecorder(sessions)
    context = _context()
    context = replace(
        context,
        config=replace(context.config, model={"max_output_tokens": 100}),
    )
    second_request = replace(
        context.request,
        tenant=context.request.tenant.model_copy(update={"request_id": "request-2"}),
    )
    since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    first = await recorder.reserve(
        context.request,
        context.config,
        since=since,
        daily_calls=1,
        daily_tokens=1000,
    )
    blocked = await recorder.reserve(
        second_request,
        context.config,
        since=since,
        daily_calls=1,
        daily_tokens=1000,
    )
    await recorder.release(context)
    after_release = await recorder.reserve(
        second_request,
        context.config,
        since=since,
        daily_calls=1,
        daily_tokens=1000,
    )

    assert first is None
    assert blocked == "DAILY_CALL_BUDGET_EXCEEDED"
    assert after_release is None
    await engine.dispose()


@pytest.mark.anyio
async def test_usage_reservation_retry_finalize_and_token_limit(tmp_path) -> None:
    """A request retry reuses its reservation and completion replaces the estimate."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'reservation-retry.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    recorder = PostgreSQLUsageRecorder(sessions)
    context = _context()
    context = replace(
        context,
        config=replace(context.config, model={"max_output_tokens": 100}),
    )
    since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    assert await recorder.reserve(
        context.request,
        context.config,
        since=since,
        daily_calls=10,
        daily_tokens=1000,
    ) is None
    # Delivery retries for the same logical request must not consume budget twice.
    assert await recorder.reserve(
        context.request,
        context.config,
        since=since,
        daily_calls=1,
        daily_tokens=1,
    ) is None
    await recorder.release(context)
    assert await recorder.reserve(
        context.request,
        context.config,
        since=since,
        daily_calls=10,
        daily_tokens=1000,
    ) is None

    result = await UsageRunner().run(context, UnusedToolInvoker())
    await recorder.record(context, result)
    # Replaying a completed request remains an idempotent no-op.
    await recorder.record(context, result)
    totals = await recorder.totals(context.request.tenant.tenant_id, since=since)
    assert totals == UsageTotals(calls=1, total_tokens=25)

    second_request = replace(
        context.request,
        tenant=context.request.tenant.model_copy(update={"request_id": "request-token-limit"}),
    )
    assert await recorder.reserve(
        second_request,
        context.config,
        since=since,
        daily_calls=10,
        daily_tokens=25,
    ) == "DAILY_TOKEN_BUDGET_EXCEEDED"
    await engine.dispose()


@pytest.mark.anyio
async def test_usage_reader_fallback_enforces_token_budget() -> None:
    """Non-database adapters retain the same conservative token decision."""

    class StaticUsageReader(UsageReader):

        async def totals(self, tenant_id, *, since):  # type: ignore[no-untyped-def]
            del tenant_id, since
            return UsageTotals(calls=1, total_tokens=9)

    context = _context()
    reason = await StaticUsageReader().reserve(
        context.request,
        replace(context.config, model={"max_output_tokens": 5}),
        since=datetime.now(timezone.utc),
        daily_calls=10,
        daily_tokens=10,
    )
    assert reason == "DAILY_TOKEN_BUDGET_EXCEEDED"


def test_token_reservation_uses_context_limit_or_utf8_upper_bound() -> None:
    """Chinese input is never underestimated by an ASCII chars/4 heuristic."""

    context = _context()
    chinese_request = replace(
        context.request,
        incoming=replace(context.request.incoming, text="你好"),
    )
    assert token_reservation(
        chinese_request,
        replace(context.config, model={"max_output_tokens": 5}),
    ) == len("你好".encode("utf-8")) + 5
    assert token_reservation(
        chinese_request,
        replace(context.config, model={"context_window_tokens": 32768}),
    ) == 32768


@pytest.mark.anyio
async def test_observed_runner_estimates_cost_from_platform_profile() -> None:
    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    audit = RecordingAudit()
    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            model={
                "input_cost_per_million": 2.0,
                "output_cost_per_million": 8.0,
            },
        ),
    )

    result = await ObservedAgentRunner(
        UnpricedUsageRunner(),
        telemetry,
        audit,  # type: ignore[arg-type]
    ).run(context, UnusedToolInvoker())

    assert result.usage.estimated_cost == pytest.approx(0.0006)
    assert audit.records[0]["cost_amount"] == pytest.approx(0.0006)


@pytest.mark.anyio
async def test_observed_runner_rejects_invalid_pricing_and_survives_release_error(
    caplog: pytest.LogCaptureFixture, ) -> None:
    """Invalid prices never create cost, and cleanup failure cannot hide model timeout."""

    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    context = _context()
    invalid_price_context = replace(
        context,
        config=replace(
            context.config,
            model={
                "input_cost_per_million": True,
                "output_cost_per_million": 8.0,
            },
        ),
    )
    result = await ObservedAgentRunner(
        UnpricedUsageRunner(),
        telemetry,
        RecordingAudit(),  # type: ignore[arg-type]
    ).run(invalid_price_context, UnusedToolInvoker())
    assert result.usage.estimated_cost == 0

    with pytest.raises(ValueError, match="timeout must be positive"):
        ObservedAgentRunner(
            UsageRunner(),
            telemetry,
            RecordingAudit(),  # type: ignore[arg-type]
            timeout_seconds=0,
        )

    caplog.set_level("ERROR")
    failing = ObservedAgentRunner(  # type: ignore[arg-type]
        HangingRunner(),
        telemetry,
        RecordingAudit(),
        timeout_seconds=0.01,
        usage_recorder=BrokenReleaseRecorder(),
    )
    with pytest.raises(TimeoutError):
        await failing.run(context, UnusedToolInvoker())
    assert "Usage reservation release failed" in caplog.text


@pytest.mark.anyio
async def test_observed_runner_bounds_a_hanging_model_provider() -> None:
    """The platform timeout releases the Worker even if an SDK hangs."""

    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    audit = RecordingAudit()
    runner = ObservedAgentRunner(  # type: ignore[arg-type]
        HangingRunner(),
        telemetry,
        audit,
        timeout_seconds=0.01,
    )

    with pytest.raises(TimeoutError):
        await runner.run(_context(), UnusedToolInvoker())

    assert audit.records[0]["reason_code"] == "RUNNER_FAILED"
    assert audit.records[0]["error_type"] == "TimeoutError"

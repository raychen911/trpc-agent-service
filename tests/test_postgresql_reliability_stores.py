from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from tests.test_agent_task_queue import _request
from tests.test_trpc_agent_runner import _context
from trpc_service.agent.contracts import (
    AgentTaskClaim,
    AgentTaskStatus,
    AgentToolCall,
    AgentToolKind,
    AgentToolResult,
)
from trpc_service.agent.ledger import ToolLedgerConflict, ToolLedgerStatus
from trpc_service.agent.recovery_state import RunnerAttemptStatus, StaleRunnerAttempt
from trpc_service.channels import ChannelBindingConfig
from trpc_service.channels.media import ChannelMediaStore
from trpc_service.storage.adapters.inmemory import build_inmemory_backend
from trpc_service.storage.adapters.postgresql_runner_recovery import (
    PostgreSQLRunnerRecoveryStore, )
from trpc_service.storage.adapters.postgresql_tool_ledger import PostgreSQLToolLedger
from trpc_service.storage.orm import Base


def _tool_call(*, call_id: str = "call-1", title: str = "incident") -> AgentToolCall:
    return AgentToolCall(
        call_id=call_id,
        name="ticket.create",
        kind=AgentToolKind.TOOL,
        logical_call_index=0,
        arguments={"title": title},
    )


def _runner_claim(*, fence: int = 1, attempt: int = 1) -> AgentTaskClaim:
    return AgentTaskClaim(
        # The durable implementation stores task IDs as UUIDs.
        task_id=str(uuid4()),
        request=_request(),
        status=AgentTaskStatus.RUNNING,
        attempt_count=attempt,
        fencing_token=fence,
    )


@pytest.mark.anyio
async def test_postgresql_tool_ledger_replays_and_protects_terminal_outcomes(
    tmp_path: Path, ) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ledger.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    ledger = PostgreSQLToolLedger(async_sessionmaker(engine, expire_on_commit=False))
    context = _context()
    call = _tool_call()
    result = AgentToolResult(
        call.call_id,
        content="T-1",
        artifact_refs=("artifact-1", ),
        attributes={"provider": "test"},
    )

    prepared = await ledger.prepare(context, call)
    await ledger.complete(context, call, result)
    # Completion is idempotent because a database acknowledgement can be lost.
    await ledger.complete(context, call, result)
    replay = await ledger.prepare(context, call)

    assert prepared.should_execute
    assert replay.status is ToolLedgerStatus.SUCCEEDED
    assert replay.result == result
    assert not replay.should_execute
    with pytest.raises(ToolLedgerConflict):
        await ledger.prepare(context, _tool_call(title="changed intent"))
    with pytest.raises(ToolLedgerConflict):
        await ledger.fail(context, call, "cannot overwrite success")

    await engine.dispose()


@pytest.mark.anyio
async def test_postgresql_tool_ledger_quarantines_unknown_outcome(tmp_path: Path, ) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'unknown-ledger.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    ledger = PostgreSQLToolLedger(async_sessionmaker(engine, expire_on_commit=False))
    context = _context()
    call = _tool_call(call_id="call-unknown")

    with pytest.raises(ToolLedgerConflict):
        await ledger.fail(context, call, "not prepared")
    await ledger.prepare(context, call)
    await ledger.mark_unknown(context, call, "provider outcome cannot be proven")
    replay = await ledger.prepare(context, call)

    assert replay.status is ToolLedgerStatus.UNKNOWN
    assert not replay.should_execute
    with pytest.raises(ToolLedgerConflict):
        await ledger.complete(context, call, AgentToolResult(call.call_id, content="late"))

    await engine.dispose()


@pytest.mark.anyio
async def test_postgresql_runner_recovery_persists_checkpoints_and_fences(tmp_path: Path, ) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runner.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = PostgreSQLRunnerRecoveryStore(async_sessionmaker(engine, expire_on_commit=False))
    first_claim = _runner_claim()
    first = await store.start(first_claim, node_id="worker-a")
    assert await store.start(first_claim, node_id="worker-a") == first

    occurred_at = datetime.now(timezone.utc)
    checkpoint = await store.checkpoint(
        first,
        stage="MODEL_COMPLETED",
        state_ref="storage://checkpoint-1",
        occurred_at=occurred_at,
    )
    completed = await store.complete(first)
    assert await store.complete(first) == completed

    assert checkpoint.sequence_no == 1
    assert checkpoint.occurred_at == occurred_at
    assert completed.status is RunnerAttemptStatus.SUCCEEDED
    with pytest.raises(StaleRunnerAttempt):
        await store.checkpoint(first, stage="TOOL_COMPLETED", state_ref=None)

    await engine.dispose()


@pytest.mark.anyio
async def test_postgresql_runner_recovery_supersedes_expired_attempt(tmp_path: Path, ) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runner-fence.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = PostgreSQLRunnerRecoveryStore(async_sessionmaker(engine, expire_on_commit=False))
    first_claim = _runner_claim()
    first = await store.start(first_claim, node_id="worker-old")
    second_claim = replace(first_claim, attempt_count=2, fencing_token=2)
    second = await store.start(second_claim, node_id="worker-new")

    with pytest.raises(StaleRunnerAttempt):
        await store.checkpoint(first, stage="STALE", state_ref=None)
    failed = await store.fail(second, "temporary failure", retryable=True)

    assert failed.status is RunnerAttemptStatus.RETRYABLE_FAILED
    assert failed.error_summary == "temporary failure"
    with pytest.raises(StaleRunnerAttempt):
        await store.mark_unknown(first, "late result")

    await engine.dispose()


@pytest.mark.anyio
async def test_channel_media_store_round_trip_is_tenant_scoped() -> None:
    backend = build_inmemory_backend()
    media = ChannelMediaStore(backend.artifact)
    binding = ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        channel_type="feishu",
    )

    artifact_id = await media.put(
        binding,
        principal_id="employee-1",
        message_id="om_media_1",
        content=b"report-content",
        filename="report.txt",
        media_type="text/plain",
    )

    assert await media.read(binding, artifact_id) == b"report-content"

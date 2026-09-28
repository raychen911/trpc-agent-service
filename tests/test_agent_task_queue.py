from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from trpc_service.agent.contracts import AgentExecutionRequest, AgentTaskStatus
from trpc_service.agent.queue import PostgreSQLAgentTaskQueue
from trpc_service.channels import ChannelBindingConfig, IncomingMessage, MessageKind
from trpc_service.storage.errors import IdempotencyConflict, StaleExecutionLease
from trpc_service.storage.orm import Base
from trpc_service.storage.runtime_orm import AgentTaskRow
from trpc_service.tenant import TenantContext


def _request() -> AgentExecutionRequest:
    tenant_id = uuid4()
    agent_app_id = uuid4()
    return AgentExecutionRequest(
        tenant=TenantContext(
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            config_version=3,
            request_id="request-1",
            trace_id="trace-1",
        ),
        session_id="session-1",
        incoming=IncomingMessage(
            external_message_id="message-1",
            principal_id="principal-1",
            conversation_id="conversation-1",
            kind=MessageKind.TEXT,
            occurred_at=datetime(2026, 8, 29, 9, 0, tzinfo=timezone.utc),
            text="hello",
        ),
        channel=ChannelBindingConfig(
            binding_id=uuid4(),
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            channel_type="wecom",
        ),
    )


@pytest.mark.anyio
async def test_task_queue_is_idempotent_and_can_be_claimed_by_another_node(
    tmp_path: Path, ) -> None:
    """A Gateway and Worker can exchange one request using only shared SQL."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'queue.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    gateway_queue = PostgreSQLAgentTaskQueue(sessions)
    worker_queue = PostgreSQLAgentTaskQueue(sessions)
    request = _request()

    task_id = await gateway_queue.enqueue(request)
    duplicate_id = await gateway_queue.enqueue(request)
    claimed = await worker_queue.claim(
        "worker-node-b:0",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )

    assert duplicate_id == task_id
    assert claimed is not None
    assert claimed.task_id == task_id
    assert claimed.request == request
    assert claimed.status is AgentTaskStatus.RUNNING
    assert await gateway_queue.claim(
        "worker-node-c:0",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    ) is None
    assert not await gateway_queue.renew(
        task_id,
        worker_id="worker-node-c:0",
        fencing_token=claimed.fencing_token,
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    with pytest.raises(IdempotencyConflict):
        await gateway_queue.enqueue(
            replace(
                request,
                incoming=replace(request.incoming, text="different payload"),
            ))

    await engine.dispose()


@pytest.mark.anyio
async def test_task_queue_releases_failed_work_and_persists_terminal_status(
    tmp_path: Path, ) -> None:
    """A failed node releases work that another node can finish and expose."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'retry.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    queue = PostgreSQLAgentTaskQueue(async_sessionmaker(engine, expire_on_commit=False))
    request = _request()
    task_id = await queue.enqueue(request)
    first = await queue.claim(
        "worker-node-a:0",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert first is not None

    await queue.fail(
        task_id,
        worker_id="worker-node-a:0",
        fencing_token=first.fencing_token,
        error_code="ModelTimeout",
        error_summary="Agent execution failed",
        next_attempt_at=datetime.now(timezone.utc),
    )
    second = await queue.claim(
        "worker-node-b:0",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert second is not None
    assert await queue.renew(
        task_id,
        worker_id="worker-node-b:0",
        fencing_token=second.fencing_token,
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    await queue.complete(
        task_id,
        worker_id="worker-node-b:0",
        fencing_token=second.fencing_token,
    )

    snapshot = await queue.get(
        request.tenant,
        request.channel.binding_id,
        request.incoming.external_message_id,
    )
    assert snapshot is not None
    assert snapshot.status is AgentTaskStatus.SUCCEEDED
    assert snapshot.attempt_count == 2
    assert snapshot.safe_error is None

    permanent_request = replace(
        request,
        tenant=request.tenant.model_copy(update={
            "request_id": "request-2",
            "trace_id": "trace-2"
        }),
        incoming=replace(request.incoming, external_message_id="message-2"),
    )
    permanent_id = await queue.enqueue(permanent_request)
    permanent_claim = await queue.claim(
        "worker-node-c:0",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert permanent_claim is not None
    await queue.fail(
        permanent_id,
        worker_id="worker-node-c:0",
        fencing_token=permanent_claim.fencing_token,
        error_code="InvalidConfiguration",
        error_summary="Agent task cannot be executed",
        next_attempt_at=None,
    )
    permanent = await queue.get(
        permanent_request.tenant,
        permanent_request.channel.binding_id,
        permanent_request.incoming.external_message_id,
    )
    assert permanent is not None
    assert permanent.status is AgentTaskStatus.PERMANENT_FAILED
    assert permanent.safe_error == "Agent task cannot be executed"

    await engine.dispose()


@pytest.mark.anyio
async def test_task_queue_preserves_fifo_within_one_session(tmp_path: Path) -> None:
    """Competing Workers cannot overtake an unfinished task in one Session."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fifo.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    queue = PostgreSQLAgentTaskQueue(async_sessionmaker(engine, expire_on_commit=False))
    first_request = _request()
    second_request = replace(
        first_request,
        tenant=first_request.tenant.model_copy(update={
            "request_id": "request-2",
            "trace_id": "trace-2",
        }),
        incoming=replace(first_request.incoming, external_message_id="message-2"),
    )
    first_id = await queue.enqueue(first_request)
    second_id = await queue.enqueue(second_request)

    first = await queue.claim(
        "worker-a:0",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    blocked = await queue.claim(
        "worker-b:0",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )

    assert first is not None and first.task_id == first_id
    assert blocked is None
    await queue.complete(
        first_id,
        worker_id="worker-a:0",
        fencing_token=first.fencing_token,
    )
    second = await queue.claim(
        "worker-b:0",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert second is not None and second.task_id == second_id

    await engine.dispose()


@pytest.mark.anyio
async def test_task_queue_fence_rejects_stale_same_worker_generation(tmp_path: Path) -> None:
    """A reused node/slot ID cannot complete a lease generation it no longer owns."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fence.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    queue = PostgreSQLAgentTaskQueue(sessions)
    task_id = await queue.enqueue(_request())
    worker_id = "reused-node:0"
    first = await queue.claim(
        worker_id,
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert first is not None

    async with sessions.begin() as database:
        await database.execute(
            update(AgentTaskRow).where(AgentTaskRow.task_id == UUID(first.task_id)).values(
                lease_until=datetime.now(timezone.utc) - timedelta(seconds=1)))
    second = await queue.claim(
        worker_id,
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert second is not None
    assert second.fencing_token > first.fencing_token

    with pytest.raises(StaleExecutionLease):
        await queue.complete(
            task_id,
            worker_id=worker_id,
            fencing_token=first.fencing_token,
        )
    await queue.complete(
        task_id,
        worker_id=worker_id,
        fencing_token=second.fencing_token,
    )
    await engine.dispose()

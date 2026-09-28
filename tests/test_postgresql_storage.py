import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from trpc_service.storage import (
    AuditRecord,
    ExecutionAlreadyRunning,
    ExecutionCommit,
    IdempotencyConflict,
    InboxClaimRequest,
    MemoryRecord,
    OutboxMessage,
    SessionEvent,
    SessionSummary,
    StaleExecutionLease,
)
from trpc_service.storage.adapters.postgresql import PostgreSQLStorage
from trpc_service.storage.orm import Base
from trpc_service.storage.runtime_orm import (
    AgentSession,
    AuditLogRow,
    InboxMessageRow,
    OutboxMessageRow,
)
from trpc_service.tenant import TenantContext


def _context() -> TenantContext:
    return TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )


@pytest.mark.anyio
async def test_postgresql_storage_persists_scoped_runtime_facts() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgreSQLStorage(sessions)
    context = _context()
    event = SessionEvent(
        event_id="event-1",
        event_type="user_message",
        occurred_at=datetime.now(timezone.utc),
        payload={"text": "hello"},
    )
    claim_request = InboxClaimRequest(
        binding_id=uuid4(),
        external_message_id="message-1",
        payload_hash="a" * 64,
        session_id="session-1",
        received_at=datetime.now(timezone.utc),
    )
    lease_until = datetime.now(timezone.utc) + timedelta(seconds=30)
    claim = await store.claim_execution(context, claim_request, "worker-1", lease_until)
    assert claim.fencing_token is not None
    assert await store.renew_execution(
        context,
        claim.inbox_id,
        worker_id="worker-1",
        fencing_token=claim.fencing_token,
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=60),
    )
    with pytest.raises(ExecutionAlreadyRunning):
        await store.claim_execution(context, claim_request, "worker-2", lease_until)

    commit = ExecutionCommit(
        session_id="session-1",
        expected_version=0,
        events=(event, ),
        state={"turn": 1},
        fencing_token=claim.fencing_token,
        inbox_id=claim.inbox_id,
        runner_request_id=claim.request_id,
        outbox=(OutboxMessage(
            outbox_id="outbox-1",
            category="IM_REPLY",
            idempotency_key="message-1:reply:0",
            destination="wecom",
            binding_id=claim_request.binding_id,
            request_id=claim.request_id,
            session_id="session-1",
            sequence_no=0,
            payload={"text": "hello"},
        ), ),
    )

    first = await store.commit_execution(context, commit)
    replay_claim = await store.claim_execution(context, claim_request, "worker-2", lease_until)

    assert replay_claim.replayed
    assert replay_claim.completed == first
    assert replay_claim.committed_outbox_ids == ("outbox-1", )
    assert await store.load(context, "session-1") == first
    assert await store.load(_context(), "session-1") is None
    with pytest.raises(StaleExecutionLease):
        await store.commit_execution(
            context,
            ExecutionCommit(
                session_id="session-1",
                expected_version=1,
                fencing_token=1,
            ),
        )

    with pytest.raises(IdempotencyConflict):
        await store.claim_execution(
            context,
            InboxClaimRequest(
                binding_id=claim_request.binding_id,
                external_message_id="message-1",
                payload_hash="b" * 64,
                session_id="session-1",
                received_at=claim_request.received_at,
            ),
            "worker-2",
            lease_until,
        )

    claimed_outbox = await store.claim_outbox(
        context,
        "outbox-1",
        worker_id="delivery-1",
        lease_until=lease_until,
    )
    assert claimed_outbox is not None
    assert claimed_outbox.payload == {"text": "hello"}
    await store.complete_outbox(
        context,
        "outbox-1",
        worker_id="delivery-1",
        attempt_no=claimed_outbox.attempt_count,
        external_receipt_id="provider-reply-1",
        completed_at=datetime.now(timezone.utc),
    )
    assert await store.claim_outbox(
        context,
        "outbox-1",
        worker_id="delivery-2",
        lease_until=lease_until,
    ) is None

    memory = MemoryRecord("memory-1", "user-1", "durable postgres memory")
    await store.upsert(context, [memory])
    assert [hit.record for hit in await store.search(context, "user-1", "postgres", 5)] == [memory]

    summary = SessionSummary("session-1", 1, "summary")
    assert await store.put_if_newer(context, summary)
    assert not await store.put_if_newer(context, summary)
    await store.append(
        context,
        AuditRecord(
            "agent.execute",
            "allow",
            datetime.now(timezone.utc),
            attributes={
                "latency_ms": 125.5,
                "cost_amount": 0.02
            },
        ),
    )
    async with sessions() as database:
        audit = await database.scalar(select(AuditLogRow))
    assert audit is not None
    assert audit.latency_ms == 126
    assert audit.cost_amount == "0.02"
    await engine.dispose()


@pytest.mark.anyio
async def test_postgresql_fence_rejects_an_execution_after_takeover() -> None:
    """The persisted Session token fences an expired Worker from commit."""

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = PostgreSQLStorage(async_sessionmaker(engine, expire_on_commit=False))
    context = _context()
    request = InboxClaimRequest(
        binding_id=uuid4(),
        external_message_id="takeover-message",
        payload_hash="c" * 64,
        session_id="takeover-session",
        received_at=datetime.now(timezone.utc),
    )
    old_claim = await store.claim_execution(
        context,
        request,
        "worker-old",
        datetime.now(timezone.utc) + timedelta(milliseconds=5),
    )
    await asyncio.sleep(0.01)
    current_claim = await store.claim_execution(
        context,
        request,
        "worker-current",
        datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert old_claim.fencing_token is not None
    assert current_claim.fencing_token is not None
    assert current_claim.fencing_token > old_claim.fencing_token
    with pytest.raises(StaleExecutionLease):
        await store.commit_execution(
            context,
            ExecutionCommit(
                "takeover-session",
                expected_version=0,
                fencing_token=old_claim.fencing_token,
                inbox_id=old_claim.inbox_id,
                runner_request_id=old_claim.request_id,
            ),
        )
    snapshot = await store.commit_execution(
        context,
        ExecutionCommit(
            "takeover-session",
            expected_version=0,
            fencing_token=current_claim.fencing_token,
            inbox_id=current_claim.inbox_id,
            runner_request_id=current_claim.request_id,
        ),
    )
    assert snapshot.version == 1
    await engine.dispose()


@pytest.mark.anyio
async def test_postgresql_replays_legacy_inbox_without_a_binding_prefix() -> None:
    """A quarantined legacy identity is replayed instead of executing again."""

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgreSQLStorage(sessions)
    context = _context()
    now = datetime.now(timezone.utc)
    async with sessions.begin() as database:
        database.add(
            AgentSession(
                tenant_id=context.tenant_id,
                agent_app_id=context.agent_app_id,
                session_id="legacy-session",
                version=1,
                last_event_seq=0,
                state={"legacy": True},
            ))
        database.add(
            InboxMessageRow(
                tenant_id=context.tenant_id,
                agent_app_id=context.agent_app_id,
                binding_id=UUID(int=0),
                external_message_id="legacy-message",
                payload_hash="0" * 64,
                id_source="LEGACY",
                request_id="legacy-request",
                trace_id="legacy-trace",
                session_id="legacy-session",
                status="SUCCEEDED",
                committed_session_version=1,
                result_state={"legacy": True},
                received_at=now,
            ))
    replay = await store.claim_execution(
        context,
        InboxClaimRequest(
            binding_id=uuid4(),
            external_message_id="legacy-message",
            payload_hash="f" * 64,
            session_id="legacy-session",
            received_at=now,
        ),
        "worker-current",
        now + timedelta(seconds=30),
    )
    assert replay.replayed
    assert replay.completed is not None
    assert replay.completed.state == {"legacy": True}
    await engine.dispose()


@pytest.mark.anyio
async def test_postgresql_execution_failure_and_lease_boundaries() -> None:
    """Retry/permanent Inbox outcomes retain fencing and backoff guarantees."""

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgreSQLStorage(sessions)
    context = _context()
    now = datetime.now(timezone.utc)
    request = InboxClaimRequest(
        binding_id=uuid4(),
        external_message_id="failed-message",
        payload_hash="d" * 64,
        session_id="failed-session",
        received_at=now,
    )

    with pytest.raises(ValueError, match="worker ID"):
        await store.claim_execution(context, request, " ", now + timedelta(seconds=30))
    with pytest.raises(ValueError, match="future"):
        await store.claim_execution(context, request, "worker", now - timedelta(seconds=1))

    claim = await store.claim_execution(context, request, "worker", now + timedelta(seconds=30))
    assert claim.fencing_token is not None
    with pytest.raises(ValueError, match="future"):
        await store.renew_execution(
            context,
            claim.inbox_id,
            worker_id="worker",
            fencing_token=claim.fencing_token,
            lease_until=now - timedelta(seconds=1),
        )
    assert not await store.renew_execution(
        context,
        claim.inbox_id,
        worker_id="other-worker",
        fencing_token=claim.fencing_token,
        lease_until=now + timedelta(seconds=30),
    )
    with pytest.raises(StaleExecutionLease):
        await store.fail_execution(
            context,
            claim.inbox_id,
            fencing_token=claim.fencing_token + 1,
            error_code="stale",
            error_summary="stale worker",
            next_attempt_at=None,
        )
    retry_at = now + timedelta(seconds=60)
    await store.fail_execution(
        context,
        claim.inbox_id,
        fencing_token=claim.fencing_token,
        error_code="TRANSIENT" * 20,
        error_summary="temporary" * 300,
        next_attempt_at=retry_at,
    )
    with pytest.raises(ExecutionAlreadyRunning, match="backoff"):
        await store.claim_execution(context, request, "worker-next", now + timedelta(seconds=90))

    missing_context = _context()
    await store.fail_execution(
        missing_context,
        claim.inbox_id,
        fencing_token=1,
        error_code="ignored",
        error_summary="outside scope",
        next_attempt_at=None,
    )
    async with sessions() as database:
        row = await database.scalar(select(InboxMessageRow))
        assert row is not None
        assert row.status == "RETRYABLE_FAILED"
        assert len(row.last_error_code or "") == 100
        assert len(row.last_error_summary or "") == 2000

    permanent_request = replace(
        request,
        binding_id=uuid4(),
        external_message_id="permanent-message",
        session_id="permanent-session",
    )
    permanent_context = context.model_copy(update={"request_id": "request-permanent"})
    permanent = await store.claim_execution(
        permanent_context,
        permanent_request,
        "worker",
        now + timedelta(seconds=30),
    )
    assert permanent.fencing_token is not None
    await store.fail_execution(
        permanent_context,
        permanent.inbox_id,
        fencing_token=permanent.fencing_token,
        error_code="INVALID",
        error_summary="invalid configuration",
        next_attempt_at=None,
    )
    with pytest.raises(RuntimeError, match="permanently failed"):
        await store.claim_execution(
            permanent_context,
            permanent_request,
            "worker-next",
            now + timedelta(seconds=90),
        )
    await engine.dispose()


@pytest.mark.anyio
async def test_postgresql_outbox_failure_and_commit_boundaries() -> None:
    """Outbox terminal states and stale attempts cannot overwrite each other."""

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = PostgreSQLStorage(sessions)
    context = _context()
    now = datetime.now(timezone.utc)
    outbox = OutboxMessage(
        outbox_id="failed-outbox",
        category="IM_REPLY",
        idempotency_key="failed-outbox-key",
        destination="wecom",
    )
    await store.commit_execution(
        context,
        ExecutionCommit("outbox-session", expected_version=0, outbox=(outbox, )),
    )

    with pytest.raises(ValueError, match="worker and future lease"):
        await store.claim_outbox(
            context,
            outbox.outbox_id,
            worker_id="",
            lease_until=now + timedelta(seconds=30),
        )
    with pytest.raises(LookupError, match="does not exist"):
        await store.complete_outbox(
            context,
            "missing-outbox",
            worker_id="worker",
            attempt_no=1,
            external_receipt_id="receipt",
            completed_at=now,
        )

    claimed = await store.claim_outbox(
        context,
        outbox.outbox_id,
        worker_id="delivery",
        lease_until=now + timedelta(seconds=30),
    )
    assert claimed is not None
    assert await store.claim_outbox(
        context,
        outbox.outbox_id,
        worker_id="other",
        lease_until=now + timedelta(seconds=30),
    ) is None
    with pytest.raises(StaleExecutionLease):
        await store.fail_outbox(
            context,
            outbox.outbox_id,
            worker_id="other",
            attempt_no=claimed.attempt_count,
            error_code="STALE",
            error_summary="stale",
            next_attempt_at=None,
            completed_at=now,
        )
    await store.fail_outbox(
        context,
        outbox.outbox_id,
        worker_id="delivery",
        attempt_no=claimed.attempt_count,
        error_code="TIMEOUT" * 20,
        error_summary="unknown" * 400,
        next_attempt_at=None,
        completed_at=now,
        outcome_unknown=True,
    )
    assert await store.claim_outbox(
        context,
        outbox.outbox_id,
        worker_id="delivery",
        lease_until=now + timedelta(seconds=30),
    ) is None

    retry_outbox = OutboxMessage(
        outbox_id="retry-outbox",
        category="AUDIT",
        idempotency_key="retry-outbox-key",
    )
    await store.commit_execution(
        context,
        ExecutionCommit("outbox-session", expected_version=1, outbox=(retry_outbox, )),
    )
    retry_claim = await store.claim_outbox(
        context,
        retry_outbox.outbox_id,
        worker_id="delivery",
        lease_until=now + timedelta(seconds=30),
    )
    assert retry_claim is not None
    retry_at = now + timedelta(seconds=60)
    await store.fail_outbox(
        context,
        retry_outbox.outbox_id,
        worker_id="delivery",
        attempt_no=retry_claim.attempt_count,
        error_code="BUSY",
        error_summary="retry later",
        next_attempt_at=retry_at,
        completed_at=now,
    )
    assert await store.claim_outbox(
        context,
        retry_outbox.outbox_id,
        worker_id="delivery",
        lease_until=now + timedelta(seconds=30),
    ) is None

    await store.fail_outbox(
        _context(),
        "missing-outbox",
        worker_id="delivery",
        attempt_no=1,
        error_code="ignored",
        error_summary="ignored",
        next_attempt_at=None,
        completed_at=now,
    )
    async with sessions() as database:
        unknown = await database.get(
            OutboxMessageRow,
            (context.tenant_id, context.agent_app_id, outbox.outbox_id),
        )
        assert unknown is not None
        assert unknown.status == "UNKNOWN"
        assert len(unknown.last_error_code or "") == 100
        assert len(unknown.last_error_summary or "") == 2000
    await engine.dispose()

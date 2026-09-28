import asyncio
import hashlib
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from trpc_service.storage import (
    ArtifactIntegrityError,
    ArtifactMetadata,
    AuditRecord,
    ExecutionAlreadyRunning,
    ExecutionCommit,
    IdempotencyConflict,
    InboxClaimRequest,
    KnowledgeDocument,
    MemoryRecord,
    OutboxMessage,
    SessionEvent,
    SessionSummary,
    SessionVersionConflict,
    StaleExecutionLease,
    StoredObjectNotFound,
)
from trpc_service.storage.adapters.inmemory import build_inmemory_backend
from trpc_service.tenant import TenantContext


def _context(*, tenant_id: UUID | None = None) -> TenantContext:
    return TenantContext(
        tenant_id=tenant_id or uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )


@pytest.mark.anyio
async def test_inmemory_session_commit_is_versioned_idempotent_and_tenant_scoped() -> None:
    store = build_inmemory_backend().session
    assert store is not None
    context = _context()
    binding_id = uuid4()
    claim_request = InboxClaimRequest(
        binding_id=binding_id,
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
        events=(SessionEvent(
            event_id="event-1",
            event_type="user_message",
            occurred_at=datetime.now(timezone.utc),
            payload={"text": "hello"},
        ), ),
        state={"turn": 1},
        fencing_token=claim.fencing_token,
        inbox_id=claim.inbox_id,
        runner_request_id=claim.request_id,
        outbox=(OutboxMessage(
            outbox_id="outbox-1",
            category="IM_REPLY",
            idempotency_key="request-1:reply:0",
            destination="wecom",
            binding_id=binding_id,
            request_id=claim.request_id,
            session_id="session-1",
            sequence_no=0,
            payload={"text": "hello"},
        ), ),
    )

    first = await store.commit_execution(context, commit)
    replay = await store.claim_execution(context, claim_request, "worker-2", lease_until)

    assert first.version == 1
    assert first.events == commit.events
    assert first.state == {"turn": 1}
    assert replay.replayed
    assert replay.completed is first
    assert replay.committed_outbox_ids == ("outbox-1", )
    assert await store.load(context, "session-1") is first
    assert await store.load(_context(), "session-1") is None

    with pytest.raises(SessionVersionConflict):
        await store.commit_execution(
            context,
            ExecutionCommit(
                session_id="session-1",
                expected_version=0,
            ),
        )

    with pytest.raises(IdempotencyConflict):
        await store.claim_execution(
            context,
            InboxClaimRequest(
                binding_id=binding_id,
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


@pytest.mark.anyio
async def test_inmemory_optional_capabilities_are_scoped_and_idempotent() -> None:
    backend = build_inmemory_backend()
    context = _context()
    assert backend.memory is not None
    assert backend.summary is not None
    assert backend.knowledge is not None
    assert backend.audit is not None

    memory = MemoryRecord(
        memory_id="memory-1",
        principal_id="user-1",
        content="Rabbit likes distributed storage",
    )
    await backend.memory.upsert(context, [memory])
    memory_hits = await backend.memory.search(context, "user-1", "distributed storage", 5)
    assert [hit.record for hit in memory_hits] == [memory]
    assert await backend.memory.search(_context(), "user-1", "distributed storage", 5) == []

    old_summary = SessionSummary("session-1", 3, "old")
    new_summary = SessionSummary("session-1", 4, "new")
    assert await backend.summary.put_if_newer(context, old_summary)
    assert not await backend.summary.put_if_newer(context, old_summary)
    assert await backend.summary.put_if_newer(context, new_summary)

    document = KnowledgeDocument(
        document_id="document-1",
        knowledge_base_id="kb-1",
        content="SeaweedFS exposes an S3-compatible API",
    )
    await backend.knowledge.index(context, [document])
    knowledge_hits = await backend.knowledge.search(context, "kb-1", "S3 API", 5)
    assert [hit.document for hit in knowledge_hits] == [document]
    assert await backend.knowledge.search(context, "other-kb", "S3 API", 5) == []

    await backend.audit.append(
        context,
        AuditRecord(
            action="agent.execute",
            decision="allow",
            occurred_at=datetime.now(timezone.utc),
        ),
    )


@pytest.mark.anyio
async def test_inmemory_rejects_a_stale_or_missing_fencing_token() -> None:
    store = build_inmemory_backend().session
    assert store is not None
    context = _context()
    await store.commit_execution(
        context,
        ExecutionCommit("session-1", expected_version=0, fencing_token=2),
    )

    with pytest.raises(StaleExecutionLease):
        await store.commit_execution(
            context,
            ExecutionCommit("session-1", expected_version=1, fencing_token=1),
        )
    with pytest.raises(StaleExecutionLease):
        await store.commit_execution(
            context,
            ExecutionCommit("session-1", expected_version=1),
        )


@pytest.mark.anyio
async def test_inmemory_outbox_preserves_an_unknown_provider_outcome() -> None:
    store = build_inmemory_backend().session
    assert store is not None
    context = _context()
    lease_until = datetime.now(timezone.utc) + timedelta(seconds=30)
    await store.commit_execution(
        context,
        ExecutionCommit(
            session_id="session-unknown",
            expected_version=0,
            outbox=(OutboxMessage(
                outbox_id="outbox-unknown",
                category="IM_REPLY",
                idempotency_key="unknown:reply:0",
                destination="wecom",
            ), ),
        ),
    )
    claimed = await store.claim_outbox(
        context,
        "outbox-unknown",
        worker_id="delivery-1",
        lease_until=lease_until,
    )
    assert claimed is not None
    assert claimed.attempt_count == 1
    await store.fail_outbox(
        context,
        "outbox-unknown",
        worker_id="delivery-1",
        attempt_no=claimed.attempt_count,
        error_code="TimeoutError",
        error_summary="provider outcome unknown",
        next_attempt_at=None,
        completed_at=datetime.now(timezone.utc),
        outcome_unknown=True,
    )
    assert await store.claim_outbox(
        context,
        "outbox-unknown",
        worker_id="delivery-2",
        lease_until=lease_until,
    ) is None


@pytest.mark.anyio
async def test_inmemory_rejects_execution_and_outbox_workers_after_takeover() -> None:
    """An expired Worker cannot finish work after a newer lease is issued."""

    store = build_inmemory_backend().session
    assert store is not None
    context = _context()
    request = InboxClaimRequest(
        binding_id=uuid4(),
        external_message_id="takeover-message",
        payload_hash="c" * 64,
        session_id="takeover-session",
        received_at=datetime.now(timezone.utc),
    )
    first_claim = await store.claim_execution(
        context,
        request,
        "worker-old",
        datetime.now(timezone.utc) + timedelta(milliseconds=5),
    )
    await asyncio.sleep(0.01)
    second_claim = await store.claim_execution(
        context,
        request,
        "worker-new",
        datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert first_claim.fencing_token is not None
    assert second_claim.fencing_token is not None
    assert second_claim.fencing_token > first_claim.fencing_token
    with pytest.raises(StaleExecutionLease):
        await store.commit_execution(
            context,
            ExecutionCommit(
                "takeover-session",
                expected_version=0,
                fencing_token=first_claim.fencing_token,
                inbox_id=first_claim.inbox_id,
                runner_request_id=first_claim.request_id,
            ),
        )
    await store.commit_execution(
        context,
        ExecutionCommit(
            "takeover-session",
            expected_version=0,
            fencing_token=second_claim.fencing_token,
            inbox_id=second_claim.inbox_id,
            runner_request_id=second_claim.request_id,
            outbox=(OutboxMessage("takeover-outbox", "IM_REPLY", "takeover-key"), ),
        ),
    )

    old_attempt = await store.claim_outbox(
        context,
        "takeover-outbox",
        worker_id="delivery-old",
        lease_until=datetime.now(timezone.utc) + timedelta(milliseconds=5),
    )
    assert old_attempt is not None
    await asyncio.sleep(0.01)
    new_attempt = await store.claim_outbox(
        context,
        "takeover-outbox",
        worker_id="delivery-new",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    assert new_attempt is not None
    with pytest.raises(StaleExecutionLease):
        await store.complete_outbox(
            context,
            "takeover-outbox",
            worker_id="delivery-old",
            attempt_no=old_attempt.attempt_count,
            external_receipt_id="stale-receipt",
            completed_at=datetime.now(timezone.utc),
        )
    await store.complete_outbox(
        context,
        "takeover-outbox",
        worker_id="delivery-new",
        attempt_no=new_attempt.attempt_count,
        external_receipt_id="current-receipt",
        completed_at=datetime.now(timezone.utc),
    )


@pytest.mark.anyio
async def test_inmemory_outbox_uses_semantic_idempotency_key() -> None:
    """Different generated IDs cannot duplicate one logical delivery task."""

    store = build_inmemory_backend().session
    assert store is not None
    context = _context()
    await store.commit_execution(
        context,
        ExecutionCommit(
            "semantic-session",
            expected_version=0,
            outbox=(OutboxMessage("outbox-first", "IM_REPLY", "semantic-key"), ),
        ),
    )
    await store.commit_execution(
        context,
        ExecutionCommit(
            "semantic-session",
            expected_version=1,
            outbox=(OutboxMessage("outbox-duplicate", "IM_REPLY", "semantic-key"), ),
        ),
    )
    lease_until = datetime.now(timezone.utc) + timedelta(seconds=30)
    assert await store.claim_outbox(
        context,
        "outbox-duplicate",
        worker_id="delivery-worker",
        lease_until=lease_until,
    ) is None
    claimed = await store.claim_outbox(
        context,
        "outbox-first",
        worker_id="delivery-worker",
        lease_until=lease_until,
    )
    assert claimed is not None
    await store.fail_outbox(
        context,
        "outbox-first",
        worker_id="delivery-worker",
        attempt_no=claimed.attempt_count,
        error_code="RateLimited",
        error_summary="retry later",
        next_attempt_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        completed_at=datetime.now(timezone.utc),
    )
    assert await store.claim_outbox(
        context,
        "outbox-first",
        worker_id="delivery-worker",
        lease_until=lease_until,
    ) is None


async def _chunks(payload: bytes) -> AsyncIterator[bytes]:
    yield payload[:2]
    yield payload[2:]


@pytest.mark.anyio
async def test_inmemory_artifact_validates_integrity_and_tenant_scope() -> None:
    artifact_store = build_inmemory_backend().artifact
    assert artifact_store is not None
    context = _context()
    payload = b"artifact"
    metadata = ArtifactMetadata(
        filename="result.txt",
        media_type="text/plain",
        checksum=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
    )

    reference = await artifact_store.put(context, _chunks(payload), metadata)

    assert b"".join([chunk async for chunk in artifact_store.open(context, reference.artifact_id)
                     ]) == payload
    assert await artifact_store.create_download_url(context, reference.artifact_id,
                                                    60) == reference.uri
    with pytest.raises(StoredObjectNotFound):
        _ = [chunk async for chunk in artifact_store.open(_context(), reference.artifact_id)]
    with pytest.raises(ArtifactIntegrityError):
        await artifact_store.put(
            context,
            _chunks(payload + b"!"),
            metadata,
        )


@pytest.mark.anyio
async def test_inmemory_knowledge_and_artifacts_are_shared_only_inside_tenant() -> None:
    """Tenant ownership allows authorized Agents to share one knowledge corpus."""

    backend = build_inmemory_backend()
    assert backend.knowledge is not None
    assert backend.artifact is not None
    owner = _context()
    sibling_agent = TenantContext(
        tenant_id=owner.tenant_id,
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-sibling",
        trace_id="trace-sibling",
    )
    foreign_tenant = _context()
    document = KnowledgeDocument("chunk-1", "handbook", "annual leave policy")
    payload = b"tenant-owned artifact"
    metadata = ArtifactMetadata(
        filename="handbook.txt",
        media_type="text/plain",
        checksum=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
    )

    await backend.knowledge.index(owner, [document])
    reference = await backend.artifact.put(owner, _chunks(payload), metadata)

    sibling_hits = await backend.knowledge.search(
        sibling_agent,
        "handbook",
        "annual leave",
        5,
    )
    sibling_payload = b"".join(
        [chunk async for chunk in backend.artifact.open(sibling_agent, reference.artifact_id)])
    foreign_hits = await backend.knowledge.search(
        foreign_tenant,
        "handbook",
        "annual leave",
        5,
    )

    assert [hit.document for hit in sibling_hits] == [document]
    assert sibling_payload == payload
    assert foreign_hits == []
    with pytest.raises(StoredObjectNotFound):
        _ = [
            chunk async for chunk in backend.artifact.open(
                foreign_tenant,
                reference.artifact_id,
            )
        ]

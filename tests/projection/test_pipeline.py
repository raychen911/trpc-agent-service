"""Durable Summary/Memory projection pipeline contracts on SQLite."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import func, select, update

from trpc_service.projection import (
    MemoryCandidate,
    ProjectionOutcome,
    ProjectionWorker,
)
from trpc_service.reliability import (
    AuditData,
    EventData,
    FinalizeDisposition,
    InboxEnvelope,
    ProjectionFinalizeDisposition,
    ProjectionInput,
    ReliabilityRepository,
    ReplyPart,
    StaleClaimError,
    canonical_json_hash,
)
from trpc_service.storage import MemoryProjection, SummaryProjection
from trpc_service.storage.database import Database
from trpc_service.storage.models import (
    AgentApp,
    ChannelBinding,
    MemoryRecord,
    ProjectionJob,
    SessionEvent,
    SessionSummary,
    Tenant,
    TenantConfigRevision,
)


@dataclass(slots=True)
class Harness:
    database: Database
    repository: ReliabilityRepository


async def _seed_tenant(database: Database, tenant_id: str) -> None:
    suffix = tenant_id.removeprefix("tenant-")
    async with database.session_factory.begin() as session:
        session.add(
            Tenant(
                tenant_id=tenant_id,
                display_name=tenant_id,
                status="active",
                active_config_revision=1,
                audit_policy={},
                budget_policy={},
            )
        )
        await session.flush()
        session.add(
            TenantConfigRevision(
                tenant_id=tenant_id,
                revision=1,
                schema_version=1,
                status="published",
                spec={"tenant_id": tenant_id, "revision": 1},
                content_hash=(suffix[0] if suffix else "a") * 64,
                created_by="test",
            )
        )
        session.add(
            AgentApp(
                tenant_id=tenant_id,
                app_id="assistant",
                revision=1,
                status="published",
                agent_name="assistant",
                prompt="test",
                model_config={},
                tool_policy={},
                storage_config={},
            )
        )
        await session.flush()
        session.add(
            ChannelBinding(
                binding_id=f"binding-{suffix}",
                tenant_id=tenant_id,
                app_id="assistant",
                app_revision=1,
                config_revision=1,
                channel_type="telegram",
                external_account_id=f"bot-{suffix}",
                callback_path=f"/callbacks/{suffix}",
                public_callback_id=f"public-{suffix}",
                route_rule={},
                secret_refs={},
                identity_policy={},
                status="active",
            )
        )


@pytest_asyncio.fixture
async def harness(tmp_path: Path) -> AsyncIterator[Harness]:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'projection-pipeline.db').as_posix()}")
    await database.create_schema()
    await _seed_tenant(database, "tenant-a")
    await _seed_tenant(database, "tenant-b")
    try:
        yield Harness(database, ReliabilityRepository(database.session_factory))
    finally:
        await database.dispose()


def _envelope(tenant_id: str, delivery_id: str, *, session_id: str = "session-shared"):
    suffix = tenant_id.removeprefix("tenant-")
    payload = {"text": delivery_id}
    return InboxEnvelope(
        tenant_id=tenant_id,
        binding_id=f"binding-{suffix}",
        session_id=f"{session_id}-{suffix}",
        app_id="assistant",
        app_revision=1,
        config_revision=1,
        scope="private",
        principal_id=f"principal-{suffix}",
        external_delivery_id=delivery_id,
        payload=payload,
        payload_hash=canonical_json_hash(payload),
        request_id=f"request-{tenant_id}-{delivery_id}",
        trace_id=f"trace-{tenant_id}-{delivery_id}",
    )


def _audit(tenant_id: str, delivery_id: str) -> AuditData:
    return AuditData(
        channel="telegram",
        user_id=f"principal-{tenant_id.removeprefix('tenant-')}",
        agent_name="assistant",
        decision="allow",
        action="agent.turn.finalize",
        resource=f"delivery/{delivery_id}",
        config_revision=1,
        policy_revision=1,
    )


async def _finalize(
    repository: ReliabilityRepository,
    tenant_id: str,
    delivery_id: str,
    event_ids: Sequence[str],
):
    await repository.accept_inbox(_envelope(tenant_id, delivery_id))
    claim = await repository.claim_next(tenant_id, f"turn-worker-{delivery_id}")
    assert claim is not None
    version = claim.expected_version
    for event_id in event_ids:
        appended = await repository.append_event_cas(
            claim,
            version,
            EventData(
                event_id=event_id,
                event_key=f"{delivery_id}:{event_id}",
                event_type="message",
                role="user",
                payload={"kind": "sealed-test-event", "event_id": event_id},
            ),
        )
        version = appended.version
    result = await repository.finalize_run(
        claim,
        final_state={"through": version},
        final_event_id=event_ids[-1] if event_ids else None,
        reply_parts=[ReplyPart(f"reply-{delivery_id}", 0, {"text": "ok"})],
        audit=_audit(tenant_id, delivery_id),
    )
    assert result.disposition is FinalizeDisposition.FINALIZED
    return claim


class RecordingSummary:
    version = "summary-test-v1"

    def __init__(self) -> None:
        self.inputs: list[ProjectionInput] = []

    async def summarize(self, projection_input: ProjectionInput) -> str:
        self.inputs.append(projection_input)
        return ",".join(event.event_id for event in projection_input.events)


class EventMemory:
    version = "memory-test-v1"

    async def extract(self, projection_input: ProjectionInput) -> Sequence[MemoryCandidate]:
        return tuple(
            MemoryCandidate(event.event_id, f"memory:{event.event_id}")
            for event in projection_input.events
        )


class FailingSummary:
    version = "summary-failing-v1"

    async def summarize(self, projection_input: ProjectionInput) -> None:
        del projection_input
        raise ConnectionError("sensitive provider detail must not be persisted")


@pytest.mark.asyncio
async def test_finalize_enqueues_once_and_worker_projects_committed_events(
    harness: Harness,
) -> None:
    turn_claim = await _finalize(
        harness.repository,
        "tenant-a",
        "delivery-1",
        ["event-1", "event-2"],
    )
    duplicate = await harness.repository.finalize_run(
        turn_claim,
        final_state={"through": 2},
        final_event_id="event-2",
        reply_parts=[ReplyPart("reply-delivery-1", 0, {"text": "ok"})],
        audit=_audit("tenant-a", "delivery-1"),
    )
    assert duplicate.disposition is FinalizeDisposition.ALREADY_FINALIZED

    async with harness.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(ProjectionJob)) == 1

    summarizer = RecordingSummary()
    worker = ProjectionWorker(
        port=harness.repository,
        summarizer=summarizer,
        memory_extractor=EventMemory(),
    )
    result = await worker.process_once("tenant-a", "projection-worker-a")
    assert result.outcome is ProjectionOutcome.SUCCEEDED
    assert [event.event_id for event in summarizer.inputs[0].events] == ["event-1", "event-2"]
    assert await worker.process_once("tenant-a", "projection-worker-a") == result.__class__(
        ProjectionOutcome.IDLE,
        "tenant-a",
        "projection-worker-a",
    )

    async with harness.database.session_factory() as session:
        summary = await session.get(SessionSummary, ("tenant-a", "session-shared-a"))
        assert summary is not None
        assert (summary.through_seq, summary.content) == (2, "event-1,event-2")
        assert await session.scalar(select(func.count()).select_from(MemoryRecord)) == 2


@pytest.mark.asyncio
async def test_projection_input_never_exposes_uncommitted_event(harness: Harness) -> None:
    await _finalize(harness.repository, "tenant-a", "delivery-1", ["committed-event"])
    await harness.repository.accept_inbox(_envelope("tenant-a", "delivery-2"))
    running = await harness.repository.claim_next("tenant-a", "turn-worker-2")
    assert running is not None
    await harness.repository.append_event_cas(
        running,
        running.expected_version,
        EventData(
            event_id="staged-event",
            event_key="delivery-2:staged",
            event_type="message",
            payload={"visibility": "must-not-leak"},
        ),
    )

    projection_claim = await harness.repository.claim_projection(
        "tenant-a",
        "projection-worker",
    )
    assert projection_claim is not None
    projection_input = await harness.repository.load_projection_input(projection_claim)
    assert projection_input.through_seq == 1
    assert [event.event_id for event in projection_input.events] == ["committed-event"]

    async with harness.database.session_factory() as session:
        staged = await session.scalar(
            select(SessionEvent).where(SessionEvent.event_id == "staged-event")
        )
        assert staged is not None and staged.visibility == "staged"


@pytest.mark.asyncio
async def test_expired_claim_takeover_fences_crashed_worker_and_is_tenant_scoped(
    harness: Harness,
) -> None:
    await _finalize(harness.repository, "tenant-a", "delivery-a", ["event-a"])
    await _finalize(harness.repository, "tenant-b", "delivery-b", ["event-b"])

    stale = await harness.repository.claim_projection("tenant-a", "worker-stale")
    assert stale is not None and stale.tenant_id == "tenant-a"
    assert await harness.repository.claim_projection("tenant-a", "worker-blocked") is None
    tenant_b = await harness.repository.claim_projection("tenant-b", "worker-b")
    assert tenant_b is not None and tenant_b.tenant_id == "tenant-b"

    async with harness.database.session_factory.begin() as session:
        await session.execute(
            update(ProjectionJob)
            .where(ProjectionJob.job_id == stale.job_id)
            .values(claim_expires_at=datetime.now(UTC) - timedelta(seconds=5))
        )
    takeover = await harness.repository.claim_projection("tenant-a", "worker-takeover")
    assert takeover is not None
    assert takeover.fencing_token > stale.fencing_token

    stale_summary = SummaryProjection(
        tenant_id="tenant-a",
        session_id=stale.session_id,
        through_seq=stale.through_seq,
        content="stale",
        summarizer_version="summary-test-v1",
    )
    with pytest.raises(StaleClaimError):
        await harness.repository.complete_projection(
            stale,
            summary=stale_summary,
            memories=(),
        )

    current_summary = SummaryProjection(
        tenant_id="tenant-a",
        session_id=takeover.session_id,
        through_seq=takeover.through_seq,
        content="current",
        summarizer_version="summary-test-v1",
    )
    completed = await harness.repository.complete_projection(
        takeover,
        summary=current_summary,
        memories=(),
    )
    assert completed.disposition is ProjectionFinalizeDisposition.FINALIZED
    duplicate = await harness.repository.complete_projection(
        takeover,
        summary=current_summary,
        memories=(),
    )
    assert duplicate.disposition is ProjectionFinalizeDisposition.ALREADY_FINALIZED


@pytest.mark.asyncio
async def test_out_of_order_jobs_never_regress_summary_or_memory(harness: Harness) -> None:
    await _finalize(harness.repository, "tenant-a", "delivery-1", ["event-1"])
    await _finalize(harness.repository, "tenant-a", "delivery-2", ["event-2"])
    older = await harness.repository.claim_projection("tenant-a", "projection-older")
    newer = await harness.repository.claim_projection("tenant-a", "projection-newer")
    assert older is not None and newer is not None
    assert older.through_seq == 1 and newer.through_seq == 2

    newer_memory = MemoryProjection(
        tenant_id="tenant-a",
        principal_id="principal-a",
        session_id=newer.session_id,
        source_event_id="event-2",
        extractor_version="memory-test-v1",
        record_version=2,
        content="newer-memory",
    )
    newer_result = await harness.repository.complete_projection(
        newer,
        summary=SummaryProjection(
            tenant_id="tenant-a",
            session_id=newer.session_id,
            through_seq=2,
            content="newer-summary",
            summarizer_version="summary-test-v1",
        ),
        memories=(newer_memory,),
    )
    assert newer_result.summary_applied is True

    older_result = await harness.repository.complete_projection(
        older,
        summary=SummaryProjection(
            tenant_id="tenant-a",
            session_id=older.session_id,
            through_seq=1,
            content="older-summary",
            summarizer_version="summary-test-v1",
        ),
        memories=(
            MemoryProjection(
                tenant_id="tenant-a",
                principal_id="principal-a",
                session_id=older.session_id,
                source_event_id="event-1",
                extractor_version="memory-test-v1",
                record_version=1,
                content="older-memory",
            ),
        ),
    )
    assert older_result.summary_applied is False
    assert older_result.memories_applied == 1

    async with harness.database.session_factory() as session:
        summary = await session.get(SessionSummary, ("tenant-a", older.session_id))
        assert summary is not None
        assert (summary.through_seq, summary.content) == (2, "newer-summary")
        versions = list(
            (
                await session.scalars(
                    select(MemoryRecord.record_version).order_by(MemoryRecord.record_version)
                )
            ).all()
        )
        assert versions == [1, 2]


@pytest.mark.asyncio
async def test_retry_is_bounded_and_then_dead_letters_without_error_message(
    harness: Harness,
) -> None:
    await _finalize(harness.repository, "tenant-a", "delivery-1", ["event-1"])
    worker = ProjectionWorker(
        port=harness.repository,
        summarizer=FailingSummary(),
        memory_extractor=EventMemory(),
        max_attempts=2,
        retry_base_delay=timedelta(milliseconds=10),
        retry_max_delay=timedelta(milliseconds=10),
    )
    first = await worker.process_once("tenant-a", "projection-worker")
    assert first.outcome is ProjectionOutcome.RETRY_WAIT

    async with harness.database.session_factory.begin() as session:
        await session.execute(
            update(ProjectionJob)
            .where(ProjectionJob.job_id == first.job_id)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    second = await worker.process_once("tenant-a", "projection-worker")
    assert second.outcome is ProjectionOutcome.DEAD_LETTER

    async with harness.database.session_factory() as session:
        job = await session.get(ProjectionJob, first.job_id)
        assert job is not None
        assert job.status == "dead_letter"
        assert job.attempt_count == 2
        assert job.last_error_type == "ConnectionError"
        assert "sensitive" not in job.last_error_type

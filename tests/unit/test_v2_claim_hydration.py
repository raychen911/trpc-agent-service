from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import envelope, repository
from tests.unit.test_postgres_repository import (
    Connection,
    Pool,
    inbound_row,
    session_row,
)
from tests.unit.test_postgres_repository import (
    acceptance as make_acceptance,
)
from tests.unit.test_postgres_v2_branch_coverage import (
    item_row,
    mailbox_row,
    ready_event,
)
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.config.settings import SchedulerVersion
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import SessionSnapshot
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import FencingConflict


class NoHistoryConnection(Connection):
    async def fetch(self, *args):
        raise AssertionError("v2 claim must not fetch session history")


async def _memory_claim(marker: str):
    repo = repository()
    accepted = await TenantRuntime(
        repo,
        routing_key=b"v2-hydration-cases" * 2,
        scheduler_version=SchedulerVersion.V2,
    ).accept("binding-unpredictable-a", envelope(marker))
    claim = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-v2",
        lease_for=timedelta(seconds=30),
    )
    assert claim.execution_lease is not None
    return repo, accepted, claim


@pytest.mark.asyncio
async def test_postgres_v2_claim_returns_metadata_anchor_without_long_reads() -> None:
    accepted = await make_acceptance()
    event = ready_event(accepted)
    mailbox = mailbox_row(
        accepted,
        status="QUEUED",
        sequence=None,
        owner=None,
        expires=None,
    )
    item = item_row(accepted)
    inbound = inbound_row(accepted)
    session = session_row(accepted)
    claimed = mailbox_row(accepted, owner="worker-v2", epoch=1)
    connection = NoHistoryConnection(
        fetchrows=[event, mailbox, item, inbound, None, session, claimed],
        fetchvals=[datetime.now(UTC)],
    )

    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-v2",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=str(event["outbox_id"]),
    )

    assert claim.claimed
    assert claim.acceptance is None
    assert claim.execution_lease is not None
    assert not claim.execution_lease.snapshot_hydrated
    assert claim.execution_lease.snapshot.events == ()
    assert claim.execution_lease.snapshot.state == {}
    statements = [args[0] for kind, args in connection.calls if kind == "fetchrow"]
    assert not any(
        token in statement
        for statement in statements
        for token in ("session_events", "envelope_json", "state_json", "event_json")
    )


@pytest.mark.asyncio
async def test_worker_hydrates_after_claim_and_renews_before_runner() -> None:
    repo = repository()
    accepted = await TenantRuntime(
        repo,
        routing_key=b"v2-hydration" * 3,
        scheduler_version=SchedulerVersion.V2,
    ).accept("binding-unpredictable-a", envelope("hydrate-after-ack"))
    original = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-v2",
        lease_for=timedelta(seconds=30),
    )
    assert original.execution_lease is not None
    lease = original.execution_lease
    placeholder = SessionSnapshot(
        tenant_id=lease.snapshot.tenant_id,
        app_id=lease.snapshot.app_id,
        session_id=lease.snapshot.session_id,
        principal_id=lease.snapshot.principal_id,
        version=lease.snapshot.version,
        next_sequence=lease.snapshot.next_sequence,
    )
    claim = original.model_copy(
        update={
            "acceptance": None,
            "execution_lease": lease.model_copy(
                update={"snapshot": placeholder, "snapshot_hydrated": False}
            ),
        }
    )
    calls: list[str] = []
    get_snapshot = repo.get_session_snapshot
    renew = repo.renew_session_ready

    async def record_snapshot(tenant_id: str, session_id: str):
        calls.append("snapshot")
        return await get_snapshot(tenant_id, session_id)

    async def record_renew(lease, *, lease_for):
        calls.append("renew")
        return await renew(lease, lease_for=lease_for)

    repo.get_session_snapshot = record_snapshot  # type: ignore[method-assign]
    repo.renew_session_ready = record_renew  # type: ignore[method-assign]

    async def loader(_config):
        from trpc_service.agent.fake import DeterministicAgent

        return DeterministicAgent(name="hydration-agent", response="hydrated")

    result = await AgentWorker(
        repo,
        worker_id="worker-v2",
        agent_loader=loader,
        lease_for=timedelta(seconds=30),
    ).process_claimed(claim)

    assert result.status == ProcessStatus.COMMITTED
    assert calls[:2] == ["snapshot", "renew"]


@pytest.mark.asyncio
async def test_worker_releases_claim_when_hydration_anchor_changes() -> None:
    repo = repository()
    accepted = await TenantRuntime(
        repo,
        routing_key=b"v2-hydration-anchor" * 2,
        scheduler_version=SchedulerVersion.V2,
    ).accept("binding-unpredictable-a", envelope("hydrate-anchor"))
    claim = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-v2",
        lease_for=timedelta(seconds=30),
    )
    assert claim.execution_lease is not None
    lease = claim.execution_lease.model_copy(update={"snapshot_hydrated": False})
    changed = await repo.get_session_snapshot(
        accepted.context.tenant_id,
        accepted.context.session_id,
    )
    assert changed is not None
    changed = changed.model_copy(update={"version": changed.version + 1})
    original_retry = repo.retry_session_ready
    calls: list[str] = []

    async def get_changed(_tenant_id: str, _session_id: str):
        calls.append("snapshot")
        return changed

    async def retry(lease_value, *, error_type, delay):
        calls.append("retry")
        return await original_retry(lease_value, error_type=error_type, delay=delay)

    repo.get_session_snapshot = get_changed  # type: ignore[method-assign]
    repo.retry_session_ready = retry  # type: ignore[method-assign]
    claim = claim.model_copy(update={"acceptance": accepted, "execution_lease": lease})

    async def loader(_config):
        raise AssertionError("model must not start before hydration succeeds")

    with pytest.raises(FencingConflict, match="changed before hydration"):
        await AgentWorker(
            repo,
            worker_id="worker-v2",
            agent_loader=loader,
            lease_for=timedelta(seconds=30),
        ).process_claimed(claim)

    assert calls[:2] == ["snapshot", "retry"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["inbound", "tenant", "session"])
async def test_worker_rejects_acceptance_that_does_not_match_claim(
    mismatch: str,
) -> None:
    repo, accepted, claim = await _memory_claim(f"acceptance-mismatch-{mismatch}")
    assert claim.execution_lease is not None
    if mismatch == "inbound":
        bad_acceptance = accepted.model_copy(update={"inbound_id": "not-the-claimed-id"})
    else:
        bad_context = accepted.context.model_copy(
            update={"tenant_id" if mismatch == "tenant" else "session_id": "other-value"}
        )
        bad_acceptance = accepted.model_copy(update={"context": bad_context})
    calls: list[str] = []
    original_retry = repo.retry_session_ready

    async def retry(lease_value, *, error_type, delay):
        calls.append("retry")
        return await original_retry(lease_value, error_type=error_type, delay=delay)

    repo.retry_session_ready = retry  # type: ignore[method-assign]

    async def loader(_config):
        raise AssertionError("model must not start for a mismatched acceptance")

    with pytest.raises(FencingConflict, match="does not match session lease"):
        await AgentWorker(
            repo,
            worker_id="worker-v2",
            agent_loader=loader,
            lease_for=timedelta(seconds=30),
        ).process_claimed(claim.model_copy(update={"acceptance": bad_acceptance}))

    assert calls == ["retry"]


@pytest.mark.asyncio
async def test_worker_releases_claim_when_authoritative_acceptance_is_missing() -> None:
    repo, _accepted, claim = await _memory_claim("acceptance-missing")
    calls: list[str] = []
    original_retry = repo.retry_session_ready

    async def missing_acceptance(_tenant_id: str, _inbound_id: str):
        return None

    async def retry(lease_value, *, error_type, delay):
        calls.append("retry")
        return await original_retry(lease_value, error_type=error_type, delay=delay)

    repo.get_acceptance = missing_acceptance  # type: ignore[method-assign]
    repo.retry_session_ready = retry  # type: ignore[method-assign]

    async def loader(_config):
        raise AssertionError("model must not start without acceptance")

    with pytest.raises(LookupError, match="claimed inbound does not exist"):
        await AgentWorker(
            repo,
            worker_id="worker-v2",
            agent_loader=loader,
            lease_for=timedelta(seconds=30),
        ).process_claimed(claim.model_copy(update={"acceptance": None}))

    assert calls == ["retry"]


@pytest.mark.asyncio
async def test_worker_releases_claim_when_hydrated_snapshot_is_missing() -> None:
    repo, accepted, claim = await _memory_claim("snapshot-missing")
    assert claim.execution_lease is not None
    lease = claim.execution_lease.model_copy(update={"snapshot_hydrated": False})
    calls: list[str] = []
    original_retry = repo.retry_session_ready

    async def missing_snapshot(_tenant_id: str, _session_id: str):
        calls.append("snapshot")
        return None

    async def retry(lease_value, *, error_type, delay):
        calls.append("retry")
        return await original_retry(lease_value, error_type=error_type, delay=delay)

    repo.get_session_snapshot = missing_snapshot  # type: ignore[method-assign]
    repo.retry_session_ready = retry  # type: ignore[method-assign]

    async def loader(_config):
        raise AssertionError("model must not start without a hydrated snapshot")

    with pytest.raises(LookupError, match="claimed session does not exist"):
        await AgentWorker(
            repo,
            worker_id="worker-v2",
            agent_loader=loader,
            lease_for=timedelta(seconds=30),
        ).process_claimed(
            claim.model_copy(update={"acceptance": accepted, "execution_lease": lease})
        )

    assert calls == ["snapshot", "retry"]


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["tenant_id", "session_id", "next_sequence"])
async def test_worker_rejects_hydrated_snapshot_identity_or_anchor_change(field: str) -> None:
    repo, accepted, claim = await _memory_claim(f"snapshot-mismatch-{field}")
    assert claim.execution_lease is not None
    lease = claim.execution_lease
    snapshot = lease.snapshot
    if field in {"tenant_id", "session_id"}:
        snapshot = snapshot.model_copy(update={field: "other-value"})
    else:
        snapshot = snapshot.model_copy(update={field: snapshot.next_sequence + 1})
    calls: list[str] = []
    original_retry = repo.retry_session_ready

    async def changed_snapshot(_tenant_id: str, _session_id: str):
        calls.append("snapshot")
        return snapshot

    async def retry(lease_value, *, error_type, delay):
        calls.append("retry")
        return await original_retry(lease_value, error_type=error_type, delay=delay)

    repo.get_session_snapshot = changed_snapshot  # type: ignore[method-assign]
    repo.retry_session_ready = retry  # type: ignore[method-assign]

    async def loader(_config):
        raise AssertionError("model must not start after hydration fencing fails")

    with pytest.raises(FencingConflict, match="changed before hydration"):
        await AgentWorker(
            repo,
            worker_id="worker-v2",
            agent_loader=loader,
            lease_for=timedelta(seconds=30),
        ).process_claimed(
            claim.model_copy(
                update={
                    "acceptance": accepted,
                    "execution_lease": lease.model_copy(update={"snapshot_hydrated": False}),
                }
            )
        )

    assert calls == ["snapshot", "retry"]


@pytest.mark.asyncio
async def test_worker_does_not_start_runner_when_hydration_renewal_is_fenced() -> None:
    repo, accepted, claim = await _memory_claim("snapshot-renew-fenced")
    assert claim.execution_lease is not None
    lease = claim.execution_lease.model_copy(update={"snapshot_hydrated": False})
    calls: list[str] = []

    async def renew(_lease, *, lease_for):
        calls.append("renew")
        raise FencingConflict("session mailbox lease is no longer current")

    repo.renew_session_ready = renew  # type: ignore[method-assign]

    async def loader(_config):
        raise AssertionError("model must not start after renewal fencing")

    with pytest.raises(FencingConflict, match="no longer current"):
        await AgentWorker(
            repo,
            worker_id="worker-v2",
            agent_loader=loader,
            lease_for=timedelta(seconds=30),
        ).process_claimed(
            claim.model_copy(
                update={
                    "acceptance": accepted,
                    "execution_lease": lease,
                }
            )
        )

    assert calls == ["renew"]

from __future__ import annotations

from datetime import timedelta

import pytest

from tests.conftest import envelope, repository
from trpc_service.channels.envelopes import OutboundEnvelope
from trpc_service.config.settings import SchedulerVersion
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import TurnCommit
from trpc_service.tenant.models import Channel


async def _claimed_v2(message_id: str = "atomic-commit"):
    repo = repository()
    accepted = await TenantRuntime(
        repo,
        routing_key=b"memory-atomic" * 3,
        scheduler_version=SchedulerVersion.V2,
    ).accept("binding-unpredictable-a", envelope(message_id))
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None and repo.mailbox.outbox
    claim = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None
    return repo, accepted, claim.execution_lease


def _outbound(accepted, *, text: str = "reply") -> OutboundEnvelope:
    return OutboundEnvelope(
        outbound_id="outbound-atomic",
        tenant_id=accepted.context.tenant_id,
        binding_id=accepted.context.channel_binding_id,
        channel=Channel.FEISHU,
        target_id="user-1",
        session_id=accepted.context.session_id,
        text=text,
    )


@pytest.mark.asyncio
async def test_v2_commit_failure_after_mailbox_transition_is_atomic(monkeypatch) -> None:
    repo, accepted, lease = await _claimed_v2()
    before_mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    before_snapshot = await repo.get_session_snapshot(
        accepted.context.tenant_id, accepted.context.session_id
    )
    assert before_mailbox is not None and before_snapshot is not None
    before_outbox = dict(repo._outbox)
    before_sessions = dict(repo._sessions)
    before_inbound_status = dict(repo._inbound_status)
    before_turn_status = dict(repo._v2_turn_status)
    before_leases = dict(repo._leases)

    original_commit = repo.mailbox.commit

    async def commit_then_fail(mailbox_lease):
        await original_commit(mailbox_lease)
        raise RuntimeError("injected mailbox commit failure")

    monkeypatch.setattr(repo.mailbox, "commit", commit_then_fail)
    with pytest.raises(RuntimeError, match="injected mailbox commit failure"):
        await repo.commit_session_ready(
            TurnCommit(
                context=accepted.context,
                lease=lease,
                state={"complete": True},
                events=(),
                outbound=_outbound(accepted),
            )
        )

    after_mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    after_snapshot = await repo.get_session_snapshot(
        accepted.context.tenant_id, accepted.context.session_id
    )
    assert after_mailbox == before_mailbox
    assert after_snapshot == before_snapshot
    assert repo._outbox == before_outbox
    assert repo._sessions == before_sessions
    assert repo._inbound_status == before_inbound_status
    assert repo._v2_turn_status == before_turn_status
    assert repo._leases == before_leases


@pytest.mark.asyncio
async def test_v2_accept_keeps_retry_wait_head_when_later_message_arrives() -> None:
    repo = repository()
    runtime = TenantRuntime(
        repo,
        routing_key=b"memory-retry-head" * 2,
        scheduler_version=SchedulerVersion.V2,
    )
    first = await runtime.accept("binding-unpredictable-a", envelope("retry-head"))
    mailbox = await repo.mailbox.get(first.context.tenant_id, first.context.session_id)
    assert mailbox is not None and repo.mailbox.outbox
    claim = await repo.claim_session_ready(
        first.context.tenant_id,
        first.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None
    await repo.retry_session_ready(
        claim.execution_lease,
        error_type="temporary",
        delay=timedelta(seconds=30),
    )
    waiting = await repo.mailbox.get(first.context.tenant_id, first.context.session_id)
    assert waiting is not None and waiting.retry_at is not None
    outbox_count = len(repo.mailbox.outbox)
    generation = waiting.queue_generation
    retry_at = waiting.retry_at

    await repo.accept_inbound_v2(
        context=first.context,
        envelope=envelope("retry-head-later"),
        trace_headers={},
    )

    updated = await repo.mailbox.get(first.context.tenant_id, first.context.session_id)
    assert updated is not None
    assert updated.status.value == "RETRY_WAIT"
    assert updated.retry_at == retry_at
    assert updated.queue_generation == generation
    assert len(repo.mailbox.outbox) == outbox_count


@pytest.mark.asyncio
async def test_v2_accept_reemits_ready_when_retry_wait_is_already_due() -> None:
    repo, accepted, lease = await _claimed_v2("retry-due")
    await repo.retry_session_ready(
        lease,
        error_type="temporary",
        delay=timedelta(0),
    )
    waiting = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert waiting is not None and waiting.status.value == "RETRY_WAIT"
    outbox_count = len(repo.mailbox.outbox)
    generation = waiting.queue_generation

    await repo.accept_inbound_v2(
        context=accepted.context,
        envelope=envelope("retry-due-later"),
        trace_headers={},
    )

    queued = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert queued is not None and queued.status.value == "QUEUED"
    assert queued.queue_generation == generation + 1
    assert len(repo.mailbox.outbox) == outbox_count + 1

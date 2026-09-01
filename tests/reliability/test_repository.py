"""Small, deterministic reliability contract tests on the SQLite dev backend."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import func, select, update

from trpc_service.reliability import (
    AppendDisposition,
    AuditData,
    EventData,
    FinalizeDisposition,
    IdempotencyConflictError,
    InboxDisposition,
    InboxEnvelope,
    OutboxDeliveryOutcome,
    ReliabilityRepository,
    ReplyCredentialData,
    ReplyPart,
    StaleClaimError,
    StaleVersionError,
    ToolEffectRequest,
    ToolReservationDisposition,
    canonical_json_hash,
)
from trpc_service.storage.database import Database
from trpc_service.storage.models import (
    AgentApp,
    AgentRun,
    AuditLog,
    ChannelBinding,
    ChannelReplyCredential,
    InboxMessage,
    ReplyOutbox,
    Session,
    SessionEvent,
    Tenant,
    TenantConfigRevision,
    ToolEffect,
)


@dataclass(slots=True)
class Harness:
    database: Database
    repository: ReliabilityRepository


@pytest_asyncio.fixture
async def harness(tmp_path: Path) -> AsyncIterator[Harness]:
    database_path = (tmp_path / "reliability.db").as_posix()
    database = Database(f"sqlite+aiosqlite:///{database_path}")
    await database.create_schema()
    async with database.session_factory.begin() as session:
        session.add(
            Tenant(
                tenant_id="tenant-a",
                display_name="Tenant A",
                status="active",
                active_config_revision=1,
                audit_policy={},
                budget_policy={},
            )
        )
        await session.flush()
        session.add(
            TenantConfigRevision(
                tenant_id="tenant-a",
                revision=1,
                schema_version=1,
                status="published",
                spec={"tenant_id": "tenant-a", "revision": 1},
                content_hash="a" * 64,
                created_by="test",
            )
        )
        session.add(
            AgentApp(
                tenant_id="tenant-a",
                app_id="app-a",
                revision=1,
                status="published",
                agent_name="support-agent",
                prompt="You are a test agent.",
                model_config={},
                tool_policy={},
                storage_config={},
            )
        )
        await session.flush()
        session.add(
            ChannelBinding(
                binding_id="binding-a",
                tenant_id="tenant-a",
                app_id="app-a",
                app_revision=1,
                config_revision=1,
                channel_type="telegram",
                external_account_id="bot-1",
                callback_path="/callbacks/binding-a",
                public_callback_id="public-binding-a",
                route_rule={},
                secret_refs={},
                identity_policy={},
                status="active",
            )
        )
        await session.flush()

    result = Harness(
        database=database,
        repository=ReliabilityRepository(database.session_factory),
    )
    try:
        yield result
    finally:
        await database.dispose()


def make_envelope(
    delivery_id: str = "delivery-1",
    *,
    payload: dict[str, object] | None = None,
) -> InboxEnvelope:
    normalized_payload = payload or {"text": "hello", "message_id": delivery_id}
    return InboxEnvelope(
        tenant_id="tenant-a",
        binding_id="binding-a",
        session_id="session-a",
        app_id="app-a",
        app_revision=1,
        config_revision=1,
        scope="private",
        principal_id="principal-a",
        external_delivery_id=delivery_id,
        payload=normalized_payload,
        payload_hash=canonical_json_hash(normalized_payload),
        request_id=f"request-{delivery_id}",
        trace_id=f"trace-{delivery_id}",
    )


def make_audit() -> AuditData:
    return AuditData(
        channel="telegram",
        user_id="principal-a",
        agent_name="support-agent",
        decision="allow",
        action="agent.run.finalize",
        resource="session/session-a",
        config_revision=1,
        policy_revision=1,
        detail={"result": "success"},
    )


def make_reply_credential(suffix: str = "one") -> ReplyCredentialData:
    encrypted_blob = f"kms-envelope:v1:{suffix}"
    return ReplyCredentialData(
        credential_kind="wecom_response_url",
        ciphertext=encrypted_blob,
        ciphertext_hash=canonical_json_hash({"encrypted_blob": encrypted_blob}),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )


@pytest.mark.asyncio
async def test_accept_duplicate_is_one_inbox_without_sequence_gap(
    harness: Harness,
) -> None:
    envelope = make_envelope()
    first, second = await asyncio.gather(
        harness.repository.accept_inbox(envelope),
        harness.repository.accept_inbox(envelope),
    )

    assert {first.disposition, second.disposition} == {
        InboxDisposition.ACCEPTED,
        InboxDisposition.DUPLICATE,
    }
    assert first.inbox_id == second.inbox_id
    assert first.accepted_seq == second.accepted_seq == 1

    async with harness.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(InboxMessage)) == 1
        stored_session = await session.get(Session, ("tenant-a", "session-a"))
        assert stored_session is not None
        assert stored_session.next_inbox_seq == 2


@pytest.mark.asyncio
async def test_reply_credential_is_atomic_encrypted_and_idempotent(
    harness: Harness,
) -> None:
    credential = make_reply_credential()
    envelope = replace(make_envelope(), reply_credential=credential)
    accepted = await harness.repository.accept_inbox(envelope)
    duplicate = await harness.repository.accept_inbox(envelope)
    assert accepted.credential_id is not None
    assert duplicate.credential_id == accepted.credential_id

    async with harness.database.session_factory() as session:
        stored = await session.get(ChannelReplyCredential, accepted.credential_id)
        assert stored is not None
        assert stored.ciphertext == credential.ciphertext
        assert stored.ciphertext_hash == credential.ciphertext_hash
        assert stored.status == "active"
        assert await session.scalar(select(func.count()).select_from(ChannelReplyCredential)) == 1

    changed_hash = replace(credential, ciphertext_hash="0" * 64)
    with pytest.raises(IdempotencyConflictError, match="credential differs"):
        await harness.repository.accept_inbox(replace(envelope, reply_credential=changed_hash))

    # AES-GCM uses a random nonce: a repeated callback legitimately creates a
    # different envelope. The stable keyed route fingerprint elects and reuses
    # the original T0 ciphertext instead of treating this as a conflict.
    reencrypted = replace(credential, ciphertext="v1.different-randomized-envelope")
    randomized_duplicate = await harness.repository.accept_inbox(
        replace(envelope, reply_credential=reencrypted)
    )
    assert randomized_duplicate.credential_id == accepted.credential_id
    async with harness.database.session_factory() as session:
        persisted = await session.get(ChannelReplyCredential, accepted.credential_id)
        assert persisted is not None
        assert persisted.ciphertext == credential.ciphertext


@pytest.mark.asyncio
async def test_duplicate_delivery_with_different_hash_conflicts(
    harness: Harness,
) -> None:
    envelope = make_envelope()
    await harness.repository.accept_inbox(envelope)

    conflicting_payload = {"text": "tampered", "message_id": "delivery-1"}
    conflicting = replace(
        envelope,
        payload=conflicting_payload,
        payload_hash=canonical_json_hash(conflicting_payload),
    )
    with pytest.raises(IdempotencyConflictError):
        await harness.repository.accept_inbox(conflicting)


@pytest.mark.asyncio
async def test_claim_preserves_per_session_head_of_line_order(
    harness: Harness,
) -> None:
    first = await harness.repository.accept_inbox(make_envelope("delivery-1"))
    second = await harness.repository.accept_inbox(make_envelope("delivery-2"))
    assert (first.accepted_seq, second.accepted_seq) == (1, 2)

    first_claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert first_claim is not None and first_claim.inbox_id == first.inbox_id
    assert await harness.repository.claim_next("tenant-a", "worker-b") is None

    await harness.repository.finalize_run(
        first_claim,
        final_state={"turn": 1},
        final_event_id=None,
        reply_parts=[ReplyPart("reply-first", 0, {"text": "done"})],
        audit=make_audit(),
    )
    second_claim = await harness.repository.claim_next("tenant-a", "worker-b")
    assert second_claim is not None and second_claim.inbox_id == second.inbox_id


@pytest.mark.asyncio
async def test_renew_and_old_fence_are_conditional(harness: Harness) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    assert await harness.repository.renew_lease(claim)

    stale = replace(claim, fencing_token=claim.fencing_token - 1)
    assert not await harness.repository.renew_lease(stale)


@pytest.mark.asyncio
async def test_retry_transition_aborts_attempt_and_releases_lease(harness: Harness) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    await harness.repository.append_event_cas(
        claim,
        claim.expected_version,
        EventData(
            event_id="event-retry",
            event_key="event-retry",
            event_type="assistant",
            payload={"text": "must not become visible"},
        ),
    )
    retry_at = datetime.now(UTC) + timedelta(minutes=5)

    await harness.repository.defer_run_retry(
        claim,
        next_attempt_at=retry_at,
        error_type="TimeoutError",
    )

    assert await harness.repository.claim_next("tenant-a", "worker-b") is None
    async with harness.database.session_factory() as session:
        stored_session = await session.get(Session, ("tenant-a", "session-a"))
        stored_inbox = await session.get(InboxMessage, claim.inbox_id)
        stored_run = await session.get(AgentRun, claim.run_id)
        event = await session.scalar(
            select(SessionEvent).where(SessionEvent.event_id == "event-retry")
        )
        assert stored_session is not None
        assert stored_session.lease_owner is None
        assert stored_session.lease_expires_at is None
        assert stored_inbox is not None
        assert stored_inbox.status == "retry_wait"
        assert stored_inbox.claim_fencing_token is None
        assert stored_run is not None
        assert stored_run.status == "retry_wait"
        assert stored_run.claim_fencing_token is None
        assert event is not None and event.visibility == "aborted"

    with pytest.raises(StaleClaimError):
        await harness.repository.defer_run_retry(
            claim,
            next_attempt_at=retry_at + timedelta(minutes=1),
            error_type="TimeoutError",
        )


@pytest.mark.asyncio
async def test_worker_loads_a_detached_claim_input(harness: Harness) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None

    claim_input = await harness.repository.load_claim_input(claim)
    assert claim_input.tenant_id == "tenant-a"
    assert claim_input.binding_id == "binding-a"
    assert claim_input.app_id == "app-a"
    assert claim_input.app_revision == 1
    assert claim_input.payload == {"text": "hello", "message_id": "delivery-1"}
    claim_input.payload["text"] = "local mutation"
    reloaded = await harness.repository.load_claim_input(claim)
    assert reloaded.payload["text"] == "hello"


@pytest.mark.asyncio
async def test_committed_session_view_excludes_current_staged_events(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope("delivery-1"))
    first_claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert first_claim is not None
    await harness.repository.append_event_cas(
        first_claim,
        0,
        EventData(
            event_id="event-committed",
            event_key="attempt-1-committed",
            event_type="assistant",
            payload={"text": "visible"},
            content_ref="object://visible",
            state_delta={"turn": 1},
        ),
    )
    await harness.repository.finalize_run(
        first_claim,
        final_state={"turn": 1},
        final_event_id="event-committed",
        reply_parts=[ReplyPart("reply-committed", 0, {"text": "visible"})],
        audit=make_audit(),
    )

    await harness.repository.accept_inbox(make_envelope("delivery-2"))
    second_claim = await harness.repository.claim_next("tenant-a", "worker-b")
    assert second_claim is not None
    await harness.repository.append_event_cas(
        second_claim,
        1,
        EventData(
            event_id="event-staged",
            event_key="attempt-2-staged",
            event_type="assistant",
            payload={"text": "must stay hidden"},
            state_delta={"turn": 2},
        ),
    )

    view = await harness.repository.load_committed_session(second_claim)
    assert view.state == {"turn": 1}
    assert view.state_version == 1
    assert view.log_version == 2
    assert [event.seq for event in view.events] == [1]
    assert view.events[0].event_id == "event-committed"
    assert view.events[0].content_ref == "object://visible"
    assert view.events[0].state_delta == {"turn": 1}


@pytest.mark.asyncio
async def test_event_retry_does_not_advance_version_twice(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    event = EventData(
        event_id="event-1",
        event_key="run-output-1",
        event_type="assistant",
        payload={"text": "answer"},
        role="assistant",
    )

    first = await harness.repository.append_event_cas(claim, 0, event)
    second = await harness.repository.append_event_cas(claim, 0, event)
    assert first.disposition is AppendDisposition.APPENDED
    assert second.disposition is AppendDisposition.ALREADY_APPENDED
    assert first.seq == second.seq == 1

    async with harness.database.session_factory() as session:
        stored_session = await session.get(Session, ("tenant-a", "session-a"))
        assert stored_session is not None
        assert stored_session.log_version == 1
        assert await session.scalar(select(func.count()).select_from(SessionEvent)) == 1


@pytest.mark.asyncio
async def test_concurrent_duplicate_event_is_one_idempotent_append(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    event = EventData(
        event_id="event-concurrent",
        event_key="event-concurrent",
        event_type="assistant",
        payload={"text": "same output"},
    )

    outcomes = await asyncio.gather(
        harness.repository.append_event_cas(claim, 0, event),
        harness.repository.append_event_cas(claim, 0, event),
    )
    assert {outcome.disposition for outcome in outcomes} == {
        AppendDisposition.APPENDED,
        AppendDisposition.ALREADY_APPENDED,
    }
    assert {outcome.seq for outcome in outcomes} == {1}


@pytest.mark.asyncio
async def test_occ_allows_only_one_append_for_one_version(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None

    outcomes = await asyncio.gather(
        harness.repository.append_event_cas(
            claim,
            0,
            EventData(
                event_id="event-a",
                event_key="event-a",
                event_type="assistant",
                payload={"winner": "a"},
            ),
        ),
        harness.repository.append_event_cas(
            claim,
            0,
            EventData(
                event_id="event-b",
                event_key="event-b",
                event_type="assistant",
                payload={"winner": "b"},
            ),
        ),
        return_exceptions=True,
    )
    assert sum(not isinstance(outcome, BaseException) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, StaleVersionError) for outcome in outcomes) == 1


@pytest.mark.asyncio
async def test_takeover_reuses_run_and_aborts_old_staged_events(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    first_claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert first_claim is not None
    first_event = await harness.repository.append_event_cas(
        first_claim,
        0,
        EventData(
            event_id="event-old-attempt",
            event_key="old-attempt",
            event_type="tool",
            payload={"phase": "started"},
        ),
    )
    assert first_event.seq == 1

    async with harness.database.session_factory.begin() as session:
        await session.execute(
            update(Session)
            .where(
                Session.tenant_id == first_claim.tenant_id,
                Session.session_id == first_claim.session_id,
            )
            .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    second_claim = await harness.repository.claim_next("tenant-a", "worker-b")
    assert second_claim is not None
    assert second_claim.run_id == first_claim.run_id
    assert second_claim.attempt_no == first_claim.attempt_no + 1
    assert second_claim.fencing_token > first_claim.fencing_token
    assert second_claim.expected_version == 1

    async with harness.database.session_factory() as session:
        old_event = await session.scalar(
            select(SessionEvent).where(SessionEvent.event_id == "event-old-attempt")
        )
        assert old_event is not None
        assert old_event.visibility == "aborted"
        assert await session.scalar(select(func.count()).select_from(AgentRun)) == 1

    with pytest.raises(StaleClaimError):
        await harness.repository.append_event_cas(
            first_claim,
            1,
            EventData(
                event_id="stale-event",
                event_key="stale-event",
                event_type="assistant",
                payload={"must": "fail"},
            ),
        )


@pytest.mark.asyncio
async def test_abort_current_staged_events_requires_live_claim(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    await harness.repository.append_event_cas(
        claim,
        0,
        EventData(
            event_id="event-abort",
            event_key="event-abort",
            event_type="assistant",
            payload={"text": "discard"},
        ),
    )

    assert await harness.repository.abort_staged_events(claim) == 1
    assert await harness.repository.abort_staged_events(claim) == 0


@pytest.mark.asyncio
async def test_finalize_atomically_commits_state_outbox_and_audit(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    appended = await harness.repository.append_event_cas(
        claim,
        0,
        EventData(
            event_id="event-final",
            event_key="event-final",
            event_type="assistant",
            payload={"text": "final answer"},
        ),
    )
    assert appended.seq == 1
    parts = [
        ReplyPart(
            reply_id="reply-z",
            part_no=0,
            payload={"text": "final answer"},
        ),
        ReplyPart(
            reply_id="reply-a",
            part_no=0,
            payload={"card": "summary"},
        ),
    ]

    finalized = await harness.repository.finalize_run(
        claim,
        final_state={"topic": "finished"},
        final_event_id="event-final",
        reply_parts=parts,
        audit=make_audit(),
    )
    assert finalized.disposition is FinalizeDisposition.FINALIZED
    assert finalized.last_seq == 1

    retried = await harness.repository.finalize_run(
        claim,
        final_state={"topic": "finished"},
        final_event_id="event-final",
        reply_parts=parts,
        audit=make_audit(),
    )
    assert retried.disposition is FinalizeDisposition.ALREADY_FINALIZED
    assert retried.outbox_ids == finalized.outbox_ids

    async with harness.database.session_factory() as session:
        stored_session = await session.get(Session, ("tenant-a", "session-a"))
        assert stored_session is not None
        assert stored_session.state == {"topic": "finished"}
        assert stored_session.state_version == stored_session.log_version == 1
        assert stored_session.lease_owner is None
        run = await session.get(AgentRun, claim.run_id)
        inbox = await session.get(InboxMessage, claim.inbox_id)
        assert run is not None and run.status == "succeeded"
        assert inbox is not None and inbox.status == "succeeded"
        event = await session.scalar(
            select(SessionEvent).where(SessionEvent.event_id == "event-final")
        )
        assert event is not None and event.visibility == "committed"
        assert await session.scalar(select(func.count()).select_from(ReplyOutbox)) == 2
        assert await session.scalar(select(func.count()).select_from(AuditLog)) == 1


@pytest.mark.asyncio
async def test_finalize_retry_rejects_changed_state_or_audit(harness: Harness) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    parts = [ReplyPart("reply-stable", 0, {"text": "stable"})]
    await harness.repository.finalize_run(
        claim,
        final_state={"status": "original"},
        final_event_id=None,
        reply_parts=parts,
        audit=make_audit(),
    )

    with pytest.raises(IdempotencyConflictError):
        await harness.repository.finalize_run(
            claim,
            final_state={"status": "changed"},
            final_event_id=None,
            reply_parts=parts,
            audit=make_audit(),
        )
    with pytest.raises(IdempotencyConflictError):
        await harness.repository.finalize_run(
            claim,
            final_state={"status": "original"},
            final_event_id=None,
            reply_parts=parts,
            audit=replace(make_audit(), decision="deny"),
        )


@pytest.mark.asyncio
async def test_outbox_orders_parts_reuses_token_and_fences_attempts(
    harness: Harness,
) -> None:
    envelope = replace(
        make_envelope(),
        reply_credential=make_reply_credential("ordered"),
    )
    accepted = await harness.repository.accept_inbox(envelope)
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    await harness.repository.finalize_run(
        claim,
        final_state={"done": True},
        final_event_id=None,
        reply_parts=[
            ReplyPart("reply-ordered", 1, {"text": "second"}),
            ReplyPart("reply-ordered", 0, {"text": "first"}),
        ],
        audit=make_audit(),
    )

    first = await harness.repository.claim_outbox("tenant-a", "dispatcher-a")
    assert first is not None and first.part_no == 0
    assert first.reply_credential is not None
    assert first.reply_credential.credential_id == accepted.credential_id
    assert first.reply_credential.ciphertext == make_reply_credential("ordered").ciphertext
    assert first.reply_credential.ciphertext not in repr(first)
    assert first.delivery_id == "delivery-1"
    assert await harness.repository.renew_outbox_claim(first)
    assert await harness.repository.claim_outbox("tenant-a", "dispatcher-b") is None

    retry_at = datetime.now(UTC) - timedelta(seconds=1)
    assert await harness.repository.record_delivery(
        first,
        OutboxDeliveryOutcome.RETRY_WAIT,
        error_type="known_not_delivered",
        next_retry_at=retry_at,
    )
    retried = await harness.repository.claim_outbox("tenant-a", "dispatcher-b")
    assert retried is not None
    assert retried.outbox_id == first.outbox_id
    assert retried.delivery_token == first.delivery_token
    assert retried.attempt_no == first.attempt_no + 1
    assert not await harness.repository.record_delivery(
        first,
        OutboxDeliveryOutcome.SENT,
    )
    assert await harness.repository.record_delivery(
        retried,
        OutboxDeliveryOutcome.SENT,
        external_message_id="external-first",
    )

    second = await harness.repository.claim_outbox("tenant-a", "dispatcher-b")
    assert second is not None and second.part_no == 1
    assert second.reply_credential is not None
    assert await harness.repository.record_delivery(
        second,
        OutboxDeliveryOutcome.UNKNOWN,
        error_type="read_timeout_after_send",
    )
    assert await harness.repository.claim_outbox("tenant-a", "dispatcher-a") is None

    async with harness.database.session_factory() as session:
        rows = list(
            (await session.scalars(select(ReplyOutbox).order_by(ReplyOutbox.part_no))).all()
        )
        assert [row.status for row in rows] == ["sent", "unknown"]
        stored_credential = await session.get(
            ChannelReplyCredential,
            accepted.credential_id,
        )
        assert stored_credential is not None
        assert stored_credential.status == "unknown"


@pytest.mark.asyncio
async def test_expired_outbox_send_is_quarantined_unknown(harness: Harness) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    await harness.repository.finalize_run(
        claim,
        final_state={},
        final_event_id=None,
        reply_parts=[ReplyPart("reply-expired", 0, {"text": "once"})],
        audit=make_audit(),
    )
    delivery = await harness.repository.claim_outbox("tenant-a", "dispatcher-a")
    assert delivery is not None
    async with harness.database.session_factory.begin() as session:
        await session.execute(
            update(ReplyOutbox)
            .where(ReplyOutbox.outbox_id == delivery.outbox_id)
            .values(claim_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    assert await harness.repository.claim_outbox("tenant-a", "dispatcher-b") is None
    assert not await harness.repository.renew_outbox_claim(delivery)
    async with harness.database.session_factory() as session:
        stored = await session.get(ReplyOutbox, delivery.outbox_id)
        assert stored is not None
        assert stored.status == "unknown"
        assert stored.last_error_type == "delivery_claim_expired"


@pytest.mark.asyncio
async def test_tool_effect_unknown_is_never_automatically_reexecuted(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    request = ToolEffectRequest(
        idempotency_key="effect-key-1",
        tool_name="create_ticket",
        tool_version="1",
        effect_class="non_idempotent_write",
        args_hash=canonical_json_hash({"title": "help"}),
    )

    reserved = await harness.repository.reserve_tool_effect(claim, request)
    assert reserved.disposition is ToolReservationDisposition.EXECUTE
    assert await harness.repository.complete_tool_effect(
        tenant_id=claim.tenant_id,
        effect_id=reserved.effect_id,
        execution_token=reserved.execution_token,
        outcome="unknown",
        error_type="read_timeout",
    )

    repeated = await harness.repository.reserve_tool_effect(claim, request)
    assert repeated.disposition is ToolReservationDisposition.UNKNOWN
    assert repeated.execution_token == reserved.execution_token
    assert repeated.attempt_count == 1

    async with harness.database.session_factory() as session:
        effect = await session.get(ToolEffect, reserved.effect_id)
        assert effect is not None
        assert effect.status == "unknown"
        assert effect.attempt_count == 1


@pytest.mark.asyncio
async def test_expired_non_idempotent_execution_becomes_unknown(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    request = ToolEffectRequest(
        idempotency_key="effect-key-expired",
        tool_name="send_irreversible",
        tool_version="1",
        effect_class="non_idempotent_write",
        args_hash=canonical_json_hash({"value": 1}),
    )
    first = await harness.repository.reserve_tool_effect(claim, request)
    assert first.disposition is ToolReservationDisposition.EXECUTE

    async with harness.database.session_factory.begin() as session:
        await session.execute(
            update(ToolEffect)
            .where(ToolEffect.effect_id == first.effect_id)
            .values(execution_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    recovered = await harness.repository.reserve_tool_effect(claim, request)
    assert recovered.disposition is ToolReservationDisposition.UNKNOWN
    assert recovered.attempt_count == 1


@pytest.mark.asyncio
async def test_safe_tool_reclaim_rotates_token_and_fences_old_completion(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    request = ToolEffectRequest(
        idempotency_key="effect-key-safe-retry",
        tool_name="upsert_ticket",
        tool_version="1",
        effect_class="idempotent_write",
        args_hash=canonical_json_hash({"ticket": "42"}),
    )
    first = await harness.repository.reserve_tool_effect(claim, request)
    assert first.disposition is ToolReservationDisposition.EXECUTE

    async with harness.database.session_factory.begin() as session:
        await session.execute(
            update(ToolEffect)
            .where(ToolEffect.effect_id == first.effect_id)
            .values(execution_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    second = await harness.repository.reserve_tool_effect(claim, request)
    assert second.disposition is ToolReservationDisposition.EXECUTE
    assert second.execution_token != first.execution_token
    assert second.attempt_count == 2
    assert not await harness.repository.complete_tool_effect(
        tenant_id=claim.tenant_id,
        effect_id=first.effect_id,
        execution_token=first.execution_token,
        outcome="succeeded",
    )
    assert await harness.repository.complete_tool_effect(
        tenant_id=claim.tenant_id,
        effect_id=second.effect_id,
        execution_token=second.execution_token,
        outcome="succeeded",
        result_hash=canonical_json_hash({"ticket_id": "42"}),
    )
    completed = await harness.repository.reserve_tool_effect(claim, request)
    assert completed.disposition is ToolReservationDisposition.SUCCEEDED


@pytest.mark.asyncio
async def test_tool_idempotency_key_cannot_change_arguments(
    harness: Harness,
) -> None:
    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    request = ToolEffectRequest(
        idempotency_key="effect-key-conflict",
        tool_name="create_ticket",
        tool_version="1",
        effect_class="idempotent_write",
        args_hash=canonical_json_hash({"title": "first"}),
    )
    await harness.repository.reserve_tool_effect(claim, request)

    with pytest.raises(IdempotencyConflictError):
        await harness.repository.reserve_tool_effect(
            claim,
            replace(
                request,
                args_hash=canonical_json_hash({"title": "different"}),
            ),
        )


@pytest.mark.asyncio
async def test_invalid_contract_inputs_fail_before_state_mutation(
    harness: Harness,
) -> None:
    assert harness.repository.dialect_name == "sqlite"
    with pytest.raises(ValueError, match="payload_hash"):
        await harness.repository.accept_inbox(replace(make_envelope(), payload_hash=""))
    with pytest.raises(ValueError, match="tenant_id"):
        await harness.repository.claim_next("", "worker-a")
    with pytest.raises(ValueError, match="lease_ttl"):
        await harness.repository.claim_next(
            "tenant-a",
            "worker-a",
            lease_ttl=timedelta(0),
        )

    await harness.repository.accept_inbox(make_envelope())
    claim = await harness.repository.claim_next("tenant-a", "worker-a")
    assert claim is not None
    with pytest.raises(ValueError, match="lease_ttl"):
        await harness.repository.renew_lease(claim, lease_ttl=timedelta(0))
    with pytest.raises(ValueError, match="visibility"):
        await harness.repository.append_event_cas(
            claim,
            0,
            EventData(
                event_id="invalid-visibility",
                event_key="invalid-visibility",
                event_type="assistant",
                payload={},
                visibility="visible-ish",
            ),
        )
    with pytest.raises(ValueError, match="at least one"):
        await harness.repository.finalize_run(
            claim,
            final_state={},
            final_event_id=None,
            reply_parts=[],
            audit=make_audit(),
        )
    duplicate_parts = [
        ReplyPart("duplicate", 0, {}),
        ReplyPart("duplicate", 0, {}),
    ]
    with pytest.raises(ValueError, match="duplicate"):
        await harness.repository.finalize_run(
            claim,
            final_state={},
            final_event_id=None,
            reply_parts=duplicate_parts,
            audit=make_audit(),
        )
    with pytest.raises(ValueError, match="non-negative"):
        await harness.repository.finalize_run(
            claim,
            final_state={},
            final_event_id=None,
            reply_parts=[ReplyPart("negative", -1, {})],
            audit=make_audit(),
        )
    with pytest.raises(ValueError, match="encrypted routing credentials"):
        await harness.repository.finalize_run(
            claim,
            final_state={},
            final_event_id=None,
            reply_parts=[
                ReplyPart(
                    "unsafe-route",
                    0,
                    {"response_url": "must-not-enter-outbox"},
                )
            ],
            audit=make_audit(),
        )

    invalid_tool = ToolEffectRequest(
        idempotency_key="invalid-tool",
        tool_name="tool",
        tool_version="1",
        effect_class="unsafe-mystery",
        args_hash=canonical_json_hash({}),
    )
    with pytest.raises(ValueError, match="effect class"):
        await harness.repository.reserve_tool_effect(claim, invalid_tool)
    valid_tool = replace(invalid_tool, effect_class="read")
    with pytest.raises(ValueError, match="execution_ttl"):
        await harness.repository.reserve_tool_effect(
            claim,
            valid_tool,
            execution_ttl=timedelta(0),
        )
    capability = canonical_json_hash({"test": "invalid-completion"})
    with pytest.raises(ValueError, match="completion"):
        await harness.repository.complete_tool_effect(
            tenant_id=claim.tenant_id,
            effect_id="effect",
            execution_token=capability,
            outcome="maybe",
        )
    with pytest.raises(ValueError, match="next_attempt_at"):
        await harness.repository.complete_tool_effect(
            tenant_id=claim.tenant_id,
            effect_id="effect",
            execution_token=capability,
            outcome="retry_wait",
        )

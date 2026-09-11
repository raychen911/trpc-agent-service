"""Atomic acceptance, membership refusal, lease fencing and private reply-context storage."""

from datetime import timedelta

from cryptography.fernet import Fernet
import pytest
from sqlalchemy import func, select

from trpc_service.channels.persistence import ContextCipher, IMAccounts, IMIngress, AccountOwnershipError
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.persistence import Database
from trpc_service.persistence.models import IMMembershipRow, IMEventReceiptRow, InboundMessageRow, IMAccountRow, utcnow
from trpc_service.persistence.models import TenantRow, ChannelBindingRow
from trpc_service.reliability.inbox import DuplicatePayloadError, MessageRecalledError
from trpc_service.channels.events import TransportEvent
from trpc_service.tenant import ChannelBindingRegistry, MessageRouter, SessionIdentityFactory
from telegram import Bot


@pytest.fixture
def system(tmp_path, tenant):
    from tests.integration.test_im_transports import telegram_binding
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'im.db'}")
    database.create_schema()
    binding = telegram_binding(tenant)
    register_test_binding(database, tenant, binding)
    tenant = tenant.model_copy(update={"channel_bindings": (binding, )})
    identities = SessionIdentityFactory(lambda _: b"k" * 32)
    router = MessageRouter(ChannelBindingRegistry([tenant]), identities)
    cipher = ContextCipher([Fernet.generate_key()])
    accounts = IMAccounts(database)
    lease = accounts.acquire(binding, "owner-a")
    ingress = IMIngress(database, cipher, identities)
    yield database, binding, router, cipher, accounts, lease, ingress
    database.close()


def make_event(binding, event_id=50, **message):
    from tests.integration.test_im_transports import update
    return TelegramAdapter(Bot("123456:synthetic-test-token")).normalize(update(event_id, **message), binding)


def grant(database, route):
    with database.sessions.begin() as session:
        session.add(
            IMMembershipRow(tenant_id=route.tenant_id,
                            binding_id=route.channel_binding_id,
                            actor_id=route.actor_id,
                            active=True,
                            grants_json=["chat"]))


def register_test_binding(database, tenant, binding):
    with database.sessions.begin() as session:
        session.add(
            TenantRow(tenant_id=tenant.tenant_id,
                      name=tenant.name,
                      status="active",
                      active_config_version=tenant.config_version))
        session.add(
            ChannelBindingRow(tenant_id=tenant.tenant_id,
                              binding_id=binding.binding_id,
                              channel=binding.channel.value,
                              webhook_public_id=binding.webhook_public_id,
                              external_account_id=binding.external_account_id,
                              agent_app_id=binding.agent_app_id,
                              credential_ref="[]",
                              enabled=True))


def test_expired_release_cleans_only_its_own_generation(system):
    db, binding, _, _, accounts, lease, _ = system
    key = (lease.channel, lease.external_account_id)
    with db.sessions.begin() as session:
        session.get(IMAccountRow, key).lease_expires_at = utcnow() - timedelta(seconds=1)
    accounts.release(lease)
    with db.sessions() as session:
        row = session.get(IMAccountRow, key)
        assert row.owner_id is None and row.lease_expires_at is None
    replacement = accounts.acquire(binding, "new-owner")
    assert replacement.generation > lease.generation
    with pytest.raises(AccountOwnershipError):
        accounts.release(lease)
    with db.sessions() as session:
        assert session.get(IMAccountRow, key).owner_id == "new-owner"
    accounts.release(replacement)


def test_member_duplicate_offset_context_and_conflicting_content(system):
    db, binding, router, cipher, accounts, lease, ingress = system
    event = make_event(binding)
    route = router.route_message(event.message)
    grant(db, route)
    first = ingress.record(event, binding, lease, route=route, next_offset=51)
    second = ingress.record(make_event(binding), binding, lease, route=route, next_offset=51)
    assert first.disposition == "accepted" and second.duplicate and first.inbound_id == second.inbound_id
    assert accounts.offset(lease) == 51
    with db.sessions() as session:
        assert session.scalar(select(func.count()).select_from(InboundMessageRow)) == 1
        stored = session.get(IMEventReceiptRow, (binding.tenant_id, binding.binding_id, "50"))
        assert "chat_id" not in stored.context_ciphertext
        key = (binding.tenant_id, binding.binding_id, "50")
        assert cipher.open(key, stored.context_ciphertext)["reply"]["chat_id"] == "42"
        with pytest.raises(ValueError):
            cipher.open(("other", key[1], key[2]), stored.context_ciphertext)
    with pytest.raises(DuplicatePayloadError):
        ingress.record(make_event(binding, text="changed"), binding, lease, route=route, next_offset=51)


def test_unauthorised_and_ignored_updates_are_durable_without_agent_input(system):
    db, binding, router, _, accounts, lease, ingress = system
    event = make_event(binding)
    receipt = ingress.record(event, binding, lease, route=router.route_message(event.message), next_offset=51)
    assert receipt.disposition == "rejected" and receipt.inbound_id is None
    assert ingress.record(make_event(binding, 51, text=None), binding, lease, next_offset=52).disposition == "ignored"
    assert accounts.offset(lease) == 52
    with db.sessions() as session:
        assert session.scalar(select(func.count()).select_from(InboundMessageRow)) == 0
        assert all(row.context_ciphertext is None for row in session.scalars(select(IMEventReceiptRow)))


def test_recall_cancels_pending_input_and_unsent_output(system):
    from trpc_service.persistence.models import OutboxMessageRow
    from trpc_service.reliability import OutboxRepository

    db, binding, router, _, _, lease, ingress = system
    event = make_event(binding)
    route = router.route_message(event.message)
    grant(db, route)
    accepted = ingress.record(event, binding, lease, route=route)
    outbox_id = OutboxRepository(db).enqueue(binding.tenant_id, accepted.inbound_id, 0, {
        "final": True, "text": "must not be sent"})
    recall = TransportEvent(event_id="recall-50", kind="recall", recalled_message_id="50",
                            external_chat_id=event.external_chat_id)
    receipt = ingress.record(recall, binding, lease)
    duplicate = ingress.record(recall, binding, lease)
    assert receipt.disposition == "recalled" and duplicate.duplicate
    with db.sessions() as session:
        inbound = session.get(InboundMessageRow, accepted.inbound_id)
        outgoing = session.get(OutboxMessageRow, outbox_id)
        assert (inbound.status, inbound.error_type) == ("failed_final", "message_recalled")
        assert (outgoing.status, outgoing.error_type) == ("dead_letter", "message_recalled")


def test_recall_fences_claimed_worker_and_cannot_cross_chat(system):
    from trpc_service.reliability import InboxRepository

    db, binding, router, _, _, lease, ingress = system
    event = make_event(binding)
    route = router.route_message(event.message)
    grant(db, route)
    accepted = ingress.record(event, binding, lease, route=route)
    inbox = InboxRepository(db)
    work = inbox.claim(worker_id="im-worker", lease_seconds=30, include_im=True)
    assert work.inbound_message_id == accepted.inbound_id

    mismatch = TransportEvent(event_id="recall-other-chat", kind="recall",
                              recalled_message_id="50", external_chat_id="other-chat")
    assert ingress.record(mismatch, binding, lease).reason == "recall_target_mismatch"
    inbox.require_active(work, "im-worker")

    recall = TransportEvent(event_id="recall-owned-chat", kind="recall",
                            recalled_message_id="50", external_chat_id=event.external_chat_id)
    assert ingress.record(recall, binding, lease).disposition == "recalled"
    with pytest.raises(MessageRecalledError):
        inbox.require_active(work, "im-worker")


def test_failure_rolls_back_inbox_receipt_and_poll_offset(system, monkeypatch):
    db, binding, router, cipher, accounts, lease, ingress = system
    event = make_event(binding)
    route = router.route_message(event.message)
    grant(db, route)
    monkeypatch.setattr(cipher, "seal", lambda *args: (_ for _ in ()).throw(RuntimeError("storage failed")))
    with pytest.raises(RuntimeError):
        ingress.record(event, binding, lease, route=route, next_offset=51)
    assert accounts.offset(lease) is None
    with db.sessions() as session:
        assert session.scalar(select(func.count()).select_from(InboundMessageRow)) == 0
        assert session.scalar(select(func.count()).select_from(IMEventReceiptRow)) == 0


def test_lease_takeover_fences_previous_connection(system):
    db, binding, router, _, accounts, lease, ingress = system
    assert accounts.acquire(binding, "owner-b") is None
    with db.sessions.begin() as session:
        session.get(IMAccountRow,
                    (lease.channel, lease.external_account_id)).lease_expires_at = utcnow() - timedelta(seconds=2)
    newer = accounts.acquire(binding, "owner-b")
    assert newer.generation == lease.generation + 1
    with pytest.raises(AccountOwnershipError):
        ingress.record(make_event(binding, text=None), binding, lease, next_offset=51)
    assert accounts.offset(newer) is None
    with pytest.raises(AccountOwnershipError):
        accounts.register(binding.model_copy(update={"tenant_id": "other_tenant"}))


@pytest.mark.asyncio
async def test_revoked_queued_input_cannot_reach_runner_or_session(system):
    from unittest.mock import Mock
    from trpc_service.persistence.models import SessionRow
    from trpc_service.reliability import (InboxRepository, OutboxRepository, PostTurnRepository, SessionCoordinator,
                                          DurableExecutionPipeline)
    db, binding, router, _, _, lease, ingress = system
    event = make_event(binding)
    route = router.route_message(event.message)
    grant(db, route)
    ingress.record(event, binding, lease, route=route)
    inbox = InboxRepository(db)
    assert inbox.claim(worker_id="simulation", lease_seconds=30) is None
    work = inbox.claim(worker_id="im-worker", lease_seconds=30, include_im=True)
    with db.sessions.begin() as session:
        member = session.get(IMMembershipRow, (binding.tenant_id, binding.binding_id, route.actor_id))
        member.active = False
        member.authz_epoch += 1
    pipeline = DurableExecutionPipeline(inbox, OutboxRepository(db), PostTurnRepository(db), SessionCoordinator(db))
    registry = Mock()
    with pytest.raises(PermissionError):
        await pipeline.execute(work, registry, worker_id="im-worker")
    registry.resolve.assert_not_called()
    with db.sessions() as session:
        assert session.scalar(select(func.count()).select_from(SessionRow)) == 0

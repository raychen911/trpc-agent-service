"""Transactional account ownership, membership and inbound IM receipts."""

from dataclasses import dataclass
from datetime import timedelta, timezone
import json

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from trpc_service.persistence.models import (
    IMAccountRow,
    IMEventReceiptRow,
    IMMembershipRow,
    InboundMessageRow,
    OutboxMessageRow,
    TenantRow,
    ChannelBindingRow,
)
from trpc_service.reliability.inbox import InboxRepository, DuplicatePayloadError, payload_hash
from trpc_service.telemetry.runtime import carrier, current_trace_id


class AccountOwnershipError(RuntimeError):
    pass


def database_now(session):
    # PostgreSQL CURRENT_TIMESTAMP is transaction-start time; leases need current wall time.
    expression = func.clock_timestamp() if session.bind.dialect.name == "postgresql" else func.current_timestamp()
    value = session.scalar(select(expression))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def as_utc(value):
    return value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value


class ContextCipher:
    """Authenticated, rotatable encryption binds reply routing to its tenant/event."""

    def __init__(self, keys: list[bytes]):
        if not keys:
            raise ValueError("at least one context encryption key is required")
        self._cipher = MultiFernet([Fernet(key) for key in keys])

    def seal(self, key: tuple[str, str, str], context: dict) -> str:
        return self._cipher.encrypt(json.dumps({
            "key": key,
            "context": context
        }, separators=(",", ":")).encode()).decode()

    def open(self, key: tuple[str, str, str], ciphertext: str) -> dict:
        try:
            value = json.loads(self._cipher.decrypt(ciphertext.encode()))
            if value["key"] != list(key):
                raise ValueError("context binding mismatch")
            return value["context"]
        except (InvalidToken, ValueError, KeyError, TypeError):
            raise ValueError("reply context cannot be authenticated") from None


@dataclass(frozen=True)
class AccountLease:
    channel: str
    external_account_id: str
    tenant_id: str
    binding_id: str
    owner_id: str
    generation: int


class IMAccounts:

    def __init__(self, database):
        self.database = database

    @staticmethod
    def register_in_session(session, binding):
        key = (binding.channel.value, binding.external_account_id)
        row = session.get(IMAccountRow, key, with_for_update=True)
        if row is None:
            try:
                with session.begin_nested():
                    row = IMAccountRow(channel=key[0],
                                       external_account_id=key[1],
                                       tenant_id=binding.tenant_id,
                                       binding_id=binding.binding_id)
                    session.add(row)
                    session.flush()
            except IntegrityError:
                row = session.get(IMAccountRow, key, with_for_update=True)
        if row is None or (row.tenant_id, row.binding_id) != (binding.tenant_id, binding.binding_id):
            raise AccountOwnershipError("IM account or binding already has a different owner")
        return row

    def register(self, binding):
        with self.database.sessions.begin() as session:
            self.register_in_session(session, binding)

    def acquire(self, binding, owner_id: str, *, seconds: float = 60) -> AccountLease | None:
        if not 5 <= seconds <= 300 or not owner_id or len(owner_id) > 128:
            raise ValueError("invalid channel lease")
        with self.database.sessions.begin() as session:
            row = self.register_in_session(session, binding)
            now = database_now(session)
            if row.lease_expires_at and as_utc(row.lease_expires_at) > now:
                return None
            row.owner_id, row.lease_expires_at = owner_id, now + timedelta(seconds=seconds)
            row.generation += 1
            return AccountLease(row.channel, row.external_account_id, row.tenant_id, row.binding_id, owner_id,
                                row.generation)

    @staticmethod
    def require(session, lease: AccountLease):
        row = session.get(IMAccountRow, (lease.channel, lease.external_account_id), with_for_update=True)
        if row is None or (row.tenant_id, row.binding_id, row.owner_id,
                           row.generation) != (lease.tenant_id, lease.binding_id, lease.owner_id, lease.generation):
            raise AccountOwnershipError("channel lease is no longer owned")
        if row.lease_expires_at is None or as_utc(row.lease_expires_at) <= database_now(session):
            raise AccountOwnershipError("channel lease has expired")
        return row

    def renew(self, lease: AccountLease, *, seconds: float = 60):
        if not 5 <= seconds <= 300:
            raise ValueError("invalid channel lease")
        with self.database.sessions.begin() as session:
            row = self.require(session, lease)
            row.lease_expires_at = database_now(session) + timedelta(seconds=seconds)

    def release(self, lease: AccountLease):
        with self.database.sessions.begin() as session:
            # Closing an owned connection may outlive its lease (e.g. host
            # suspension). Clearing that same generation is safe even after
            # expiry; a stale owner must never clear a replacement's lease.
            row = session.get(IMAccountRow, (lease.channel, lease.external_account_id), with_for_update=True)
            if row is None or (row.tenant_id, row.binding_id, row.owner_id,
                               row.generation) != (lease.tenant_id, lease.binding_id, lease.owner_id, lease.generation):
                raise AccountOwnershipError("channel lease is no longer owned")
            row.owner_id = row.lease_expires_at = None

    def offset(self, lease: AccountLease):
        with self.database.sessions.begin() as session:
            return self.require(session, lease).poll_offset


@dataclass(frozen=True)
class EventReceipt:
    disposition: str
    duplicate: bool
    inbound_id: str | None
    reason: str | None


class IMIngress:
    """Call only after transport authentication against the supplied immutable binding."""

    def __init__(self, database, cipher: ContextCipher, identities):
        self.database, self.cipher, self.identities = database, cipher, identities
        self.inbox = InboxRepository(database)

    def authorized(self, event, binding) -> bool:
        if not event.external_actor_id:
            return False
        actor_id = self.identities.internal_user_id(binding.tenant_id, binding.binding_id,
                                                    event.external_actor_id)
        with self.database.sessions() as session:
            member = session.get(IMMembershipRow, (binding.tenant_id, binding.binding_id, actor_id))
            return bool(member and member.active and "chat" in member.grants_json)

    def record(self, event, binding, lease: AccountLease, *, route=None, trace_id="", next_offset=None):
        if (binding.tenant_id, binding.binding_id, binding.channel.value,
                binding.external_account_id) != (lease.tenant_id, lease.binding_id, lease.channel,
                                                 lease.external_account_id):
            raise AccountOwnershipError("authenticated account does not match binding")
        if event.message and (event.message.channel, event.message.webhook_public_id) != (binding.channel,
                                                                                          binding.webhook_public_id):
            raise AccountOwnershipError("event does not match authenticated binding")
        if event.pending_media:
            raise ValueError("provider media must be staged before durable ingress")
        actor_id = (self.identities.internal_user_id(binding.tenant_id, binding.binding_id, event.external_actor_id)
                    if event.external_actor_id else None)
        if route and (route.tenant_id, route.channel_binding_id, route.agent_app_id,
                      route.actor_id) != (binding.tenant_id, binding.binding_id, binding.agent_app_id, actor_id):
            raise AccountOwnershipError("execution route does not match authenticated event")
        if next_offset is not None and (lease.channel != "telegram" or next_offset != int(event.event_id) + 1):
            raise ValueError("poll offset must acknowledge exactly this update")
        key = (binding.tenant_id, binding.binding_id, event.event_id)
        identity = event.model_dump(mode="json", exclude={"reply_context"})
        if event.message:
            identity["message"] = event.message.model_dump(mode="json", exclude={"request_id", "received_at"})
        digest = payload_hash(identity)
        with self.database.sessions.begin() as session:
            tenant = session.get(TenantRow, binding.tenant_id, with_for_update=True)
            active = session.get(ChannelBindingRow, (binding.tenant_id, binding.binding_id))
            if tenant is None or tenant.status != "active" or active is None or not active.enabled or (
                    active.channel, active.external_account_id,
                    active.agent_app_id) != (binding.channel.value, binding.external_account_id, binding.agent_app_id):
                raise AccountOwnershipError("IM binding is no longer active")
            account = IMAccounts.require(session, lease)
            existing = session.get(IMEventReceiptRow, key)
            if existing:
                if existing.content_hash != digest:
                    raise DuplicatePayloadError("event id was reused with different content")
                if next_offset is not None:
                    account.poll_offset = max(account.poll_offset or 0, next_offset)
                return EventReceipt(existing.disposition, True, existing.inbound_id, existing.reason)
            member = session.get(IMMembershipRow,
                                 (key[0], key[1], actor_id), with_for_update=True) if actor_id else None
            allowed = member and member.active and "chat" in member.grants_json
            disposition, reason, inbound_id = "ignored", event.reason, None
            if event.kind == "recall":
                original = session.get(IMEventReceiptRow, (key[0], key[1], event.recalled_message_id))
                if original is None or original.inbound_id is None:
                    disposition, reason = "ignored", "recall_target_not_found"
                else:
                    inbound = session.get(InboundMessageRow, original.inbound_id, with_for_update=True)
                    if inbound is not None:
                        original_chat = (inbound.payload_json.get("message") or {}).get("external_chat_id")
                        if event.external_chat_id and original_chat != event.external_chat_id:
                            disposition, reason = "ignored", "recall_target_mismatch"
                            inbound = None
                    if inbound is not None:
                        if inbound.status in {"pending", "retry", "processing"}:
                            inbound.status = "failed_final"
                            inbound.lease_owner = inbound.lease_expires_at = inbound.next_retry_at = None
                        inbound.error_type = "message_recalled"
                        for outgoing in session.scalars(select(OutboxMessageRow).where(
                                OutboxMessageRow.inbound_message_id == inbound.inbound_message_id,
                                OutboxMessageRow.status.in_(("pending", "retry"))).with_for_update()):
                            outgoing.status, outgoing.error_type = "dead_letter", "message_recalled"
                        disposition, reason = "recalled", None
                    elif reason != "recall_target_mismatch":
                        disposition, reason = "ignored", "recall_target_not_found"
            elif event.kind != "ignored":
                disposition, reason = "rejected", "membership_required"
                if allowed:
                    if event.kind == "action":
                        from trpc_service.governance.actions import ActionService, ActionConflict
                        try:
                            with session.begin_nested():
                                result = ActionService(self.database,
                                                       self.cipher).decide_event(session, event, binding, actor_id)
                            disposition, reason = "action_" + result["status"], None
                        except ActionConflict:
                            disposition, reason = "rejected", "action_conflict"
                        except (PermissionError, LookupError, ValueError):
                            disposition, reason = "rejected", "action_not_authorized"
                    else:
                        if route is None:
                            raise ValueError("chat event requires a trusted execution route")
                        message = event.message
                        payload = {
                            "message": message.model_dump(mode="json"),
                            "route": route.model_dump(mode="json"),
                            "trace_context": carrier(),
                            "authz_epoch": member.authz_epoch
                        }
                        receipt = self.inbox.accept_in_session(session,
                                                               tenant_id=key[0],
                                                               channel_binding_id=key[1],
                                                               external_message_id=key[2],
                                                               payload=payload,
                                                               identity_payload=identity["message"],
                                                               request_id=message.request_id,
                                                               trace_id=trace_id or current_trace_id(),
                                                               partition_key=route.partition_key,
                                                               config_version=route.config_version,
                                                               storage_revision=route.storage_revision)
                        disposition, reason, inbound_id = "accepted", None, receipt.inbound_message_id
            # Unauthorised message bodies never enter durable storage.
            context = ({
                "reply": event.reply_context,
                "action": event.action_data
            } if disposition in {"accepted", "pending_action"} else None)
            session.add(
                IMEventReceiptRow(tenant_id=key[0],
                                  binding_id=key[1],
                                  external_event_id=key[2],
                                  event_kind=event.kind,
                                  content_hash=digest,
                                  disposition=disposition,
                                  reason=reason,
                                  actor_id=actor_id,
                                  authz_epoch=member.authz_epoch if member else None,
                                  inbound_id=inbound_id,
                                  owner_generation=lease.generation,
                                  context_ciphertext=self.cipher.seal(key, context) if context else None))
            if next_offset is not None:
                account.poll_offset = max(account.poll_offset or 0, next_offset)
            session.flush()
            return EventReceipt(disposition, False, inbound_id, reason)

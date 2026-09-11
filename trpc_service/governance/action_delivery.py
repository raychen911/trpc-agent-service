"""Encrypted action notifications delivered by the existing fenced IM account owner."""

import asyncio
from datetime import timedelta

from sqlalchemy import select

from trpc_service.channels.events import DeliveryResult
from trpc_service.channels.persistence import IMAccounts, as_utc, database_now
from trpc_service.persistence.models import ActionNoticeRow, PendingActionRow, IMMembershipRow
from trpc_service.persistence.models import TenantRow, ChannelBindingRow
from trpc_service.telemetry.runtime import operation, count
from .model_filter import database_call


class ActionNoticeWorker:

    def __init__(self, database, lease, adapter, cipher, *, timeout=10, max_attempts=5):
        if not 0 < timeout <= 20 or not 1 <= max_attempts <= 10:
            raise ValueError("notification attempts must be bounded")
        self.database, self.lease, self.adapter, self.cipher = database, lease, adapter, cipher
        self.timeout, self.max_attempts = timeout, max_attempts

    def prepare(self):
        with self.database.sessions.begin() as session:
            tenant = session.get(TenantRow, self.lease.tenant_id, with_for_update=True)
            binding = session.get(ChannelBindingRow, (self.lease.tenant_id, self.lease.binding_id))
            if tenant is None or tenant.status != "active" or binding is None or not binding.enabled:
                return None
            IMAccounts.require(session, self.lease)
            row = session.scalar(
                select(ActionNoticeRow).where(
                    ActionNoticeRow.tenant_id == self.lease.tenant_id,
                    ActionNoticeRow.binding_id == self.lease.binding_id,
                    ActionNoticeRow.status.in_(("pending", "retry", "sending")),
                    (ActionNoticeRow.next_retry_at.is_(None)) |
                    (ActionNoticeRow.next_retry_at <= database_now(session)),
                ).order_by(ActionNoticeRow.created_at, ActionNoticeRow.notice_id).limit(1).with_for_update())
            if row is None:
                return None
            if row.status == "sending":
                row.status = "unknown_outcome"
                row.receipt_json = DeliveryResult(outcome="unknown", error_type="owner_interrupted").model_dump()
                return {"internal": True}
            action = session.get(PendingActionRow, (row.tenant_id, row.action_id))
            member = session.get(IMMembershipRow, (row.tenant_id, row.binding_id, action.requester_actor_id))
            if (member is None or not member.active or member.authz_epoch != action.requester_epoch
                    or "chat" not in member.grants_json):
                row.status = "authorization_revoked"
                return {"internal": True}
            if ((row.kind == "confirmation" and
                 (action.status != "pending" or as_utc(action.expires_at) <= database_now(session)))
                    or (row.kind == "approved" and action.status != "approved")):
                row.status = "superseded"
                return {"internal": True}
            payload = self.cipher.open((row.tenant_id, "notice", row.notice_id), row.encrypted_payload)
            # One bounded message: never split a nonce/capability across replies.
            byte_limit = 3000 if self.lease.channel == "feishu" else 20480
            if len(payload["text"]) > 4096 or len(payload["text"].encode()) > byte_limit:
                row.status = "dead_letter"
                return {"internal": True}
            row.status, row.owner_generation = "sending", self.lease.generation
            row.attempt += 1
            row.next_retry_at = None
            return {"id": row.notice_id, "attempt": row.attempt, **payload}

    def finish(self, work, result):
        with self.database.sessions.begin() as session:
            IMAccounts.require(session, self.lease)
            row = session.get(ActionNoticeRow, work["id"], with_for_update=True)
            if (row.tenant_id != self.lease.tenant_id or row.binding_id != self.lease.binding_id
                    or row.status != "sending" or row.owner_generation != self.lease.generation
                    or row.attempt != work["attempt"]):
                raise RuntimeError("notification attempt is no longer owned")
            row.receipt_json = result.model_dump(mode="json")
            if result.outcome == "accepted":
                row.status = "delivered"
            elif result.outcome == "unknown":
                row.status = "unknown_outcome"
            elif result.retryable and row.attempt < self.max_attempts:
                row.status = "retry"
                row.next_retry_at = database_now(session) + timedelta(seconds=result.retry_after or 2**row.attempt)
            else:
                row.status = "dead_letter"

    async def run_once(self):
        if self.lease.channel == "wecom" and not self.adapter.client.is_authenticated:
            return False
        work = await database_call(self.prepare)
        if not work:
            return False
        if work.get("internal"):
            return True
        with operation("im.action_reply",
                       parent=work.get("trace_context"),
                       attributes={
                           "tenant.id": self.lease.tenant_id,
                           "channel": self.lease.channel
                       }):
            try:
                kwargs = {"stream_id": work["id"], "final": True} if self.lease.channel == "wecom" else {}
                if self.lease.channel == "feishu":
                    kwargs = {"idempotency_key": work["id"]}
                result = await asyncio.wait_for(self.adapter.send_text(work["reply"], work["text"], **kwargs),
                                                self.timeout)
            except Exception:
                result = DeliveryResult(outcome="unknown", error_type="transport_outcome_unknown")
            await database_call(self.finish, work, result)
            count("im.action_delivery", channel=self.lease.channel, outcome=result.outcome)
        return True

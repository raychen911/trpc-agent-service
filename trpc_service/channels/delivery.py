"""IM delivery ledger: write intent before network IO, never retry an uncertain send."""

import asyncio
from datetime import timedelta
import uuid

from sqlalchemy import select
from sqlalchemy.orm import aliased

from trpc_service.persistence.models import IMDeliveryAttemptRow, IMEventReceiptRow, OutboxMessageRow
from trpc_service.persistence.models import InboundMessageRow
from trpc_service.persistence.models import TenantRow, ChannelBindingRow, IMMembershipRow
from trpc_service.telemetry.runtime import operation, count
from .events import DeliveryResult, split_text
from .persistence import IMAccounts, as_utc, database_now


class IMDeliveryWorker:
    """One account owner sends sequentially; each chunk has an independent durable attempt."""

    def __init__(self, database, lease, adapter, cipher, *, timeout=10, max_attempts=5):
        if not 0 < timeout <= 20:
            raise ValueError("IM delivery timeout must be within the account lease renewal interval")
        self.database, self.lease, self.adapter, self.cipher = database, lease, adapter, cipher
        self.timeout, self.max_attempts = timeout, max_attempts

    def prepare(self):
        with self.database.sessions.begin() as session:
            tenant = session.get(TenantRow, self.lease.tenant_id, with_for_update=True)
            binding = session.get(ChannelBindingRow, (self.lease.tenant_id, self.lease.binding_id))
            if tenant is None or tenant.status != "active" or binding is None or not binding.enabled:
                return None
            IMAccounts.require(session, self.lease)
            now = database_now(session)
            # The account lease serializes prepare/finish. Only final responses are currently
            # sent; intermediate snapshots are acknowledged internally, never delivered as final.
            earlier = aliased(OutboxMessageRow)
            current_input = aliased(InboundMessageRow)
            earlier_input = aliased(InboundMessageRow)
            prior_reply = aliased(OutboxMessageRow)
            row = session.scalar(
                select(OutboxMessageRow).join(
                    IMEventReceiptRow, IMEventReceiptRow.inbound_id == OutboxMessageRow.inbound_message_id).where(
                        IMEventReceiptRow.tenant_id == self.lease.tenant_id,
                        IMEventReceiptRow.binding_id == self.lease.binding_id,
                        OutboxMessageRow.status.in_(("pending", "retry", "im_sending")),
                        ~select(earlier.outbox_message_id).where(
                            earlier.tenant_id == OutboxMessageRow.tenant_id, earlier.inbound_message_id
                            == OutboxMessageRow.inbound_message_id, earlier.part_no < OutboxMessageRow.part_no,
                            earlier.status != "delivered").exists(),
                        ~select(current_input.inbound_message_id).where(
                            current_input.inbound_message_id == OutboxMessageRow.inbound_message_id,
                            select(earlier_input.inbound_message_id).where(
                                earlier_input.tenant_id == current_input.tenant_id,
                                earlier_input.partition_key == current_input.partition_key,
                                (earlier_input.created_at < current_input.created_at) |
                                ((earlier_input.created_at == current_input.created_at) &
                                 (earlier_input.inbound_message_id < current_input.inbound_message_id)),
                                earlier_input.status.not_in(
                                    ("succeeded", "failed_final")) | select(prior_reply.outbox_message_id).where(
                                        prior_reply.inbound_message_id == earlier_input.inbound_message_id,
                                        prior_reply.status != "delivered").exists(),
                            ).exists(),
                        ).exists(),
                    ).order_by(OutboxMessageRow.created_at,
                               OutboxMessageRow.outbox_message_id).limit(1).with_for_update(of=OutboxMessageRow))
            if row is None:
                return None
            if row.next_retry_at and as_utc(row.next_retry_at) > now:
                return None
            if not row.payload_json.get("final"):
                row.status, row.delivery_id = "delivered", "internal:intermediate_snapshot"
                return {"internal": True}
            if row.payload_json.get("action_id"):
                from trpc_service.persistence.models import PendingActionRow
                action = session.get(PendingActionRow, (row.tenant_id, row.payload_json["action_id"]))
                inbound = session.get(InboundMessageRow, row.inbound_message_id)
                if action is not None and action.execution_id == inbound.execution_id:
                    row.status, row.delivery_id = "delivered", "internal:action_notification"
                else:
                    row.status, row.error_type = "dead_letter", "untrusted_action_reference"
                return {"internal": True}
            attempts = list(
                session.scalars(
                    select(IMDeliveryAttemptRow).where(
                        IMDeliveryAttemptRow.tenant_id == self.lease.tenant_id,
                        IMDeliveryAttemptRow.outbox_id == row.outbox_message_id).order_by(
                            IMDeliveryAttemptRow.network_started_at)))
            if any(attempt.outcome in {"sending", "unknown"} for attempt in attempts):
                for attempt in attempts:
                    if attempt.outcome == "sending":
                        attempt.outcome = "unknown"
                        attempt.receipt_json = DeliveryResult(outcome="unknown",
                                                              error_type="owner_interrupted").model_dump()
                        attempt.completed_at = now
                row.status, row.error_type = "delivery_unknown", "owner_interrupted"
                return {"internal": True}
            text = row.payload_json.get("text", "")
            inbound = session.get(InboundMessageRow, row.inbound_message_id)
            if inbound is None or inbound.error_type == "message_recalled":
                row.status, row.error_type = "dead_letter", "message_recalled"
                return {"internal": True}
            if not text:
                row.status, row.error_type = "dead_letter", "unsupported_reply"
                return {"internal": True}
            limit = getattr(self.adapter, "text_limit", 4096)
            unit = getattr(self.adapter, "text_unit", "characters")
            chunks = split_text(text, limit, unit=unit)
            completed = {attempt.part_no for attempt in attempts if attempt.outcome == "accepted"}
            part = next((index for index in range(len(chunks)) if index not in completed), None)
            if part is None:
                row.status = "delivered"
                return {"internal": True}
            receipt = session.scalar(
                select(IMEventReceiptRow).where(IMEventReceiptRow.inbound_id == row.inbound_message_id,
                                                IMEventReceiptRow.tenant_id == self.lease.tenant_id))
            key = (receipt.tenant_id, receipt.binding_id, receipt.external_event_id)
            member = session.get(IMMembershipRow, (receipt.tenant_id, receipt.binding_id, receipt.actor_id))
            if member is None or not member.active or member.authz_epoch != receipt.authz_epoch:
                row.status, row.error_type = "dead_letter", "authorization_revoked"
                return {"internal": True}
            context = self.cipher.open(key, receipt.context_ciphertext)["reply"]
            attempt_id = uuid.uuid4().hex
            session.add(
                IMDeliveryAttemptRow(attempt_id=attempt_id,
                                     tenant_id=row.tenant_id,
                                     outbox_id=row.outbox_message_id,
                                     part_no=part,
                                     owner_generation=self.lease.generation,
                                     outcome="sending",
                                     network_started_at=now))
            row.status = "im_sending"
            row.attempt += 1
            return {
                "attempt_id": attempt_id,
                "outbox_id": row.outbox_message_id,
                "part": part,
                "text": chunks[part],
                "context": context,
                "last": part == len(chunks) - 1,
                "parent": inbound.payload_json.get("trace_context"),
                "failures": sum(a.part_no == part and a.outcome == "rejected" for a in attempts)
            }

    def finish(self, work, result):
        with self.database.sessions.begin() as session:
            IMAccounts.require(session, self.lease)
            row = session.get(OutboxMessageRow, work["outbox_id"], with_for_update=True)
            attempt = session.get(IMDeliveryAttemptRow, work["attempt_id"], with_for_update=True)
            if attempt.outcome != "sending" or attempt.owner_generation != self.lease.generation:
                raise RuntimeError("delivery attempt is no longer owned")
            attempt.outcome, attempt.receipt_json = result.outcome, result.model_dump(mode="json")
            attempt.completed_at = database_now(session)
            row.error_type = result.error_type
            row.next_retry_at = None
            if result.outcome == "accepted":
                row.status = "delivered" if work["last"] else "pending"
                row.delivery_id = result.external_message_id or result.provider_request_id
            elif result.outcome == "unknown":
                row.status = "delivery_unknown"
            elif result.retryable and work["failures"] + 1 < self.max_attempts:
                row.status = "retry"
                row.next_retry_at = attempt.completed_at + timedelta(
                    seconds=result.retry_after or 2**(work["failures"] + 1))
            else:
                row.status = "dead_letter"

    async def run_once(self):
        if self.lease.channel == "wecom" and not self.adapter.client.is_authenticated:
            return False
        work = await asyncio.to_thread(self.prepare)
        if not work:
            return False
        if work.get("internal"):
            return True
        with operation("im.reply",
                       parent=work["parent"],
                       attributes={
                           "tenant.id": self.lease.tenant_id,
                           "channel": self.lease.channel
                       }):
            try:
                kwargs = ({"stream_id": work["outbox_id"], "final": True, "part_no": work["part"]}
                          if self.lease.channel == "wecom" else {})
                if self.lease.channel == "feishu":
                    kwargs = {"idempotency_key": work["outbox_id"] + "-" + str(work["part"])}
                result = await asyncio.wait_for(self.adapter.send_text(work["context"], work["text"], **kwargs),
                                                self.timeout)
            except Exception:
                result = DeliveryResult(outcome="unknown", error_type="transport_outcome_unknown")
            # Cancellation leaves 'sending'; takeover resolves it as unknown, never as pending.
            await asyncio.to_thread(self.finish, work, result)
            if result.error_type == "rate_limited":
                count("im.rate_limited", channel=self.lease.channel)
            count("im.delivery", channel=self.lease.channel, outcome=result.outcome)
        return True

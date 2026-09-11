"""Reply outbox with idempotent enqueue and leased dispatch."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import aliased

from trpc_service.persistence import Database
from trpc_service.persistence.models import OutboxMessageRow
from trpc_service.persistence.models import InboundMessageRow
from trpc_service.persistence.models import IMEventReceiptRow
from trpc_service.persistence.models import utcnow


@dataclass(frozen=True)
class OutboxItem:
    outbox_message_id: str
    tenant_id: str
    inbound_message_id: str
    part_no: int
    payload: dict
    attempt: int


class OutboxRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    def enqueue(self, tenant_id: str, inbound_message_id: str, part_no: int, payload: dict) -> str:
        message_id = uuid.uuid4().hex
        with self._database.sessions() as session:
            session.add(
                OutboxMessageRow(
                    outbox_message_id=message_id,
                    tenant_id=tenant_id,
                    inbound_message_id=inbound_message_id,
                    part_no=part_no,
                    payload_json=payload,
                    status="pending",
                ))
            try:
                session.commit()
                return message_id
            except IntegrityError:
                session.rollback()
                existing = session.scalar(
                    select(OutboxMessageRow).where(
                        OutboxMessageRow.tenant_id == tenant_id,
                        OutboxMessageRow.inbound_message_id == inbound_message_id,
                        OutboxMessageRow.part_no == part_no,
                    ))
                if existing is None or existing.payload_json != payload:
                    raise ValueError("outbox idempotency key was reused with a different payload")
                return existing.outbox_message_id

    def claim(self, *, limit: int, worker_id: str, lease_seconds: float) -> list[OutboxItem]:
        now = utcnow()
        earlier = aliased(OutboxMessageRow)
        current_input = aliased(InboundMessageRow)
        earlier_input = aliased(InboundMessageRow)
        prior_reply = aliased(OutboxMessageRow)
        with self._database.sessions.begin() as session:
            rows = list(
                session.scalars(
                    select(OutboxMessageRow).where(
                        ~select(IMEventReceiptRow.external_event_id).where(
                            IMEventReceiptRow.inbound_id == OutboxMessageRow.inbound_message_id).exists(),
                        or_(
                            OutboxMessageRow.status.in_(("pending", "retry")),
                            and_(
                                OutboxMessageRow.status == "dispatching",
                                OutboxMessageRow.lease_expires_at < now,
                            ),
                        ),
                        or_(OutboxMessageRow.next_retry_at.is_(None), OutboxMessageRow.next_retry_at <= now),
                        ~select(earlier.outbox_message_id).where(
                            earlier.tenant_id == OutboxMessageRow.tenant_id,
                            earlier.inbound_message_id == OutboxMessageRow.inbound_message_id,
                            earlier.part_no < OutboxMessageRow.part_no,
                            earlier.status != "delivered",
                        ).exists(),
                        ~select(current_input.inbound_message_id).where(
                            current_input.inbound_message_id == OutboxMessageRow.inbound_message_id,
                            select(earlier_input.inbound_message_id).where(
                                earlier_input.tenant_id == current_input.tenant_id,
                                earlier_input.partition_key == current_input.partition_key,
                                or_(earlier_input.created_at < current_input.created_at,
                                    and_(earlier_input.created_at == current_input.created_at,
                                         earlier_input.inbound_message_id < current_input.inbound_message_id)),
                                or_(earlier_input.status.not_in(("succeeded", "failed_final")),
                                    select(prior_reply.outbox_message_id).where(
                                        prior_reply.inbound_message_id == earlier_input.inbound_message_id,
                                        prior_reply.status != "delivered").exists()),
                            ).exists(),
                        ).exists(),
                    ).order_by(OutboxMessageRow.created_at).limit(limit).with_for_update(skip_locked=True)))
            for row in rows:
                row.status = "dispatching"
                row.lease_owner = worker_id
                row.lease_expires_at = now + timedelta(seconds=lease_seconds)
                row.attempt += 1
            return [
                OutboxItem(row.outbox_message_id, row.tenant_id, row.inbound_message_id, row.part_no,
                           row.payload_json, row.attempt) for row in rows
            ]

    def mark_delivered(self, message_id: str, worker_id: str, delivery_id: str) -> None:
        with self._database.sessions.begin() as session:
            row = session.get(OutboxMessageRow, message_id)
            if row is None:
                raise LookupError("outbox message not found")
            if row.status != "dispatching" or row.lease_owner != worker_id:
                raise RuntimeError("outbox message is not owned by this worker")
            row.status = "delivered"
            row.delivery_id = delivery_id
            row.lease_owner = None
            row.lease_expires_at = None

    def retry(self, message_id: str, worker_id: str, error_type: str, delay_seconds: float) -> None:
        with self._database.sessions.begin() as session:
            row = session.get(OutboxMessageRow, message_id)
            if row is None or row.status != "dispatching" or row.lease_owner != worker_id:
                raise RuntimeError("outbox message is not owned by this worker")
            row.status = "retry"
            row.error_type = error_type
            row.next_retry_at = utcnow() + timedelta(seconds=delay_seconds)
            row.lease_owner = None
            row.lease_expires_at = None

    def list_for_inbound(self, tenant_id: str, inbound_message_id: str) -> list[OutboxItem]:
        with self._database.sessions() as session:
            rows = session.scalars(
                select(OutboxMessageRow).where(
                    OutboxMessageRow.tenant_id == tenant_id,
                    OutboxMessageRow.inbound_message_id == inbound_message_id,
                ).order_by(OutboxMessageRow.part_no))
            return [
                OutboxItem(row.outbox_message_id, row.tenant_id, row.inbound_message_id, row.part_no,
                           dict(row.payload_json), row.attempt) for row in rows
            ]

    def dead_letter(self, message_id: str, worker_id: str, error_type: str) -> None:
        with self._database.sessions.begin() as session:
            row = session.get(OutboxMessageRow, message_id)
            if row is None or row.status != "dispatching" or row.lease_owner != worker_id:
                raise RuntimeError("outbox message is not owned by this worker")
            row.status, row.error_type = "dead_letter", error_type
            row.lease_owner = row.lease_expires_at = row.next_retry_at = None


class PermanentDeliveryError(Exception):
    """The receiver explicitly rejected a request that must not be retried."""


class DeliveryRateLimited(Exception):
    def __init__(self, retry_after_seconds):
        import math
        if not math.isfinite(retry_after_seconds) or retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must be finite and nonnegative")
        self.retry_after_seconds = retry_after_seconds
        super().__init__("receiver rate limited")


class OutboxDispatcher:
    """Delivery failures never invoke the Agent; receiver deduplicates by row ID."""

    def __init__(self, repository, sender, *, worker_id, max_attempts=5, timeout_seconds=10):
        self.repository, self.sender = repository, sender
        self.worker_id, self.max_attempts, self.timeout_seconds = worker_id, max_attempts, timeout_seconds

    async def run_once(self):
        import asyncio

        items = self.repository.claim(limit=1, worker_id=self.worker_id,
                                      lease_seconds=self.timeout_seconds * 2 + 5)
        if not items:
            return False
        item = items[0]
        try:
            from trpc_service.telemetry.runtime import operation
            with self.repository._database.sessions() as session:
                inbound = session.get(InboundMessageRow, item.inbound_message_id)
                parent = inbound.payload_json.get("trace_context") if inbound else None
            with operation("outbox.send", parent=parent):
                delivery_id = await asyncio.wait_for(self.sender(item), self.timeout_seconds)
            if not delivery_id:
                raise ValueError("delivery confirmation is required")
            self.repository.mark_delivered(item.outbox_message_id, self.worker_id, delivery_id)
        except PermanentDeliveryError:
            self.repository.dead_letter(item.outbox_message_id, self.worker_id, "permanent_rejection")
        except Exception as error:
            if item.attempt >= self.max_attempts:
                self.repository.dead_letter(item.outbox_message_id, self.worker_id, type(error).__name__)
            else:
                self.repository.retry(item.outbox_message_id, self.worker_id, type(error).__name__,
                                      error.retry_after_seconds if isinstance(error, DeliveryRateLimited)
                                      else min(2 ** item.attempt, 60))
        return True

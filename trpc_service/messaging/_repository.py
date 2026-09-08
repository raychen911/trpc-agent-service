"""Reliable messaging repositories with identical in-memory and SQL semantics."""

from __future__ import annotations

import asyncio
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Optional
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import JSON
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import UniqueConstraint
from sqlalchemy import and_
from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.engine import Engine

from trpc_service.channels._models import InboundMessage
from trpc_service.channels._models import split_text
from trpc_service.channels._models import split_text_bytes
from trpc_service.log import safe_error_message
from trpc_service.tenant._persistence import mysql_sync_url
from ._models import ClaimResult
from ._models import ClaimStatus
from ._models import OutboxMessage
from ._models import utcnow

if TYPE_CHECKING:
    from trpc_service.agent._queue import TaskMessage


def _delivery_parts(channel: str, text: str) -> list[str]:
    """Split before persistence so every provider call has a checkpoint."""
    if not text:
        return []
    if channel in {"wecom", "wechat_kf"}:
        return split_text_bytes(text, 1900)
    if channel == "qq":
        return split_text(text, 1800)
    return split_text(text, 3500)


class MessageStoreABC(ABC):
    """Port used by Redis consumers and Outbox relays."""

    @abstractmethod
    async def claim_inbound(
        self,
        task: TaskMessage,
        *,
        owner: str,
        lease_seconds: float,
    ) -> ClaimResult:
        """Claim or recover an inbound processing receipt."""

    @abstractmethod
    async def renew_inbound(
        self,
        task: TaskMessage,
        *,
        owner: str,
        fencing_token: int,
        lease_seconds: float,
    ) -> bool:
        """Extend a receipt lease only while owner and fencing token match."""

    @abstractmethod
    async def complete_with_outbox(
        self,
        task: TaskMessage,
        inbound: InboundMessage,
        text: str,
        *,
        owner: str,
        fencing_token: int,
    ) -> Optional[OutboxMessage]:
        """Atomically mark the turn complete and persist its reply intent."""

    @abstractmethod
    async def abandon_inbound(self, task: TaskMessage, *, owner: str, fencing_token: int, error: str) -> None:
        """Release a failed receipt so it can be recovered."""

    @abstractmethod
    async def claim_outbox(self, *, owner: str, limit: int, lease_seconds: float) -> list[OutboxMessage]:
        """Lease ready outbound messages."""

    @abstractmethod
    async def checkpoint_part(
        self,
        event_id: str,
        *,
        owner: str,
        part_index: int,
        provider_message_id: Optional[str] = None,
    ) -> None:
        """Commit one successfully delivered message part."""

    @abstractmethod
    async def fail_delivery(
        self,
        event_id: str,
        *,
        owner: str,
        part_index: int,
        error: str,
        max_attempts: int,
    ) -> None:
        """Schedule retry or move an exhausted delivery to dead letter."""

    @abstractmethod
    async def replay_dead_letters(self, event_id: Optional[str] = None) -> int:
        """Move selected dead letters back to the pending queue."""


class InMemoryMessageStore(MessageStoreABC):
    """Deterministic store for tests and single-process acceptance runs."""

    def __init__(self) -> None:
        self._receipts: dict[str, dict[str, Any]] = {}
        self._outbox: dict[str, OutboxMessage] = {}
        self._lock = asyncio.Lock()

    async def claim_inbound(self, task: TaskMessage, *, owner: str, lease_seconds: float) -> ClaimResult:
        async with self._lock:
            now = utcnow()
            receipt = self._receipts.get(task.idempotency_key)
            if receipt is not None and receipt["status"] == "completed":
                return ClaimResult(status=ClaimStatus.COMPLETED, fencing_token=receipt["fencing_token"])
            if receipt is not None and receipt["lease_until"] > now:
                return ClaimResult(status=ClaimStatus.BUSY, fencing_token=receipt["fencing_token"])
            token = int(receipt["fencing_token"] if receipt else 0) + 1
            self._receipts[task.idempotency_key] = {
                "status": "processing",
                "owner": owner,
                "lease_until": now + timedelta(seconds=lease_seconds),
                "fencing_token": token,
                "error": None,
            }
            return ClaimResult(status=ClaimStatus.ACQUIRED, fencing_token=token)

    async def renew_inbound(
        self,
        task: TaskMessage,
        *,
        owner: str,
        fencing_token: int,
        lease_seconds: float,
    ) -> bool:
        async with self._lock:
            try:
                receipt = self._owned_receipt(task, owner, fencing_token)
            except RuntimeError:
                return False
            receipt["lease_until"] = utcnow() + timedelta(seconds=lease_seconds)
            return True

    async def complete_with_outbox(
        self,
        task: TaskMessage,
        inbound: InboundMessage,
        text: str,
        *,
        owner: str,
        fencing_token: int,
    ) -> Optional[OutboxMessage]:
        async with self._lock:
            receipt = self._owned_receipt(task, owner, fencing_token)
            parts = _delivery_parts(task.channel, text)
            message = None
            if parts:
                message = OutboxMessage(
                    event_id=uuid4().hex,
                    tenant_id=task.tenant_id,
                    channel=task.channel,
                    message_id=inbound.message_id,
                    turn_id=task.turn_id,
                    config_revision=task.config_revision,
                    inbound=inbound.model_dump(mode="json"),
                    parts=parts,
                )
                self._outbox[message.event_id] = message
            receipt["status"] = "completed"
            receipt["lease_until"] = utcnow()
            return message.model_copy(deep=True) if message else None

    def _owned_receipt(self, task: TaskMessage, owner: str, fencing_token: int) -> dict[str, Any]:
        receipt = self._receipts.get(task.idempotency_key)
        if (receipt is None or receipt["status"] != "processing" or receipt["owner"] != owner
                or receipt["fencing_token"] != fencing_token):
            raise RuntimeError("inbound receipt ownership was lost")
        return receipt

    async def abandon_inbound(self, task: TaskMessage, *, owner: str, fencing_token: int, error: str) -> None:
        async with self._lock:
            receipt = self._owned_receipt(task, owner, fencing_token)
            receipt["status"] = "retry"
            receipt["lease_until"] = utcnow()
            receipt["error"] = safe_error_message(Exception(error))

    async def claim_outbox(self, *, owner: str, limit: int, lease_seconds: float) -> list[OutboxMessage]:
        async with self._lock:
            now = utcnow()
            ready = [
                message for message in self._outbox.values()
                if message.status in {"pending", "retry", "delivering"} and message.available_at <= now
            ][:limit]
            for message in ready:
                message.status = "delivering"
                message.lease_owner = owner
                message.available_at = now + timedelta(seconds=lease_seconds)
            return [message.model_copy(deep=True) for message in ready]

    async def checkpoint_part(
        self,
        event_id: str,
        *,
        owner: str,
        part_index: int,
        provider_message_id: Optional[str] = None,
    ) -> None:
        del provider_message_id
        async with self._lock:
            message = self._owned_outbox(event_id, owner, part_index)
            message.next_part += 1
            message.lease_owner = None
            message.available_at = utcnow()
            message.status = "delivered" if message.next_part >= len(message.parts) else "pending"

    def _owned_outbox(self, event_id: str, owner: str, part_index: int) -> OutboxMessage:
        message = self._outbox.get(event_id)
        if (message is None or message.status != "delivering" or message.lease_owner != owner
                or message.next_part != part_index):
            raise RuntimeError("outbox ownership or checkpoint was lost")
        return message

    async def fail_delivery(
        self,
        event_id: str,
        *,
        owner: str,
        part_index: int,
        error: str,
        max_attempts: int,
    ) -> None:
        async with self._lock:
            message = self._owned_outbox(event_id, owner, part_index)
            message.attempt_count += 1
            message.lease_owner = None
            message.status = "dead_letter" if message.attempt_count >= max_attempts else "retry"
            delay = min(300, 2**message.attempt_count)
            message.available_at = utcnow() + timedelta(seconds=delay)

    async def replay_dead_letters(self, event_id: Optional[str] = None) -> int:
        async with self._lock:
            selected = [
                message for message in self._outbox.values()
                if message.status == "dead_letter" and (event_id is None or message.event_id == event_id)
            ]
            for message in selected:
                message.status = "pending"
                message.attempt_count = 0
                message.available_at = utcnow()
            return len(selected)

    def get_outbox(self, event_id: str) -> Optional[OutboxMessage]:
        message = self._outbox.get(event_id)
        return message.model_copy(deep=True) if message else None


metadata = MetaData()
receipt_table = Table(
    "inbound_receipt",
    metadata,
    Column("tenant_id", String(128), primary_key=True),
    Column("channel", String(64), primary_key=True),
    Column("message_id", String(255), primary_key=True),
    Column("request_id", String(128), nullable=False),
    Column("trace_id", String(64), nullable=True),
    Column("task_id", String(128), nullable=True),
    Column("turn_id", String(64), nullable=False),
    Column("config_revision", BigInteger, nullable=True),
    Column("status", String(32), nullable=False),
    Column("lease_owner", String(255), nullable=True),
    Column("lease_until", DateTime(timezone=True), nullable=False),
    Column("fencing_token", BigInteger, nullable=False, default=0),
    Column("attempt_count", Integer, nullable=False, default=0),
    Column("last_error", String(1024), nullable=True),
    Column("result_checksum", String(64), nullable=True),
    Column("reply_status", String(32), nullable=True),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
delivery_outbox_table = Table(
    "delivery_outbox",
    metadata,
    Column("event_id", String(64), primary_key=True),
    Column("tenant_id", String(128), nullable=False),
    Column("channel", String(64), nullable=False),
    Column("message_id", String(255), nullable=False),
    Column("turn_id", String(64), nullable=False),
    Column("config_revision", BigInteger, nullable=True),
    Column("inbound", JSON, nullable=False),
    Column("parts", JSON, nullable=False),
    Column("next_part", Integer, nullable=False, default=0),
    Column("attempt_count", Integer, nullable=False, default=0),
    Column("status", String(32), nullable=False),
    Column("lease_owner", String(255), nullable=True),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("last_error", String(1024), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("delivered_at", DateTime(timezone=True), nullable=True),
    UniqueConstraint("tenant_id", "channel", "message_id", name="uq_delivery_message"),
)
delivery_attempt_table = Table(
    "delivery_attempt",
    metadata,
    Column("attempt_id", String(64), primary_key=True),
    Column("event_id", String(64), nullable=False),
    Column("part_index", Integer, nullable=False),
    Column("outcome", String(32), nullable=False),
    Column("provider_message_id", String(255), nullable=True),
    Column("error", String(1024), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


class SqlMessageStore(MessageStoreABC):
    """MySQL-backed receipt/outbox store; methods are async at the service port."""

    def __init__(self, db_url: str, *, create_schema: bool = True) -> None:
        self._engine: Engine = create_engine(mysql_sync_url(db_url), pool_pre_ping=True)
        if create_schema:
            metadata.create_all(self._engine)

    @staticmethod
    def _naive_utc(value: datetime) -> datetime:
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    async def claim_inbound(self, task: TaskMessage, *, owner: str, lease_seconds: float) -> ClaimResult:
        return await asyncio.to_thread(self._claim_inbound, task, owner, lease_seconds)

    def _claim_inbound(self, task: TaskMessage, owner: str, lease_seconds: float) -> ClaimResult:
        now = self._naive_utc(utcnow())
        key = and_(
            receipt_table.c.tenant_id == task.tenant_id,
            receipt_table.c.channel == task.channel,
            receipt_table.c.message_id == task.inbound.get("message_id", ""),
        )
        with self._engine.begin() as connection:
            row = connection.execute(select(receipt_table).where(key).with_for_update()).mappings().first()
            if row is None:
                connection.execute(
                    insert(receipt_table).values(
                        tenant_id=task.tenant_id,
                        channel=task.channel,
                        message_id=task.inbound.get("message_id", ""),
                        request_id=task.turn_id,
                        trace_id=task.trace_headers.get("traceparent"),
                        task_id=task.turn_id,
                        turn_id=task.turn_id,
                        config_revision=task.config_revision,
                        status="processing",
                        lease_owner=owner,
                        lease_until=now + timedelta(seconds=lease_seconds),
                        fencing_token=1,
                        attempt_count=1,
                        expires_at=now + timedelta(days=30),
                        created_at=now,
                        updated_at=now,
                    ))
                return ClaimResult(status=ClaimStatus.ACQUIRED, fencing_token=1)
            if row["status"] == "completed":
                return ClaimResult(status=ClaimStatus.COMPLETED, fencing_token=int(row["fencing_token"]))
            lease_until = row["lease_until"]
            if getattr(lease_until, "tzinfo", None) is not None:
                lease_until = lease_until.astimezone(timezone.utc).replace(tzinfo=None)
            if lease_until is not None and lease_until > now:
                return ClaimResult(status=ClaimStatus.BUSY, fencing_token=int(row["fencing_token"]))
            token = int(row["fencing_token"]) + 1
            connection.execute(
                update(receipt_table).where(key).values(
                    status="processing",
                    lease_owner=owner,
                    lease_until=now + timedelta(seconds=lease_seconds),
                    fencing_token=token,
                    attempt_count=int(row["attempt_count"]) + 1,
                    updated_at=now,
                ))
            return ClaimResult(status=ClaimStatus.ACQUIRED, fencing_token=token)

    async def complete_with_outbox(
        self,
        task: TaskMessage,
        inbound: InboundMessage,
        text: str,
        *,
        owner: str,
        fencing_token: int,
    ) -> Optional[OutboxMessage]:
        return await asyncio.to_thread(
            self._complete_with_outbox,
            task,
            inbound,
            text,
            owner,
            fencing_token,
        )

    async def renew_inbound(
        self,
        task: TaskMessage,
        *,
        owner: str,
        fencing_token: int,
        lease_seconds: float,
    ) -> bool:
        return await asyncio.to_thread(
            self._renew_inbound,
            task,
            owner,
            fencing_token,
            lease_seconds,
        )

    def _renew_inbound(
        self,
        task: TaskMessage,
        owner: str,
        fencing_token: int,
        lease_seconds: float,
    ) -> bool:
        now = self._naive_utc(utcnow())
        with self._engine.begin() as connection:
            result = connection.execute(
                update(receipt_table).where(
                    receipt_table.c.tenant_id == task.tenant_id,
                    receipt_table.c.channel == task.channel,
                    receipt_table.c.message_id == task.inbound.get("message_id", ""),
                    receipt_table.c.status == "processing",
                    receipt_table.c.lease_owner == owner,
                    receipt_table.c.fencing_token == fencing_token,
                ).values(
                    lease_until=now + timedelta(seconds=lease_seconds),
                    updated_at=now,
                ))
        return result.rowcount == 1

    def _complete_with_outbox(
        self,
        task: TaskMessage,
        inbound: InboundMessage,
        text: str,
        owner: str,
        fencing_token: int,
    ) -> Optional[OutboxMessage]:
        now = self._naive_utc(utcnow())
        key = and_(
            receipt_table.c.tenant_id == task.tenant_id,
            receipt_table.c.channel == task.channel,
            receipt_table.c.message_id == inbound.message_id,
            receipt_table.c.status == "processing",
            receipt_table.c.lease_owner == owner,
            receipt_table.c.fencing_token == fencing_token,
        )
        parts = _delivery_parts(task.channel, text)
        message = None
        with self._engine.begin() as connection:
            result = connection.execute(
                update(receipt_table).where(key).values(
                    status="completed",
                    lease_until=now,
                    updated_at=now,
                ))
            if result.rowcount != 1:
                raise RuntimeError("inbound receipt ownership was lost")
            if parts:
                message = OutboxMessage(
                    event_id=uuid4().hex,
                    tenant_id=task.tenant_id,
                    channel=task.channel,
                    message_id=inbound.message_id,
                    turn_id=task.turn_id,
                    config_revision=task.config_revision,
                    inbound=inbound.model_dump(mode="json"),
                    parts=parts,
                    available_at=now.replace(tzinfo=timezone.utc),
                    created_at=now.replace(tzinfo=timezone.utc),
                )
                connection.execute(insert(delivery_outbox_table).values(**message.model_dump(mode="python")))
        return message

    async def abandon_inbound(self, task: TaskMessage, *, owner: str, fencing_token: int, error: str) -> None:
        await asyncio.to_thread(self._abandon_inbound, task, owner, fencing_token, error)

    def _abandon_inbound(self, task: TaskMessage, owner: str, fencing_token: int, error: str) -> None:
        now = self._naive_utc(utcnow())
        with self._engine.begin() as connection:
            connection.execute(
                update(receipt_table).where(
                    receipt_table.c.tenant_id == task.tenant_id,
                    receipt_table.c.channel == task.channel,
                    receipt_table.c.message_id == task.inbound.get("message_id", ""),
                    receipt_table.c.lease_owner == owner,
                    receipt_table.c.fencing_token == fencing_token,
                ).values(
                    status="retry",
                    lease_until=now,
                    last_error=safe_error_message(Exception(error))[:1024],
                    updated_at=now,
                ))

    async def claim_outbox(self, *, owner: str, limit: int, lease_seconds: float) -> list[OutboxMessage]:
        return await asyncio.to_thread(self._claim_outbox, owner, limit, lease_seconds)

    def _claim_outbox(self, owner: str, limit: int, lease_seconds: float) -> list[OutboxMessage]:
        now = self._naive_utc(utcnow())
        with self._engine.begin() as connection:
            query = select(delivery_outbox_table).where(
                delivery_outbox_table.c.status.in_(("pending", "retry", "delivering")),
                delivery_outbox_table.c.available_at <= now,
            ).order_by(delivery_outbox_table.c.created_at).limit(limit)
            if connection.dialect.name != "sqlite":
                query = query.with_for_update(skip_locked=True)
            rows = connection.execute(query).mappings().all()
            event_ids = [row["event_id"] for row in rows]
            if event_ids:
                connection.execute(
                    update(delivery_outbox_table).where(
                        delivery_outbox_table.c.event_id.in_(event_ids),
                        delivery_outbox_table.c.status.in_(("pending", "retry", "delivering")),
                    ).values(
                        status="delivering",
                        lease_owner=owner,
                        available_at=now + timedelta(seconds=lease_seconds),
                    ))
            return [
                OutboxMessage.model_validate({
                    **{
                        key: row[key]
                        for key in OutboxMessage.model_fields
                    },
                    "status": "delivering",
                    "lease_owner": owner,
                }) for row in rows
            ]

    async def checkpoint_part(
        self,
        event_id: str,
        *,
        owner: str,
        part_index: int,
        provider_message_id: Optional[str] = None,
    ) -> None:
        await asyncio.to_thread(self._checkpoint_part, event_id, owner, part_index, provider_message_id)

    def _checkpoint_part(
        self,
        event_id: str,
        owner: str,
        part_index: int,
        provider_message_id: Optional[str],
    ) -> None:
        now = self._naive_utc(utcnow())
        with self._engine.begin() as connection:
            row = connection.execute(
                select(delivery_outbox_table).where(
                    delivery_outbox_table.c.event_id == event_id).with_for_update()).mappings().first()
            if (row is None or row["status"] != "delivering" or row["lease_owner"] != owner
                    or int(row["next_part"]) != part_index):
                raise RuntimeError("outbox ownership or checkpoint was lost")
            next_part = part_index + 1
            delivered = next_part >= len(row["parts"])
            connection.execute(
                insert(delivery_attempt_table).values(
                    attempt_id=uuid4().hex,
                    event_id=event_id,
                    part_index=part_index,
                    outcome="success",
                    provider_message_id=provider_message_id,
                    created_at=now,
                ))
            connection.execute(
                update(delivery_outbox_table).where(delivery_outbox_table.c.event_id == event_id).values(
                    next_part=next_part,
                    status="delivered" if delivered else "pending",
                    lease_owner=None,
                    available_at=now,
                    delivered_at=now if delivered else None,
                ))

    async def fail_delivery(
        self,
        event_id: str,
        *,
        owner: str,
        part_index: int,
        error: str,
        max_attempts: int,
    ) -> None:
        await asyncio.to_thread(self._fail_delivery, event_id, owner, part_index, error, max_attempts)

    def _fail_delivery(
        self,
        event_id: str,
        owner: str,
        part_index: int,
        error: str,
        max_attempts: int,
    ) -> None:
        now = self._naive_utc(utcnow())
        safe_error = safe_error_message(Exception(error))[:1024]
        with self._engine.begin() as connection:
            row = connection.execute(
                select(delivery_outbox_table).where(
                    delivery_outbox_table.c.event_id == event_id).with_for_update()).mappings().first()
            if (row is None or row["status"] != "delivering" or row["lease_owner"] != owner
                    or int(row["next_part"]) != part_index):
                raise RuntimeError("outbox ownership or checkpoint was lost")
            attempts = int(row["attempt_count"]) + 1
            status = "dead_letter" if attempts >= max_attempts else "retry"
            connection.execute(
                insert(delivery_attempt_table).values(
                    attempt_id=uuid4().hex,
                    event_id=event_id,
                    part_index=part_index,
                    outcome=status,
                    error=safe_error,
                    created_at=now,
                ))
            connection.execute(
                update(delivery_outbox_table).where(delivery_outbox_table.c.event_id == event_id).values(
                    attempt_count=attempts,
                    status=status,
                    lease_owner=None,
                    last_error=safe_error,
                    available_at=now + timedelta(seconds=min(300, 2**attempts)),
                ))

    async def replay_dead_letters(self, event_id: Optional[str] = None) -> int:
        return await asyncio.to_thread(self._replay_dead_letters, event_id)

    def _replay_dead_letters(self, event_id: Optional[str]) -> int:
        clauses = [delivery_outbox_table.c.status == "dead_letter"]
        if event_id is not None:
            clauses.append(delivery_outbox_table.c.event_id == event_id)
        with self._engine.begin() as connection:
            result = connection.execute(
                update(delivery_outbox_table).where(*clauses).values(
                    status="pending",
                    attempt_count=0,
                    available_at=self._naive_utc(utcnow()),
                ))
        return int(result.rowcount)

    async def close(self) -> None:
        await asyncio.to_thread(self._engine.dispose)

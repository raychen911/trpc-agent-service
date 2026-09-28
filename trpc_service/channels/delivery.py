"""Lease-based asynchronous IM delivery shared by every Worker node."""

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import logging
from typing import Protocol
from uuid import UUID

from sqlalchemy import and_, exists, or_, select
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.recovery import (
    FailureDisposition,
    RecoveryDecision,
    RecoveryPolicy,
)
from trpc_service.channels.contracts import (
    ChannelBindingConfig,
    DeliveryReceipt,
    MessageKind,
    OutgoingMessage,
)
from trpc_service.channels.registry import ChannelAdapterRegistry
from trpc_service.channels.models import ChannelBinding
from trpc_service.config.runtime import LeasedWorkerConfig
from trpc_service.metrics import PlatformTelemetry
from trpc_service.storage.types import OutboxMessage
from trpc_service.storage.orm import as_utc, utc_now
from trpc_service.storage.runtime_orm import (
    AgentTaskRow,
    InboxMessageRow,
    OutboxAttemptRow,
    OutboxMessageRow,
)
from trpc_service.tenant.context import TenantContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DeliveryTaskClaim:
    """One globally due reply plus the immutable binding needed to deliver it."""

    context: TenantContext
    binding: ChannelBindingConfig
    message: OutboxMessage
    trace_context: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.message.category != "IM_REPLY":
            raise ValueError("Delivery Worker can only claim IM reply tasks")
        if self.message.binding_id != self.binding.binding_id:
            raise ValueError("delivery task does not match its Channel Binding")
        if (self.context.tenant_id != self.binding.tenant_id
                or self.context.agent_app_id != self.binding.agent_app_id):
            raise ValueError("delivery task crosses its tenant or Agent boundary")


class DeliveryTaskQueue(Protocol):
    """Durable queue seam implemented by the primary SQL fact store."""

    async def claim(
        self,
        worker_id: str,
        *,
        lease_until: datetime,
    ) -> DeliveryTaskClaim | None:
        ...

    async def complete(
        self,
        claim: DeliveryTaskClaim,
        *,
        worker_id: str,
        receipt: DeliveryReceipt,
    ) -> None:
        ...

    async def renew(
        self,
        claim: DeliveryTaskClaim,
        *,
        worker_id: str,
        lease_until: datetime,
    ) -> bool:
        ...

    async def fail(
        self,
        claim: DeliveryTaskClaim,
        *,
        worker_id: str,
        decision: RecoveryDecision,
        next_attempt_at: datetime | None,
    ) -> None:
        ...

    async def replay(self, tenant_id: UUID, outbox_id: str) -> bool:
        ...


class PostgreSQLDeliveryTaskQueue:
    """Use the primary SQL Outbox as the cross-node delivery authority."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        allowed_channel_types: Sequence[str] | None = None,
    ) -> None:
        self._sessions = sessions
        self._allowed_channel_types = (None if allowed_channel_types is None else tuple(
            dict.fromkeys(channel_type.strip().lower() for channel_type in allowed_channel_types
                          if channel_type.strip())))

    @staticmethod
    def _message(row: OutboxMessageRow) -> OutboxMessage:
        return OutboxMessage(
            outbox_id=row.outbox_id,
            category=row.category,
            idempotency_key=row.idempotency_key,
            destination=row.destination,
            binding_id=row.binding_id,
            request_id=row.request_id,
            session_id=row.session_id,
            sequence_no=row.sequence_no,
            attempt_count=row.attempt_count,
            retry_count=row.retry_count,
            payload=row.payload,
        )

    async def claim(
        self,
        worker_id: str,
        *,
        lease_until: datetime,
    ) -> DeliveryTaskClaim | None:
        """Lease the oldest due active-binding reply with SKIP LOCKED."""

        now = utc_now()
        if not worker_id.strip() or as_utc(lease_until) <= now:
            raise ValueError("delivery worker and future lease are required")
        if self._allowed_channel_types == ():
            return None
        channel_predicates = (() if self._allowed_channel_types is None else
                              (ChannelBinding.channel_type.in_(self._allowed_channel_types), ))
        earlier = aliased(OutboxMessageRow)
        same_stream = or_(
            and_(OutboxMessageRow.session_id.is_not(None),
                 earlier.session_id == OutboxMessageRow.session_id),
            and_(OutboxMessageRow.session_id.is_(None), OutboxMessageRow.request_id.is_not(None),
                 earlier.request_id == OutboxMessageRow.request_id),
        )
        has_predecessor = exists(
            select(earlier.outbox_id).where(
                earlier.tenant_id == OutboxMessageRow.tenant_id,
                earlier.agent_app_id == OutboxMessageRow.agent_app_id,
                earlier.binding_id == OutboxMessageRow.binding_id,
                earlier.category == "IM_REPLY",
                same_stream,
                earlier.status.in_(("PENDING", "PROCESSING", "RETRYABLE_FAILED", "UNKNOWN")),
                or_(
                    earlier.created_at < OutboxMessageRow.created_at,
                    and_(earlier.created_at == OutboxMessageRow.created_at, earlier.sequence_no
                         < OutboxMessageRow.sequence_no),
                    and_(earlier.created_at == OutboxMessageRow.created_at,
                         earlier.sequence_no == OutboxMessageRow.sequence_no, earlier.outbox_id
                         < OutboxMessageRow.outbox_id),
                ),
            ))
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(OutboxMessageRow).join(
                    ChannelBinding,
                    (ChannelBinding.tenant_id == OutboxMessageRow.tenant_id)
                    & (ChannelBinding.binding_id == OutboxMessageRow.binding_id),
                ).where(
                    OutboxMessageRow.category == "IM_REPLY",
                    ChannelBinding.status == "active",
                    *channel_predicates,
                    ~has_predecessor,
                    or_(
                        OutboxMessageRow.status.in_(("PENDING", "RETRYABLE_FAILED")),
                        (OutboxMessageRow.status == "PROCESSING")
                        & (OutboxMessageRow.lease_until.is_not(None))
                        & (OutboxMessageRow.lease_until <= now),
                    ),
                    or_(OutboxMessageRow.next_attempt_at.is_(None), OutboxMessageRow.next_attempt_at
                        <= now),
                ).order_by(
                    OutboxMessageRow.priority,
                    OutboxMessageRow.created_at,
                    OutboxMessageRow.sequence_no,
                    OutboxMessageRow.outbox_id,
                ).limit(1).with_for_update(skip_locked=True, of=OutboxMessageRow))
            if row is None or row.binding_id is None:
                return None
            binding = await database.scalar(
                select(ChannelBinding).where(
                    ChannelBinding.tenant_id == row.tenant_id,
                    ChannelBinding.binding_id == row.binding_id,
                    ChannelBinding.status == "active",
                ))
            if binding is None:
                return None
            task = (None if row.request_id is None else await database.scalar(
                select(AgentTaskRow).where(
                    AgentTaskRow.tenant_id == row.tenant_id,
                    AgentTaskRow.agent_app_id == row.agent_app_id,
                    AgentTaskRow.request_id == row.request_id,
                ).order_by(
                    AgentTaskRow.created_at.desc(),
                    AgentTaskRow.task_id.desc(),
                ).limit(1)))
            raw_trace_context = ({} if task is None else task.request_payload.get(
                "trace_context", {}))
            if not isinstance(raw_trace_context, Mapping):
                raise RuntimeError("persisted delivery trace context must be an object")
            trace_context = {str(key): str(value) for key, value in raw_trace_context.items()}
            row.status = "PROCESSING"
            row.attempt_count += 1
            row.retry_count += 1
            row.lease_owner = worker_id
            row.lease_until = lease_until
            row.next_attempt_at = None
            database.add(
                OutboxAttemptRow(
                    tenant_id=row.tenant_id,
                    agent_app_id=row.agent_app_id,
                    outbox_id=row.outbox_id,
                    attempt_no=row.attempt_count,
                    worker_id=worker_id,
                    started_at=now,
                ))
            return DeliveryTaskClaim(
                context=TenantContext(
                    tenant_id=row.tenant_id,
                    agent_app_id=row.agent_app_id,
                    config_version=1 if task is None else task.config_version,
                    request_id=row.request_id or f"delivery:{row.outbox_id}",
                    trace_id=(f"delivery:{row.outbox_id}" if task is None else task.trace_id),
                ),
                binding=binding.to_config(),
                message=self._message(row),
                trace_context=trace_context,
            )

    @staticmethod
    async def _owned(
        database: AsyncSession,
        claim: DeliveryTaskClaim,
        worker_id: str,
    ) -> OutboxMessageRow:
        row = await database.scalar(
            select(OutboxMessageRow).where(
                OutboxMessageRow.tenant_id == claim.context.tenant_id,
                OutboxMessageRow.agent_app_id == claim.context.agent_app_id,
                OutboxMessageRow.outbox_id == claim.message.outbox_id,
            ).with_for_update())
        if (row is None or row.status != "PROCESSING" or row.lease_owner != worker_id
                or row.attempt_count != claim.message.attempt_count or row.lease_until is None
                or as_utc(row.lease_until) <= utc_now()):
            raise RuntimeError("delivery task lease is stale or expired")
        return row

    @staticmethod
    async def _attempt(database: AsyncSession, row: OutboxMessageRow) -> OutboxAttemptRow | None:
        attempt: OutboxAttemptRow | None = await database.scalar(
            select(OutboxAttemptRow).where(
                OutboxAttemptRow.tenant_id == row.tenant_id,
                OutboxAttemptRow.agent_app_id == row.agent_app_id,
                OutboxAttemptRow.outbox_id == row.outbox_id,
                OutboxAttemptRow.attempt_no == row.attempt_count,
            ).with_for_update())
        return attempt

    async def renew(
        self,
        claim: DeliveryTaskClaim,
        *,
        worker_id: str,
        lease_until: datetime,
    ) -> bool:
        """Renew an owned delivery while the IM provider call is still running."""

        if as_utc(lease_until) <= utc_now():
            raise ValueError("renewed delivery lease must expire in the future")
        async with self._sessions.begin() as database:
            try:
                row = await self._owned(database, claim, worker_id)
            except RuntimeError:
                return False
            row.lease_until = lease_until
            return True

    async def complete(
        self,
        claim: DeliveryTaskClaim,
        *,
        worker_id: str,
        receipt: DeliveryReceipt,
    ) -> None:
        """Persist the provider receipt and atomically mark its Inbox replied."""

        async with self._sessions.begin() as database:
            row = await self._owned(database, claim, worker_id)
            row.status = "DELIVERED"
            row.external_receipt_id = receipt.external_delivery_id[:255]
            row.delivered_at = receipt.accepted_at
            row.lease_owner = None
            row.lease_until = None
            attempt = await self._attempt(database, row)
            if attempt is not None:
                attempt.finished_at = receipt.accepted_at
                attempt.result = "DELIVERED"
                attempt.external_receipt_id = receipt.external_delivery_id[:255]
            inbox = await database.scalar(
                select(InboxMessageRow).where(
                    InboxMessageRow.tenant_id == row.tenant_id,
                    InboxMessageRow.agent_app_id == row.agent_app_id,
                    InboxMessageRow.reply_outbox_id == row.outbox_id,
                ).with_for_update())
            if inbox is not None:
                inbox.status = "REPLIED"

    async def fail(
        self,
        claim: DeliveryTaskClaim,
        *,
        worker_id: str,
        decision: RecoveryDecision,
        next_attempt_at: datetime | None,
    ) -> None:
        """Persist retry, DLQ, or UNKNOWN without exception text."""

        async with self._sessions.begin() as database:
            row = await self._owned(database, claim, worker_id)
            if decision.disposition is FailureDisposition.UNKNOWN:
                row.status = "UNKNOWN"
            elif next_attempt_at is not None:
                row.status = "RETRYABLE_FAILED"
            else:
                row.status = "DEAD_LETTER"
            row.next_attempt_at = next_attempt_at
            row.lease_owner = None
            row.lease_until = None
            row.last_error_code = decision.error_code
            row.last_error_summary = decision.safe_summary
            attempt = await self._attempt(database, row)
            if attempt is not None:
                attempt.finished_at = utc_now()
                attempt.result = row.status
                attempt.error_summary = decision.safe_summary

    async def replay(self, tenant_id: UUID, outbox_id: str) -> bool:
        """Move one tenant-scoped terminal delivery back to PENDING."""

        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(OutboxMessageRow).where(
                    OutboxMessageRow.tenant_id == tenant_id,
                    OutboxMessageRow.outbox_id == outbox_id,
                    OutboxMessageRow.category == "IM_REPLY",
                    OutboxMessageRow.status.in_(("DEAD_LETTER", "UNKNOWN")),
                ).with_for_update())
            if row is None:
                return False
            row.status = "PENDING"
            row.next_attempt_at = None
            row.lease_owner = None
            row.lease_until = None
            row.last_error_code = None
            row.last_error_summary = None
            # Keep attempt_count monotonic because OutboxAttempt rows are an
            # immutable audit trail. Only the current retry budget is reset.
            row.retry_count = 0
            return True


def outgoing_from_outbox(message: OutboxMessage) -> OutgoingMessage:
    """Validate a persisted reply before it crosses an IM provider boundary."""

    payload = message.payload
    artifact_refs = payload.get("artifact_refs", [])
    attributes = payload.get("attributes", {})
    if (not isinstance(artifact_refs, Sequence) or isinstance(artifact_refs, (str, bytes))
            or not isinstance(attributes, Mapping)):
        raise ValueError("persisted reply payload has invalid collection fields")
    text = payload.get("text")
    return OutgoingMessage(
        delivery_id=str(payload["delivery_id"]),
        conversation_id=str(payload["conversation_id"]),
        kind=MessageKind(str(payload["kind"])),
        text=None if text is None else str(text),
        artifact_refs=tuple(str(item) for item in artifact_refs),
        attributes={
            str(key): value
            for key, value in attributes.items()
        },
    )


class DeliveryTaskLeaseLost(RuntimeError):
    """A stale Delivery Worker must stop before recording a provider result."""


class DeliveryWorkerService:
    """Deliver committed Outbox facts independently of Agent execution."""

    def __init__(
        self,
        *,
        queue: DeliveryTaskQueue,
        channels: ChannelAdapterRegistry,
        node_id: str,
        concurrency: int,
        runtime: LeasedWorkerConfig,
        telemetry: PlatformTelemetry | None = None,
    ) -> None:
        if not node_id.strip() or concurrency < 1:
            raise ValueError("Delivery Worker identity or concurrency is invalid")
        self._queue = queue
        self._channels = channels
        self._node_id = node_id
        self._concurrency = concurrency
        self._runtime = runtime
        self._telemetry = telemetry
        self._recovery = RecoveryPolicy()
        self._stop = asyncio.Event()
        self._tasks: tuple[asyncio.Task[None], ...] = ()

    async def start(self) -> None:
        """Start local delivery slots; every node competes on the same SQL queue."""

        if self._tasks:
            return
        self._stop.clear()
        self._tasks = tuple(
            asyncio.create_task(
                self._run_slot(slot),
                name=f"delivery-worker:{self._node_id}:{slot}",
            ) for slot in range(self._concurrency))

    async def close(self) -> None:
        """Stop new claims and drain the currently leased deliveries."""

        self._stop.set()
        tasks, self._tasks = self._tasks, ()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_slot(self, slot: int) -> None:
        claim_failures = 0
        while not self._stop.is_set():
            try:
                processed = await self.run_once(slot)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                claim_failures += 1
                logger.error(
                    "Delivery Worker slot %s failed while claiming work with %s",
                    slot,
                    type(error).__name__,
                )
                delay = self._recovery.retry_delay_seconds(
                    operation_key=f"{self._node_id}:delivery-claim:{slot}",
                    attempt_count=claim_failures,
                    base_seconds=self._runtime.retry_base_seconds,
                    maximum_seconds=self._runtime.retry_max_seconds,
                    jitter_ratio=self._runtime.retry_jitter_ratio,
                )
                await self._wait_for_stop(delay)
                continue
            claim_failures = 0
            if not processed:
                await self._wait_for_stop(self._runtime.poll_interval_seconds)

    async def _wait_for_stop(self, delay_seconds: float) -> None:
        """Sleep interruptibly during idle polling and SQL recovery backoff."""

        try:
            await asyncio.wait_for(self._stop.wait(), delay_seconds)
        except TimeoutError:
            pass

    async def run_once(self, slot: int) -> bool:
        """Process at most one due reply for deterministic recovery checks."""

        if slot < 0 or slot >= self._concurrency:
            raise ValueError("Delivery Worker slot is outside configured concurrency")
        worker_id = f"{self._node_id}:delivery:{slot}"
        claim = await self._queue.claim(
            worker_id,
            lease_until=datetime.now(timezone.utc) + timedelta(seconds=self._runtime.lease_seconds),
        )
        if claim is None:
            return False
        started = datetime.now(timezone.utc)
        try:
            receipt = await self._deliver_while_owned(claim, worker_id)
        except asyncio.CancelledError:
            # Graceful shutdown leaves the lease for another node to reclaim.
            raise
        except DeliveryTaskLeaseLost:
            # The current owner or a later reclaimer owns the terminal update.
            logger.error("Delivery task %s lost its Worker lease", claim.message.outbox_id)
            self._record_metric(claim, "lease_lost", started)
            return True
        except Exception as error:
            decision = self._recovery.classify_delivery(error)
            await self._queue.fail(
                claim,
                worker_id=worker_id,
                decision=decision,
                next_attempt_at=self._next_attempt(claim, decision),
            )
            self._record_metric(claim, "error", started)
            return True
        await self._queue.complete(claim, worker_id=worker_id, receipt=receipt)
        self._record_metric(claim, "success", started)
        return True

    async def _deliver_while_owned(
        self,
        claim: DeliveryTaskClaim,
        worker_id: str,
    ) -> DeliveryReceipt:
        """Renew the SQL lease and cancel delivery after ownership loss."""

        stop = asyncio.Event()
        delivery = asyncio.create_task(
            self._deliver(claim),
            name=f"delivery-execute:{claim.message.outbox_id}",
        )
        renewal = asyncio.create_task(
            self._renew_while_running(claim, worker_id, stop),
            name=f"delivery-renew:{claim.message.outbox_id}",
        )
        try:
            done, _ = await asyncio.wait(
                {delivery, renewal},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if renewal in done:
                await renewal
                raise RuntimeError("Delivery task renewal ended unexpectedly")
            return await delivery
        finally:
            stop.set()
            if not delivery.done():
                delivery.cancel()
            await asyncio.gather(delivery, renewal, return_exceptions=True)

    async def _deliver(self, claim: DeliveryTaskClaim) -> DeliveryReceipt:
        """Resolve and invoke one provider-neutral Channel Adapter."""

        adapter = self._channels.resolve(claim.binding.channel_type)
        if claim.message.destination != claim.binding.channel_type:
            raise ValueError("Outbox destination does not match Channel Binding")
        parent = (None if self._telemetry is None else self._telemetry.extract_context(
            dict(claim.trace_context)))
        span = (self._telemetry.start_span(
            "channel.deliver",
            context=parent,
            attributes={
                "tenant.id": str(claim.context.tenant_id),
                "channel.type": claim.binding.channel_type,
                "request.id": claim.context.request_id,
                "retry.count": self._retry_count(claim) - 1,
            },
        ) if self._telemetry is not None else None)
        outgoing = outgoing_from_outbox(claim.message)
        if span is None:
            return await adapter.deliver(outgoing, claim.binding)
        with span:
            return await adapter.deliver(outgoing, claim.binding)

    async def _renew_while_running(
        self,
        claim: DeliveryTaskClaim,
        worker_id: str,
        stop: asyncio.Event,
    ) -> None:
        """Keep a long provider call owned by exactly one Delivery Worker."""

        interval = self._runtime.lease_seconds / 3
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                renewed = await self._queue.renew(
                    claim,
                    worker_id=worker_id,
                    lease_until=datetime.now(timezone.utc) +
                    timedelta(seconds=self._runtime.lease_seconds),
                )
                if not renewed:
                    raise DeliveryTaskLeaseLost(
                        f"Delivery task lease was lost: {claim.message.outbox_id}")

    def _next_attempt(
        self,
        claim: DeliveryTaskClaim,
        decision: RecoveryDecision,
    ) -> datetime | None:
        retry_count = self._retry_count(claim)
        if (decision.disposition is not FailureDisposition.RETRY
                or retry_count >= self._runtime.max_attempts):
            return None
        delay = self._recovery.retry_delay_seconds(
            operation_key=claim.message.outbox_id,
            attempt_count=retry_count,
            base_seconds=self._runtime.retry_base_seconds,
            maximum_seconds=self._runtime.retry_max_seconds,
            jitter_ratio=self._runtime.retry_jitter_ratio,
            retry_after_seconds=decision.retry_after_seconds,
        )
        return datetime.now(timezone.utc) + timedelta(seconds=delay)

    @staticmethod
    def _retry_count(claim: DeliveryTaskClaim) -> int:
        """Support old queue implementations while using the new replay budget."""

        # A claimed production row always has retry_count >= 1. The fallback
        # keeps custom adapters and rolling-upgrade nodes compatible until they
        # emit the new field.
        return claim.message.retry_count or claim.message.attempt_count

    def _record_metric(
        self,
        claim: DeliveryTaskClaim,
        result: str,
        started: datetime,
    ) -> None:
        if self._telemetry is not None:
            self._telemetry.record_im_delivery(
                channel_type=claim.binding.channel_type,
                result=result,
                duration_seconds=(datetime.now(timezone.utc) - started).total_seconds(),
            )

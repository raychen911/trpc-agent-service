import asyncio
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.gateway.contracts import AgentReply, GatewayResponse, NormalizedMessage
from trpc_service.metrics import PlatformMetrics, tracer
from trpc_service.storage.keys import idempotency_key
from trpc_service.storage.models import AgentExecution, InboundMessage, OutboxMessage


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class InboundRecord:
    id: str
    message: NormalizedMessage
    attempts: int
    status: str


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    record: InboundRecord
    created: bool


@dataclass(frozen=True, slots=True)
class ExecutionRecord:
    id: str
    status: str
    reply: AgentReply | None


class ExecutionUncertainError(RuntimeError):
    pass


class SqlInboundQueue:
    def __init__(self, factory: sessionmaker[Session]) -> None:
        self._factory = factory

    async def enqueue(self, message: NormalizedMessage) -> EnqueueResult:
        if "_trace_context" not in message.metadata:
            carrier: dict[str, str] = {}
            inject(carrier)
            message = replace(
                message,
                metadata={**dict(message.metadata), "_trace_context": carrier},
            )
        return await asyncio.to_thread(self._enqueue_sync, message)

    async def get(self, message_id: str) -> InboundRecord | None:
        return await asyncio.to_thread(self._get_sync, message_id)

    def _get_sync(self, message_id: str) -> InboundRecord | None:
        with self._factory() as session:
            row = session.get(InboundMessage, message_id)
            return self._record(row) if row is not None else None

    async def execution_id(self, message_id: str) -> str | None:
        return await asyncio.to_thread(self._execution_id_sync, message_id)

    def _execution_id_sync(self, message_id: str) -> str | None:
        with self._factory() as session:
            return session.scalar(
                select(AgentExecution.id).where(AgentExecution.inbound_message_id == message_id)
            )

    def _enqueue_sync(self, message: NormalizedMessage) -> EnqueueResult:
        key = idempotency_key(message.tenant_id, message.channel, message.external_message_id)
        try:
            with self._factory.begin() as session:
                row = InboundMessage(
                    tenant_id=message.tenant_id,
                    agent_app_id=message.agent_app_id,
                    channel=message.channel,
                    account_id=message.account_id,
                    external_message_id=message.external_message_id,
                    payload=asdict(message),
                    available_at=_utcnow(),
                )
                session.add(row)
                session.flush()
                execution = AgentExecution(
                    inbound_message_id=row.id,
                    tenant_id=message.tenant_id,
                    agent_app_id=message.agent_app_id,
                    session_key=message.session_id,
                    idempotency_key=key,
                    trace_id=message.trace_id,
                )
                session.add(execution)
                session.flush()
                result = self._record(row)
            return EnqueueResult(result, True)
        except IntegrityError:
            with self._factory() as session:
                row = session.scalar(
                    select(InboundMessage).where(
                        InboundMessage.tenant_id == message.tenant_id,
                        InboundMessage.channel == message.channel,
                        InboundMessage.external_message_id == message.external_message_id,
                    )
                )
                if row is None:
                    raise
                return EnqueueResult(self._record(row), False)

    async def claim_batch(self, worker_id: str, limit: int = 20) -> tuple[InboundRecord, ...]:
        return await asyncio.to_thread(self._claim_batch_sync, worker_id, limit)

    def _claim_batch_sync(self, worker_id: str, limit: int) -> tuple[InboundRecord, ...]:
        now = _utcnow()
        lease_until = now + timedelta(seconds=90)
        claimed = []
        with self._factory.begin() as session:
            eligible = or_(
                and_(
                    InboundMessage.status.in_(("pending", "failed")),
                    InboundMessage.available_at <= now,
                ),
                and_(
                    InboundMessage.status == "processing",
                    InboundMessage.locked_until < now,
                ),
            )
            statement = (
                select(InboundMessage)
                .where(eligible)
                .order_by(InboundMessage.created_at)
                .limit(limit)
            )
            if session.bind and session.bind.dialect.name == "postgresql":
                statement = statement.with_for_update(skip_locked=True)
            for row in session.scalars(statement):
                result = session.execute(
                    update(InboundMessage)
                    .where(InboundMessage.id == row.id, eligible)
                    .values(
                        status="processing",
                        attempts=InboundMessage.attempts + 1,
                        locked_by=worker_id,
                        locked_until=lease_until,
                    )
                    .execution_options(synchronize_session=False)
                )
                if result.rowcount == 1:
                    session.flush()
                    session.refresh(row)
                    claimed.append(self._record(row))
        return tuple(claimed)

    async def complete(self, record: InboundRecord, result: GatewayResponse) -> None:
        await asyncio.to_thread(self._complete_sync, record, result)

    def _complete_sync(self, record: InboundRecord, result: GatewayResponse) -> None:
        now = _utcnow()
        with self._factory.begin() as session:
            row = session.get(InboundMessage, record.id)
            if row is None:
                return
            response = asdict(result)
            row.status = "completed"
            row.result = response
            row.locked_by = None
            row.locked_until = None
            execution = session.scalar(
                select(AgentExecution).where(AgentExecution.inbound_message_id == record.id)
            )
            if result.reply_text:
                outbox = OutboxMessage(
                    tenant_id=record.message.tenant_id,
                    topic=f"im.reply.{record.message.channel}",
                    payload={
                        "account_id": record.message.account_id,
                        "conversation_id": record.message.conversation_id,
                        "sender_user_id": record.message.sender_user_id,
                        "text": result.reply_text,
                        "metadata": dict(record.message.metadata),
                        "trace_id": record.message.trace_id,
                        "delivery": dict(result.delivery),
                    },
                    dedupe_key=f"im-reply:{record.id}",
                    available_at=now,
                )
                session.add(outbox)
                if execution is not None:
                    execution.status = "delivery_enqueued"
            elif execution is not None:
                execution.status = "platform_committed"

    async def fail(
        self,
        record: InboundRecord,
        error: str,
        *,
        uncertain: bool = False,
        max_attempts: int = 5,
    ) -> None:
        await asyncio.to_thread(self._fail_sync, record, error, uncertain, max_attempts)

    def _fail_sync(
        self,
        record: InboundRecord,
        error: str,
        uncertain: bool,
        max_attempts: int,
    ) -> None:
        terminal = uncertain or record.attempts >= max_attempts
        with self._factory.begin() as session:
            row = session.get(InboundMessage, record.id)
            if row is None:
                return
            row.status = "uncertain" if uncertain else "failed"
            row.available_at = _utcnow() + timedelta(seconds=min(300, 2 ** min(record.attempts, 8)))
            row.locked_by = None
            row.locked_until = None
            row.last_error = error[:4000]
            if terminal and not uncertain:
                row.available_at = _utcnow() + timedelta(days=3650)

    @staticmethod
    def _record(row: InboundMessage) -> InboundRecord:
        return InboundRecord(
            id=row.id,
            message=NormalizedMessage(**row.payload),
            attempts=row.attempts,
            status=row.status,
        )


class ExecutionLedger:
    def __init__(self, factory: sessionmaker[Session]) -> None:
        self._factory = factory

    async def prepare(self, message: NormalizedMessage) -> ExecutionRecord:
        return await asyncio.to_thread(self._prepare_sync, message)

    def _prepare_sync(self, message: NormalizedMessage) -> ExecutionRecord:
        key = idempotency_key(message.tenant_id, message.channel, message.external_message_id)
        with self._factory.begin() as session:
            row = session.scalar(
                select(AgentExecution).where(AgentExecution.idempotency_key == key)
            )
            if row is None:
                row = AgentExecution(
                    tenant_id=message.tenant_id,
                    agent_app_id=message.agent_app_id,
                    session_key=message.session_id,
                    idempotency_key=key,
                    trace_id=message.trace_id,
                )
                session.add(row)
                session.flush()
            return self._execution_record(row)

    async def mark_runner_started(self, execution_id: str) -> None:
        await asyncio.to_thread(self._set_status, execution_id, "runner_started", None)

    async def mark_runner_completed(self, execution_id: str, reply: AgentReply) -> None:
        await asyncio.to_thread(self._set_status, execution_id, "runner_completed", asdict(reply))

    async def mark_uncertain(self, execution_id: str, error: str) -> None:
        await asyncio.to_thread(self._set_status, execution_id, "uncertain", None, error)

    async def get(self, execution_id: str) -> ExecutionRecord | None:
        return await asyncio.to_thread(self._get_sync, execution_id)

    def _get_sync(self, execution_id: str) -> ExecutionRecord | None:
        with self._factory() as session:
            row = session.get(AgentExecution, execution_id)
            return self._execution_record(row) if row is not None else None

    async def resolve_uncertain(self, execution_id: str, action: str) -> ExecutionRecord:
        return await asyncio.to_thread(self._resolve_uncertain_sync, execution_id, action)

    def _resolve_uncertain_sync(self, execution_id: str, action: str) -> ExecutionRecord:
        if action not in {"retry", "fail"}:
            raise ValueError("action must be 'retry' or 'fail'")
        with self._factory.begin() as session:
            row = session.get(AgentExecution, execution_id)
            if row is None:
                raise LookupError("agent execution not found")
            if row.status != "uncertain":
                raise ValueError("only uncertain executions require manual resolution")
            row.status = "pending" if action == "retry" else "failed"
            row.last_error = None if action == "retry" else row.last_error
            if action == "retry":
                row.runner_reply = None
            if action == "retry" and row.inbound_message_id:
                inbound = session.get(InboundMessage, row.inbound_message_id)
                if inbound is not None:
                    inbound.status = "failed"
                    inbound.available_at = _utcnow()
                    inbound.locked_by = None
                    inbound.locked_until = None
            session.flush()
            return self._execution_record(row)

    def _set_status(
        self,
        execution_id: str,
        status: str,
        reply: dict[str, Any] | None,
        error: str | None = None,
    ) -> None:
        with self._factory.begin() as session:
            row = session.get(AgentExecution, execution_id)
            if row is None:
                raise LookupError("agent execution not found")
            row.status = status
            if reply is not None:
                row.runner_reply = reply
            row.last_error = error

    @staticmethod
    def _execution_record(row: AgentExecution) -> ExecutionRecord:
        reply = AgentReply(**row.runner_reply) if row.runner_reply else None
        return ExecutionRecord(row.id, row.status, reply)


class InboundWorker:
    def __init__(
        self,
        queue: SqlInboundQueue,
        router,
        worker_id: str,
        metrics: PlatformMetrics | None = None,
    ) -> None:
        self._queue = queue
        self._router = router
        self._worker_id = worker_id
        self._metrics = metrics

    async def poll_once(self, limit: int = 20) -> int:
        records = await self._queue.claim_batch(self._worker_id, limit)
        for record in records:
            carrier = record.message.metadata.get("_trace_context", {})
            context = extract(carrier) if isinstance(carrier, dict) else None
            try:
                with tracer.start_as_current_span(
                    "im.consume", context=context, kind=trace.SpanKind.CONSUMER
                ) as span:
                    span.set_attribute("messaging.system", record.message.channel)
                    span.set_attribute("messaging.message.id", record.message.external_message_id)
                    result = await self._router.dispatch(record.message)
                    await self._queue.complete(record, result)
            except ExecutionUncertainError as error:
                await self._queue.fail(record, str(error), uncertain=True)
                if self._metrics is not None:
                    self._metrics.inbound_messages.labels(record.message.channel, "uncertain").inc()
            except Exception as error:
                await self._queue.fail(record, str(error))
                if self._metrics is not None:
                    self._metrics.inbound_messages.labels(record.message.channel, "failed").inc()
            else:
                if self._metrics is not None:
                    self._metrics.inbound_messages.labels(record.message.channel, "completed").inc()
        return len(records)

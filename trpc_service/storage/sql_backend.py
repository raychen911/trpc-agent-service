import asyncio
import copy
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import Select, and_, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.storage.contracts import (
    AuditEntry,
    EventRecord,
    OutboxRecord,
    SessionIdentity,
    SessionSnapshot,
    SummaryRecord,
    TurnCommit,
    TurnCommitResult,
)
from trpc_service.storage.exceptions import (
    DuplicateMessageError,
    StorageError,
    VersionConflictError,
)
from trpc_service.storage.models import (
    AgentExecution,
    AgentSession,
    AuditLog,
    Memory,
    OutboxDeadLetter,
    OutboxMessage,
    SessionEvent,
    Summary,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SqlDataPlane:
    """Durable SQL store for session events, summaries, audit, and outbox."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._factory = session_factory

    @staticmethod
    def _session_query(identity: SessionIdentity) -> Select[tuple[AgentSession]]:
        return select(AgentSession).where(
            AgentSession.tenant_id == identity.tenant_id,
            AgentSession.agent_app_id == identity.agent_app_id,
            AgentSession.session_key == identity.session_id,
        )

    @staticmethod
    def _snapshot(row: AgentSession, identity: SessionIdentity) -> SessionSnapshot:
        return SessionSnapshot(
            identity=identity,
            state=copy.deepcopy(row.state),
            version=row.version,
            updated_at=row.updated_at,
        )

    async def get_session(self, identity: SessionIdentity) -> SessionSnapshot | None:
        return await asyncio.to_thread(self._get_session_sync, identity)

    def _get_session_sync(self, identity: SessionIdentity) -> SessionSnapshot | None:
        with self._factory() as session:
            row = session.scalar(self._session_query(identity))
            if row is None:
                return None
            self._ensure_user(row, identity)
            return self._snapshot(row, identity)

    async def create_session(self, identity: SessionIdentity) -> SessionSnapshot:
        return await asyncio.to_thread(self._create_session_sync, identity)

    def _create_session_sync(self, identity: SessionIdentity) -> SessionSnapshot:
        try:
            with self._factory.begin() as session:
                row = session.scalar(self._session_query(identity))
                if row is None:
                    row = AgentSession(
                        tenant_id=identity.tenant_id,
                        agent_app_id=identity.agent_app_id,
                        user_id=identity.user_id,
                        session_key=identity.session_id,
                        state={},
                        version=0,
                    )
                    session.add(row)
                    session.flush()
                self._ensure_user(row, identity)
                snapshot = self._snapshot(row, identity)
            return snapshot
        except IntegrityError as error:
            raise StorageError("could not create session") from error

    @staticmethod
    def _ensure_user(row: AgentSession, identity: SessionIdentity) -> None:
        if row.user_id != identity.user_id:
            raise StorageError("session belongs to a different user")

    async def compare_and_swap_state(
        self,
        identity: SessionIdentity,
        expected_version: int,
        next_state: Mapping[str, object],
    ) -> SessionSnapshot:
        return await asyncio.to_thread(
            self._compare_and_swap_state_sync,
            identity,
            expected_version,
            dict(next_state),
        )

    def _compare_and_swap_state_sync(
        self,
        identity: SessionIdentity,
        expected_version: int,
        next_state: Mapping[str, object],
    ) -> SessionSnapshot:
        now = _utcnow()
        with self._factory.begin() as session:
            row = session.scalar(self._session_query(identity))
            if row is None:
                if expected_version != 0:
                    raise VersionConflictError("session does not exist")
                row = AgentSession(
                    tenant_id=identity.tenant_id,
                    agent_app_id=identity.agent_app_id,
                    user_id=identity.user_id,
                    session_key=identity.session_id,
                    state={},
                    version=0,
                )
                session.add(row)
                session.flush()
            self._ensure_user(row, identity)
            result = session.execute(
                update(AgentSession)
                .where(AgentSession.id == row.id, AgentSession.version == expected_version)
                .values(state=dict(next_state), version=expected_version + 1, updated_at=now)
            )
            if result.rowcount != 1:
                raise VersionConflictError(
                    f"expected session version {expected_version}, got {row.version}"
                )
            snapshot = SessionSnapshot(
                identity, copy.deepcopy(next_state), expected_version + 1, now
            )
        return snapshot

    async def commit_turn(self, commit: TurnCommit) -> TurnCommitResult:
        return await asyncio.to_thread(self._commit_turn_sync, commit)

    def _commit_turn_sync(self, commit: TurnCommit) -> TurnCommitResult:
        try:
            with self._factory.begin() as session:
                row = session.scalar(self._session_query(commit.identity).with_for_update())
                if row is None:
                    if commit.expected_version != 0:
                        raise VersionConflictError("session does not exist")
                    row = AgentSession(
                        tenant_id=commit.identity.tenant_id,
                        agent_app_id=commit.identity.agent_app_id,
                        user_id=commit.identity.user_id,
                        session_key=commit.identity.session_id,
                        state={},
                        version=0,
                    )
                    session.add(row)
                    session.flush()
                self._ensure_user(row, commit.identity)
                if row.version != commit.expected_version:
                    raise VersionConflictError(
                        f"expected session version {commit.expected_version}, got {row.version}"
                    )

                next_version = commit.expected_version + 1
                now = _utcnow()

                # 1. Event is inserted first.
                event_row = SessionEvent(
                    tenant_id=commit.identity.tenant_id,
                    session_id=row.id,
                    sequence_no=next_version,
                    event_type=commit.event.event_type,
                    role=commit.event.role,
                    payload=dict(commit.event.payload),
                    channel_type=commit.event.channel,
                    external_message_id=commit.event.external_message_id,
                    trace_id=commit.event.trace_id,
                )
                session.add(event_row)
                session.flush()

                # 2. Session state advances with a CAS predicate.
                result = session.execute(
                    update(AgentSession)
                    .where(
                        AgentSession.id == row.id,
                        AgentSession.version == commit.expected_version,
                    )
                    .values(
                        state=dict(commit.next_state),
                        version=next_version,
                        updated_at=now,
                    )
                )
                if result.rowcount != 1:
                    raise VersionConflictError("session changed during turn commit")

                # 3. Summary observes the newly committed event sequence.
                summary_row = None
                if commit.summary_content is not None:
                    summary_row = Summary(
                        tenant_id=commit.identity.tenant_id,
                        session_id=row.id,
                        version=next_version,
                        through_sequence=next_version,
                        content=commit.summary_content,
                    )
                    session.add(summary_row)
                    session.flush()

                # 4. Durable Memory rows are written before their vector side effects.
                generated_outbox = []
                for memory in commit.memories:
                    memory_row = session.scalar(
                        select(Memory).where(
                            Memory.tenant_id == commit.identity.tenant_id,
                            Memory.agent_app_id == commit.identity.agent_app_id,
                            Memory.user_id == commit.identity.user_id,
                            Memory.memory_key == memory.memory_key,
                        )
                    )
                    if memory_row is None:
                        memory_row = Memory(
                            tenant_id=commit.identity.tenant_id,
                            agent_app_id=commit.identity.agent_app_id,
                            user_id=commit.identity.user_id,
                            memory_key=memory.memory_key,
                            content=memory.text,
                            topics=list(memory.topics),
                            version=1,
                        )
                        session.add(memory_row)
                    else:
                        memory_row.content = memory.text
                        memory_row.topics = list(memory.topics)
                        memory_row.version += 1
                    session.flush()
                    generated_outbox.append(
                        (
                            "memory.upsert",
                            {
                                "agent_app_id": commit.identity.agent_app_id,
                                "user_id": commit.identity.user_id,
                                "memory_id": memory.memory_key,
                                "text": memory.text,
                                "metadata": dict(memory.metadata),
                            },
                            (
                                f"memory:{commit.identity.tenant_id}:"
                                f"{commit.identity.agent_app_id}:{commit.identity.user_id}:"
                                f"{memory.memory_key}:v{memory_row.version}"
                            ),
                        )
                    )

                # 5. Vector side effects are enqueued in the same transaction.
                outbox_rows = []
                for item in commit.outbox:
                    existing = session.scalar(
                        select(OutboxMessage).where(OutboxMessage.dedupe_key == item.dedupe_key)
                    )
                    if existing is not None:
                        outbox_rows.append(existing)
                        continue
                    outbox_row = OutboxMessage(
                        tenant_id=commit.identity.tenant_id,
                        topic=item.topic,
                        payload=dict(item.payload),
                        dedupe_key=item.dedupe_key,
                        available_at=now,
                    )
                    session.add(outbox_row)
                    session.flush()
                    outbox_rows.append(outbox_row)
                for topic, payload, dedupe_key in generated_outbox:
                    outbox_row = OutboxMessage(
                        tenant_id=commit.identity.tenant_id,
                        topic=topic,
                        payload=payload,
                        dedupe_key=dedupe_key,
                        available_at=now,
                    )
                    session.add(outbox_row)
                    session.flush()
                    outbox_rows.append(outbox_row)

                # The cached Runner result and platform Event/State commit share this transaction.
                if commit.execution_id is not None:
                    execution = session.get(AgentExecution, commit.execution_id)
                    if execution is None:
                        raise StorageError("agent execution ledger row not found")
                    execution.status = "platform_committed"
                    execution.platform_event_id = event_row.id

                result_value = TurnCommitResult(
                    session=SessionSnapshot(
                        commit.identity,
                        copy.deepcopy(dict(commit.next_state)),
                        next_version,
                        now,
                    ),
                    event=self._event_record(event_row, commit.identity.session_id),
                    summary=(
                        self._summary_record(summary_row, commit.identity.session_id)
                        if summary_row is not None
                        else None
                    ),
                    outbox=tuple(self._outbox_record(item) for item in outbox_rows),
                )
            return result_value
        except IntegrityError as error:
            if commit.event.channel and commit.event.external_message_id:
                raise DuplicateMessageError(
                    "external message has already been committed"
                ) from error
            raise StorageError("turn transaction failed") from error

    @staticmethod
    def _event_record(row: SessionEvent, business_session_id: str) -> EventRecord:
        return EventRecord(
            id=row.id,
            tenant_id=row.tenant_id,
            session_id=business_session_id,
            sequence_no=row.sequence_no,
            event_type=row.event_type,
            role=row.role,
            payload=copy.deepcopy(row.payload),
            trace_id=row.trace_id,
            channel=row.channel_type,
            external_message_id=row.external_message_id,
            created_at=row.created_at,
        )

    @staticmethod
    def _summary_record(row: Summary, business_session_id: str) -> SummaryRecord:
        return SummaryRecord(
            id=row.id,
            tenant_id=row.tenant_id,
            session_id=business_session_id,
            version=row.version,
            through_sequence=row.through_sequence,
            content=row.content,
            created_at=row.created_at,
        )

    @staticmethod
    def _outbox_record(row: OutboxMessage) -> OutboxRecord:
        return OutboxRecord(
            id=row.id,
            tenant_id=row.tenant_id,
            topic=row.topic,
            payload=copy.deepcopy(row.payload),
            dedupe_key=row.dedupe_key,
            attempts=row.attempts,
            status=row.status,
            available_at=row.available_at,
            created_at=row.created_at,
        )

    async def list_events(
        self, identity: SessionIdentity, after_sequence: int = -1
    ) -> Sequence[EventRecord]:
        return await asyncio.to_thread(self._list_events_sync, identity, after_sequence)

    def _list_events_sync(
        self, identity: SessionIdentity, after_sequence: int
    ) -> Sequence[EventRecord]:
        with self._factory() as session:
            session_row = session.scalar(self._session_query(identity))
            if session_row is None:
                return ()
            rows = session.scalars(
                select(SessionEvent)
                .where(
                    SessionEvent.session_id == session_row.id,
                    SessionEvent.sequence_no > after_sequence,
                )
                .order_by(SessionEvent.sequence_no)
            )
            return tuple(self._event_record(row, identity.session_id) for row in rows)

    async def latest_summary(self, identity: SessionIdentity) -> SummaryRecord | None:
        return await asyncio.to_thread(self._latest_summary_sync, identity)

    def _latest_summary_sync(self, identity: SessionIdentity) -> SummaryRecord | None:
        with self._factory() as session:
            session_row = session.scalar(self._session_query(identity))
            if session_row is None:
                return None
            row = session.scalar(
                select(Summary)
                .where(Summary.session_id == session_row.id)
                .order_by(Summary.version.desc())
                .limit(1)
            )
            return self._summary_record(row, identity.session_id) if row else None

    async def append_audit(self, entry: AuditEntry) -> str:
        return await asyncio.to_thread(self._append_audit_sync, entry)

    def _append_audit_sync(self, entry: AuditEntry) -> str:
        with self._factory.begin() as session:
            row = AuditLog(
                tenant_id=entry.tenant_id,
                agent_app_id=entry.agent_app_id,
                channel=entry.channel,
                user_id=entry.user_id,
                session_id=entry.session_id,
                agent_name=entry.agent_name,
                tool_name=entry.tool_name,
                decision=entry.decision,
                latency_ms=entry.latency_ms,
                error_type=entry.error_type,
                cost=Decimal(str(entry.cost)),
                trace_id=entry.trace_id,
                request_id=entry.request_id,
                details=dict(entry.details),
            )
            session.add(row)
            session.flush()
            record_id = row.id
        return record_id

    async def claim_batch(self, worker_id: str, limit: int = 100) -> Sequence[OutboxRecord]:
        return await asyncio.to_thread(self._claim_batch_sync, worker_id, limit)

    def _claim_batch_sync(self, worker_id: str, limit: int) -> Sequence[OutboxRecord]:
        now = _utcnow()
        lease_until = now + timedelta(seconds=60)
        claimed: list[OutboxRecord] = []
        with self._factory.begin() as session:
            eligible = or_(
                and_(
                    OutboxMessage.status.in_(("pending", "failed")),
                    OutboxMessage.available_at <= now,
                ),
                and_(
                    OutboxMessage.status == "processing",
                    OutboxMessage.locked_until < now,
                ),
            )
            statement = (
                select(OutboxMessage)
                .where(eligible)
                .order_by(OutboxMessage.created_at)
                .limit(limit)
            )
            if session.bind and session.bind.dialect.name == "postgresql":
                statement = statement.with_for_update(skip_locked=True)
            for row in session.scalars(statement):
                result = session.execute(
                    update(OutboxMessage)
                    .where(
                        OutboxMessage.id == row.id,
                        eligible,
                    )
                    .values(
                        status="processing",
                        attempts=OutboxMessage.attempts + 1,
                        locked_by=worker_id,
                        locked_until=lease_until,
                    )
                    .execution_options(synchronize_session=False)
                )
                if result.rowcount == 1:
                    session.flush()
                    session.refresh(row)
                    claimed.append(self._outbox_record(row))
        return tuple(claimed)

    async def mark_processed(self, record_id: str) -> None:
        await asyncio.to_thread(self._mark_processed_sync, record_id)

    def _mark_processed_sync(self, record_id: str) -> None:
        with self._factory.begin() as session:
            session.execute(
                update(OutboxMessage)
                .where(OutboxMessage.id == record_id)
                .values(
                    status="processed",
                    processed_at=_utcnow(),
                    locked_by=None,
                    locked_until=None,
                    last_error=None,
                )
            )

    async def mark_failed(self, record_id: str, error: str) -> None:
        await asyncio.to_thread(self._mark_failed_sync, record_id, error)

    def _mark_failed_sync(self, record_id: str, error: str) -> None:
        with self._factory.begin() as session:
            row = session.get(OutboxMessage, record_id)
            if row is None:
                return
            delay = min(300, 2 ** min(row.attempts, 8))
            row.status = "failed"
            row.available_at = _utcnow() + timedelta(seconds=delay)
            row.locked_by = None
            row.locked_until = None
            row.last_error = error[:4000]

    async def mark_dead_letter(self, record_id: str, error: str) -> None:
        await asyncio.to_thread(self._mark_dead_letter_sync, record_id, error)

    def _mark_dead_letter_sync(self, record_id: str, error: str) -> None:
        with self._factory.begin() as session:
            row = session.get(OutboxMessage, record_id)
            if row is None:
                return
            session.add(
                OutboxDeadLetter(
                    original_outbox_id=row.id,
                    tenant_id=row.tenant_id,
                    topic=row.topic,
                    payload=copy.deepcopy(row.payload),
                    attempts=row.attempts,
                    last_error=error[:4000],
                )
            )
            row.status = "processed"
            row.processed_at = _utcnow()
            row.locked_by = None
            row.locked_until = None
            row.last_error = f"DEAD_LETTER: {error}"[:4000]

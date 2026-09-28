"""PostgreSQL execution journal for Session, Inbox and Outbox facts."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.storage.adapters.postgresql_auxiliary import (
    PostgreSQLAuditStore,
    PostgreSQLMemoryStore,
    PostgreSQLSummaryStore,
)
from trpc_service.storage.errors import (
    ExecutionAlreadyRunning,
    IdempotencyConflict,
    SessionVersionConflict,
    StaleExecutionLease,
)
from trpc_service.storage.orm import as_utc, utc_now
from trpc_service.storage.ports import OutboxStore, SessionStore
from trpc_service.storage.runtime_orm import (
    AgentSession,
    InboxMessageRow,
    OutboxAttemptRow,
    OutboxMessageRow,
    RunnerRequestRow,
    SessionEventRow,
    SessionExecutionFence,
)
from trpc_service.storage.types import (
    ExecutionClaim,
    ExecutionCommit,
    InboxClaimRequest,
    OutboxMessage,
    SessionEvent,
    SessionSnapshot,
)
from trpc_service.tenant.context import TenantContext


class PostgreSQLExecutionStore(SessionStore, OutboxStore):
    """Keep the transactionally coupled Session, Inbox and Outbox journal together."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @staticmethod
    def _session_query(context: TenantContext, session_id: str) -> Select[tuple[AgentSession]]:
        return select(AgentSession).where(
            AgentSession.tenant_id == context.tenant_id,
            AgentSession.agent_app_id == context.agent_app_id,
            AgentSession.session_id == session_id,
        )

    async def _claim_session_fence(
        self,
        database: AsyncSession,
        context: TenantContext,
        session_id: str,
        worker_id: str,
        lease_until: datetime,
        now: datetime,
    ) -> int:
        """Lock one Session fence row and issue its next monotonic token."""

        query = select(SessionExecutionFence).where(
            SessionExecutionFence.tenant_id == context.tenant_id,
            SessionExecutionFence.agent_app_id == context.agent_app_id,
            SessionExecutionFence.session_id == session_id,
        )
        fence = await database.scalar(query.with_for_update())
        if fence is None:
            session = await database.scalar(
                self._session_query(context, session_id).with_for_update())
            candidate = SessionExecutionFence(
                tenant_id=context.tenant_id,
                agent_app_id=context.agent_app_id,
                session_id=session_id,
                issued_token=(0 if session is None or session.last_fencing_token is None else
                              session.last_fencing_token),
            )
            try:
                async with database.begin_nested():
                    database.add(candidate)
                    await database.flush()
            except IntegrityError:
                # A different Inbox for this Session created the authority row.
                fence = await database.scalar(query.with_for_update())
                if fence is None:
                    raise
            else:
                fence = candidate
        if fence.lease_until is not None and as_utc(fence.lease_until) > now:
            raise ExecutionAlreadyRunning("Session is owned by another Worker")
        fence.issued_token += 1
        fence.lease_owner = worker_id
        fence.lease_until = lease_until
        await database.flush()
        return fence.issued_token

    async def _snapshot(
        self,
        database: AsyncSession,
        context: TenantContext,
        session_id: str,
        *,
        version: int | None = None,
        state: dict[str, object] | None = None,
    ) -> SessionSnapshot | None:
        row = await database.scalar(self._session_query(context, session_id))
        if row is None:
            return None
        snapshot_version = row.version if version is None else version
        events = (await database.scalars(
            select(SessionEventRow).where(
                SessionEventRow.tenant_id == context.tenant_id,
                SessionEventRow.agent_app_id == context.agent_app_id,
                SessionEventRow.session_id == session_id,
                SessionEventRow.committed_version <= snapshot_version,
            ).order_by(SessionEventRow.seq_no))).all()
        return SessionSnapshot(
            session_id=session_id,
            version=snapshot_version,
            events=tuple(
                SessionEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    occurred_at=event.occurred_at,
                    payload=event.payload,
                ) for event in events),
            state=row.state if state is None else state,
        )

    async def load(
        self,
        context: TenantContext,
        session_id: str,
    ) -> SessionSnapshot | None:
        """Load a Session snapshot from the mandatory tenant and Agent scope."""

        async with self._sessions() as database:
            return await self._snapshot(database, context, session_id)

    @staticmethod
    def _inbox_query(
        context: TenantContext,
        request: InboxClaimRequest,
    ) -> Select[tuple[InboxMessageRow]]:
        return select(InboxMessageRow).where(
            InboxMessageRow.tenant_id == context.tenant_id,
            InboxMessageRow.binding_id == request.binding_id,
            InboxMessageRow.external_message_id == request.external_message_id,
        )

    @staticmethod
    def _legacy_inbox_query(
        context: TenantContext,
        request: InboxClaimRequest,
    ) -> Select[tuple[InboxMessageRow]]:
        """Find a migrated Inbox whose old schema had no usable Binding ID."""

        return select(InboxMessageRow).where(
            InboxMessageRow.tenant_id == context.tenant_id,
            InboxMessageRow.agent_app_id == context.agent_app_id,
            InboxMessageRow.binding_id == UUID(int=0),
            InboxMessageRow.external_message_id == request.external_message_id,
            InboxMessageRow.id_source == "LEGACY",
        )

    async def _replayed_claim(
        self,
        database: AsyncSession,
        context: TenantContext,
        inbox: InboxMessageRow,
    ) -> ExecutionClaim:
        """Rebuild an exact idempotent claim result without re-running the Agent."""

        if inbox.committed_session_version is None or inbox.result_state is None:
            raise RuntimeError("completed Inbox has no committed Session snapshot")
        snapshot = await self._snapshot(
            database,
            context,
            inbox.session_id,
            version=inbox.committed_session_version,
            state=inbox.result_state,
        )
        if snapshot is None:
            raise RuntimeError("completed Inbox references a missing Session")
        outbox_ids = tuple((await database.scalars(
            select(OutboxMessageRow.outbox_id).where(
                OutboxMessageRow.tenant_id == context.tenant_id,
                OutboxMessageRow.agent_app_id == context.agent_app_id,
                OutboxMessageRow.request_id == inbox.request_id,
            ).order_by(OutboxMessageRow.sequence_no))).all())
        return ExecutionClaim(
            inbox_id=str(inbox.inbox_id),
            request_id=inbox.request_id,
            session_version=snapshot.version,
            replayed=True,
            completed=snapshot,
            committed_outbox_ids=outbox_ids,
        )

    async def claim_execution(
        self,
        context: TenantContext,
        request: InboxClaimRequest,
        worker_id: str,
        lease_until: datetime,
    ) -> ExecutionClaim:
        """Atomically claim an inbound message or return its prior result."""

        if not worker_id.strip():
            raise ValueError("execution worker ID cannot be empty")
        now = utc_now()
        if as_utc(lease_until) <= now:
            raise ValueError("execution lease must expire in the future")

        async with self._sessions.begin() as database:
            inbox = await database.scalar(self._inbox_query(context, request).with_for_update())
            if inbox is None:
                # Old rows without a Binding prefix were quarantined under the
                # zero UUID. Match them only inside the same tenant and Agent.
                inbox = await database.scalar(
                    self._legacy_inbox_query(context, request).with_for_update())
            if inbox is None:
                inbox = InboxMessageRow(
                    tenant_id=context.tenant_id,
                    binding_id=request.binding_id,
                    agent_app_id=context.agent_app_id,
                    external_message_id=request.external_message_id,
                    payload_hash=request.payload_hash,
                    id_source=request.id_source,
                    request_id=context.request_id,
                    trace_id=context.trace_id,
                    session_id=request.session_id,
                    status="RUNNING",
                    attempt_count=1,
                    lease_owner=worker_id,
                    lease_until=lease_until,
                    received_at=request.received_at,
                    started_at=now,
                )
                try:
                    async with database.begin_nested():
                        database.add(inbox)
                        await database.flush()
                except IntegrityError:
                    # A concurrent insert won the provider identity unique key.
                    inbox = await database.scalar(
                        self._inbox_query(context, request).with_for_update())
                    if inbox is None:
                        raise
                else:
                    fencing_token = await self._claim_session_fence(
                        database,
                        context,
                        request.session_id,
                        worker_id,
                        lease_until,
                        now,
                    )
                    database.add(
                        RunnerRequestRow(
                            tenant_id=context.tenant_id,
                            request_id=context.request_id,
                            agent_app_id=context.agent_app_id,
                            inbox_id=inbox.inbox_id,
                            session_id=request.session_id,
                            config_version=context.config_version,
                            status="RUNNING",
                            fencing_token=fencing_token,
                            started_at=now,
                        ))
                    return ExecutionClaim(
                        inbox_id=str(inbox.inbox_id),
                        request_id=inbox.request_id,
                        fencing_token=fencing_token,
                        session_version=0,
                    )

            if inbox.status in {"SUCCEEDED", "REPLIED"}:
                # Legacy tables did not retain normalized content, so their
                # synthetic migration hash cannot be compared to a new payload.
                if inbox.id_source != "LEGACY" and inbox.payload_hash != request.payload_hash:
                    raise IdempotencyConflict(
                        "external message ID was reused with a different payload")
                return await self._replayed_claim(database, context, inbox)
            if inbox.payload_hash != request.payload_hash:
                raise IdempotencyConflict("external message ID was reused with a different payload")
            if inbox.status == "PERMANENT_FAILED":
                raise RuntimeError("Inbox message has permanently failed")
            if (inbox.status == "RUNNING" and inbox.lease_until is not None
                    and as_utc(inbox.lease_until) > now):
                raise ExecutionAlreadyRunning("Inbox message is owned by another Worker")
            if inbox.next_attempt_at is not None and as_utc(inbox.next_attempt_at) > now:
                raise ExecutionAlreadyRunning("Inbox retry backoff has not elapsed")

            fencing_token = await self._claim_session_fence(
                database,
                context,
                request.session_id,
                worker_id,
                lease_until,
                now,
            )
            inbox.status = "RUNNING"
            inbox.attempt_count += 1
            inbox.lease_owner = worker_id
            inbox.lease_until = lease_until
            inbox.next_attempt_at = None
            inbox.started_at = now
            inbox.last_error_code = None
            inbox.last_error_summary = None
            session_version = await database.scalar(
                select(AgentSession.version).where(
                    AgentSession.tenant_id == context.tenant_id,
                    AgentSession.agent_app_id == context.agent_app_id,
                    AgentSession.session_id == request.session_id,
                ))
            runner = await database.scalar(
                select(RunnerRequestRow).where(
                    RunnerRequestRow.tenant_id == context.tenant_id,
                    RunnerRequestRow.request_id == inbox.request_id,
                ).with_for_update())
            if runner is not None:
                runner.status = "RUNNING"
                runner.attempt_count += 1
                runner.started_at = now
                runner.fencing_token = fencing_token
                runner.last_error = None
            return ExecutionClaim(
                inbox_id=str(inbox.inbox_id),
                request_id=inbox.request_id,
                fencing_token=fencing_token,
                session_version=session_version or 0,
            )

    async def renew_execution(
        self,
        context: TenantContext,
        inbox_id: str,
        *,
        worker_id: str,
        fencing_token: int,
        lease_until: datetime,
    ) -> bool:
        """Atomically extend the Inbox and authoritative Session fence leases."""

        now = utc_now()
        if as_utc(lease_until) <= now:
            raise ValueError("renewed execution lease must expire in the future")
        async with self._sessions.begin() as database:
            inbox = await database.scalar(
                select(InboxMessageRow).where(
                    InboxMessageRow.tenant_id == context.tenant_id,
                    InboxMessageRow.agent_app_id == context.agent_app_id,
                    InboxMessageRow.inbox_id == UUID(inbox_id),
                ).with_for_update())
            if (inbox is None or inbox.status != "RUNNING" or inbox.lease_owner != worker_id
                    or inbox.lease_until is None or as_utc(inbox.lease_until) <= now):
                return False
            fence = await database.scalar(
                select(SessionExecutionFence).where(
                    SessionExecutionFence.tenant_id == context.tenant_id,
                    SessionExecutionFence.agent_app_id == context.agent_app_id,
                    SessionExecutionFence.session_id == inbox.session_id,
                ).with_for_update())
            if (fence is None or fence.lease_owner != worker_id
                    or fence.issued_token != fencing_token or fence.lease_until is None
                    or as_utc(fence.lease_until) <= now):
                return False
            inbox.lease_until = lease_until
            fence.lease_until = lease_until
            return True

    async def commit_execution(
        self,
        context: TenantContext,
        commit: ExecutionCommit,
    ) -> SessionSnapshot:
        """Commit Session, Inbox, Runner and Outbox facts in one transaction."""

        async with self._sessions.begin() as database:
            inbox: InboxMessageRow | None = None
            runner: RunnerRequestRow | None = None
            fence: SessionExecutionFence | None = None
            if commit.inbox_id is not None:
                inbox = await database.scalar(
                    select(InboxMessageRow).where(
                        InboxMessageRow.tenant_id == context.tenant_id,
                        InboxMessageRow.agent_app_id == context.agent_app_id,
                        InboxMessageRow.inbox_id == UUID(commit.inbox_id),
                    ).with_for_update())
                if inbox is None:
                    raise LookupError("Inbox claim does not exist in the execution scope")
                if inbox.status in {"SUCCEEDED", "REPLIED"}:
                    replay = await self._replayed_claim(database, context, inbox)
                    assert replay.completed is not None
                    return replay.completed
                if inbox.status != "RUNNING":
                    raise StaleExecutionLease("Inbox claim is no longer running")
                if inbox.session_id != commit.session_id or (commit.runner_request_id is not None
                                                             and commit.runner_request_id
                                                             != inbox.request_id):
                    raise StaleExecutionLease("execution claim scope does not match commit")
                runner = await database.scalar(
                    select(RunnerRequestRow).where(
                        RunnerRequestRow.tenant_id == context.tenant_id,
                        RunnerRequestRow.request_id == inbox.request_id,
                    ).with_for_update())
                fence = await database.scalar(
                    select(SessionExecutionFence).where(
                        SessionExecutionFence.tenant_id == context.tenant_id,
                        SessionExecutionFence.agent_app_id == context.agent_app_id,
                        SessionExecutionFence.session_id == commit.session_id,
                    ).with_for_update())
                now = utc_now()
                if (commit.fencing_token is None or runner is None
                        or runner.fencing_token != commit.fencing_token or fence is None
                        or fence.issued_token != commit.fencing_token or fence.lease_until is None
                        or as_utc(fence.lease_until) <= now):
                    raise StaleExecutionLease("Session execution lease is stale or expired")

            session = await database.scalar(
                self._session_query(context, commit.session_id).with_for_update())
            current_version = 0 if session is None else session.version
            if current_version != commit.expected_version:
                raise SessionVersionConflict(
                    f"expected Session version {commit.expected_version}, got {current_version}")
            last_fencing_token = None if session is None else session.last_fencing_token
            if last_fencing_token is not None and (commit.fencing_token is None
                                                   or commit.fencing_token <= last_fencing_token):
                raise StaleExecutionLease(f"fencing token must be newer than {last_fencing_token}")

            now = utc_now()
            next_version = current_version + 1
            current_seq = 0 if session is None else session.last_event_seq
            if session is None:
                session = AgentSession(
                    tenant_id=context.tenant_id,
                    agent_app_id=context.agent_app_id,
                    session_id=commit.session_id,
                    version=next_version,
                    last_event_seq=len(commit.events),
                    last_fencing_token=commit.fencing_token,
                    state=dict(commit.state),
                    last_activity_at=now,
                )
                database.add(session)
            else:
                session.version = next_version
                session.last_event_seq = current_seq + len(commit.events)
                if commit.fencing_token is not None:
                    session.last_fencing_token = commit.fencing_token
                session.state = dict(commit.state)
                session.last_activity_at = now

            request_id = commit.runner_request_id or context.request_id
            for offset, event in enumerate(commit.events, start=1):
                database.add(
                    SessionEventRow(
                        tenant_id=context.tenant_id,
                        agent_app_id=context.agent_app_id,
                        session_id=commit.session_id,
                        seq_no=current_seq + offset,
                        event_id=event.event_id,
                        event_type=event.event_type,
                        request_id=request_id,
                        trace_id=context.trace_id,
                        occurred_at=event.occurred_at,
                        committed_version=next_version,
                        payload=dict(event.payload),
                    ))

            if inbox is not None:
                inbox.status = "SUCCEEDED"
                inbox.committed_session_version = next_version
                inbox.result_state = dict(commit.state)
                inbox.completed_at = now
                inbox.lease_owner = None
                inbox.lease_until = None
                inbox.reply_outbox_id = next(
                    (message.outbox_id
                     for message in commit.outbox if message.category == "IM_REPLY"),
                    None,
                )
                assert fence is not None
                fence.lease_owner = None
                fence.lease_until = None

            if commit.runner_request_id is not None:
                if runner is None or runner.request_id != commit.runner_request_id:
                    runner = await database.scalar(
                        select(RunnerRequestRow).where(
                            RunnerRequestRow.tenant_id == context.tenant_id,
                            RunnerRequestRow.request_id == commit.runner_request_id,
                        ).with_for_update())
                if runner is not None:
                    runner.status = "COMPLETED"
                    runner.completed_at = now

            for message in commit.outbox:
                exists = await database.scalar(
                    select(OutboxMessageRow.outbox_id).where(
                        OutboxMessageRow.tenant_id == context.tenant_id,
                        OutboxMessageRow.agent_app_id == context.agent_app_id,
                        OutboxMessageRow.category == message.category,
                        OutboxMessageRow.idempotency_key == message.idempotency_key,
                    ))
                if exists is None:
                    database.add(
                        OutboxMessageRow(
                            tenant_id=context.tenant_id,
                            agent_app_id=context.agent_app_id,
                            outbox_id=message.outbox_id,
                            request_id=message.request_id or request_id,
                            session_id=message.session_id or commit.session_id,
                            category=message.category,
                            destination=message.destination or message.category,
                            binding_id=message.binding_id,
                            sequence_no=message.sequence_no,
                            idempotency_key=message.idempotency_key,
                            payload=dict(message.payload),
                            status="PENDING",
                        ))
            await database.flush()
            snapshot = await self._snapshot(database, context, commit.session_id)
            assert snapshot is not None
            return snapshot

    async def fail_execution(
        self,
        context: TenantContext,
        inbox_id: str,
        *,
        fencing_token: int,
        error_code: str,
        error_summary: str,
        next_attempt_at: datetime | None,
    ) -> None:
        """Persist a safe failure summary and release the execution lease."""

        async with self._sessions.begin() as database:
            inbox = await database.scalar(
                select(InboxMessageRow).where(
                    InboxMessageRow.tenant_id == context.tenant_id,
                    InboxMessageRow.agent_app_id == context.agent_app_id,
                    InboxMessageRow.inbox_id == UUID(inbox_id),
                ).with_for_update())
            if inbox is None or inbox.status in {"SUCCEEDED", "REPLIED"}:
                return
            runner = await database.scalar(
                select(RunnerRequestRow).where(
                    RunnerRequestRow.tenant_id == context.tenant_id,
                    RunnerRequestRow.request_id == inbox.request_id,
                ).with_for_update())
            fence = await database.scalar(
                select(SessionExecutionFence).where(
                    SessionExecutionFence.tenant_id == context.tenant_id,
                    SessionExecutionFence.agent_app_id == context.agent_app_id,
                    SessionExecutionFence.session_id == inbox.session_id,
                ).with_for_update())
            if (runner is None or runner.fencing_token != fencing_token or fence is None
                    or fence.issued_token != fencing_token or fence.lease_until is None
                    or as_utc(fence.lease_until) <= utc_now()):
                raise StaleExecutionLease("Session execution lease is stale")
            inbox.status = "RETRYABLE_FAILED" if next_attempt_at is not None else "PERMANENT_FAILED"
            inbox.next_attempt_at = next_attempt_at
            inbox.lease_owner = None
            inbox.lease_until = None
            inbox.last_error_code = error_code[:100]
            inbox.last_error_summary = error_summary[:2000]
            fence.lease_owner = None
            fence.lease_until = None
            if runner is not None:
                runner.status = inbox.status
                runner.last_error = inbox.last_error_summary

    @staticmethod
    def _to_outbox(row: OutboxMessageRow) -> OutboxMessage:
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

    async def claim_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        lease_until: datetime,
    ) -> OutboxMessage | None:
        """Claim one due Outbox task and append an immutable Attempt row."""

        now = utc_now()
        if not worker_id.strip() or as_utc(lease_until) <= now:
            raise ValueError("Outbox worker and future lease are required")
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(OutboxMessageRow).where(
                    OutboxMessageRow.tenant_id == context.tenant_id,
                    OutboxMessageRow.agent_app_id == context.agent_app_id,
                    OutboxMessageRow.outbox_id == outbox_id,
                ).with_for_update())
            if row is None or row.status in {"DELIVERED", "DEAD_LETTER", "CANCELLED", "UNKNOWN"}:
                return None
            if row.status == "PROCESSING" and row.lease_until is not None:
                if as_utc(row.lease_until) > now:
                    return None
            if row.next_attempt_at is not None and as_utc(row.next_attempt_at) > now:
                return None
            row.status = "PROCESSING"
            row.attempt_count += 1
            row.retry_count += 1
            row.lease_owner = worker_id
            row.lease_until = lease_until
            database.add(
                OutboxAttemptRow(
                    tenant_id=context.tenant_id,
                    agent_app_id=context.agent_app_id,
                    outbox_id=outbox_id,
                    attempt_no=row.attempt_count,
                    worker_id=worker_id,
                    started_at=now,
                ))
            return self._to_outbox(row)

    async def complete_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        external_receipt_id: str,
        completed_at: datetime,
    ) -> None:
        """Make delivery completion and provider receipt durable."""

        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(OutboxMessageRow).where(
                    OutboxMessageRow.tenant_id == context.tenant_id,
                    OutboxMessageRow.agent_app_id == context.agent_app_id,
                    OutboxMessageRow.outbox_id == outbox_id,
                ).with_for_update())
            if row is None:
                raise LookupError("Outbox task does not exist in the execution scope")
            if row.status == "DELIVERED":
                return
            if (row.status != "PROCESSING" or row.lease_owner != worker_id
                    or row.attempt_count != attempt_no or row.lease_until is None
                    or as_utc(row.lease_until) <= utc_now()):
                raise StaleExecutionLease("Outbox delivery lease is stale or expired")
            row.status = "DELIVERED"
            row.external_receipt_id = external_receipt_id[:255]
            row.delivered_at = completed_at
            row.lease_owner = None
            row.lease_until = None
            attempt = await database.scalar(
                select(OutboxAttemptRow).where(
                    OutboxAttemptRow.tenant_id == context.tenant_id,
                    OutboxAttemptRow.agent_app_id == context.agent_app_id,
                    OutboxAttemptRow.outbox_id == outbox_id,
                    OutboxAttemptRow.attempt_no == row.attempt_count,
                ).with_for_update())
            if attempt is not None:
                attempt.finished_at = completed_at
                attempt.result = "DELIVERED"
                attempt.external_receipt_id = external_receipt_id[:255]
            if row.category == "IM_REPLY":
                inbox = await database.scalar(
                    select(InboxMessageRow).where(
                        InboxMessageRow.tenant_id == context.tenant_id,
                        InboxMessageRow.agent_app_id == context.agent_app_id,
                        InboxMessageRow.reply_outbox_id == outbox_id,
                    ).with_for_update())
                if inbox is not None:
                    inbox.status = "REPLIED"

    async def fail_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        error_code: str,
        error_summary: str,
        next_attempt_at: datetime | None,
        completed_at: datetime,
        outcome_unknown: bool = False,
    ) -> None:
        """Persist one failed Attempt and either retry or dead-letter the task."""

        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(OutboxMessageRow).where(
                    OutboxMessageRow.tenant_id == context.tenant_id,
                    OutboxMessageRow.agent_app_id == context.agent_app_id,
                    OutboxMessageRow.outbox_id == outbox_id,
                ).with_for_update())
            if row is None or row.status == "DELIVERED":
                return
            if (row.status != "PROCESSING" or row.lease_owner != worker_id
                    or row.attempt_count != attempt_no or row.lease_until is None
                    or as_utc(row.lease_until) <= utc_now()):
                raise StaleExecutionLease("Outbox delivery lease is stale")
            if outcome_unknown:
                row.status = "UNKNOWN"
            else:
                row.status = ("RETRYABLE_FAILED" if next_attempt_at is not None else "DEAD_LETTER")
            row.next_attempt_at = next_attempt_at
            row.lease_owner = None
            row.lease_until = None
            row.last_error_code = error_code[:100]
            row.last_error_summary = error_summary[:2000]
            attempt = await database.scalar(
                select(OutboxAttemptRow).where(
                    OutboxAttemptRow.tenant_id == context.tenant_id,
                    OutboxAttemptRow.agent_app_id == context.agent_app_id,
                    OutboxAttemptRow.outbox_id == outbox_id,
                    OutboxAttemptRow.attempt_no == row.attempt_count,
                ).with_for_update())
            if attempt is not None:
                attempt.finished_at = completed_at
                attempt.result = row.status
                attempt.error_summary = row.last_error_summary


class PostgreSQLStorage(
        PostgreSQLExecutionStore,
        PostgreSQLMemoryStore,
        PostgreSQLSummaryStore,
        PostgreSQLAuditStore,
):
    """Compatibility aggregate; new composition registers capability stores separately."""

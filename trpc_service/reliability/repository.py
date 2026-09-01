"""SQL-backed reliability repository.

PostgreSQL is the production authority. SQLite deliberately uses ``BEGIN
IMMEDIATE`` so its local-development contract has one writer; it is not presented
as a substitute for PostgreSQL's row locking or multi-worker behavior.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import exists, func, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from trpc_service.reliability.types import (
    AppendDisposition,
    AuditData,
    ClaimInput,
    CommittedEvent,
    CommittedSessionView,
    EventAppend,
    EventData,
    FinalizeDisposition,
    FinalizeResult,
    IdempotencyConflictError,
    InboxAcceptance,
    InboxDisposition,
    InboxEnvelope,
    InvalidStateTransitionError,
    OutboxDeliveryClaim,
    OutboxDeliveryOutcome,
    ProjectionClaim,
    ProjectionEvent,
    ProjectionFinalizeDisposition,
    ProjectionFinalizeResult,
    ProjectionInput,
    ReliabilityInvariantError,
    ReplyCredentialData,
    ReplyCredentialRef,
    ReplyPart,
    SessionClaim,
    StaleClaimError,
    StaleVersionError,
    ToolEffectRequest,
    ToolReservation,
    ToolReservationDisposition,
)
from trpc_service.storage.models import (
    AgentRun,
    AuditLog,
    ChannelReplyCredential,
    InboxMessage,
    ProjectionJob,
    ReplyOutbox,
    Session,
    SessionEvent,
    ToolEffect,
    new_id,
)
from trpc_service.storage.projections import MemoryProjection, SqlProjectionStore, SummaryProjection

_TERMINAL_INBOX = ("succeeded", "dead_letter")
_CLAIMABLE_INBOX = ("received", "retry_wait", "running")
_EVENT_VISIBILITIES = {"staged"}
_TOOL_EFFECT_CLASSES = {"read", "idempotent_write", "non_idempotent_write"}
_SAFE_TOOL_RECLAIM = {"read", "idempotent_write"}
_TOOL_COMPLETIONS = {"succeeded", "retry_wait", "failed_final", "unknown"}
_OUTBOX_COMPLETIONS = set(OutboxDeliveryOutcome)
_CLAIMABLE_PROJECTION = ("pending", "retry_wait", "processing")
_FORBIDDEN_OUTBOX_KEYS = {
    "access_token",
    "bot_token",
    "chat_id",
    "message_thread_id",
    "reply_token",
    "response_url",
    "secret",
    "thread_id",
    "webhook_url",
}


def canonical_json_hash(value: Any) -> str:
    """Hash a deterministic UTF-8 JSON representation.

    Callers must pass already-normalized protocol data. Transport signatures and
    secrets should not be present in the value.
    """

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _rowcount(result: Any) -> int:
    """Return a known DML row count from SQLAlchemy's dialect-neutral result."""

    rowcount = result.rowcount
    if not isinstance(rowcount, int) or rowcount < 0:
        raise ReliabilityInvariantError("database did not report a DML row count")
    return rowcount


def _contains_outbound_route_secret(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            str(key).lower() in _FORBIDDEN_OUTBOX_KEYS or _contains_outbound_route_secret(child)
            for key, child in value.items()
        )
    if isinstance(value, list):
        return any(_contains_outbound_route_secret(child) for child in value)
    return False


class ReliabilityRepository:
    """Own all mutation paths for the canonical reliability tables."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        bind = session_factory.kw.get("bind")
        if bind is None:
            raise ValueError("session_factory must be bound to an engine")
        self._session_factory = session_factory
        self._dialect = bind.dialect.name

    @property
    def dialect_name(self) -> str:
        """Return the active SQLAlchemy dialect name."""

        return self._dialect

    @asynccontextmanager
    async def _transaction(
        self,
        tenant_id: str,
        *,
        write: bool = True,
    ) -> AsyncIterator[AsyncSession]:
        """Open a tenant-scoped transaction under RLS and SQLite's write lock."""

        if not tenant_id or len(tenant_id) > 64:
            raise ValueError("tenant_id must be a non-empty value of at most 64 characters")

        async with self._session_factory() as database:
            try:
                if self._dialect == "sqlite" and write:
                    await database.execute(text("BEGIN IMMEDIATE"))
                else:
                    await database.begin()
                if self._dialect == "postgresql":
                    await database.execute(
                        text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
                        {"tenant_id": tenant_id},
                    )
                yield database
                await database.commit()
            except BaseException:
                await database.rollback()
                raise

    async def _db_now(self, database: AsyncSession) -> datetime:
        expression = (
            func.clock_timestamp() if self._dialect == "postgresql" else func.current_timestamp()
        )
        value = await database.scalar(select(expression))
        if not isinstance(value, datetime):
            raise ReliabilityInvariantError("database clock did not return a datetime")
        return _aware_utc(value)

    @staticmethod
    def _same_inbox(
        existing: InboxMessage,
        envelope: InboxEnvelope,
        credential_id: str | None,
    ) -> InboxAcceptance:
        if existing.payload_hash != envelope.payload_hash:
            raise IdempotencyConflictError(
                "external delivery id was reused with a different payload hash"
            )
        return InboxAcceptance(
            disposition=InboxDisposition.DUPLICATE,
            inbox_id=existing.inbox_id,
            accepted_seq=existing.accepted_seq,
            status=existing.status,
            credential_id=credential_id,
        )

    @staticmethod
    def _validate_credential_data(credential: ReplyCredentialData) -> None:
        if not credential.credential_kind:
            raise ValueError("credential_kind must not be empty")
        if not credential.ciphertext:
            raise ValueError("encrypted credential ciphertext must not be empty")
        if len(credential.ciphertext_hash) != 64 or any(
            character not in "0123456789abcdef" for character in credential.ciphertext_hash
        ):
            raise ValueError("ciphertext_hash must be a lowercase SHA-256 hex digest")

    async def _existing_credential_id(
        self,
        database: AsyncSession,
        existing: InboxMessage,
        supplied: ReplyCredentialData | None,
    ) -> str | None:
        statement = select(ChannelReplyCredential).where(
            ChannelReplyCredential.tenant_id == existing.tenant_id,
            ChannelReplyCredential.inbox_id == existing.inbox_id,
        )
        if supplied is not None:
            statement = statement.where(
                ChannelReplyCredential.credential_kind == supplied.credential_kind
            )
        statement = statement.order_by(ChannelReplyCredential.credential_id).limit(1)
        stored = await database.scalar(statement)
        if supplied is None:
            return stored.credential_id if stored is not None else None
        if stored is None:
            raise IdempotencyConflictError(
                "duplicate delivery supplied a credential absent from T0 acceptance"
            )
        # Authenticated encryption intentionally uses a fresh nonce, so the same
        # legitimate route produces a different ciphertext on an at-least-once
        # callback. ``ciphertext_hash`` is the ingress' keyed, stable plaintext
        # fingerprint; matching it means the original T0 ciphertext must be reused.
        if stored.ciphertext_hash != supplied.ciphertext_hash:
            raise IdempotencyConflictError(
                "duplicate delivery credential differs from the encrypted T0 value"
            )
        return stored.credential_id

    async def _read_inbox_acceptance(
        self,
        envelope: InboxEnvelope,
    ) -> InboxAcceptance | None:
        async with self._transaction(envelope.tenant_id, write=False) as database:
            existing = await database.scalar(
                select(InboxMessage).where(
                    InboxMessage.tenant_id == envelope.tenant_id,
                    InboxMessage.binding_id == envelope.binding_id,
                    InboxMessage.external_delivery_id == envelope.external_delivery_id,
                )
            )
            if existing is None:
                return None
            credential_id = await self._existing_credential_id(
                database,
                existing,
                envelope.reply_credential,
            )
            return self._same_inbox(existing, envelope, credential_id)

    async def accept_inbox(self, envelope: InboxEnvelope) -> InboxAcceptance:
        """Durably accept one normalized delivery.

        A concurrent Session-row or delivery-key collision rolls back the whole
        allocation transaction before retrying, so ``accepted_seq`` cannot develop
        a committed gap.
        """

        if not envelope.payload_hash:
            raise ValueError("payload_hash must not be empty")
        if envelope.config_revision < 1 or envelope.app_revision < 1:
            raise ValueError("configuration and app revisions must be positive")
        if envelope.reply_credential is not None:
            self._validate_credential_data(envelope.reply_credential)

        last_error: IntegrityError | None = None
        for _ in range(3):
            try:
                return await self._accept_inbox_once(envelope)
            except IntegrityError as error:
                last_error = error
                existing = await self._read_inbox_acceptance(envelope)
                if existing is not None:
                    return existing
        assert last_error is not None
        raise last_error

    async def _accept_inbox_once(self, envelope: InboxEnvelope) -> InboxAcceptance:
        async with self._transaction(envelope.tenant_id) as database:
            now = await self._db_now(database)
            existing = await database.scalar(
                select(InboxMessage).where(
                    InboxMessage.tenant_id == envelope.tenant_id,
                    InboxMessage.binding_id == envelope.binding_id,
                    InboxMessage.external_delivery_id == envelope.external_delivery_id,
                )
            )
            if existing is not None:
                existing_credential_id = await self._existing_credential_id(
                    database,
                    existing,
                    envelope.reply_credential,
                )
                return self._same_inbox(
                    existing,
                    envelope,
                    existing_credential_id,
                )

            session_statement = select(Session).where(
                Session.tenant_id == envelope.tenant_id,
                Session.session_id == envelope.session_id,
            )
            if self._dialect == "postgresql":
                session_statement = session_statement.with_for_update()
            session = await database.scalar(session_statement)

            if session is None:
                session = Session(
                    tenant_id=envelope.tenant_id,
                    session_id=envelope.session_id,
                    app_id=envelope.app_id,
                    app_revision=envelope.app_revision,
                    binding_id=envelope.binding_id,
                    scope=envelope.scope,
                    principal_id=envelope.principal_id,
                    next_inbox_seq=1,
                    log_version=0,
                    state_version=0,
                    fencing_token=0,
                    state={},
                )
                database.add(session)
                await database.flush()
            elif (
                session.binding_id != envelope.binding_id
                or session.app_id != envelope.app_id
                or session.app_revision != envelope.app_revision
            ):
                raise ReliabilityInvariantError(
                    "session routing metadata differs from the trusted binding"
                )

            accepted_seq = session.next_inbox_seq
            session.next_inbox_seq += 1
            inbox = InboxMessage(
                tenant_id=envelope.tenant_id,
                binding_id=envelope.binding_id,
                session_id=envelope.session_id,
                config_revision=envelope.config_revision,
                accepted_seq=accepted_seq,
                external_delivery_id=envelope.external_delivery_id,
                payload_hash=envelope.payload_hash,
                payload=envelope.payload,
                status="received",
                attempt_count=0,
                next_attempt_at=now,
                request_id=envelope.request_id,
                trace_id=envelope.trace_id,
                received_at=now,
            )
            database.add(inbox)
            await database.flush()
            created_credential_id: str | None = None
            credential = envelope.reply_credential
            if credential is not None:
                if _aware_utc(credential.expires_at) <= now:
                    raise ValueError("reply credential must be live at T0 acceptance")
                created_credential_id = new_id()
                database.add(
                    ChannelReplyCredential(
                        credential_id=created_credential_id,
                        tenant_id=envelope.tenant_id,
                        inbox_id=inbox.inbox_id,
                        credential_kind=credential.credential_kind,
                        ciphertext=credential.ciphertext,
                        ciphertext_hash=credential.ciphertext_hash,
                        status="active",
                        expires_at=_aware_utc(credential.expires_at),
                        created_at=now,
                    )
                )
                await database.flush()
            result = InboxAcceptance(
                disposition=InboxDisposition.ACCEPTED,
                inbox_id=inbox.inbox_id,
                accepted_seq=accepted_seq,
                status=inbox.status,
                credential_id=created_credential_id,
            )
        return result

    async def claim_next(
        self,
        tenant_id: str,
        worker_id: str,
        *,
        lease_ttl: timedelta = timedelta(seconds=20),
    ) -> SessionClaim | None:
        """Claim one tenant's earliest runnable Inbox under a Session lease."""

        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")

        async with self._transaction(tenant_id) as database:
            now = await self._db_now(database)
            earlier = aliased(InboxMessage)
            has_earlier_unfinished = exists(
                select(1).where(
                    earlier.tenant_id == tenant_id,
                    earlier.tenant_id == InboxMessage.tenant_id,
                    earlier.session_id == InboxMessage.session_id,
                    earlier.accepted_seq < InboxMessage.accepted_seq,
                    earlier.status.not_in(_TERMINAL_INBOX),
                )
            )
            statement = (
                select(Session, InboxMessage)
                .join(
                    InboxMessage,
                    (InboxMessage.tenant_id == Session.tenant_id)
                    & (InboxMessage.session_id == Session.session_id),
                )
                .where(
                    Session.tenant_id == tenant_id,
                    InboxMessage.tenant_id == tenant_id,
                    InboxMessage.status.in_(_CLAIMABLE_INBOX),
                    InboxMessage.next_attempt_at <= now,
                    ~has_earlier_unfinished,
                    or_(
                        Session.lease_expires_at.is_(None),
                        Session.lease_expires_at <= now,
                    ),
                )
                .order_by(InboxMessage.received_at, InboxMessage.accepted_seq)
                .limit(1)
            )
            if self._dialect == "postgresql":
                statement = statement.with_for_update(of=Session, skip_locked=True)

            row = (await database.execute(statement)).first()
            if row is None:
                return None
            session, inbox = row[0], row[1]

            session.fencing_token += 1
            session.lease_owner = worker_id
            session.lease_expires_at = now + lease_ttl
            fence = session.fencing_token

            run_statement = select(AgentRun).where(
                AgentRun.tenant_id == inbox.tenant_id,
                AgentRun.inbox_id == inbox.inbox_id,
            )
            if self._dialect == "postgresql":
                run_statement = run_statement.with_for_update()
            run = await database.scalar(run_statement)
            if run is None:
                run = AgentRun(
                    tenant_id=inbox.tenant_id,
                    inbox_id=inbox.inbox_id,
                    session_id=inbox.session_id,
                    request_id=inbox.request_id,
                    trace_id=inbox.trace_id,
                    app_id=session.app_id,
                    app_revision=session.app_revision,
                    status="pending",
                    attempt_no=0,
                    start_version=session.log_version,
                    last_seq=session.log_version,
                )
                database.add(run)
                await database.flush()
            elif run.status not in {"pending", "running", "retry_wait"}:
                raise InvalidStateTransitionError(
                    f"cannot claim run {run.run_id!r} from status {run.status!r}"
                )

            next_attempt = run.attempt_no + 1
            if run.attempt_no:
                await database.execute(
                    update(SessionEvent)
                    .where(
                        SessionEvent.tenant_id == run.tenant_id,
                        SessionEvent.run_id == run.run_id,
                        SessionEvent.visibility == "staged",
                        SessionEvent.attempt_no < next_attempt,
                    )
                    .values(visibility="aborted")
                )

            run.status = "running"
            run.attempt_no = next_attempt
            run.claim_fencing_token = fence
            run.error_type = None
            run.error_code = None
            inbox.status = "running"
            inbox.claim_fencing_token = fence
            inbox.attempt_count += 1
            inbox.error_type = None
            await database.flush()

            claim = SessionClaim(
                tenant_id=session.tenant_id,
                session_id=session.session_id,
                inbox_id=inbox.inbox_id,
                run_id=run.run_id,
                worker_id=worker_id,
                fencing_token=fence,
                attempt_no=next_attempt,
                expected_version=session.log_version,
                lease_expires_at=_aware_utc(session.lease_expires_at),
                request_id=run.request_id,
                trace_id=run.trace_id,
            )
        return claim

    async def load_claim_input(self, claim: SessionClaim) -> ClaimInput:
        """Load a detached, tenant-scoped Worker input without exposing ORM rows."""

        async with self._transaction(claim.tenant_id, write=False) as database:
            now = await self._db_now(database)
            session = await self._load_active_session(database, claim, now)
            run = await database.scalar(
                select(AgentRun).where(
                    AgentRun.tenant_id == claim.tenant_id,
                    AgentRun.run_id == claim.run_id,
                )
            )
            inbox = await database.scalar(
                select(InboxMessage).where(
                    InboxMessage.tenant_id == claim.tenant_id,
                    InboxMessage.inbox_id == claim.inbox_id,
                )
            )
            if (
                run is None
                or run.status != "running"
                or run.attempt_no != claim.attempt_no
                or run.claim_fencing_token != claim.fencing_token
            ):
                raise StaleClaimError("AgentRun is not owned by the supplied claim")
            if (
                inbox is None
                or inbox.status != "running"
                or inbox.claim_fencing_token != claim.fencing_token
                or inbox.session_id != claim.session_id
            ):
                raise StaleClaimError("Inbox is not owned by the supplied claim")
            return ClaimInput(
                tenant_id=claim.tenant_id,
                session_id=claim.session_id,
                inbox_id=claim.inbox_id,
                run_id=claim.run_id,
                binding_id=inbox.binding_id,
                app_id=session.app_id,
                app_revision=session.app_revision,
                config_revision=inbox.config_revision,
                scope=session.scope,
                principal_id=session.principal_id,
                accepted_seq=inbox.accepted_seq,
                external_delivery_id=inbox.external_delivery_id,
                payload=json.loads(json.dumps(inbox.payload)),
                request_id=inbox.request_id,
                trace_id=inbox.trace_id,
                attempt_no=claim.attempt_no,
                fencing_token=claim.fencing_token,
            )

    async def load_committed_session(
        self,
        claim: SessionClaim,
    ) -> CommittedSessionView:
        """Read session-scope state plus committed events under a live claim.

        ``Session.state`` is the only projection owned here. App/user-scoped
        ``ScopedState`` remains a separate OCC service; this method deliberately
        makes no cross-scope atomicity promise.
        """

        async with self._transaction(claim.tenant_id, write=False) as database:
            now = await self._db_now(database)
            session = await self._load_active_session(database, claim, now)
            has_future_committed = await database.scalar(
                select(
                    exists().where(
                        SessionEvent.tenant_id == claim.tenant_id,
                        SessionEvent.session_id == claim.session_id,
                        SessionEvent.visibility == "committed",
                        SessionEvent.seq > session.state_version,
                    )
                )
            )
            if has_future_committed:
                raise ReliabilityInvariantError(
                    "committed event exists beyond the Session state watermark"
                )
            stored_events = list(
                (
                    await database.scalars(
                        select(SessionEvent)
                        .where(
                            SessionEvent.tenant_id == claim.tenant_id,
                            SessionEvent.session_id == claim.session_id,
                            SessionEvent.visibility == "committed",
                            SessionEvent.seq <= session.state_version,
                        )
                        .order_by(SessionEvent.seq)
                    )
                ).all()
            )
            events = tuple(
                CommittedEvent(
                    seq=event.seq,
                    event_id=event.event_id,
                    event_type=event.event_type,
                    role=event.role,
                    content_ref=event.content_ref,
                    payload=json.loads(json.dumps(event.payload)),
                    state_delta=json.loads(json.dumps(event.state_delta)),
                    framework_event_id=event.framework_event_id,
                    created_at=_aware_utc(event.created_at),
                )
                for event in stored_events
            )
            return CommittedSessionView(
                tenant_id=claim.tenant_id,
                session_id=claim.session_id,
                state=json.loads(json.dumps(session.state)),
                state_version=session.state_version,
                log_version=session.log_version,
                events=events,
            )

    async def renew_lease(
        self,
        claim: SessionClaim,
        *,
        lease_ttl: timedelta = timedelta(seconds=20),
    ) -> bool:
        """Extend a live lease without changing its fencing token."""

        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")
        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            result = await database.execute(
                update(Session)
                .where(
                    Session.tenant_id == claim.tenant_id,
                    Session.session_id == claim.session_id,
                    Session.fencing_token == claim.fencing_token,
                    Session.lease_owner == claim.worker_id,
                    Session.lease_expires_at > now,
                )
                .values(lease_expires_at=now + lease_ttl)
            )
            renewed = _rowcount(result) == 1
        return renewed

    async def _load_active_session(
        self,
        database: AsyncSession,
        claim: SessionClaim,
        now: datetime,
        *,
        lock: bool = False,
    ) -> Session:
        statement = select(Session).where(
            Session.tenant_id == claim.tenant_id,
            Session.session_id == claim.session_id,
        )
        if lock and self._dialect == "postgresql":
            statement = statement.with_for_update()
        session = await database.scalar(statement)
        if (
            session is None
            or session.fencing_token != claim.fencing_token
            or session.lease_owner != claim.worker_id
            or session.lease_expires_at is None
            or _aware_utc(session.lease_expires_at) <= now
        ):
            raise StaleClaimError("session lease expired or fencing token was superseded")
        return session

    async def _read_event_for_retry(
        self,
        claim: SessionClaim,
        event: EventData,
        payload_hash: str,
    ) -> EventAppend | None:
        async with self._transaction(claim.tenant_id, write=False) as database:
            existing = await database.scalar(
                select(SessionEvent).where(
                    SessionEvent.tenant_id == claim.tenant_id,
                    or_(
                        (SessionEvent.run_id == claim.run_id)
                        & (SessionEvent.event_key == event.event_key),
                        SessionEvent.event_id == event.event_id,
                    ),
                )
            )
            if existing is None:
                return None
            if existing.session_id != claim.session_id or existing.run_id != claim.run_id:
                raise IdempotencyConflictError(
                    "event id was already used by a different session or Run"
                )
            if existing.attempt_no != claim.attempt_no:
                raise InvalidStateTransitionError(
                    "event key belongs to a previous attempt; use an attempt-scoped key"
                )
            if existing.visibility == "aborted":
                raise InvalidStateTransitionError(
                    "an aborted event cannot be revived by an idempotent retry"
                )
            if (
                existing.payload_hash != payload_hash
                or existing.payload != event.payload
                or existing.event_key != event.event_key
                or existing.event_type != event.event_type
                or existing.role != event.role
                or existing.content_ref != event.content_ref
                or existing.state_delta != event.state_delta
                or existing.framework_event_id != event.framework_event_id
            ):
                raise IdempotencyConflictError(
                    "event id or event key was reused with different content"
                )
            return EventAppend(
                disposition=AppendDisposition.ALREADY_APPENDED,
                event_id=existing.event_id,
                seq=existing.seq,
                version=existing.seq,
            )

    async def append_event_cas(
        self,
        claim: SessionClaim,
        expected_version: int,
        event: EventData,
    ) -> EventAppend:
        """Append one event under both OCC and the current fencing capability."""

        if event.visibility not in _EVENT_VISIBILITIES:
            raise ValueError(f"invalid event visibility: {event.visibility!r}")
        payload_hash = event.payload_hash or canonical_json_hash(event.payload)

        existing = await self._read_event_for_retry(claim, event, payload_hash)
        if existing is not None:
            return existing

        try:
            async with self._transaction(claim.tenant_id) as database:
                now = await self._db_now(database)
                new_version = expected_version + 1
                result = await database.execute(
                    update(Session)
                    .where(
                        Session.tenant_id == claim.tenant_id,
                        Session.session_id == claim.session_id,
                        Session.log_version == expected_version,
                        Session.fencing_token == claim.fencing_token,
                        Session.lease_owner == claim.worker_id,
                        Session.lease_expires_at > now,
                    )
                    .values(log_version=new_version)
                )
                if _rowcount(result) != 1:
                    session = await database.scalar(
                        select(Session).where(
                            Session.tenant_id == claim.tenant_id,
                            Session.session_id == claim.session_id,
                        )
                    )
                    if (
                        session is None
                        or session.fencing_token != claim.fencing_token
                        or session.lease_owner != claim.worker_id
                        or session.lease_expires_at is None
                        or _aware_utc(session.lease_expires_at) <= now
                    ):
                        raise StaleClaimError(
                            "session lease expired or fencing token was superseded"
                        )
                    raise StaleVersionError(
                        f"expected session version {expected_version}, found {session.log_version}"
                    )

                database.add(
                    SessionEvent(
                        tenant_id=claim.tenant_id,
                        session_id=claim.session_id,
                        seq=new_version,
                        event_id=event.event_id,
                        run_id=claim.run_id,
                        attempt_no=claim.attempt_no,
                        event_key=event.event_key,
                        event_type=event.event_type,
                        visibility=event.visibility,
                        role=event.role,
                        content_ref=event.content_ref,
                        payload=event.payload,
                        payload_hash=payload_hash,
                        state_delta=event.state_delta,
                        framework_event_id=event.framework_event_id,
                    )
                )
                await database.flush()
                appended = EventAppend(
                    disposition=AppendDisposition.APPENDED,
                    event_id=event.event_id,
                    seq=new_version,
                    version=new_version,
                )
            return appended
        except StaleVersionError:
            retry = await self._read_event_for_retry(claim, event, payload_hash)
            if retry is not None:
                return retry
            raise
        except IntegrityError:
            retry = await self._read_event_for_retry(claim, event, payload_hash)
            if retry is not None:
                return retry
            raise StaleVersionError("another event won the session sequence allocation") from None

    async def abort_staged_events(self, claim: SessionClaim) -> int:
        """Abort the current attempt's unpublished events under its live fence."""

        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            await self._load_active_session(database, claim, now, lock=True)
            result = await database.execute(
                update(SessionEvent)
                .where(
                    SessionEvent.tenant_id == claim.tenant_id,
                    SessionEvent.run_id == claim.run_id,
                    SessionEvent.attempt_no == claim.attempt_no,
                    SessionEvent.visibility == "staged",
                )
                .values(visibility="aborted")
            )
            count = _rowcount(result)
        return count

    async def defer_run_retry(
        self,
        claim: SessionClaim,
        *,
        next_attempt_at: datetime,
        error_type: str,
    ) -> None:
        """Atomically abort an attempt, schedule retry, and release its Session lease."""

        if not error_type or len(error_type) > 128:
            raise ValueError("error_type must contain 1..128 sanitized characters")
        retry_at = _aware_utc(next_attempt_at)
        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            if retry_at <= now:
                raise ValueError("next_attempt_at must be later than the database clock")
            session = await self._load_active_session(database, claim, now, lock=True)
            run_statement = select(AgentRun).where(
                AgentRun.tenant_id == claim.tenant_id,
                AgentRun.run_id == claim.run_id,
            )
            inbox_statement = select(InboxMessage).where(
                InboxMessage.tenant_id == claim.tenant_id,
                InboxMessage.inbox_id == claim.inbox_id,
            )
            if self._dialect == "postgresql":
                run_statement = run_statement.with_for_update()
                inbox_statement = inbox_statement.with_for_update()
            run = await database.scalar(run_statement)
            inbox = await database.scalar(inbox_statement)
            if (
                run is None
                or run.status != "running"
                or run.attempt_no != claim.attempt_no
                or run.claim_fencing_token != claim.fencing_token
                or inbox is None
                or inbox.status != "running"
                or inbox.claim_fencing_token != claim.fencing_token
            ):
                raise StaleClaimError("retry transition is not owned by the supplied claim")

            await database.execute(
                update(SessionEvent)
                .where(
                    SessionEvent.tenant_id == claim.tenant_id,
                    SessionEvent.run_id == claim.run_id,
                    SessionEvent.attempt_no == claim.attempt_no,
                    SessionEvent.visibility == "staged",
                )
                .values(visibility="aborted")
            )
            run.status = "retry_wait"
            run.error_type = error_type
            run.claim_fencing_token = None
            inbox.status = "retry_wait"
            inbox.error_type = error_type
            inbox.next_attempt_at = retry_at
            inbox.claim_fencing_token = None
            session.lease_owner = None
            session.lease_expires_at = None
            await database.flush()

    @staticmethod
    def _verify_existing_parts(
        existing: Sequence[ReplyOutbox],
        parts: Sequence[ReplyPart],
    ) -> tuple[str, ...]:
        expected = sorted(
            (
                part.reply_id,
                part.part_no,
                part.payload_hash or canonical_json_hash(part.payload),
            )
            for part in parts
        )
        actual = sorted((row.reply_id, row.part_no, row.payload_hash) for row in existing)
        if expected != actual:
            raise IdempotencyConflictError(
                "finalize retry supplied reply parts different from committed Outbox"
            )
        by_key = {(row.reply_id, row.part_no): row.outbox_id for row in existing}
        return tuple(by_key[(part.reply_id, part.part_no)] for part in parts)

    @staticmethod
    def _audit_record_hash(
        audit_id: str,
        created_at: datetime,
        claim: SessionClaim,
        audit: AuditData,
    ) -> str:
        return canonical_json_hash(
            {
                "audit_id": audit_id,
                "tenant_id": claim.tenant_id,
                "session_id": claim.session_id,
                "run_id": claim.run_id,
                "request_id": claim.request_id,
                "trace_id": claim.trace_id,
                "created_at": created_at.isoformat(),
                "channel": audit.channel,
                "user_id": audit.user_id,
                "agent_name": audit.agent_name,
                "tool_name": audit.tool_name,
                "decision": audit.decision,
                "reason": audit.reason,
                "latency_ms": audit.latency_ms,
                "error_type": audit.error_type,
                "cost_micros": audit.cost_micros,
                "action": audit.action,
                "resource": audit.resource,
                "config_revision": audit.config_revision,
                "policy_revision": audit.policy_revision,
                "idempotency_key": audit.idempotency_key,
                "detail": audit.detail,
            }
        )

    async def _ensure_projection_job(
        self,
        database: AsyncSession,
        *,
        run: AgentRun,
        inbox: InboxMessage,
        now: datetime,
    ) -> ProjectionJob:
        """Idempotently couple one durable projection job to a successful run."""

        statement = select(ProjectionJob).where(
            ProjectionJob.tenant_id == run.tenant_id,
            ProjectionJob.run_id == run.run_id,
        )
        if self._dialect == "postgresql":
            statement = statement.with_for_update()
        existing = await database.scalar(statement)
        if existing is not None:
            if (
                existing.session_id != run.session_id
                or existing.config_revision != inbox.config_revision
                or existing.through_seq != run.last_seq
            ):
                raise ReliabilityInvariantError(
                    "ProjectionJob metadata differs from its successful AgentRun"
                )
            return existing

        job = ProjectionJob(
            # AgentRun identifiers are globally unique primary keys and make
            # migration/backfill plus finalize retries deterministic.
            job_id=run.run_id,
            tenant_id=run.tenant_id,
            run_id=run.run_id,
            session_id=run.session_id,
            config_revision=inbox.config_revision,
            through_seq=run.last_seq,
            status="pending",
            attempt_count=0,
            fencing_token=0,
            next_attempt_at=now,
            created_at=now,
        )
        database.add(job)
        await database.flush()
        return job

    async def finalize_run(
        self,
        claim: SessionClaim,
        *,
        final_state: dict[str, Any],
        final_event_id: str | None,
        reply_parts: Sequence[ReplyPart],
        audit: AuditData,
    ) -> FinalizeResult:
        """Atomically publish staged events, session state, Outbox and Audit.

        ``final_state`` is session scope only. App/user ``ScopedState`` projections
        are intentionally outside this transaction and require their own OCC API.
        """

        if not reply_parts:
            raise ValueError("finalization requires at least one reply part")
        part_keys = {(part.reply_id, part.part_no) for part in reply_parts}
        if len(part_keys) != len(reply_parts):
            raise ValueError("reply parts contain duplicate reply_id/part_no keys")
        if any(part.part_no < 0 for part in reply_parts):
            raise ValueError("reply part numbers must be non-negative")
        if any(_contains_outbound_route_secret(part.payload) for part in reply_parts):
            raise ValueError(
                "Outbox payload must reference encrypted routing credentials, not embed them"
            )

        async with self._transaction(claim.tenant_id) as database:
            run_statement = select(AgentRun).where(
                AgentRun.tenant_id == claim.tenant_id,
                AgentRun.run_id == claim.run_id,
            )
            if self._dialect == "postgresql":
                run_statement = run_statement.with_for_update()
            run = await database.scalar(run_statement)
            if run is None:
                raise ReliabilityInvariantError("claim references a missing AgentRun")

            if run.status == "succeeded":
                stored_session = await database.scalar(
                    select(Session).where(
                        Session.tenant_id == claim.tenant_id,
                        Session.session_id == claim.session_id,
                    )
                )
                if stored_session is None:
                    raise ReliabilityInvariantError("succeeded Run references a missing Session")
                if run.final_event_id != final_event_id or stored_session.state != final_state:
                    raise IdempotencyConflictError(
                        "finalize retry differs from the committed final state"
                    )
                if stored_session.state_version != run.last_seq:
                    raise ReliabilityInvariantError(
                        "succeeded Run and Session state watermark disagree"
                    )
                stored_inbox = await database.scalar(
                    select(InboxMessage).where(
                        InboxMessage.tenant_id == claim.tenant_id,
                        InboxMessage.inbox_id == claim.inbox_id,
                    )
                )
                if stored_inbox is None or stored_inbox.status != "succeeded":
                    raise ReliabilityInvariantError(
                        "succeeded Run references a missing or unfinished Inbox"
                    )
                existing = list(
                    (
                        await database.scalars(
                            select(ReplyOutbox)
                            .where(
                                ReplyOutbox.tenant_id == claim.tenant_id,
                                ReplyOutbox.run_id == claim.run_id,
                            )
                            .order_by(ReplyOutbox.reply_id, ReplyOutbox.part_no)
                        )
                    ).all()
                )
                existing_outbox_ids = self._verify_existing_parts(
                    existing,
                    reply_parts,
                )
                existing_audit = await database.scalar(
                    select(AuditLog).where(
                        AuditLog.tenant_id == claim.tenant_id,
                        AuditLog.request_id == claim.request_id,
                        AuditLog.action == audit.action,
                    )
                )
                if existing_audit is None:
                    raise ReliabilityInvariantError(
                        "succeeded Run exists without its atomic Audit record"
                    )
                expected_audit_hash = self._audit_record_hash(
                    existing_audit.audit_id,
                    _aware_utc(existing_audit.created_at),
                    claim,
                    audit,
                )
                if existing_audit.record_hash != expected_audit_hash:
                    raise IdempotencyConflictError(
                        "finalize retry differs from the committed Audit record"
                    )
                await self._ensure_projection_job(
                    database,
                    run=run,
                    inbox=stored_inbox,
                    now=await self._db_now(database),
                )
                return FinalizeResult(
                    disposition=FinalizeDisposition.ALREADY_FINALIZED,
                    run_id=run.run_id,
                    last_seq=run.last_seq,
                    outbox_ids=existing_outbox_ids,
                )

            if (
                run.status != "running"
                or run.attempt_no != claim.attempt_no
                or run.claim_fencing_token != claim.fencing_token
            ):
                raise StaleClaimError("Run is not owned by the supplied attempt and fence")

            now = await self._db_now(database)
            session = await self._load_active_session(
                database,
                claim,
                now,
                lock=True,
            )
            inbox = await database.scalar(
                select(InboxMessage).where(
                    InboxMessage.tenant_id == claim.tenant_id,
                    InboxMessage.inbox_id == claim.inbox_id,
                )
            )
            if (
                inbox is None
                or inbox.status != "running"
                or inbox.claim_fencing_token != claim.fencing_token
            ):
                raise StaleClaimError("Inbox is not owned by the supplied fencing token")

            await database.execute(
                update(SessionEvent)
                .where(
                    SessionEvent.tenant_id == claim.tenant_id,
                    SessionEvent.run_id == claim.run_id,
                    SessionEvent.attempt_no == claim.attempt_no,
                    SessionEvent.visibility == "staged",
                )
                .values(visibility="committed")
            )

            if final_event_id is not None:
                final_event = await database.scalar(
                    select(SessionEvent).where(
                        SessionEvent.tenant_id == claim.tenant_id,
                        SessionEvent.run_id == claim.run_id,
                        SessionEvent.attempt_no == claim.attempt_no,
                        SessionEvent.event_id == final_event_id,
                        SessionEvent.visibility == "committed",
                    )
                )
                if final_event is None:
                    raise ReliabilityInvariantError(
                        "final_event_id is not a committed event of this attempt"
                    )

            session.state = final_state
            session.state_version = session.log_version
            session.lease_owner = None
            session.lease_expires_at = None
            run.status = "succeeded"
            run.last_seq = session.log_version
            run.final_event_id = final_event_id
            run.completed_at = now
            inbox.status = "succeeded"
            inbox.processed_at = now

            await self._ensure_projection_job(
                database,
                run=run,
                inbox=inbox,
                now=now,
            )

            created_outbox_ids: list[str] = []
            for part in reply_parts:
                outbox_id = new_id()
                created_outbox_ids.append(outbox_id)
                database.add(
                    ReplyOutbox(
                        outbox_id=outbox_id,
                        tenant_id=claim.tenant_id,
                        run_id=claim.run_id,
                        binding_id=inbox.binding_id,
                        session_id=claim.session_id,
                        reply_id=part.reply_id,
                        part_no=part.part_no,
                        payload=part.payload,
                        payload_hash=part.payload_hash or canonical_json_hash(part.payload),
                        status="pending",
                        attempts=0,
                        next_retry_at=now,
                    )
                )

            audit_id = new_id()
            database.add(
                AuditLog(
                    audit_id=audit_id,
                    tenant_id=claim.tenant_id,
                    channel=audit.channel,
                    user_id=audit.user_id,
                    session_id=claim.session_id,
                    agent_name=audit.agent_name,
                    tool_name=audit.tool_name,
                    decision=audit.decision,
                    reason=audit.reason,
                    latency_ms=audit.latency_ms,
                    error_type=audit.error_type,
                    cost_micros=audit.cost_micros,
                    trace_id=claim.trace_id,
                    request_id=claim.request_id,
                    invocation_id=run.invocation_id,
                    action=audit.action,
                    resource=audit.resource,
                    config_revision=audit.config_revision,
                    policy_revision=audit.policy_revision,
                    idempotency_key=audit.idempotency_key,
                    detail=audit.detail,
                    record_hash=self._audit_record_hash(
                        audit_id,
                        now,
                        claim,
                        audit,
                    ),
                    created_at=now,
                )
            )
            await database.flush()
            result = FinalizeResult(
                disposition=FinalizeDisposition.FINALIZED,
                run_id=run.run_id,
                last_seq=run.last_seq,
                outbox_ids=tuple(created_outbox_ids),
            )
        return result

    async def claim_projection(
        self,
        tenant_id: str,
        worker_id: str,
        *,
        lease_ttl: timedelta = timedelta(seconds=30),
    ) -> ProjectionClaim | None:
        """Claim one ready Summary/Memory job using the database clock.

        An expired ``processing`` lease is deliberately reclaimable. Every claim
        increments the persistent fence, so the crashed worker cannot later
        publish stale output.
        """

        if not worker_id or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1..128 characters")
        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")

        async with self._transaction(tenant_id) as database:
            now = await self._db_now(database)
            statement = (
                select(ProjectionJob)
                .where(
                    ProjectionJob.tenant_id == tenant_id,
                    ProjectionJob.status.in_(_CLAIMABLE_PROJECTION),
                    or_(
                        (
                            ProjectionJob.status.in_(("pending", "retry_wait"))
                            & (ProjectionJob.next_attempt_at <= now)
                        ),
                        (
                            (ProjectionJob.status == "processing")
                            & or_(
                                ProjectionJob.claim_expires_at.is_(None),
                                ProjectionJob.claim_expires_at <= now,
                            )
                        ),
                    ),
                )
                .order_by(
                    ProjectionJob.next_attempt_at,
                    ProjectionJob.session_id,
                    ProjectionJob.through_seq,
                    ProjectionJob.created_at,
                    ProjectionJob.job_id,
                )
                .limit(1)
            )
            if self._dialect == "postgresql":
                statement = statement.with_for_update(of=ProjectionJob, skip_locked=True)
            job = await database.scalar(statement)
            if job is None:
                return None

            job.status = "processing"
            job.attempt_count += 1
            job.fencing_token += 1
            job.claimed_by = worker_id
            job.claim_expires_at = now + lease_ttl
            job.last_error_type = None
            await database.flush()
            return ProjectionClaim(
                tenant_id=job.tenant_id,
                job_id=job.job_id,
                run_id=job.run_id,
                session_id=job.session_id,
                config_revision=job.config_revision,
                through_seq=job.through_seq,
                worker_id=worker_id,
                fencing_token=job.fencing_token,
                attempt_no=job.attempt_count,
                lease_expires_at=_aware_utc(job.claim_expires_at),
            )

    async def _load_active_projection_job(
        self,
        database: AsyncSession,
        claim: ProjectionClaim,
        now: datetime,
        *,
        lock: bool = False,
    ) -> ProjectionJob:
        statement = select(ProjectionJob).where(
            ProjectionJob.tenant_id == claim.tenant_id,
            ProjectionJob.job_id == claim.job_id,
        )
        if lock and self._dialect == "postgresql":
            statement = statement.with_for_update()
        job = await database.scalar(statement)
        if (
            job is None
            or job.run_id != claim.run_id
            or job.session_id != claim.session_id
            or job.config_revision != claim.config_revision
            or job.through_seq != claim.through_seq
            or job.status != "processing"
            or job.fencing_token != claim.fencing_token
            or job.attempt_count != claim.attempt_no
            or job.claimed_by != claim.worker_id
            or job.claim_expires_at is None
            or _aware_utc(job.claim_expires_at) <= now
        ):
            raise StaleClaimError("projection lease expired or fencing token was superseded")
        return job

    async def renew_projection_claim(
        self,
        claim: ProjectionClaim,
        *,
        lease_ttl: timedelta = timedelta(seconds=30),
    ) -> bool:
        """Extend a live projection lease without changing its fence."""

        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")
        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            result = await database.execute(
                update(ProjectionJob)
                .where(
                    ProjectionJob.tenant_id == claim.tenant_id,
                    ProjectionJob.job_id == claim.job_id,
                    ProjectionJob.run_id == claim.run_id,
                    ProjectionJob.status == "processing",
                    ProjectionJob.fencing_token == claim.fencing_token,
                    ProjectionJob.attempt_count == claim.attempt_no,
                    ProjectionJob.claimed_by == claim.worker_id,
                    ProjectionJob.claim_expires_at > now,
                )
                .values(claim_expires_at=now + lease_ttl)
            )
            return _rowcount(result) == 1

    async def load_projection_input(self, claim: ProjectionClaim) -> ProjectionInput:
        """Load only committed canonical events at or below the job watermark."""

        async with self._transaction(claim.tenant_id, write=False) as database:
            now = await self._db_now(database)
            job = await self._load_active_projection_job(database, claim, now)
            run = await database.scalar(
                select(AgentRun).where(
                    AgentRun.tenant_id == claim.tenant_id,
                    AgentRun.run_id == claim.run_id,
                )
            )
            session = await database.scalar(
                select(Session).where(
                    Session.tenant_id == claim.tenant_id,
                    Session.session_id == claim.session_id,
                )
            )
            if run is None or run.status != "succeeded" or run.last_seq != job.through_seq:
                raise ReliabilityInvariantError(
                    "ProjectionJob does not reference a successful run at its watermark"
                )
            if session is None or job.through_seq > session.state_version:
                raise ReliabilityInvariantError(
                    "ProjectionJob watermark is ahead of committed Session state"
                )
            rows = list(
                (
                    await database.scalars(
                        select(SessionEvent)
                        .where(
                            SessionEvent.tenant_id == claim.tenant_id,
                            SessionEvent.session_id == claim.session_id,
                            SessionEvent.visibility == "committed",
                            SessionEvent.seq <= job.through_seq,
                        )
                        .order_by(SessionEvent.seq)
                    )
                ).all()
            )
            events = tuple(
                ProjectionEvent(
                    seq=row.seq,
                    event_id=row.event_id,
                    event_type=row.event_type,
                    role=row.role,
                    content_ref=row.content_ref,
                    payload=json.loads(json.dumps(row.payload)),
                    state_delta=json.loads(json.dumps(row.state_delta)),
                    created_at=_aware_utc(row.created_at),
                )
                for row in rows
            )
            return ProjectionInput(
                tenant_id=job.tenant_id,
                job_id=job.job_id,
                run_id=job.run_id,
                session_id=job.session_id,
                principal_id=session.principal_id,
                config_revision=job.config_revision,
                through_seq=job.through_seq,
                state_version=session.state_version,
                events=events,
            )

    @staticmethod
    def _projection_result_hash(
        summary: SummaryProjection | None,
        memories: Sequence[MemoryProjection],
    ) -> str:
        summary_payload = None
        if summary is not None:
            summary_payload = {
                "tenant_id": summary.tenant_id,
                "session_id": summary.session_id,
                "through_seq": summary.through_seq,
                "content": summary.content,
                "summarizer_version": summary.summarizer_version,
            }
        memory_payloads = sorted(
            (
                {
                    "tenant_id": item.tenant_id,
                    "principal_id": item.principal_id,
                    "session_id": item.session_id,
                    "source_event_id": item.source_event_id,
                    "extractor_version": item.extractor_version,
                    "record_version": item.record_version,
                    "content": item.content,
                    "metadata": item.metadata,
                }
                for item in memories
            ),
            key=lambda item: (
                str(item["source_event_id"]),
                str(item["extractor_version"]),
            ),
        )
        return canonical_json_hash({"summary": summary_payload, "memories": memory_payloads})

    async def complete_projection(
        self,
        claim: ProjectionClaim,
        *,
        summary: SummaryProjection | None,
        memories: Sequence[MemoryProjection],
    ) -> ProjectionFinalizeResult:
        """Atomically publish monotonic outputs and mark the fenced job complete."""

        if summary is not None and (
            summary.tenant_id != claim.tenant_id
            or summary.session_id != claim.session_id
            or summary.through_seq != claim.through_seq
            or not summary.summarizer_version
            or len(summary.summarizer_version) > 64
        ):
            raise ValueError("summary does not match the claimed tenant/session watermark")
        memory_keys = [(item.source_event_id, item.extractor_version) for item in memories]
        if len(memory_keys) != len(set(memory_keys)):
            raise ValueError("projection completion contains duplicate memory keys")
        result_hash = self._projection_result_hash(summary, memories)

        async with self._transaction(claim.tenant_id) as database:
            statement = select(ProjectionJob).where(
                ProjectionJob.tenant_id == claim.tenant_id,
                ProjectionJob.job_id == claim.job_id,
            )
            if self._dialect == "postgresql":
                statement = statement.with_for_update()
            job = await database.scalar(statement)
            if job is None:
                raise ReliabilityInvariantError("projection claim references a missing job")
            if job.status == "succeeded":
                if job.result_hash != result_hash:
                    raise IdempotencyConflictError(
                        "projection retry differs from the committed outputs"
                    )
                return ProjectionFinalizeResult(
                    disposition=ProjectionFinalizeDisposition.ALREADY_FINALIZED,
                    job_id=job.job_id,
                    summary_applied=False,
                    memories_applied=0,
                )

            now = await self._db_now(database)
            await self._load_active_projection_job(database, claim, now, lock=True)
            session = await database.scalar(
                select(Session).where(
                    Session.tenant_id == claim.tenant_id,
                    Session.session_id == claim.session_id,
                )
            )
            if session is None or claim.through_seq > session.state_version:
                raise ReliabilityInvariantError(
                    "projection output watermark is ahead of committed Session state"
                )

            event_versions: dict[str, int] = {}
            if memories:
                source_ids = {item.source_event_id for item in memories}
                event_rows = (
                    await database.execute(
                        select(SessionEvent.event_id, SessionEvent.seq).where(
                            SessionEvent.tenant_id == claim.tenant_id,
                            SessionEvent.session_id == claim.session_id,
                            SessionEvent.visibility == "committed",
                            SessionEvent.seq <= claim.through_seq,
                            SessionEvent.event_id.in_(source_ids),
                        )
                    )
                ).all()
                event_versions = {row.event_id: row.seq for row in event_rows}
                if set(event_versions) != source_ids:
                    raise ValueError("memory source must be a visible committed event")

            for memory in memories:
                if (
                    memory.tenant_id != claim.tenant_id
                    or memory.session_id != claim.session_id
                    or memory.principal_id != session.principal_id
                    or memory.record_version != event_versions[memory.source_event_id]
                    or not memory.extractor_version
                    or len(memory.extractor_version) > 64
                ):
                    raise ValueError(
                        "memory does not match the claimed tenant/session/event version"
                    )

            summary_applied = False
            if summary is not None:
                summary_applied = await SqlProjectionStore.put_summary_if_newer_in_session(
                    database,
                    summary,
                )
            memories_applied = 0
            for memory in memories:
                if await SqlProjectionStore.put_memory_once_in_session(database, memory):
                    memories_applied += 1

            job.status = "succeeded"
            job.result_hash = result_hash
            job.claimed_by = None
            job.claim_expires_at = None
            job.last_error_type = None
            job.completed_at = now
            await database.flush()
            return ProjectionFinalizeResult(
                disposition=ProjectionFinalizeDisposition.FINALIZED,
                job_id=job.job_id,
                summary_applied=summary_applied,
                memories_applied=memories_applied,
            )

    async def defer_projection_retry(
        self,
        claim: ProjectionClaim,
        *,
        retry_delay: timedelta,
        error_type: str,
    ) -> None:
        """Return a live projection job to ``retry_wait`` under its fence."""

        if retry_delay <= timedelta(0):
            raise ValueError("retry_delay must be positive")
        if not error_type or len(error_type) > 128:
            raise ValueError("error_type must contain 1..128 sanitized characters")
        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            job = await self._load_active_projection_job(database, claim, now, lock=True)
            job.status = "retry_wait"
            job.next_attempt_at = now + retry_delay
            job.claimed_by = None
            job.claim_expires_at = None
            job.last_error_type = error_type
            await database.flush()

    async def dead_letter_projection(
        self,
        claim: ProjectionClaim,
        *,
        error_type: str,
    ) -> None:
        """Terminally quarantine a projection job under its live fence."""

        if not error_type or len(error_type) > 128:
            raise ValueError("error_type must contain 1..128 sanitized characters")
        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            job = await self._load_active_projection_job(database, claim, now, lock=True)
            job.status = "dead_letter"
            job.claimed_by = None
            job.claim_expires_at = None
            job.last_error_type = error_type
            job.completed_at = now
            await database.flush()

    async def _load_reply_credential(
        self,
        database: AsyncSession,
        *,
        tenant_id: str,
        run_id: str,
        now: datetime,
    ) -> ReplyCredentialRef | None:
        run = await database.scalar(
            select(AgentRun).where(
                AgentRun.tenant_id == tenant_id,
                AgentRun.run_id == run_id,
            )
        )
        if run is None:
            raise ReliabilityInvariantError("Outbox references a missing AgentRun")
        statement = (
            select(ChannelReplyCredential)
            .where(
                ChannelReplyCredential.tenant_id == tenant_id,
                ChannelReplyCredential.inbox_id == run.inbox_id,
                ChannelReplyCredential.status == "active",
            )
            .order_by(ChannelReplyCredential.credential_id)
            .limit(2)
        )
        if self._dialect == "postgresql":
            statement = statement.with_for_update()
        credentials = list((await database.scalars(statement)).all())
        if len(credentials) > 1:
            raise ReliabilityInvariantError("one Inbox has multiple active outbound credentials")
        if not credentials:
            return None
        credential = credentials[0]
        if _aware_utc(credential.expires_at) <= now:
            credential.status = "expired"
            return None
        return ReplyCredentialRef(
            credential_id=credential.credential_id,
            credential_kind=credential.credential_kind,
            ciphertext=credential.ciphertext,
            ciphertext_hash=credential.ciphertext_hash,
            expires_at=_aware_utc(credential.expires_at),
        )

    async def _quarantine_expired_outbox_claims(
        self,
        database: AsyncSession,
        *,
        tenant_id: str,
        now: datetime,
    ) -> None:
        statement = (
            select(ReplyOutbox, AgentRun.inbox_id)
            .join(
                AgentRun,
                (AgentRun.tenant_id == ReplyOutbox.tenant_id)
                & (AgentRun.run_id == ReplyOutbox.run_id),
            )
            .where(
                ReplyOutbox.tenant_id == tenant_id,
                ReplyOutbox.status == "sending",
                ReplyOutbox.claim_expires_at.is_not(None),
                ReplyOutbox.claim_expires_at <= now,
            )
            .order_by(ReplyOutbox.claim_expires_at, ReplyOutbox.outbox_id)
            .limit(100)
        )
        if self._dialect == "postgresql":
            statement = statement.with_for_update(of=ReplyOutbox, skip_locked=True)
        expired = list((await database.execute(statement)).all())
        if not expired:
            return
        inbox_ids: set[str] = set()
        for row in expired:
            outbox, inbox_id = row[0], row[1]
            inbox_ids.add(inbox_id)
            outbox.status = "unknown"
            outbox.claimed_by = None
            outbox.claim_expires_at = None
            outbox.last_error_type = "delivery_claim_expired"
        await database.execute(
            update(ChannelReplyCredential)
            .where(
                ChannelReplyCredential.tenant_id == tenant_id,
                ChannelReplyCredential.inbox_id.in_(inbox_ids),
                ChannelReplyCredential.status == "active",
            )
            .values(status="unknown")
        )

    async def claim_outbox(
        self,
        tenant_id: str,
        dispatcher_id: str,
        *,
        lease_ttl: timedelta = timedelta(seconds=20),
    ) -> OutboxDeliveryClaim | None:
        """Claim the next ordered outbound part; ambiguous expired sends go UNKNOWN."""

        if not dispatcher_id:
            raise ValueError("dispatcher_id must not be empty")
        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")
        async with self._transaction(tenant_id) as database:
            now = await self._db_now(database)
            await self._quarantine_expired_outbox_claims(
                database,
                tenant_id=tenant_id,
                now=now,
            )
            prior = aliased(ReplyOutbox)
            has_unsent_prior_part = exists(
                select(1).where(
                    prior.tenant_id == ReplyOutbox.tenant_id,
                    prior.run_id == ReplyOutbox.run_id,
                    prior.reply_id == ReplyOutbox.reply_id,
                    prior.part_no < ReplyOutbox.part_no,
                    prior.status != "sent",
                )
            )
            statement = (
                select(ReplyOutbox)
                .where(
                    ReplyOutbox.tenant_id == tenant_id,
                    ReplyOutbox.status.in_(("pending", "retry_wait")),
                    ReplyOutbox.next_retry_at <= now,
                    ~has_unsent_prior_part,
                )
                .order_by(
                    ReplyOutbox.next_retry_at,
                    ReplyOutbox.created_at,
                    ReplyOutbox.outbox_id,
                )
                .limit(1)
            )
            if self._dialect == "postgresql":
                statement = statement.with_for_update(
                    of=ReplyOutbox,
                    skip_locked=True,
                )
            outbox = await database.scalar(statement)
            if outbox is None:
                return None

            delivery_token = outbox.delivery_token or new_id()
            outbox.delivery_token = delivery_token
            outbox.status = "sending"
            outbox.claimed_by = dispatcher_id
            outbox.claim_expires_at = now + lease_ttl
            outbox.attempts += 1
            credential = await self._load_reply_credential(
                database,
                tenant_id=tenant_id,
                run_id=outbox.run_id,
                now=now,
            )
            external_delivery_id = await database.scalar(
                select(InboxMessage.external_delivery_id)
                .join(
                    AgentRun,
                    (AgentRun.tenant_id == InboxMessage.tenant_id)
                    & (AgentRun.inbox_id == InboxMessage.inbox_id),
                )
                .where(
                    AgentRun.tenant_id == tenant_id,
                    AgentRun.run_id == outbox.run_id,
                )
            )
            if external_delivery_id is None:
                raise ReliabilityInvariantError("Outbox run has no external delivery identity")
            await database.flush()
            claim = OutboxDeliveryClaim(
                tenant_id=tenant_id,
                outbox_id=outbox.outbox_id,
                run_id=outbox.run_id,
                binding_id=outbox.binding_id,
                session_id=outbox.session_id,
                delivery_id=external_delivery_id,
                reply_id=outbox.reply_id,
                part_no=outbox.part_no,
                payload=json.loads(json.dumps(outbox.payload)),
                payload_hash=outbox.payload_hash,
                dispatcher_id=dispatcher_id,
                delivery_token=delivery_token,
                attempt_no=outbox.attempts,
                claim_expires_at=_aware_utc(outbox.claim_expires_at),
                reply_credential=credential,
            )
        return claim

    async def renew_outbox_claim(
        self,
        claim: OutboxDeliveryClaim,
        *,
        lease_ttl: timedelta = timedelta(seconds=20),
    ) -> bool:
        """Extend one live delivery attempt without rotating its stable token."""

        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")
        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            result = await database.execute(
                update(ReplyOutbox)
                .where(
                    ReplyOutbox.tenant_id == claim.tenant_id,
                    ReplyOutbox.outbox_id == claim.outbox_id,
                    ReplyOutbox.status == "sending",
                    ReplyOutbox.claimed_by == claim.dispatcher_id,
                    ReplyOutbox.delivery_token == claim.delivery_token,
                    ReplyOutbox.attempts == claim.attempt_no,
                    ReplyOutbox.claim_expires_at > now,
                )
                .values(claim_expires_at=now + lease_ttl)
            )
            renewed = _rowcount(result) == 1
        return renewed

    async def record_delivery(
        self,
        claim: OutboxDeliveryClaim,
        outcome: OutboxDeliveryOutcome,
        *,
        external_message_id: str | None = None,
        error_type: str | None = None,
        next_retry_at: datetime | None = None,
    ) -> bool:
        """Record one outbound result under token plus attempt fencing."""

        try:
            normalized_outcome = OutboxDeliveryOutcome(outcome)
        except ValueError as error:
            raise ValueError(f"invalid Outbox delivery outcome: {outcome!r}") from error
        if normalized_outcome not in _OUTBOX_COMPLETIONS:
            raise ValueError(f"invalid Outbox delivery outcome: {outcome!r}")
        if normalized_outcome is OutboxDeliveryOutcome.RETRY_WAIT and next_retry_at is None:
            raise ValueError("retry_wait requires next_retry_at")

        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            values: dict[str, Any] = {
                "status": normalized_outcome.value,
                "claimed_by": None,
                "claim_expires_at": None,
                "last_error_type": error_type,
            }
            if external_message_id is not None:
                values["external_message_id"] = external_message_id
            if normalized_outcome is OutboxDeliveryOutcome.SENT:
                values["delivered_at"] = now
            if next_retry_at is not None:
                values["next_retry_at"] = _aware_utc(next_retry_at)
            result = await database.execute(
                update(ReplyOutbox)
                .where(
                    ReplyOutbox.tenant_id == claim.tenant_id,
                    ReplyOutbox.outbox_id == claim.outbox_id,
                    ReplyOutbox.status == "sending",
                    ReplyOutbox.claimed_by == claim.dispatcher_id,
                    ReplyOutbox.delivery_token == claim.delivery_token,
                    ReplyOutbox.attempts == claim.attempt_no,
                )
                .values(**values)
            )
            changed = _rowcount(result) == 1
            if changed and claim.reply_credential is not None:
                if normalized_outcome is OutboxDeliveryOutcome.SENT:
                    unsent_remains = await database.scalar(
                        select(
                            exists().where(
                                ReplyOutbox.tenant_id == claim.tenant_id,
                                ReplyOutbox.run_id == claim.run_id,
                                ReplyOutbox.status != "sent",
                            )
                        )
                    )
                    if not unsent_remains:
                        await database.execute(
                            update(ChannelReplyCredential)
                            .where(
                                ChannelReplyCredential.tenant_id == claim.tenant_id,
                                ChannelReplyCredential.credential_id
                                == claim.reply_credential.credential_id,
                                ChannelReplyCredential.status == "active",
                            )
                            .values(status="consumed", consumed_at=now)
                        )
                elif normalized_outcome is OutboxDeliveryOutcome.UNKNOWN:
                    await database.execute(
                        update(ChannelReplyCredential)
                        .where(
                            ChannelReplyCredential.tenant_id == claim.tenant_id,
                            ChannelReplyCredential.credential_id
                            == claim.reply_credential.credential_id,
                            ChannelReplyCredential.status == "active",
                        )
                        .values(status="unknown")
                    )
        return changed

    @staticmethod
    def _tool_reservation(
        effect: ToolEffect,
        disposition: ToolReservationDisposition,
    ) -> ToolReservation:
        return ToolReservation(
            disposition=disposition,
            effect_id=effect.effect_id,
            status=effect.status,
            execution_token=effect.execution_token,
            attempt_count=effect.attempt_count,
            result_ref=effect.result_ref,
            result_hash=effect.result_hash,
        )

    async def reserve_tool_effect(
        self,
        claim: SessionClaim,
        request: ToolEffectRequest,
        *,
        execution_ttl: timedelta = timedelta(seconds=30),
    ) -> ToolReservation:
        """Reserve one stable Tool Effect, retrying a concurrent first insert."""

        try:
            return await self._reserve_tool_effect_once(
                claim,
                request,
                execution_ttl=execution_ttl,
            )
        except IntegrityError:
            # PostgreSQL cannot row-lock a missing idempotency key. The unique
            # constraint elects one creator; after it commits, the loser rereads.
            return await self._reserve_tool_effect_once(
                claim,
                request,
                execution_ttl=execution_ttl,
            )

    async def _reserve_tool_effect_once(
        self,
        claim: SessionClaim,
        request: ToolEffectRequest,
        *,
        execution_ttl: timedelta = timedelta(seconds=30),
    ) -> ToolReservation:
        """Reserve one stable tool effect and enforce the UNKNOWN boundary."""

        if request.effect_class not in _TOOL_EFFECT_CLASSES:
            raise ValueError(f"invalid tool effect class: {request.effect_class!r}")
        if execution_ttl <= timedelta(0):
            raise ValueError("execution_ttl must be positive")

        async with self._transaction(claim.tenant_id) as database:
            now = await self._db_now(database)
            await self._load_active_session(database, claim, now, lock=True)
            statement = select(ToolEffect).where(
                ToolEffect.tenant_id == claim.tenant_id,
                ToolEffect.idempotency_key == request.idempotency_key,
            )
            if self._dialect == "postgresql":
                statement = statement.with_for_update()
            effect = await database.scalar(statement)

            if effect is None:
                effect = ToolEffect(
                    tenant_id=claim.tenant_id,
                    run_id=claim.run_id,
                    session_id=claim.session_id,
                    idempotency_key=request.idempotency_key,
                    tool_name=request.tool_name,
                    tool_version=request.tool_version,
                    effect_class=request.effect_class,
                    args_hash=request.args_hash,
                    status="executing",
                    execution_token=new_id(),
                    execution_owner=claim.worker_id,
                    execution_expires_at=now + execution_ttl,
                    downstream_key=request.downstream_key,
                    attempt_count=1,
                    next_attempt_at=now,
                )
                database.add(effect)
                await database.flush()
                reservation = self._tool_reservation(
                    effect,
                    ToolReservationDisposition.EXECUTE,
                )
                return reservation

            if (
                effect.args_hash != request.args_hash
                or effect.tool_name != request.tool_name
                or effect.tool_version != request.tool_version
                or effect.effect_class != request.effect_class
            ):
                raise IdempotencyConflictError(
                    "tool idempotency key was reused with different operation content"
                )

            if effect.status == "succeeded":
                return self._tool_reservation(
                    effect,
                    ToolReservationDisposition.SUCCEEDED,
                )
            if effect.status == "unknown":
                return self._tool_reservation(
                    effect,
                    ToolReservationDisposition.UNKNOWN,
                )
            if effect.status == "failed_final":
                return self._tool_reservation(
                    effect,
                    ToolReservationDisposition.FAILED_FINAL,
                )
            if effect.status == "retry_wait" and _aware_utc(effect.next_attempt_at) > now:
                return self._tool_reservation(
                    effect,
                    ToolReservationDisposition.RETRY_WAIT,
                )
            if (
                effect.status == "executing"
                and effect.execution_expires_at is not None
                and _aware_utc(effect.execution_expires_at) > now
            ):
                return self._tool_reservation(
                    effect,
                    ToolReservationDisposition.IN_PROGRESS,
                )
            if effect.status == "executing" and effect.effect_class not in _SAFE_TOOL_RECLAIM:
                effect.status = "unknown"
                effect.execution_owner = None
                effect.execution_expires_at = None
                await database.flush()
                return self._tool_reservation(
                    effect,
                    ToolReservationDisposition.UNKNOWN,
                )
            if effect.status not in {"reserved", "executing", "retry_wait"}:
                raise InvalidStateTransitionError(
                    f"cannot reserve ToolEffect from status {effect.status!r}"
                )

            effect.status = "executing"
            effect.execution_token = new_id()
            effect.execution_owner = claim.worker_id
            effect.execution_expires_at = now + execution_ttl
            effect.attempt_count += 1
            await database.flush()
            return self._tool_reservation(
                effect,
                ToolReservationDisposition.EXECUTE,
            )

    async def complete_tool_effect(
        self,
        *,
        tenant_id: str,
        effect_id: str,
        execution_token: str,
        outcome: str,
        result_ref: str | None = None,
        result_hash: str | None = None,
        downstream_id: str | None = None,
        error_type: str | None = None,
        next_attempt_at: datetime | None = None,
    ) -> bool:
        """Persist the external outcome under the Tool Effect's own fence."""

        if outcome not in _TOOL_COMPLETIONS:
            raise ValueError(f"invalid ToolEffect completion: {outcome!r}")
        if outcome == "retry_wait" and next_attempt_at is None:
            raise ValueError("retry_wait requires next_attempt_at")

        async with self._transaction(tenant_id) as database:
            values: dict[str, Any] = {
                "status": outcome,
                "execution_owner": None,
                "execution_expires_at": None,
                "result_ref": result_ref,
                "result_hash": result_hash,
                "downstream_id": downstream_id,
                "error_type": error_type,
            }
            if next_attempt_at is not None:
                values["next_attempt_at"] = _aware_utc(next_attempt_at)
            result = await database.execute(
                update(ToolEffect)
                .where(
                    ToolEffect.tenant_id == tenant_id,
                    ToolEffect.effect_id == effect_id,
                    ToolEffect.execution_token == execution_token,
                    ToolEffect.status == "executing",
                )
                .values(**values)
            )
            changed = _rowcount(result) == 1
        return changed

"""Durable per-session mailbox state and fenced lease transitions.

The mailbox is intentionally separate from the Redis delivery mechanism.  Redis
can wake a worker, but this store is the authority for ordering, ownership, and
recovery of one session's accepted messages.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import asyncpg

from trpc_service.storage.models import (
    Acceptance,
    MailboxClaimStatus,
    MailboxItem,
    MailboxLease,
    MailboxStatus,
    OutboxRecord,
    SessionClaim,
    SessionMailbox,
)
from trpc_service.storage.protocols import FencingConflict

_RECONCILE_READY_GRACE = timedelta(seconds=5)
# A published wake-up is only a hint that Redis accepted the notification.  A
# short observation window prevents the reconciler from treating a normal
# dispatcher hand-off as a Redis loss on every recovery tick.  The migration
# stores the last replay timestamp per generation as the durable guard; this
# grace period is the bounded evidence window between repeat recovery attempts.
_RECONCILE_READY_REPLAY_GRACE = timedelta(seconds=30)


def _now(value: datetime | None) -> datetime:
    return value if value is not None else datetime.now(UTC)


def _validate_lease_for(lease_for: timedelta) -> None:
    if lease_for <= timedelta(0):
        raise ValueError("mailbox lease must be positive")


def _validate_priority(priority: int) -> None:
    if isinstance(priority, bool) or priority < 0:
        raise ValueError("mailbox priority must be non-negative")


class InMemorySessionMailboxStore:
    """Deterministic mailbox implementation used by contract tests and local mode."""

    def __init__(self) -> None:
        self._mailboxes: dict[tuple[str, str], SessionMailbox] = {}
        self._items: dict[tuple[str, str], dict[int, MailboxItem]] = defaultdict(dict)
        self._locks: defaultdict[tuple[str, str], asyncio.Lock] = defaultdict(asyncio.Lock)
        self._outbox: list[OutboxRecord] = []

    @property
    def outbox(self) -> tuple[OutboxRecord, ...]:
        """Wake-up records emitted by mailbox transitions, for local wiring/tests."""

        return tuple(self._outbox)

    async def get(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        return self._mailboxes.get((tenant_id, session_id))

    async def accept(
        self,
        tenant_id: str,
        session_id: str,
        inbound_id: str,
        *,
        priority: int = 0,
        retry_at: datetime | None = None,
        trace_id: str | None = None,
        now: datetime | None = None,
    ) -> SessionMailbox:
        _validate_ids(tenant_id, session_id, inbound_id)
        _validate_uuid_if_present(inbound_id)
        _validate_priority(priority)
        current_time = _now(now)
        key = (tenant_id, session_id)
        async with self._locks[key]:
            mailbox = self._mailboxes.get(key)
            items = self._items[key]
            existing = next(
                (item for item in items.values() if item.inbound_id == inbound_id),
                None,
            )
            if existing is not None:
                return mailbox if mailbox is not None else self._ensure_mailbox(key, current_time)

            if mailbox is None:
                mailbox = self._ensure_mailbox(key, current_time)
            sequence = mailbox.accepted_sequence + 1
            items[sequence] = MailboxItem(
                tenant_id=tenant_id,
                session_id=session_id,
                inbound_id=inbound_id,
                sequence=sequence,
                trace_id=trace_id or inbound_id,
                priority=priority,
                retry_count=0,
                attempt=0,
                retry_at=retry_at,
                accepted_at=current_time,
            )
            previous_status = mailbox.status
            current_head_waiting = (
                previous_status == MailboxStatus.RETRY_WAIT
                and mailbox.retry_at is not None
                and mailbox.retry_at > current_time
            )
            if previous_status == MailboxStatus.RETRY_WAIT:
                if current_head_waiting:
                    # A later item's retry time must not replace the current
                    # head timer or make the head runnable prematurely.
                    status = MailboxStatus.RETRY_WAIT
                    effective_retry_at = mailbox.retry_at
                else:
                    # The current head is due.  A future retry time belongs to
                    # the newly appended item and is evaluated after the head
                    # is resolved; it must not block the head now.
                    status = MailboxStatus.QUEUED
                    effective_retry_at = None
            elif previous_status == MailboxStatus.IDLE:
                if retry_at is not None and retry_at > current_time:
                    status = MailboxStatus.RETRY_WAIT
                    effective_retry_at = retry_at
                else:
                    status = MailboxStatus.QUEUED
                    effective_retry_at = None
            else:
                status = previous_status
                effective_retry_at = mailbox.retry_at
            updated = mailbox.model_copy(
                update={
                    "status": status,
                    "accepted_sequence": sequence,
                    "queue_generation": mailbox.queue_generation,
                    "priority": max(mailbox.priority, priority),
                    "retry_at": effective_retry_at,
                    "updated_at": current_time,
                }
            )
            if (
                previous_status in (MailboxStatus.IDLE, MailboxStatus.RETRY_WAIT)
                and status == MailboxStatus.QUEUED
            ):
                updated = self._emit_ready(updated, items[sequence], current_time)
            self._mailboxes[key] = updated
            return updated

    async def claim(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
        now: datetime | None = None,
    ) -> MailboxLease | None:
        _validate_ids(tenant_id, session_id, owner_id)
        _validate_lease_for(lease_for)
        current_time = _now(now)
        key = (tenant_id, session_id)
        async with self._locks[key]:
            mailbox = self._mailboxes.get(key)
            if mailbox is None:
                return None
            if mailbox.lease_expires_at is not None and mailbox.lease_expires_at > current_time:
                return None
            if mailbox.processing_sequence is not None:
                # An expired lease is eligible for takeover; its sequence remains
                # in-flight until a fenced commit or retry resolves it.
                sequence = mailbox.processing_sequence
            else:
                if mailbox.resolved_sequence >= mailbox.accepted_sequence:
                    self._mailboxes[key] = mailbox.model_copy(
                        update={"status": MailboxStatus.IDLE, "updated_at": current_time}
                    )
                    return None
                sequence = mailbox.resolved_sequence + 1
                item = self._items[key].get(sequence)
                if item is None:
                    return None
                if item.retry_at is not None and item.retry_at > current_time:
                    self._mailboxes[key] = mailbox.model_copy(
                        update={
                            "status": MailboxStatus.RETRY_WAIT,
                            "retry_at": item.retry_at,
                            "updated_at": current_time,
                        }
                    )
                    return None

            item = self._items[key].get(sequence)
            if item is None:
                return None
            epoch = mailbox.lease_epoch + 1
            expires_at = current_time + lease_for
            attempt = item.attempt + 1
            updated_item = item.model_copy(update={"attempt": attempt})
            self._items[key][sequence] = updated_item
            self._mailboxes[key] = mailbox.model_copy(
                update={
                    "status": MailboxStatus.RUNNING,
                    "processing_sequence": sequence,
                    "processing_inbound_id": item.inbound_id,
                    "lease_owner": owner_id,
                    "lease_epoch": epoch,
                    "lease_expires_at": expires_at,
                    "attempt": attempt,
                    "retry_count": item.retry_count,
                    "priority": item.priority,
                    "retry_at": item.retry_at,
                    "updated_at": current_time,
                }
            )
            return MailboxLease(
                tenant_id=tenant_id,
                session_id=session_id,
                inbound_id=item.inbound_id,
                sequence=sequence,
                owner_id=owner_id,
                epoch=epoch,
                expires_at=expires_at,
                attempt=attempt,
                retry_count=item.retry_count,
                priority=item.priority,
            )

    async def claim_session(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
        expected_generation: int | None = None,
        expected_epoch: int | None = None,
        acceptance: Acceptance | None = None,
        now: datetime | None = None,
    ) -> SessionClaim:
        """Claim one sequence while making no-work/ownership outcomes explicit."""

        current_time = _now(now)
        if expected_generation is None:
            expected_generation = expected_epoch
        before = await self.get(tenant_id, session_id)
        if before is None:
            before = SessionMailbox(tenant_id=tenant_id, session_id=session_id)
        if expected_generation is not None and before.queue_generation != expected_generation:
            return SessionClaim(
                status=MailboxClaimStatus.STALE, mailbox=before, acceptance=acceptance
            )
        lease = await self.claim(
            tenant_id,
            session_id,
            owner_id=owner_id,
            lease_for=lease_for,
            now=current_time,
        )
        mailbox = await self.get(tenant_id, session_id) or before
        if lease is not None:
            return SessionClaim(
                status=MailboxClaimStatus.CLAIMED,
                mailbox=mailbox,
                lease=lease,
                acceptance=acceptance,
            )
        if mailbox.lease_expires_at is not None and mailbox.lease_expires_at > current_time:
            return SessionClaim(
                status=MailboxClaimStatus.RUNNING, mailbox=mailbox, acceptance=acceptance
            )
        if expected_generation is not None and mailbox.queue_generation != expected_generation:
            return SessionClaim(
                status=MailboxClaimStatus.STALE, mailbox=mailbox, acceptance=acceptance
            )
        return SessionClaim(status=MailboxClaimStatus.EMPTY, mailbox=mailbox, acceptance=acceptance)

    async def renew(
        self,
        lease: MailboxLease,
        *,
        lease_for: timedelta,
        now: datetime | None = None,
    ) -> MailboxLease:
        _validate_lease_for(lease_for)
        current_time = _now(now)
        key = (lease.tenant_id, lease.session_id)
        async with self._locks[key]:
            mailbox = self._mailboxes.get(key)
            if not _owns(mailbox, lease, current_time):
                raise FencingConflict("mailbox lease is no longer current")
            assert mailbox is not None and mailbox.lease_expires_at is not None
            expires_at = max(
                current_time + lease_for,
                mailbox.lease_expires_at + timedelta(microseconds=1),
            )
            self._mailboxes[key] = mailbox.model_copy(
                update={"lease_expires_at": expires_at, "updated_at": current_time}
            )
            return lease.model_copy(update={"expires_at": expires_at})

    async def renew_many(
        self,
        leases: tuple[MailboxLease, ...],
        *,
        lease_for: timedelta,
        now: datetime | None = None,
    ) -> tuple[MailboxLease, ...]:
        """Renew a batch, failing closed if any member is stale."""

        renewed: list[MailboxLease] = []
        for lease in leases:
            renewed.append(await self.renew(lease, lease_for=lease_for, now=now))
        return tuple(renewed)

    async def commit(
        self,
        lease: MailboxLease,
        *,
        now: datetime | None = None,
    ) -> SessionMailbox:
        current_time = _now(now)
        key = (lease.tenant_id, lease.session_id)
        async with self._locks[key]:
            mailbox = self._require_owned(key, lease, current_time)
            if mailbox.processing_sequence != lease.sequence:
                raise FencingConflict("mailbox sequence is no longer current")
            next_item = self._items[key].get(lease.sequence + 1)
            resolved = mailbox.model_copy(
                update={
                    "resolved_sequence": lease.sequence,
                    "processing_sequence": None,
                    "processing_inbound_id": None,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "status": _ready_status(mailbox, lease.sequence, current_time),
                    "retry_at": None,
                    "updated_at": current_time,
                }
            )
            if next_item is not None:
                if next_item.retry_at is not None and next_item.retry_at > current_time:
                    resolved = resolved.model_copy(
                        update={
                            "status": MailboxStatus.RETRY_WAIT,
                            "retry_at": next_item.retry_at,
                        }
                    )
                else:
                    resolved = resolved.model_copy(update={"status": MailboxStatus.QUEUED})
                    resolved = self._emit_ready(resolved, next_item, current_time)
            self._mailboxes[key] = resolved
            return resolved

    async def reschedule(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
        now: datetime | None = None,
    ) -> SessionMailbox:
        return await self._release(
            lease,
            retry_at=retry_at,
            priority=priority,
            increment_retry=False,
            now=now,
        )

    async def retry(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
        now: datetime | None = None,
    ) -> SessionMailbox:
        return await self._release(
            lease,
            retry_at=retry_at,
            priority=priority,
            increment_retry=True,
            now=now,
        )

    async def retry_without_wakeup(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None,
        now: datetime | None = None,
    ) -> SessionMailbox:
        """Move a claimed item to retry wait without publishing a ready event."""

        return await self._release(
            lease,
            retry_at=retry_at,
            priority=None,
            increment_retry=True,
            now=now,
            emit_ready=False,
        )

    async def _release(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None,
        priority: int | None,
        increment_retry: bool,
        now: datetime | None,
        emit_ready: bool = True,
    ) -> SessionMailbox:
        current_time = _now(now)
        if priority is not None:
            _validate_priority(priority)
        key = (lease.tenant_id, lease.session_id)
        async with self._locks[key]:
            mailbox = self._require_owned(key, lease, current_time)
            if mailbox.processing_sequence != lease.sequence:
                raise FencingConflict("mailbox sequence is no longer current")
            item = self._items[key][lease.sequence]
            retries = item.retry_count + (1 if increment_retry else 0)
            updated_item = item.model_copy(
                update={
                    "retry_count": retries,
                    "retry_at": retry_at,
                    "priority": item.priority if priority is None else priority,
                }
            )
            self._items[key][lease.sequence] = updated_item
            status = _ready_status(
                mailbox.model_copy(update={"retry_at": retry_at}),
                mailbox.resolved_sequence,
                current_time,
            )
            if not emit_ready:
                status = MailboxStatus.RETRY_WAIT
            updated = mailbox.model_copy(
                update={
                    "status": status,
                    "processing_sequence": None,
                    "processing_inbound_id": None,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "retry_count": retries,
                    "attempt": item.attempt,
                    "priority": updated_item.priority,
                    "retry_at": retry_at,
                    "queue_generation": mailbox.queue_generation,
                    "updated_at": current_time,
                }
            )
            if status == MailboxStatus.QUEUED and emit_ready:
                updated = self._emit_ready(updated, updated_item, current_time)
            self._mailboxes[key] = updated
            return updated

    async def recover(
        self,
        tenant_id: str,
        session_id: str,
        *,
        now: datetime | None = None,
    ) -> SessionMailbox | None:
        current_time = _now(now)
        key = (tenant_id, session_id)
        async with self._locks[key]:
            mailbox = self._mailboxes.get(key)
            if mailbox is None or mailbox.lease_expires_at is None:
                return None
            if mailbox.lease_expires_at > current_time:
                return None
            sequence = mailbox.processing_sequence or mailbox.resolved_sequence + 1
            item = self._items[key].get(sequence)
            if item is None or sequence > mailbox.accepted_sequence:
                status = MailboxStatus.IDLE
                retry_at = None
            elif item.retry_at is not None and item.retry_at > current_time:
                status = MailboxStatus.RETRY_WAIT
                retry_at = item.retry_at
            else:
                status = MailboxStatus.QUEUED
                retry_at = None
            updated = mailbox.model_copy(
                update={
                    "status": status,
                    "processing_sequence": None,
                    "processing_inbound_id": None,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "retry_at": retry_at,
                    "queue_generation": mailbox.queue_generation,
                    "updated_at": current_time,
                }
            )
            if status == MailboxStatus.QUEUED and item is not None:
                updated = self._emit_ready(updated, item, current_time)
            self._mailboxes[key] = updated
            return updated

    async def reconcile(
        self,
        tenant_id: str,
        session_id: str,
        *,
        now: datetime | None = None,
    ) -> SessionMailbox | None:
        current_time = _now(now)
        key = (tenant_id, session_id)
        async with self._locks[key]:
            mailbox = self._mailboxes.get(key)
            if mailbox is None:
                return None
            expired = (
                mailbox.lease_expires_at is not None and mailbox.lease_expires_at <= current_time
            )
            active = bool(
                mailbox.processing_sequence is not None
                and mailbox.processing_inbound_id is not None
                and mailbox.lease_owner is not None
                and mailbox.lease_expires_at is not None
                and not expired
            )
            sequence = mailbox.processing_sequence or mailbox.resolved_sequence + 1
            item = self._items[key].get(sequence)
            if item is None or sequence > mailbox.accepted_sequence:
                status = MailboxStatus.IDLE
                retry_at = None
            elif item.retry_at is not None and item.retry_at > current_time:
                status = MailboxStatus.RETRY_WAIT
                retry_at = item.retry_at
            else:
                status = MailboxStatus.QUEUED
                retry_at = None
            if active:
                status = MailboxStatus.RUNNING
            queued_is_recent = (
                not active
                and status == MailboxStatus.QUEUED
                and item is not None
                and mailbox.status == MailboxStatus.QUEUED
                and current_time - mailbox.updated_at < _RECONCILE_READY_GRACE
            )
            if queued_is_recent:
                return mailbox
            updated = mailbox.model_copy(
                update={
                    "status": status,
                    "processing_sequence": mailbox.processing_sequence if active else None,
                    "processing_inbound_id": (mailbox.processing_inbound_id if active else None),
                    "lease_owner": mailbox.lease_owner if active else None,
                    "lease_expires_at": mailbox.lease_expires_at if active else None,
                    "retry_at": mailbox.retry_at if active else retry_at,
                    "updated_at": current_time,
                }
            )
            if not active and status == MailboxStatus.QUEUED and item is not None:
                if mailbox.status == MailboxStatus.QUEUED and mailbox.queue_generation >= 1:
                    # A long-lived QUEUED mailbox is still the same unit of
                    # work. Reconciliation replays that durable generation;
                    # it must not manufacture a new one on every pass.
                    updated = self._requeue_ready(updated, item, current_time)
                else:
                    # A real state transition (for example RETRY_WAIT after
                    # retry_at becomes due) is a new scheduling decision and
                    # therefore gets a fresh generation.
                    updated = self._emit_ready(updated, item, current_time)
            self._mailboxes[key] = updated
            return updated

    async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int:
        del owner_id
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        current_time = datetime.now(UTC)
        handled = 0
        for tenant_id, session_id in tuple(self._mailboxes):
            if handled >= limit:
                break
            mailbox = self._mailboxes[(tenant_id, session_id)]
            if mailbox.lease_expires_at is not None and mailbox.lease_expires_at <= current_time:
                if await self.recover(tenant_id, session_id, now=current_time) is not None:
                    handled += 1
        return handled

    async def schedule_retries(self, *, owner_id: str, limit: int) -> int:
        del owner_id
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        current_time = datetime.now(UTC)
        handled = 0
        for tenant_id, session_id in tuple(self._mailboxes):
            if handled >= limit:
                break
            mailbox = self._mailboxes[(tenant_id, session_id)]
            if (
                mailbox.status == MailboxStatus.RETRY_WAIT
                and mailbox.retry_at is not None
                and mailbox.retry_at <= current_time
            ):
                if await self.reconcile(tenant_id, session_id, now=current_time) is not None:
                    handled += 1
        return handled

    async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int:
        del owner_id
        if limit < 1:
            raise ValueError("recovery limit must be positive")
        handled = 0
        current_time = datetime.now(UTC)
        for tenant_id, session_id in tuple(self._mailboxes):
            if handled >= limit:
                break
            # Expired RUNNING leases belong exclusively to the lease sweeper.
            # Reconciler must not race a takeover or clear a valid executor's
            # state while it is only repairing notification/projection drift.
            if self._mailboxes[(tenant_id, session_id)].status == MailboxStatus.RUNNING:
                continue
            if await self.reconcile(tenant_id, session_id, now=current_time) is not None:
                handled += 1
        return handled

    # Explicit aliases make the fencing boundary obvious to callers that do not
    # want to use the short transition names.
    accept_inbound = accept
    claim_next = claim
    claim_session_lease = claim_session
    renew_lease = renew
    renew_session_leases = renew_many
    commit_fenced = commit
    reschedule_fenced = reschedule
    retry_fenced = retry
    recover_expired = recover
    reconcile_state = reconcile

    def _ensure_mailbox(self, key: tuple[str, str], now: datetime) -> SessionMailbox:
        mailbox = SessionMailbox(tenant_id=key[0], session_id=key[1], updated_at=now)
        self._mailboxes[key] = mailbox
        return mailbox

    def _emit_ready(
        self, mailbox: SessionMailbox, item: MailboxItem, now: datetime
    ) -> SessionMailbox:
        generation = mailbox.queue_generation + 1
        updated = mailbox.model_copy(update={"queue_generation": generation})
        return self._append_ready(updated, item, now, generation)

    def _requeue_ready(
        self, mailbox: SessionMailbox, item: MailboxItem, now: datetime
    ) -> SessionMailbox:
        """Re-publish an existing generation without creating a new wake-up.

        ``queue_generation`` describes a durable queued state, not an attempt
        counter.  A Redis outage can therefore require the same wake-up to be
        replayed, but a normal reconciliation pass must not manufacture a new
        generation (and consequently invalidate the wake-up already in flight).
        The in-memory adapter retains outbox records, so an existing record is
        already replayable; only a missing record is reconstructed.
        """

        generation = max(1, mailbox.queue_generation)
        updated = mailbox.model_copy(update={"queue_generation": generation})
        has_record = any(
            record.event_type == "session.ready.v2"
            and record.tenant_id == mailbox.tenant_id
            and record.aggregate_id == mailbox.session_id
            and record.payload.get("generation") == generation
            for record in self._outbox
        )
        if not has_record:
            updated = self._append_ready(updated, item, now, generation)
        return updated

    def _append_ready(
        self,
        mailbox: SessionMailbox,
        item: MailboxItem,
        now: datetime,
        generation: int,
    ) -> SessionMailbox:
        self._outbox.append(
            OutboxRecord(
                outbox_id=str(uuid4()),
                tenant_id=mailbox.tenant_id,
                event_type="session.ready.v2",
                aggregate_id=mailbox.session_id,
                payload={
                    "generation": generation,
                    "priority": item.priority,
                    "trace_id": item.trace_id or item.inbound_id,
                    "created_at": now,
                },
            )
        )
        return mailbox

    def _require_owned(
        self,
        key: tuple[str, str],
        lease: MailboxLease,
        now: datetime,
    ) -> SessionMailbox:
        mailbox = self._mailboxes.get(key)
        if not _owns(mailbox, lease, now):
            raise FencingConflict("mailbox lease is no longer current")
        assert mailbox is not None
        return mailbox


def _validate_ids(*values: str) -> None:
    if any(not value for value in values):
        raise ValueError("mailbox identifiers must be non-empty")


def _validate_uuid_if_present(value: str) -> None:
    try:
        UUID(value)
    except ValueError:
        # In-memory tests and migration tools may use deterministic IDs; the
        # PostgreSQL implementation enforces UUIDs only when linking rows.
        return


def _owns(mailbox: SessionMailbox | None, lease: MailboxLease, now: datetime) -> bool:
    return bool(
        mailbox
        and mailbox.status == MailboxStatus.RUNNING
        and mailbox.processing_sequence == lease.sequence
        and mailbox.processing_inbound_id == lease.inbound_id
        and mailbox.lease_owner == lease.owner_id
        and mailbox.lease_epoch == lease.epoch
        and mailbox.lease_expires_at is not None
        and mailbox.lease_expires_at > now
    )


def _ready_status(
    mailbox: SessionMailbox,
    resolved_sequence: int,
    now: datetime,
) -> MailboxStatus:
    if resolved_sequence >= mailbox.accepted_sequence:
        return MailboxStatus.IDLE
    if mailbox.retry_at is not None and mailbox.retry_at > now:
        return MailboxStatus.RETRY_WAIT
    return MailboxStatus.QUEUED


class PostgresSessionMailboxStore:
    """PostgreSQL mailbox with server-clock fenced transitions."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        ready_replay_cooldown_seconds: int = 30,
    ) -> None:
        if (
            isinstance(ready_replay_cooldown_seconds, bool)
            or ready_replay_cooldown_seconds < 5
            or ready_replay_cooldown_seconds > 86_400
        ):
            raise ValueError("ready replay cooldown must be between 5 and 86400 seconds")
        self._pool = pool
        self._ready_replay_cooldown_seconds = ready_replay_cooldown_seconds

    @asynccontextmanager
    async def _transaction(self, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            yield connection

    async def get(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        async with self._transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT tenant_id,session_id,status,accepted_sequence,resolved_sequence,
                       processing_sequence,processing_inbound_id,queue_generation,
                       lease_owner,lease_epoch,lease_expires_at,retry_count,attempt,
                       priority,retry_at,updated_at
                  FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                tenant_id,
                session_id,
            )
        return _mailbox_from_row(row) if row is not None else None

    async def accept(
        self,
        tenant_id: str,
        session_id: str,
        inbound_id: str,
        *,
        priority: int = 0,
        retry_at: datetime | None = None,
        trace_id: str | None = None,
        now: datetime | None = None,
    ) -> SessionMailbox:
        del now  # PostgreSQL uses clock_timestamp() for all transition predicates.
        _validate_priority(priority)
        inbound_uuid = _require_uuid(inbound_id)
        async with self._transaction(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO session_mailboxes (tenant_id,session_id)
                VALUES ($1,$2)
                ON CONFLICT (tenant_id,session_id) DO NOTHING
                """,
                tenant_id,
                session_id,
            )
            mailbox = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                tenant_id,
                session_id,
            )
            assert mailbox is not None
            existing = await connection.fetchrow(
                """
                SELECT sequence FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND inbound_id=$3
                """,
                tenant_id,
                session_id,
                inbound_uuid,
            )
            if existing is not None:
                return _mailbox_from_row(mailbox)
            await connection.execute(
                """
                INSERT INTO session_mailbox_items (
                    tenant_id,session_id,sequence,inbound_id,trace_id,priority,retry_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                """,
                tenant_id,
                session_id,
                int(mailbox["accepted_sequence"]) + 1,
                inbound_uuid,
                trace_id or inbound_id,
                priority,
                retry_at,
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            previous_status = MailboxStatus(str(mailbox["status"]))
            current_retry_at = mailbox["retry_at"]
            current_head_waiting = (
                previous_status == MailboxStatus.RETRY_WAIT
                and current_retry_at is not None
                and current_retry_at > server_now
            )
            if previous_status == MailboxStatus.RETRY_WAIT:
                if current_head_waiting:
                    # The existing head still gates this session.  A later
                    # item's retry time must never replace the head's timer.
                    status = MailboxStatus.RETRY_WAIT
                    next_retry_at = current_retry_at
                else:
                    # The current head is due.  A future retry time belongs to
                    # the newly appended item and is evaluated after the head
                    # is resolved; it must not block the head now.
                    status = MailboxStatus.QUEUED
                    next_retry_at = None
            elif previous_status == MailboxStatus.IDLE:
                if retry_at is not None and retry_at > server_now:
                    status = MailboxStatus.RETRY_WAIT
                    next_retry_at = retry_at
                else:
                    status = MailboxStatus.QUEUED
                    next_retry_at = None
            else:
                status = previous_status
                next_retry_at = current_retry_at
            should_emit = status == MailboxStatus.QUEUED and previous_status in (
                MailboxStatus.IDLE,
                MailboxStatus.RETRY_WAIT,
            )
            updated = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET accepted_sequence=accepted_sequence+1,
                       status=$3,
                       queue_generation=queue_generation+$4,
                       priority=greatest(priority,$5),
                       retry_at=$6,
                       updated_at=clock_timestamp()
                  WHERE tenant_id=$1 AND session_id=$2
                  RETURNING *
                """,
                tenant_id,
                session_id,
                status.value,
                1 if should_emit else 0,
                priority,
                next_retry_at,
            )
            assert updated is not None
            if should_emit:
                await self._emit_ready_outbox(
                    connection,
                    updated,
                    {
                        "inbound_id": inbound_uuid,
                        "priority": priority,
                        "trace_id": trace_id or inbound_id,
                    },
                )
        return _mailbox_from_row(updated)

    async def claim(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
        now: datetime | None = None,
    ) -> MailboxLease | None:
        del now
        _validate_lease_for(lease_for)
        async with self._transaction(tenant_id) as connection:
            return await self._claim_locked(
                connection,
                tenant_id,
                session_id,
                owner_id=owner_id,
                lease_for=lease_for,
            )

    async def _claim_locked(
        self,
        connection: asyncpg.Connection,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
    ) -> MailboxLease | None:
        mailbox = await connection.fetchrow(
            """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
            tenant_id,
            session_id,
        )
        if mailbox is None:
            return None
        if mailbox["lease_expires_at"] is not None and mailbox[
            "lease_expires_at"
        ] > await connection.fetchval("SELECT clock_timestamp()"):
            return None
        processing_sequence = mailbox["processing_sequence"]
        sequence = (
            int(processing_sequence)
            if processing_sequence is not None
            else int(mailbox["resolved_sequence"]) + 1
        )
        if processing_sequence is None and sequence > int(mailbox["accepted_sequence"]):
            await connection.execute(
                """
                UPDATE session_mailboxes SET status='IDLE',updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                tenant_id,
                session_id,
            )
            return None
        item = await connection.fetchrow(
            """
            SELECT * FROM session_mailbox_items
             WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
               AND (retry_at IS NULL OR retry_at <= clock_timestamp())
            """,
            tenant_id,
            session_id,
            sequence,
        )
        if item is None:
            future_retry = await connection.fetchval(
                """
                SELECT retry_at FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                   AND retry_at > clock_timestamp()
                """,
                tenant_id,
                session_id,
                sequence,
            )
            if future_retry is not None:
                await connection.execute(
                    """
                    UPDATE session_mailboxes
                       SET status='RETRY_WAIT',retry_at=$4,updated_at=clock_timestamp()
                     WHERE tenant_id=$1 AND session_id=$2
                    """,
                    tenant_id,
                    session_id,
                    sequence,
                    future_retry,
                )
            return None
        epoch = int(mailbox["lease_epoch"]) + 1
        updated_item = await connection.fetchrow(
            """
            UPDATE session_mailbox_items
               SET attempt=attempt+1
             WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
             RETURNING attempt,retry_count,priority,retry_at,inbound_id
            """,
            tenant_id,
            session_id,
            sequence,
        )
        assert updated_item is not None
        updated_mailbox = await connection.fetchrow(
            """
            UPDATE session_mailboxes
               SET status='RUNNING',processing_sequence=$3,processing_inbound_id=$4,
                   lease_owner=$5,lease_epoch=$6,
                   lease_expires_at=clock_timestamp()+$7::interval,
                   attempt=$8,retry_count=$9,priority=$10,retry_at=$11,
                   updated_at=clock_timestamp()
             WHERE tenant_id=$1 AND session_id=$2
             RETURNING lease_expires_at
            """,
            tenant_id,
            session_id,
            sequence,
            updated_item["inbound_id"],
            owner_id,
            epoch,
            lease_for,
            updated_item["attempt"],
            updated_item["retry_count"],
            updated_item["priority"],
            updated_item["retry_at"],
        )
        assert updated_mailbox is not None
        return MailboxLease(
            tenant_id=tenant_id,
            session_id=session_id,
            inbound_id=str(updated_item["inbound_id"]),
            sequence=sequence,
            owner_id=owner_id,
            epoch=epoch,
            expires_at=updated_mailbox["lease_expires_at"],
            attempt=updated_item["attempt"],
            retry_count=updated_item["retry_count"],
            priority=updated_item["priority"],
        )

    async def claim_session(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
        expected_generation: int | None = None,
        expected_epoch: int | None = None,
        acceptance: Acceptance | None = None,
        now: datetime | None = None,
    ) -> SessionClaim:
        del now
        if expected_generation is None:
            expected_generation = expected_epoch
        _validate_lease_for(lease_for)
        async with self._transaction(tenant_id) as connection:
            current_time = await connection.fetchval("SELECT clock_timestamp()")
            before_row = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                tenant_id,
                session_id,
            )
            before = (
                _mailbox_from_row(before_row)
                if before_row is not None
                else SessionMailbox(tenant_id=tenant_id, session_id=session_id)
            )
            if expected_generation is not None and before.queue_generation != expected_generation:
                return SessionClaim(
                    status=MailboxClaimStatus.STALE, mailbox=before, acceptance=acceptance
                )
            lease = await self._claim_locked(
                connection,
                tenant_id,
                session_id,
                owner_id=owner_id,
                lease_for=lease_for,
            )
            after_row = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                tenant_id,
                session_id,
            )
            mailbox = _mailbox_from_row(after_row) if after_row is not None else before
            if lease is not None:
                return SessionClaim(
                    status=MailboxClaimStatus.CLAIMED,
                    mailbox=mailbox,
                    lease=lease,
                    acceptance=acceptance,
                )
            if mailbox.lease_expires_at is not None and mailbox.lease_expires_at > current_time:
                return SessionClaim(
                    status=MailboxClaimStatus.RUNNING,
                    mailbox=mailbox,
                    acceptance=acceptance,
                )
            if expected_generation is not None and mailbox.queue_generation != expected_generation:
                return SessionClaim(
                    status=MailboxClaimStatus.STALE,
                    mailbox=mailbox,
                    acceptance=acceptance,
                )
            return SessionClaim(
                status=MailboxClaimStatus.EMPTY,
                mailbox=mailbox,
                acceptance=acceptance,
            )

    async def renew(
        self,
        lease: MailboxLease,
        *,
        lease_for: timedelta,
        now: datetime | None = None,
    ) -> MailboxLease:
        del now
        _validate_lease_for(lease_for)
        async with self._transaction(lease.tenant_id) as connection:
            row = await self._renew_row(connection, lease, lease_for)
        if row is None:
            raise FencingConflict("mailbox lease is no longer current")
        return lease.model_copy(update={"expires_at": row["lease_expires_at"]})

    async def renew_many(
        self,
        leases: tuple[MailboxLease, ...],
        *,
        lease_for: timedelta,
        now: datetime | None = None,
    ) -> tuple[MailboxLease, ...]:
        del now
        _validate_lease_for(lease_for)
        if not leases:
            return ()
        renewed: list[MailboxLease] = []
        async with self._transaction(leases[0].tenant_id) as connection:
            for lease in leases:
                if lease.tenant_id != leases[0].tenant_id:
                    raise ValueError("mailbox renew batch must use one tenant")
                row = await self._renew_row(connection, lease, lease_for)
                if row is None:
                    raise FencingConflict("mailbox lease is no longer current")
                renewed.append(lease.model_copy(update={"expires_at": row["lease_expires_at"]}))
        return tuple(renewed)

    @staticmethod
    async def _renew_row(
        connection: asyncpg.Connection,
        lease: MailboxLease,
        lease_for: timedelta,
    ) -> Mapping[str, Any] | None:
        return cast(
            Mapping[str, Any] | None,
            await connection.fetchrow(
                """
            UPDATE session_mailboxes
               SET lease_expires_at=greatest(
                       clock_timestamp()+$5::interval,
                       lease_expires_at+interval '1 microsecond'
                   ),
                   updated_at=clock_timestamp()
             WHERE tenant_id=$1 AND session_id=$2 AND status='RUNNING'
               AND processing_sequence=$3 AND processing_inbound_id=$4
               AND lease_owner=$6 AND lease_epoch=$7
               AND lease_expires_at > clock_timestamp()
             RETURNING lease_expires_at
            """,
                lease.tenant_id,
                lease.session_id,
                lease.sequence,
                UUID(lease.inbound_id),
                lease_for,
                lease.owner_id,
                lease.epoch,
            ),
        )

    async def commit(
        self,
        lease: MailboxLease,
        *,
        now: datetime | None = None,
    ) -> SessionMailbox:
        del now
        async with self._transaction(lease.tenant_id) as connection:
            await self._assert_owned(connection, lease)
            await connection.execute(
                """
                UPDATE session_mailbox_items
                   SET resolved_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                """,
                lease.tenant_id,
                lease.session_id,
                lease.sequence,
            )
            next_item = await connection.fetchrow(
                """
                SELECT inbound_id,trace_id,priority,retry_at
                  FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                """,
                lease.tenant_id,
                lease.session_id,
                lease.sequence + 1,
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if next_item is None:
                next_status = MailboxStatus.IDLE
                next_retry_at = None
                generation_increment = 0
            elif next_item["retry_at"] is not None and next_item["retry_at"] > server_now:
                next_status = MailboxStatus.RETRY_WAIT
                next_retry_at = next_item["retry_at"]
                generation_increment = 0
            else:
                next_status = MailboxStatus.QUEUED
                next_retry_at = None
                generation_increment = 1
            row = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET resolved_sequence=$3,processing_sequence=NULL,
                       processing_inbound_id=NULL,lease_owner=NULL,
                       lease_expires_at=NULL,
                       status=$4,retry_at=$5,
                       queue_generation=queue_generation+$6,
                       updated_at=clock_timestamp()
                  WHERE tenant_id=$1 AND session_id=$2
                 RETURNING *
                """,
                lease.tenant_id,
                lease.session_id,
                lease.sequence,
                next_status.value,
                next_retry_at,
                generation_increment,
            )
            assert row is not None
            if next_status == MailboxStatus.QUEUED and next_item is not None:
                await self._emit_ready_outbox(connection, row, next_item)
        return _mailbox_from_row(row)

    async def reschedule(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
        now: datetime | None = None,
    ) -> SessionMailbox:
        return await self._release(
            lease,
            retry_at=retry_at,
            priority=priority,
            increment_retry=False,
            now=now,
        )

    async def retry(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
        now: datetime | None = None,
    ) -> SessionMailbox:
        return await self._release(
            lease,
            retry_at=retry_at,
            priority=priority,
            increment_retry=True,
            now=now,
        )

    async def retry_without_wakeup(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None,
        now: datetime | None = None,
    ) -> SessionMailbox:
        return await self._release(
            lease,
            retry_at=retry_at,
            priority=None,
            increment_retry=True,
            now=now,
            emit_ready=False,
        )

    async def _release(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None,
        priority: int | None,
        increment_retry: bool,
        now: datetime | None,
        emit_ready: bool = True,
    ) -> SessionMailbox:
        del now
        if priority is not None:
            _validate_priority(priority)
        async with self._transaction(lease.tenant_id) as connection:
            await self._assert_owned(connection, lease)
            row = await connection.fetchrow(
                """
                UPDATE session_mailbox_items
                   SET retry_count=retry_count+$4,
                       priority=coalesce($5,priority),retry_at=$6
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                  RETURNING retry_count,priority,retry_at,trace_id,inbound_id
                """,
                lease.tenant_id,
                lease.session_id,
                lease.sequence,
                1 if increment_retry else 0,
                priority,
                retry_at,
            )
            assert row is not None
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if row["retry_at"] is not None and row["retry_at"] > server_now:
                next_status = MailboxStatus.RETRY_WAIT
                next_retry_at = row["retry_at"]
                generation_increment = 0
            else:
                next_status = MailboxStatus.QUEUED
                next_retry_at = None
                generation_increment = 1
            if not emit_ready:
                next_status = MailboxStatus.RETRY_WAIT
                next_retry_at = row["retry_at"]
                generation_increment = 0
            mailbox = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET status=$3,
                       processing_sequence=NULL,processing_inbound_id=NULL,
                       lease_owner=NULL,lease_expires_at=NULL,
                       retry_count=$4,priority=$5,retry_at=$6,
                       queue_generation=queue_generation+$7,updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                 RETURNING *
                """,
                lease.tenant_id,
                lease.session_id,
                next_status.value,
                row["retry_count"],
                row["priority"],
                next_retry_at,
                generation_increment,
            )
            assert mailbox is not None
            if next_status == MailboxStatus.QUEUED and emit_ready:
                await self._emit_ready_outbox(connection, mailbox, row)
        return _mailbox_from_row(mailbox)

    async def recover(
        self,
        tenant_id: str,
        session_id: str,
        *,
        now: datetime | None = None,
    ) -> SessionMailbox | None:
        del now
        async with self._transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at <= clock_timestamp()
                 FOR UPDATE
                """,
                tenant_id,
                session_id,
            )
            if row is None:
                return None
            # The mailbox row is deliberately locked first.  ``sessions`` is
            # the authoritative execution lease, so lock it second and never
            # recover a mailbox while another worker still owns that lease.
            session = await connection.fetchrow(
                """
                SELECT lease_owner,lease_epoch,lease_expires_at
                  FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                tenant_id,
                session_id,
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if (
                session is not None
                and session["lease_owner"] is not None
                and session["lease_expires_at"] is not None
                and session["lease_expires_at"] > server_now
            ):
                return None
            sequence = row["processing_sequence"] or int(row["resolved_sequence"]) + 1
            item = await connection.fetchrow(
                """
                SELECT inbound_id,trace_id,priority,retry_at
                  FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                """,
                tenant_id,
                session_id,
                sequence,
            )
            if item is None or sequence > int(row["accepted_sequence"]):
                next_status = MailboxStatus.IDLE
                next_retry_at = None
                generation_increment = 0
            elif item["retry_at"] is not None and item["retry_at"] > server_now:
                next_status = MailboxStatus.RETRY_WAIT
                next_retry_at = item["retry_at"]
                generation_increment = 0
            else:
                next_status = MailboxStatus.QUEUED
                next_retry_at = None
                generation_increment = 1
            row = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET status=$3,processing_sequence=NULL,processing_inbound_id=NULL,
                       lease_owner=NULL,lease_expires_at=NULL,retry_at=$4,
                       queue_generation=queue_generation+$5,updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                  RETURNING *
                """,
                tenant_id,
                session_id,
                next_status.value,
                next_retry_at,
                generation_increment,
            )
            assert row is not None
            if next_status == MailboxStatus.QUEUED and item is not None:
                await self._emit_ready_outbox(connection, row, item)
        return _mailbox_from_row(row) if row is not None else None

    async def reconcile(
        self,
        tenant_id: str,
        session_id: str,
        *,
        now: datetime | None = None,
    ) -> SessionMailbox | None:
        del now
        async with self._transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                  WHERE tenant_id=$1 AND session_id=$2
                  FOR UPDATE
                """,
                tenant_id,
                session_id,
            )
            if row is None:
                return None
            # Keep the same lock order as the worker and lease sweeper:
            # mailbox first, authoritative session lease second.  A mailbox
            # may look stale while its session lease has already been renewed
            # or taken over by a replacement worker; touching it here would
            # clear a live execution.
            session = await connection.fetchrow(
                """
                SELECT lease_owner,lease_epoch,lease_expires_at
                  FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                tenant_id,
                session_id,
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if (
                session is not None
                and session["lease_owner"] is not None
                and session["lease_expires_at"] is not None
                and session["lease_expires_at"] > server_now
            ):
                return _mailbox_from_row(row)
            sequence = row["processing_sequence"] or int(row["resolved_sequence"]) + 1
            item = await connection.fetchrow(
                """
                SELECT inbound_id,trace_id,priority,retry_at
                  FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                """,
                tenant_id,
                session_id,
                sequence,
            )
            if item is None or sequence > int(row["accepted_sequence"]):
                next_status = MailboxStatus.IDLE
                next_retry_at = None
                generation_increment = 0
            elif item["retry_at"] is not None and item["retry_at"] > server_now:
                next_status = MailboxStatus.RETRY_WAIT
                next_retry_at = item["retry_at"]
                generation_increment = 0
            else:
                next_status = MailboxStatus.QUEUED
                next_retry_at = None
                queued_is_recent = (
                    row["status"] == MailboxStatus.QUEUED.value
                    and row["updated_at"] > server_now - _RECONCILE_READY_GRACE
                )
                if queued_is_recent:
                    return _mailbox_from_row(row)
                # A queued mailbox already has a durable ready generation.
                # Reconciliation may need to replay its outbox record after a
                # Redis loss, but it must not advance the generation merely
                # because the executor has not reached this session yet.
                generation_increment = (
                    0
                    if row["status"] == MailboxStatus.QUEUED.value
                    and int(row["queue_generation"]) >= 1
                    else 1
                )
            row = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET status=$3,processing_sequence=NULL,processing_inbound_id=NULL,
                       lease_owner=NULL,lease_expires_at=NULL,retry_at=$4,
                       queue_generation=queue_generation+$5,updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                  RETURNING *
                """,
                tenant_id,
                session_id,
                next_status.value,
                next_retry_at,
                generation_increment,
            )
            assert row is not None
            if next_status == MailboxStatus.QUEUED and item is not None:
                if generation_increment:
                    await self._emit_ready_outbox(connection, row, item)
                else:
                    await self._requeue_ready_outbox(
                        connection,
                        row,
                        item,
                        replay_cooldown=timedelta(seconds=self._ready_replay_cooldown_seconds),
                    )
        return _mailbox_from_row(row) if row is not None else None

    async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int:
        del owner_id
        if limit < 1 or limit > 1000:
            raise ValueError("recovery limit must be between 1 and 1000")
        async with self._pool.acquire() as connection:
            return int(await connection.fetchval("SELECT sweep_expired_session_leases($1)", limit))

    async def schedule_retries(self, *, owner_id: str, limit: int) -> int:
        del owner_id
        if limit < 1 or limit > 1000:
            raise ValueError("recovery limit must be between 1 and 1000")
        async with self._pool.acquire() as connection:
            return int(
                await connection.fetchval("SELECT schedule_session_mailbox_retries($1)", limit)
            )

    async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int:
        del owner_id
        if limit < 1 or limit > 1000:
            raise ValueError("recovery limit must be between 1 and 1000")
        async with self._pool.acquire() as connection:
            return int(
                await connection.fetchval(
                    "SELECT reconcile_session_mailboxes_v2($1,$2)",
                    limit,
                    self._ready_replay_cooldown_seconds,
                )
            )

    async def _assert_owned(
        self,
        connection: asyncpg.Connection,
        lease: MailboxLease,
    ) -> Mapping[str, Any]:
        row = cast(
            Mapping[str, Any] | None,
            await connection.fetchrow(
                """
            SELECT * FROM session_mailboxes
             WHERE tenant_id=$1 AND session_id=$2
             FOR UPDATE
            """,
                lease.tenant_id,
                lease.session_id,
            ),
        )
        if (
            row is None
            or row["status"] != MailboxStatus.RUNNING.value
            or row["processing_sequence"] != lease.sequence
            or str(row["processing_inbound_id"]) != lease.inbound_id
            or row["lease_owner"] != lease.owner_id
            or int(row["lease_epoch"]) != lease.epoch
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= await connection.fetchval("SELECT clock_timestamp()")
        ):
            raise FencingConflict("mailbox lease is no longer current")
        return row

    @staticmethod
    async def _emit_ready_outbox(
        connection: asyncpg.Connection,
        mailbox: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> None:
        """Insert the idempotent wake-up for an already incremented generation."""

        trace_id = item.get("trace_id") or item.get("inbound_id") or mailbox["session_id"]
        await connection.execute(
            """
            INSERT INTO outbox_events (
                tenant_id,aggregate_type,aggregate_id,event_type,payload_json
            ) VALUES (
                $1,'session',$2,'session.ready.v2',
                jsonb_build_object(
                    'generation',$3::bigint,
                    'priority',$4::integer,
                    'trace_id',$5::text,
                    'created_at',to_char(clock_timestamp() AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                )
            ) ON CONFLICT DO NOTHING
            """,
            mailbox["tenant_id"],
            mailbox["session_id"],
            int(mailbox["queue_generation"]),
            int(item.get("priority", 0)),
            str(trace_id),
        )

    @staticmethod
    async def _requeue_ready_outbox(
        connection: asyncpg.Connection,
        mailbox: Mapping[str, Any],
        item: Mapping[str, Any],
        *,
        replay_cooldown: timedelta = _RECONCILE_READY_REPLAY_GRACE,
    ) -> None:
        """Replay the current generation without adding an outbox row.

        A queued mailbox can outlive its Redis notification (for example when
        Redis is rebuilt).  The notification is a wake-up, so the existing
        unique ``session.ready.v2`` event is sufficient: make a published
        event available again, leave a recent/pending/claimed event alone,
        and only insert when the event is genuinely absent.  The replay
        timestamp is a rolling cooldown marker rather than a permanent
        one-shot flag, so a later publish-then-loss can be repaired again.
        The generation and event id remain durable and therefore continue to
        satisfy claim authentication.
        """

        trace_id = item.get("trace_id") or item.get("inbound_id") or mailbox["session_id"]
        updated = await connection.execute(
            """
            UPDATE outbox_events
               SET published_at=NULL,
                   claimed_by=NULL,
                   claim_expires_at=NULL,
                   available_at=clock_timestamp(),
                   last_error_type=NULL,
                   ready_replayed_at=clock_timestamp()
             WHERE tenant_id=$1
               AND aggregate_type='session'
               AND aggregate_id=$2
               AND event_type='session.ready.v2'
               AND (payload_json->>'generation')::bigint=$3::bigint
               AND published_at IS NOT NULL
               AND greatest(published_at,coalesce(ready_replayed_at,published_at))
                  <= clock_timestamp()-$4::interval
               AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp())
            """,
            mailbox["tenant_id"],
            mailbox["session_id"],
            int(mailbox["queue_generation"]),
            replay_cooldown,
        )
        if updated == "UPDATE 1":
            return
        await connection.execute(
            """
            INSERT INTO outbox_events (
                tenant_id,aggregate_type,aggregate_id,event_type,payload_json
            ) VALUES (
                $1,'session',$2,'session.ready.v2',
                jsonb_build_object(
                    'generation',$3::bigint,
                    'priority',$4::integer,
                    'trace_id',$5::text,
                    'created_at',to_char(clock_timestamp() AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                )
            ) ON CONFLICT DO NOTHING
            """,
            mailbox["tenant_id"],
            mailbox["session_id"],
            int(mailbox["queue_generation"]),
            int(item.get("priority", 0)),
            str(trace_id),
        )

    claim_next = claim
    claim_session_lease = claim_session
    renew_lease = renew
    renew_session_leases = renew_many
    commit_fenced = commit
    reschedule_fenced = reschedule
    retry_fenced = retry
    recover_expired = recover
    reconcile_state = reconcile


def _require_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as exc:
        raise ValueError("PostgreSQL mailbox inbound_id must be a UUID") from exc


def _mailbox_from_row(row: Mapping[str, Any]) -> SessionMailbox:
    return SessionMailbox(
        tenant_id=str(row["tenant_id"]),
        session_id=str(row["session_id"]),
        status=MailboxStatus(str(row["status"])),
        accepted_sequence=int(row["accepted_sequence"]),
        resolved_sequence=int(row["resolved_sequence"]),
        processing_sequence=(
            int(row["processing_sequence"]) if row["processing_sequence"] is not None else None
        ),
        processing_inbound_id=(
            str(row["processing_inbound_id"]) if row["processing_inbound_id"] is not None else None
        ),
        queue_generation=int(row["queue_generation"]),
        lease_owner=(str(row["lease_owner"]) if row["lease_owner"] is not None else None),
        lease_epoch=int(row["lease_epoch"]),
        lease_expires_at=cast(datetime | None, row["lease_expires_at"]),
        retry_count=int(row["retry_count"]),
        attempt=int(row["attempt"]),
        priority=int(row["priority"]),
        retry_at=cast(datetime | None, row["retry_at"]),
        updated_at=cast(datetime, row["updated_at"]),
    )


__all__ = ["InMemorySessionMailboxStore", "PostgresSessionMailboxStore"]

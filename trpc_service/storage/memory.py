"""Deterministic single-process repository for development and contract tests."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from trpc_service.channels.envelopes import DeliveryReceipt, InboundEnvelope
from trpc_service.storage.mailbox import InMemorySessionMailboxStore
from trpc_service.storage.models import (
    Acceptance,
    BindingRoute,
    CommitResult,
    DeliveryAttempt,
    MailboxClaimStatus,
    MailboxLease,
    MailboxStatus,
    OutboxRecord,
    SequencedEvent,
    SessionClaim,
    SessionLease,
    SessionMailbox,
    SessionSnapshot,
    TurnCommit,
)
from trpc_service.storage.protocols import DeliveryInProgress, FencingConflict
from trpc_service.tenant.models import Channel, ChannelBinding, TenantConfig, TenantContext


class InMemoryRuntimeRepository:
    """Reference semantics; explicitly unsuitable for multi-node production."""

    def __init__(self) -> None:
        self._routes: dict[str, BindingRoute] = {}
        self._configs: dict[tuple[str, str, int], TenantConfig] = {}
        self._acceptances: dict[tuple[str, str, str, str], Acceptance] = {}
        self._acceptances_by_id: dict[str, Acceptance] = {}
        self._acceptance_sequence: dict[str, int] = {}
        self._next_acceptance_sequence = 1
        self._sessions: dict[tuple[str, str], SessionSnapshot] = {}
        self._leases: dict[tuple[str, str], SessionLease] = {}
        self._committed_inbound: set[tuple[str, str]] = set()
        self._inbound_status: dict[tuple[str, str], str] = {}
        self._v2_turn_status: dict[tuple[str, str], str] = {}
        self._outbox: dict[str, OutboxRecord] = {}
        self._outbox_claims: dict[str, tuple[str, datetime]] = {}
        self.dead_letters: list[tuple[OutboxRecord, str]] = []
        self.delivery_receipts: list[DeliveryReceipt] = []
        # Outbound ids are only unique inside a tenant in PostgreSQL.  Keep
        # the same composite scope in the in-memory contract implementation.
        self._delivery_attempts: dict[tuple[str, str], tuple[str, int, str]] = {}
        self._session_locks: defaultdict[tuple[str, str], asyncio.Lock] = defaultdict(asyncio.Lock)
        self._lock = asyncio.Lock()
        self._mailbox_store = InMemorySessionMailboxStore()

    @property
    def mailbox(self) -> InMemorySessionMailboxStore:
        return self._mailbox_store

    def add_route(self, route: BindingRoute) -> None:
        self._routes[route.binding.binding_id] = route

    def add_config(self, config: TenantConfig) -> None:
        self._configs[(config.tenant_id, config.app_id, config.version)] = config

    async def resolve_binding(self, binding_id: str) -> BindingRoute | None:
        return self._routes.get(binding_id)

    async def list_bindings(self, channel: Channel) -> tuple[ChannelBinding, ...]:
        return tuple(
            route.binding
            for route in self._routes.values()
            if route.binding.channel == channel and route.binding.enabled and route.tenant_active
        )

    async def get_config(self, tenant_id: str, app_id: str, version: int) -> TenantConfig:
        try:
            return self._configs[(tenant_id, app_id, version)]
        except KeyError as exc:
            raise LookupError("pinned tenant configuration does not exist") from exc

    async def get_acceptance(self, tenant_id: str, inbound_id: str) -> Acceptance | None:
        acceptance = self._acceptances_by_id.get(inbound_id)
        if acceptance and acceptance.context.tenant_id != tenant_id:
            return None
        if acceptance and (tenant_id, inbound_id) in self._committed_inbound:
            return acceptance.model_copy(update={"duplicate": True})
        return acceptance

    async def get_session_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        return self._sessions.get((tenant_id, session_id))

    async def accept_mailbox(
        self,
        tenant_id: str,
        session_id: str,
        inbound_id: str,
        *,
        priority: int = 0,
        retry_at: datetime | None = None,
        trace_id: str | None = None,
    ) -> SessionMailbox:
        outbox_start = len(self._mailbox_store.outbox)
        mailbox = await self._mailbox_store.accept(
            tenant_id,
            session_id,
            inbound_id,
            priority=priority,
            retry_at=retry_at,
            trace_id=trace_id,
        )
        self._sync_new_mailbox_outbox(outbox_start)
        return mailbox

    async def claim_mailbox(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
        expected_generation: int | None = None,
        expected_epoch: int | None = None,
        acceptance: Acceptance | None = None,
    ) -> SessionClaim:
        return await self._mailbox_store.claim_session(
            tenant_id,
            session_id,
            owner_id=owner_id,
            lease_for=lease_for,
            expected_generation=expected_generation,
            expected_epoch=expected_epoch,
            acceptance=acceptance,
        )

    async def claim_session_ready(
        self,
        tenant_id: str,
        session_id: str,
        *,
        owner_id: str,
        lease_for: timedelta,
        expected_generation: int | None = None,
        expected_event_id: str | None = None,
        acceptance: Acceptance | None = None,
    ) -> SessionClaim:
        key = (tenant_id, session_id)
        async with self._session_locks[key]:
            mailbox_before = await self._mailbox_store.get(*key)
            if mailbox_before is None:
                mailbox_before = SessionMailbox(tenant_id=tenant_id, session_id=session_id)

            # A Redis wake-up is only a hint.  When a caller supplies its
            # durable event id, require the corresponding mailbox outbox
            # record before looking at the mailbox generation or taking a
            # lease.  ``None`` remains a compatibility escape hatch for
            # direct in-memory tests that call the repository without Redis.
            if expected_event_id is not None:
                ready_event = next(
                    (
                        record
                        for record in self._mailbox_store.outbox
                        if record.outbox_id == expected_event_id
                    ),
                    None,
                )
                payload = ready_event.payload if ready_event is not None else {}
                event_generation = payload.get("generation")
                if (
                    ready_event is None
                    or ready_event.tenant_id != tenant_id
                    or ready_event.event_type != "session.ready.v2"
                    or ready_event.aggregate_id != session_id
                    or isinstance(event_generation, bool)
                    or not isinstance(event_generation, int)
                    or event_generation < 1
                    or (expected_generation is not None and event_generation != expected_generation)
                ):
                    return SessionClaim(
                        status=MailboxClaimStatus.STALE,
                        mailbox=mailbox_before,
                        acceptance=acceptance,
                    )
                expected_generation = event_generation

            # v1 and v2 share the in-memory session lease map.  Check the
            # existing lease before claiming the mailbox so a v2 wake-up
            # cannot first mutate the mailbox and then overwrite an active
            # v1 lease with a second executable lease.
            current = self._leases.get(key)
            if current is not None and current.expires_at > datetime.now(UTC):
                return SessionClaim(
                    status=MailboxClaimStatus.RUNNING,
                    mailbox=mailbox_before,
                    acceptance=acceptance,
                )
            claim = await self.claim_mailbox(
                tenant_id,
                session_id,
                owner_id=owner_id,
                lease_for=lease_for,
                expected_generation=expected_generation,
                acceptance=acceptance,
            )
            if not claim.claimed or claim.lease is None:
                return claim
            accepted = self._acceptances_by_id.get(claim.lease.inbound_id)
            if accepted is None:
                # A mailbox row without its authoritative inbound is corrupt;
                # leave the fenced mailbox claim for recovery rather than
                # manufacturing an executable turn.
                return claim.model_copy(update={"status": MailboxClaimStatus.EMPTY, "lease": None})
            if (
                tenant_id,
                claim.lease.inbound_id,
            ) in self._committed_inbound or self._inbound_status.get(
                (tenant_id, claim.lease.inbound_id)
            ) == "committed":
                mailbox = await self._mailbox_store.commit(claim.lease)
                self._leases.pop(key, None)
                return SessionClaim(
                    status=MailboxClaimStatus.EMPTY,
                    mailbox=mailbox,
                    acceptance=accepted.model_copy(update={"duplicate": True}),
                )
            snapshot = self._sessions.get(key)
            if snapshot is None:
                snapshot = SessionSnapshot(
                    tenant_id=tenant_id,
                    app_id=accepted.context.app_id,
                    session_id=session_id,
                    principal_id=accepted.context.principal_id,
                )
                self._sessions[key] = snapshot
            previous = self._leases.get(key)
            turn_id = (
                previous.turn_id
                if previous is not None and previous.inbound_id == claim.lease.inbound_id
                else str(uuid4())
            )
            self._leases[key] = SessionLease(
                tenant_id=tenant_id,
                session_id=session_id,
                turn_id=turn_id,
                inbound_id=claim.lease.inbound_id,
                worker_id=owner_id,
                fencing_token=claim.lease.epoch,
                expires_at=claim.lease.expires_at,
                attempt=claim.lease.attempt,
                snapshot=snapshot,
            )
            self._v2_turn_status[(tenant_id, turn_id)] = "processing"
            self._inbound_status[(tenant_id, claim.lease.inbound_id)] = "processing"
            return claim.model_copy(
                update={
                    "acceptance": accepted,
                    "execution_lease": self._leases[key],
                }
            )

    async def renew_mailbox(self, lease: MailboxLease, *, lease_for: timedelta) -> MailboxLease:
        return await self._mailbox_store.renew(lease, lease_for=lease_for)

    async def renew_session_ready(
        self, lease: SessionLease, *, lease_for: timedelta
    ) -> SessionLease:
        key = (lease.tenant_id, lease.session_id)
        async with self._session_locks[key]:
            current = self._leases.get(key)
            if current is None or not self._owns(current, lease):
                raise FencingConflict("session mailbox lease is no longer current")
            mailbox = await self._mailbox_store.get(*key)
            if mailbox is None or mailbox.processing_sequence is None:
                raise FencingConflict("session mailbox lease is no longer current")
            mailbox_lease = MailboxLease(
                tenant_id=lease.tenant_id,
                session_id=lease.session_id,
                inbound_id=lease.inbound_id,
                sequence=mailbox.processing_sequence,
                owner_id=lease.worker_id,
                epoch=lease.fencing_token,
                expires_at=mailbox.lease_expires_at or lease.expires_at,
                attempt=lease.attempt,
                retry_count=mailbox.retry_count,
                priority=mailbox.priority,
            )
            renewed = await self._mailbox_store.renew(mailbox_lease, lease_for=lease_for)
            updated = current.model_copy(update={"expires_at": renewed.expires_at})
            self._leases[key] = updated
            return updated

    async def commit_session_ready(self, commit: TurnCommit) -> CommitResult:
        lease = commit.lease
        key = (lease.tenant_id, lease.session_id)
        async with self._session_locks[key]:
            current = self._leases.get(key)
            if current is None or not self._owns(current, lease):
                raise FencingConflict("stale worker cannot commit this session")
            mailbox = await self._mailbox_store.get(*key)
            if mailbox is None or mailbox.processing_inbound_id != lease.inbound_id:
                raise FencingConflict("stale worker cannot commit this session")
            mailbox_lease = self._mailbox_lease(mailbox, lease)
            snapshot = self._sessions[key]
            first = snapshot.next_sequence if commit.events else None
            sequenced = tuple(
                SequencedEvent(**event.model_dump(), sequence=snapshot.next_sequence + index)
                for index, event in enumerate(commit.events)
            )
            next_sequence = snapshot.next_sequence + len(sequenced)
            updated_snapshot = snapshot.model_copy(
                update={
                    "version": snapshot.version + 1,
                    "next_sequence": next_sequence,
                    "state": commit.state,
                    "events": snapshot.events + sequenced,
                }
            )
            outbound_id = None
            outbound_record: OutboxRecord | None = None
            if commit.outbound:
                outbound_id = commit.outbound.outbound_id
                outbox_id = str(uuid4())
                outbound_record = OutboxRecord(
                    outbox_id=outbox_id,
                    tenant_id=lease.tenant_id,
                    event_type=f"outbound.{commit.outbound.channel.value}.ready",
                    aggregate_id=outbound_id,
                    payload=commit.outbound.model_dump(mode="json"),
                    trace_headers=commit.outbound.trace_headers,
                )
            # Prepare every value that can validate or raise before the
            # mailbox transition.  The mailbox store is the single awaited
            # operation; all repository dictionaries are applied only after
            # its fenced commit succeeds.
            mailbox_before = mailbox
            mailbox_outbox_size = len(self._mailbox_store._outbox)
            try:
                await self._mailbox_store.commit(mailbox_lease)
            except BaseException:
                # A fault-injection wrapper may raise after delegating to the
                # in-memory mailbox.  Restore that store as well, so a failed
                # fenced commit cannot leave a committed mailbox with the
                # old session/turn dictionaries.
                self._mailbox_store._mailboxes[key] = mailbox_before
                del self._mailbox_store._outbox[mailbox_outbox_size:]
                raise
            self._sync_new_mailbox_outbox(mailbox_outbox_size)
            if outbound_record is not None:
                self._outbox[outbound_record.outbox_id] = outbound_record
            self._sessions[key] = updated_snapshot
            self._committed_inbound.add((lease.tenant_id, lease.inbound_id))
            self._inbound_status[(lease.tenant_id, lease.inbound_id)] = "committed"
            self._v2_turn_status[(lease.tenant_id, lease.turn_id)] = "committed"
            self._leases.pop(key, None)
            return CommitResult(
                turn_id=lease.turn_id,
                first_sequence=first,
                last_sequence=next_sequence - 1 if sequenced else None,
                outbound_id=outbound_id,
            )

    async def retry_session_ready(
        self, lease: SessionLease, *, error_type: str, delay: timedelta
    ) -> None:
        if delay < timedelta(0):
            raise ValueError("retry delay must be non-negative")
        key = (lease.tenant_id, lease.session_id)
        async with self._session_locks[key]:
            current = self._leases.get(key)
            if current is None or not self._owns(current, lease):
                raise FencingConflict("stale worker cannot retry this session")
            mailbox = await self._mailbox_store.get(*key)
            if mailbox is None:
                raise FencingConflict("session mailbox lease is no longer current")
            retry_at = datetime.now(UTC) + delay
            await self._mailbox_store.retry_without_wakeup(
                self._mailbox_lease(mailbox, lease), retry_at=retry_at
            )
            self._v2_turn_status[(lease.tenant_id, lease.turn_id)] = "failed"
            self._inbound_status[(lease.tenant_id, lease.inbound_id)] = "accepted"
            self._leases.pop(key, None)

    async def fail_session_ready(self, lease: SessionLease, *, error_type: str) -> None:
        key = (lease.tenant_id, lease.session_id)
        async with self._session_locks[key]:
            current = self._leases.get(key)
            if current is None or not self._owns(current, lease):
                raise FencingConflict("stale worker cannot fail this session")
            mailbox = await self._mailbox_store.get(*key)
            if mailbox is None:
                raise FencingConflict("session mailbox lease is no longer current")
            outbox_start = len(self._mailbox_store.outbox)
            await self._mailbox_store.commit(self._mailbox_lease(mailbox, lease))
            self._sync_new_mailbox_outbox(outbox_start)
            self._v2_turn_status[(lease.tenant_id, lease.turn_id)] = "failed"
            self._inbound_status[(lease.tenant_id, lease.inbound_id)] = "failed"
            self._leases.pop(key, None)

    def _sync_new_mailbox_outbox(self, start: int) -> None:
        """Expose newly created mailbox wake-ups through the runtime outbox.

        The mailbox store keeps an append-only diagnostic history, while the
        runtime outbox contains only records that still need dispatch.  Every
        real state transition therefore copies just the newly appended slice.
        """

        for record in self._mailbox_store.outbox[start:]:
            self._outbox.setdefault(record.outbox_id, record)

    @staticmethod
    def _mailbox_was_rescheduled(
        before: SessionMailbox | None,
        after: SessionMailbox,
    ) -> bool:
        return bool(
            after.status == MailboxStatus.QUEUED
            and (
                before is None
                or before.status != after.status
                or before.queue_generation != after.queue_generation
                or before.updated_at != after.updated_at
            )
        )

    def _rearm_mailbox_outbox(self, mailbox: SessionMailbox) -> None:
        """Re-open the current durable generation after a suspected Redis loss."""

        record = next(
            (
                candidate
                for candidate in reversed(self._mailbox_store.outbox)
                if candidate.event_type == "session.ready.v2"
                and candidate.tenant_id == mailbox.tenant_id
                and candidate.aggregate_id == mailbox.session_id
                and candidate.payload.get("generation") == mailbox.queue_generation
            ),
            None,
        )
        if record is None or record.outbox_id in self._outbox:
            return
        self._outbox_claims.pop(record.outbox_id, None)
        self._outbox[record.outbox_id] = record

    @staticmethod
    def _mailbox_lease(mailbox: SessionMailbox, lease: SessionLease) -> MailboxLease:
        if (
            mailbox.status.value != "RUNNING"
            or mailbox.processing_sequence is None
            or mailbox.processing_inbound_id != lease.inbound_id
            or mailbox.lease_owner != lease.worker_id
            or mailbox.lease_epoch != lease.fencing_token
        ):
            raise FencingConflict("session mailbox lease is no longer current")
        return MailboxLease(
            tenant_id=lease.tenant_id,
            session_id=lease.session_id,
            inbound_id=lease.inbound_id,
            sequence=mailbox.processing_sequence,
            owner_id=lease.worker_id,
            epoch=lease.fencing_token,
            expires_at=mailbox.lease_expires_at or lease.expires_at,
            attempt=lease.attempt,
            retry_count=mailbox.retry_count,
            priority=mailbox.priority,
        )

    async def renew_mailboxes(
        self, leases: tuple[MailboxLease, ...], *, lease_for: timedelta
    ) -> tuple[MailboxLease, ...]:
        return await self._mailbox_store.renew_many(leases, lease_for=lease_for)

    async def commit_mailbox(self, lease: MailboxLease) -> SessionMailbox:
        outbox_start = len(self._mailbox_store.outbox)
        mailbox = await self._mailbox_store.commit(lease)
        self._sync_new_mailbox_outbox(outbox_start)
        return mailbox

    async def reschedule_mailbox(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
    ) -> SessionMailbox:
        outbox_start = len(self._mailbox_store.outbox)
        mailbox = await self._mailbox_store.reschedule(lease, retry_at=retry_at, priority=priority)
        self._sync_new_mailbox_outbox(outbox_start)
        return mailbox

    async def retry_mailbox(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
    ) -> SessionMailbox:
        outbox_start = len(self._mailbox_store.outbox)
        mailbox = await self._mailbox_store.retry(lease, retry_at=retry_at, priority=priority)
        self._sync_new_mailbox_outbox(outbox_start)
        return mailbox

    async def recover_mailbox(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        outbox_start = len(self._mailbox_store.outbox)
        mailbox = await self._mailbox_store.recover(tenant_id, session_id)
        self._sync_new_mailbox_outbox(outbox_start)
        return mailbox

    async def reconcile_mailbox(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        before = await self._mailbox_store.get(tenant_id, session_id)
        outbox_start = len(self._mailbox_store.outbox)
        mailbox = await self._mailbox_store.reconcile(tenant_id, session_id)
        self._sync_new_mailbox_outbox(outbox_start)
        if mailbox is not None and self._mailbox_was_rescheduled(before, mailbox):
            self._rearm_mailbox_outbox(mailbox)
        return mailbox

    async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int:
        outbox_start = len(self._mailbox_store.outbox)
        count = await self._mailbox_store.sweep_expired_leases(owner_id=owner_id, limit=limit)
        self._sync_new_mailbox_outbox(outbox_start)
        return count

    async def schedule_retries(self, *, owner_id: str, limit: int) -> int:
        outbox_start = len(self._mailbox_store.outbox)
        count = await self._mailbox_store.schedule_retries(owner_id=owner_id, limit=limit)
        self._sync_new_mailbox_outbox(outbox_start)
        return count

    async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int:
        before = dict(self._mailbox_store._mailboxes)
        outbox_start = len(self._mailbox_store.outbox)
        count = await self._mailbox_store.reconcile_sessions(owner_id=owner_id, limit=limit)
        self._sync_new_mailbox_outbox(outbox_start)
        for key, mailbox in self._mailbox_store._mailboxes.items():
            if self._mailbox_was_rescheduled(before.get(key), mailbox):
                self._rearm_mailbox_outbox(mailbox)
        return count

    async def accept_inbound(
        self,
        *,
        context: TenantContext,
        envelope: InboundEnvelope,
        trace_headers: dict[str, str],
    ) -> Acceptance:
        key = (
            context.tenant_id,
            envelope.channel.value,
            envelope.account_id,
            envelope.external_message_id,
        )
        async with self._lock:
            existing = self._acceptances.get(key)
            if existing:
                return existing.model_copy(update={"duplicate": True})
            acceptance = Acceptance(
                inbound_id=str(uuid4()),
                context=context,
                envelope=envelope,
            )
            self._acceptances[key] = acceptance
            self._acceptances_by_id[acceptance.inbound_id] = acceptance
            self._acceptance_sequence[acceptance.inbound_id] = self._next_acceptance_sequence
            self._next_acceptance_sequence += 1
            outbox_id = str(uuid4())
            self._outbox[outbox_id] = OutboxRecord(
                outbox_id=outbox_id,
                tenant_id=context.tenant_id,
                event_type="inbound.accepted",
                aggregate_id=acceptance.inbound_id,
                payload={"inbound_id": acceptance.inbound_id},
                trace_headers=trace_headers,
            )
            return acceptance

    async def accept_inbound_v2(
        self,
        *,
        context: TenantContext,
        envelope: InboundEnvelope,
        trace_headers: dict[str, str],
        priority: int = 0,
        retry_at: datetime | None = None,
    ) -> Acceptance:
        key = (
            context.tenant_id,
            envelope.channel.value,
            envelope.account_id,
            envelope.external_message_id,
        )
        async with self._lock:
            existing = self._acceptances.get(key)
            if existing:
                return existing.model_copy(update={"duplicate": True})
            acceptance = Acceptance(
                inbound_id=str(uuid4()),
                context=context,
                envelope=envelope,
            )
            self._acceptances[key] = acceptance
            self._acceptances_by_id[acceptance.inbound_id] = acceptance
            self._acceptance_sequence[acceptance.inbound_id] = self._next_acceptance_sequence
            self._next_acceptance_sequence += 1
            mailbox_outbox_start = len(self._mailbox_store.outbox)
            mailbox = await self._mailbox_store.get(context.tenant_id, context.session_id)
            effective_retry_at = retry_at
            if (
                mailbox is not None
                and mailbox.status.value == "RETRY_WAIT"
                and mailbox.retry_at is not None
                and mailbox.retry_at > datetime.now(UTC)
            ):
                effective_retry_at = (
                    max(mailbox.retry_at, retry_at) if retry_at is not None else mailbox.retry_at
                )
            await self._mailbox_store.accept(
                context.tenant_id,
                context.session_id,
                acceptance.inbound_id,
                priority=priority,
                retry_at=effective_retry_at,
                trace_id=context.trace_id,
            )
            for record in self._mailbox_store.outbox[mailbox_outbox_start:]:
                self._outbox[record.outbox_id] = record
            del trace_headers  # persisted in the inbox; wake-up carries trace_id
            return acceptance

    async def acquire(
        self,
        *,
        acceptance: Acceptance,
        worker_id: str,
        lease_for: timedelta,
    ) -> SessionLease | None:
        key = (acceptance.context.tenant_id, acceptance.context.session_id)
        async with self._session_locks[key]:
            if (acceptance.context.tenant_id, acceptance.inbound_id) in self._committed_inbound:
                return None
            mailbox = await self._mailbox_store.get(*key)
            if mailbox is not None and mailbox.accepted_sequence > mailbox.resolved_sequence:
                return None
            if (
                mailbox is not None
                and mailbox.status.value == "RUNNING"
                and mailbox.lease_expires_at is not None
                and mailbox.lease_expires_at > datetime.now(UTC)
            ):
                return None
            current_order = self._acceptance_sequence[acceptance.inbound_id]
            earlier_pending = any(
                item.context.tenant_id == acceptance.context.tenant_id
                and item.context.session_id == acceptance.context.session_id
                and self._acceptance_sequence[item.inbound_id] < current_order
                and (item.context.tenant_id, item.inbound_id) not in self._committed_inbound
                for item in self._acceptances_by_id.values()
            )
            if earlier_pending:
                return None
            now = datetime.now(UTC)
            current = self._leases.get(key)
            if current and current.expires_at > now:
                return None
            snapshot = self._sessions.get(key)
            if snapshot is None:
                snapshot = SessionSnapshot(
                    tenant_id=acceptance.context.tenant_id,
                    app_id=acceptance.context.app_id,
                    session_id=acceptance.context.session_id,
                    principal_id=acceptance.context.principal_id,
                )
                self._sessions[key] = snapshot
            epoch = current.fencing_token + 1 if current else 1
            attempt = (
                current.attempt + 1
                if current and current.inbound_id == acceptance.inbound_id
                else 1
            )
            lease = SessionLease(
                tenant_id=acceptance.context.tenant_id,
                session_id=acceptance.context.session_id,
                turn_id=current.turn_id
                if current and current.inbound_id == acceptance.inbound_id
                else str(uuid4()),
                inbound_id=acceptance.inbound_id,
                worker_id=worker_id,
                fencing_token=epoch,
                expires_at=now + lease_for,
                attempt=attempt,
                snapshot=snapshot,
            )
            self._leases[key] = lease
            return lease

    async def renew(self, lease: SessionLease, *, lease_for: timedelta) -> SessionLease:
        key = (lease.tenant_id, lease.session_id)
        async with self._session_locks[key]:
            current = self._leases.get(key)
            now = datetime.now(UTC)
            if current is None or not self._owns(current, lease) or current.expires_at <= now:
                raise FencingConflict("session lease is no longer current")
            expires_at = max(now + lease_for, current.expires_at + timedelta(microseconds=1))
            renewed = current.model_copy(update={"expires_at": expires_at})
            self._leases[key] = renewed
            return renewed

    async def commit(self, commit: TurnCommit) -> CommitResult:
        lease = commit.lease
        key = (lease.tenant_id, lease.session_id)
        async with self._session_locks[key]:
            current = self._leases.get(key)
            if (
                current is None
                or not self._owns(current, lease)
                or current.expires_at <= datetime.now(UTC)
            ):
                raise FencingConflict("stale worker cannot commit this session")
            snapshot = self._sessions[key]
            first = snapshot.next_sequence if commit.events else None
            sequenced = tuple(
                SequencedEvent(**event.model_dump(), sequence=snapshot.next_sequence + index)
                for index, event in enumerate(commit.events)
            )
            next_sequence = snapshot.next_sequence + len(sequenced)
            self._sessions[key] = snapshot.model_copy(
                update={
                    "version": snapshot.version + 1,
                    "next_sequence": next_sequence,
                    "state": commit.state,
                    "events": snapshot.events + sequenced,
                }
            )
            outbound_id = None
            if commit.outbound:
                outbound_id = commit.outbound.outbound_id
                outbox_id = str(uuid4())
                self._outbox[outbox_id] = OutboxRecord(
                    outbox_id=outbox_id,
                    tenant_id=lease.tenant_id,
                    event_type=f"outbound.{commit.outbound.channel.value}.ready",
                    aggregate_id=outbound_id,
                    payload=commit.outbound.model_dump(mode="json"),
                    trace_headers=commit.outbound.trace_headers,
                )
            self._committed_inbound.add((lease.tenant_id, lease.inbound_id))
            self._leases.pop(key, None)
            return CommitResult(
                turn_id=lease.turn_id,
                first_sequence=first,
                last_sequence=next_sequence - 1 if sequenced else None,
                outbound_id=outbound_id,
            )

    async def fail(self, lease: SessionLease, *, error_type: str) -> None:
        key = (lease.tenant_id, lease.session_id)
        async with self._session_locks[key]:
            if self._owns(self._leases.get(key), lease):
                self._leases.pop(key, None)

    async def claim_outbox(
        self,
        *,
        event_type: str,
        owner_id: str,
        limit: int,
        lease_for: timedelta,
    ) -> tuple[OutboxRecord, ...]:
        now = datetime.now(UTC)
        claimed: list[OutboxRecord] = []
        async with self._lock:
            for record in self._outbox.values():
                if record.event_type != event_type:
                    continue
                current = self._outbox_claims.get(record.outbox_id)
                if current and current[1] > now:
                    continue
                self._outbox_claims[record.outbox_id] = (owner_id, now + lease_for)
                claimed_record = record.model_copy(update={"attempts": record.attempts + 1})
                # Keep the attempt count on the authoritative in-memory
                # record.  A release followed by a reclaim must eventually
                # reach the dispatcher max-attempt/DLQ boundary just like the
                # PostgreSQL implementation does.
                self._outbox[record.outbox_id] = claimed_record
                claimed.append(claimed_record)
                if len(claimed) >= limit:
                    break
        return tuple(claimed)

    async def mark_outbox_published(self, tenant_id: str, outbox_id: str, *, owner_id: str) -> None:
        async with self._lock:
            claim = self._outbox_claims.get(outbox_id)
            if not claim or claim[0] != owner_id:
                raise FencingConflict("outbox claim is no longer current")
            self._outbox.pop(outbox_id, None)
            self._outbox_claims.pop(outbox_id, None)

    async def release_outbox(
        self,
        tenant_id: str,
        outbox_id: str,
        *,
        owner_id: str,
        delay: timedelta,
        error_type: str,
    ) -> None:
        async with self._lock:
            claim = self._outbox_claims.get(outbox_id)
            if claim is None or claim[0] != owner_id:
                raise FencingConflict("outbox claim is no longer current")
            self._outbox_claims[outbox_id] = ("", datetime.now(UTC) + delay)

    async def record_delivery(
        self, tenant_id: str, receipt: DeliveryReceipt, *, retrying: bool = False
    ) -> None:
        self.delivery_receipts.append(receipt)

    async def begin_delivery(self, record: OutboxRecord, *, owner_id: str) -> DeliveryAttempt:
        async with self._lock:
            claim = self._outbox_claims.get(record.outbox_id)
            if claim is None or claim[0] != owner_id:
                raise FencingConflict("outbox claim is no longer current")
            outbound_id = str(record.aggregate_id)
            attempt_key = (record.tenant_id, outbound_id)
            current = self._delivery_attempts.get(attempt_key)
            if current is not None and current[2] == "sending":
                raise DeliveryInProgress(
                    "provider delivery is still unresolved",
                    attempt_number=current[1],
                )
            if current is not None and current[2] == "delivered":
                raise FencingConflict("outbound message is already delivered")
            if current is not None and current[2] == "ambiguous":
                raise FencingConflict("ambiguous delivery requires manual replay")
            attempt_number = (current[1] + 1) if current is not None else 1
            self._delivery_attempts[attempt_key] = (owner_id, attempt_number, "sending")
            return DeliveryAttempt(
                tenant_id=record.tenant_id,
                outbound_id=outbound_id,
                attempt_number=attempt_number,
                owner_id=owner_id,
            )

    async def finish_delivery(
        self,
        record: OutboxRecord,
        *,
        owner_id: str,
        attempt_number: int,
        receipt: DeliveryReceipt,
        retry_delay: timedelta = timedelta(seconds=1),
    ) -> None:
        async with self._lock:
            claim = self._outbox_claims.get(record.outbox_id)
            attempt_key = (record.tenant_id, str(record.aggregate_id))
            current = self._delivery_attempts.get(attempt_key)
            if receipt.outbound_id != record.aggregate_id:
                raise ValueError("delivery receipt does not match outbox record")
            if (
                claim is None
                or claim[0] != owner_id
                or current is None
                or current[1] != attempt_number
                or current[2] != "sending"
            ):
                raise FencingConflict("delivery attempt is no longer current")
            # The outbox claim is the fencing boundary for the provider
            # attempt.  A new owner may take over an expired outbox claim
            # while the previous provider request is still unresolved.  The
            # PostgreSQL implementation permits that owner to close the
            # *same* attempt (normally as ``ambiguous``), so the in-memory
            # implementation must not retain the old attempt owner as a
            # second, incompatible fence.  A late response from the old
            # owner is still rejected because its outbox claim no longer
            # matches above.
            self.delivery_receipts.append(receipt)
            self._delivery_attempts[attempt_key] = (
                owner_id,
                attempt_number,
                receipt.status.value,
            )
            if receipt.status.value == "delivered":
                self._outbox.pop(record.outbox_id, None)
                self._outbox_claims.pop(record.outbox_id, None)
            elif receipt.status.value == "failed" and receipt.retryable:
                self._outbox_claims[record.outbox_id] = (
                    "",
                    datetime.now(UTC) + retry_delay,
                )
            else:
                self.dead_letters.append((record, receipt.status.value))
                self._outbox.pop(record.outbox_id, None)
                self._outbox_claims.pop(record.outbox_id, None)

    async def dead_letter_outbox(
        self,
        record: OutboxRecord,
        *,
        owner_id: str,
        reason: str,
    ) -> None:
        async with self._lock:
            claim = self._outbox_claims.get(record.outbox_id)
            if not claim or claim[0] != owner_id:
                raise FencingConflict("outbox claim is no longer current")
            self.dead_letters.append((record, reason))
            self._outbox.pop(record.outbox_id, None)
            self._outbox_claims.pop(record.outbox_id, None)

    def snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        return self._sessions.get((tenant_id, session_id))

    @staticmethod
    def _owns(current: SessionLease | None, expected: SessionLease) -> bool:
        return bool(
            current
            and current.worker_id == expected.worker_id
            and current.fencing_token == expected.fencing_token
            and current.turn_id == expected.turn_id
        )


__all__ = ["InMemoryRuntimeRepository"]

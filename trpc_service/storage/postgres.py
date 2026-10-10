"""PostgreSQL authoritative repository with RLS, leases, and fencing tokens."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from trpc_service.channels.envelopes import DeliveryReceipt, InboundEnvelope
from trpc_service.faults import FaultStage, FaultStageController, FaultStageEvent
from trpc_service.metrics.prometheus import (
    observe_session_ready_claim,
    observe_session_ready_commit,
    observe_session_ready_failure,
    observe_session_ready_lease_renewal,
    observe_session_ready_retry,
)
from trpc_service.storage.mailbox import PostgresSessionMailboxStore, _mailbox_from_row
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
    WeComBindingLeaseGrant,
)
from trpc_service.storage.protocols import DeliveryInProgress, FencingConflict
from trpc_service.tenant.models import Channel, ChannelBinding, TenantConfig, TenantContext


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _im_provider_event_hash(
    tenant_id: str,
    binding_id: str,
    channel: Channel,
    external_message_id: str,
) -> str:
    if channel == Channel.FEISHU:
        material = b"trpc.feishu.callback.message-id.v1\0" + external_message_id.encode("utf-8")
    elif channel == Channel.WECOM_AI_BOT:
        material = "\0".join(
            (
                "trpc-wecom-evidence-v1",
                "provider-event",
                tenant_id,
                binding_id,
                external_message_id,
            )
        ).encode("utf-8")
    else:  # pragma: no cover - Channel currently has only the two production values.
        raise ValueError("unsupported IM channel")
    return hashlib.sha256(material).hexdigest()


class PostgresRuntimeRepository:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        fault_stages: FaultStageController | None = None,
        ready_replay_cooldown_seconds: int = 30,
    ) -> None:
        self._pool = pool
        # The controller owns its independent marker connection.  Keeping only
        # the optional hook here ensures the business transaction never writes
        # a fault marker and preserves the default repository path.
        self._fault_stages = fault_stages
        self._mailbox_store = PostgresSessionMailboxStore(
            pool,
            ready_replay_cooldown_seconds=ready_replay_cooldown_seconds,
        )

    @property
    def pool(self) -> asyncpg.Pool:
        return self._pool

    @property
    def mailbox(self) -> PostgresSessionMailboxStore:
        return self._mailbox_store

    @classmethod
    async def create(
        cls,
        dsn: str,
        *,
        min_size: int = 2,
        max_size: int = 20,
        ready_replay_cooldown_seconds: int = 30,
    ) -> PostgresRuntimeRepository:
        pool = await asyncpg.create_pool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            command_timeout=30,
            server_settings={
                "application_name": "trpc-agent-service",
                "tcp_keepalives_idle": "10",
                "tcp_keepalives_interval": "5",
                "tcp_keepalives_count": "3",
                "tcp_user_timeout": "30000",
            },
        )
        return cls(pool, ready_replay_cooldown_seconds=ready_replay_cooldown_seconds)

    async def close(self) -> None:
        await self._pool.close()

    async def ready(self) -> bool:
        try:
            async with self._pool.acquire() as connection:
                return bool(await connection.fetchval("SELECT 1"))
        except (asyncpg.PostgresError, OSError):
            return False

    @asynccontextmanager
    async def _tenant_transaction(self, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            yield connection

    async def resolve_binding(self, binding_id: str) -> BindingRoute | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow("SELECT * FROM resolve_channel_binding($1)", binding_id)
        if row is None:
            return None
        binding = ChannelBinding(
            binding_id=row["binding_id"],
            tenant_id=row["tenant_id"],
            app_id=row["app_id"],
            channel=row["channel"],
            account_id=row["account_id"],
            secret_refs=_json(row["secret_refs"]),
            enabled=row["enabled"],
            control_version=row["control_version"],
            capabilities=frozenset(_json(row["capabilities"])),
        )
        return BindingRoute(
            binding=binding,
            tenant_active=row["tenant_active"],
            active_config_version=row["active_config_version"],
            candidate_config_version=row["candidate_config_version"],
            candidate_percent=float(row["candidate_percent"]),
        )

    async def list_bindings(self, channel: Channel) -> tuple[ChannelBinding, ...]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch("SELECT * FROM list_channel_bindings($1)", channel.value)
        return tuple(
            ChannelBinding(
                binding_id=row["binding_id"],
                tenant_id=row["tenant_id"],
                app_id=row["app_id"],
                channel=row["channel"],
                account_id=row["account_id"],
                secret_refs=_json(row["secret_refs"]),
                enabled=row["enabled"],
                control_version=row["control_version"],
                capabilities=frozenset(_json(row["capabilities"])),
            )
            for row in rows
        )

    async def get_config(self, tenant_id: str, app_id: str, version: int) -> TenantConfig:
        async with self._tenant_transaction(tenant_id) as connection:
            value = await connection.fetchval(
                """
                SELECT config_json FROM config_revisions
                 WHERE tenant_id = $1 AND app_id = $2 AND version = $3
                """,
                tenant_id,
                app_id,
                version,
            )
        if value is None:
            raise LookupError("pinned tenant configuration does not exist")
        return TenantConfig.model_validate(_json(value))

    async def get_acceptance(self, tenant_id: str, inbound_id: str) -> Acceptance | None:
        async with self._tenant_transaction(tenant_id) as connection:
            row = await connection.fetchrow(
                """
                SELECT * FROM inbound_messages
                 WHERE tenant_id = $1 AND inbound_id = $2
                """,
                tenant_id,
                UUID(inbound_id),
            )
        if row is None:
            return None
        acceptance = self._acceptance(row)
        if row.get("status") == "committed":
            return acceptance.model_copy(update={"duplicate": True})
        return acceptance

    async def get_session_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        async with self._tenant_transaction(tenant_id) as connection:
            session = await connection.fetchrow(
                "SELECT * FROM sessions WHERE tenant_id=$1 AND session_id=$2",
                tenant_id,
                session_id,
            )
            if session is None:
                return None
            event_rows = await connection.fetch(
                """
                SELECT sequence,event_id,author,event_timestamp,event_json,state_delta
                  FROM session_events
                 WHERE tenant_id=$1 AND session_id=$2 ORDER BY sequence
                """,
                tenant_id,
                session_id,
            )
        return SessionSnapshot(
            tenant_id=tenant_id,
            app_id=session["app_id"],
            session_id=session_id,
            principal_id=session["principal_id"],
            version=session["version"],
            next_sequence=session["next_sequence"],
            state=_json(session["state_json"]),
            events=tuple(
                SequencedEvent(
                    sequence=row["sequence"],
                    event_id=row["event_id"],
                    author=row["author"],
                    timestamp=row["event_timestamp"],
                    event=_json(row["event_json"]),
                    state_delta=_json(row["state_delta"]),
                )
                for row in event_rows
            ),
        )

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
        return await self._mailbox_store.accept(
            tenant_id,
            session_id,
            inbound_id,
            priority=priority,
            retry_at=retry_at,
            trace_id=trace_id,
        )

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

    @observe_session_ready_claim
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
        if lease_for <= timedelta(0):
            raise ValueError("session lease must be positive")
        if expected_event_id is None:
            # PostgreSQL is the production authority: a Redis wake-up without
            # its durable outbox identity cannot be authenticated and must
            # fail closed.  The in-memory adapter keeps the optional value
            # only for direct, non-Redis compatibility tests.
            return SessionClaim(
                status=MailboxClaimStatus.STALE,
                mailbox=SessionMailbox(tenant_id=tenant_id, session_id=session_id),
                acceptance=acceptance,
            )
        async with self._tenant_transaction(tenant_id) as connection:
            # Redis is only a wake-up transport. Authenticate its event id
            # against the tenant-scoped durable outbox before accepting any
            # session or generation fields from the message. Missing ids have
            # already failed closed above; only the in-memory adapter permits
            # direct local calls without a Redis delivery envelope.
            try:
                event_uuid = UUID(expected_event_id)
            except (AttributeError, TypeError, ValueError):
                return SessionClaim(
                    status=MailboxClaimStatus.STALE,
                    mailbox=SessionMailbox(tenant_id=tenant_id, session_id=session_id),
                    acceptance=acceptance,
                )
            ready_event = await connection.fetchrow(
                """
                SELECT outbox_id,tenant_id,aggregate_type,aggregate_id,
                       event_type,payload_json
                  FROM outbox_events
                 WHERE tenant_id=$1 AND outbox_id=$2::uuid
                   AND event_type='session.ready.v2'
                   AND aggregate_type='session'
                   AND aggregate_id=$3::text
                """,
                tenant_id,
                event_uuid,
                session_id,
            )
            payload = _json(ready_event["payload_json"]) if ready_event is not None else {}
            event_generation = payload.get("generation") if isinstance(payload, Mapping) else None
            if (
                ready_event is None
                or ready_event["tenant_id"] != tenant_id
                or ready_event["aggregate_type"] != "session"
                or str(ready_event["aggregate_id"]) != session_id
                or ready_event["event_type"] != "session.ready.v2"
                or isinstance(event_generation, bool)
                or not isinstance(event_generation, int)
                or event_generation < 1
                or (expected_generation is not None and event_generation != expected_generation)
            ):
                return SessionClaim(
                    status=MailboxClaimStatus.STALE,
                    mailbox=SessionMailbox(tenant_id=tenant_id, session_id=session_id),
                    acceptance=acceptance,
                )
            expected_generation = event_generation
            mailbox_row = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                tenant_id,
                session_id,
            )
            if mailbox_row is None:
                mailbox = SessionMailbox(tenant_id=tenant_id, session_id=session_id)
                status = (
                    MailboxClaimStatus.STALE
                    if expected_generation not in (None, 0)
                    else MailboxClaimStatus.EMPTY
                )
                return SessionClaim(status=status, mailbox=mailbox, acceptance=acceptance)
            mailbox = _mailbox_from_row(mailbox_row)
            if expected_generation is not None and mailbox.queue_generation != expected_generation:
                return SessionClaim(
                    status=MailboxClaimStatus.STALE,
                    mailbox=mailbox,
                    acceptance=acceptance,
                )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if mailbox.lease_expires_at is not None and mailbox.lease_expires_at > server_now:
                return SessionClaim(
                    status=MailboxClaimStatus.RUNNING,
                    mailbox=mailbox,
                    acceptance=acceptance,
                )

            sequence = mailbox.processing_sequence or mailbox.resolved_sequence + 1
            if sequence > mailbox.accepted_sequence:
                return SessionClaim(
                    status=MailboxClaimStatus.EMPTY,
                    mailbox=mailbox,
                    acceptance=acceptance,
                )
            item = await connection.fetchrow(
                """
                SELECT * FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                 FOR UPDATE
                """,
                tenant_id,
                session_id,
                sequence,
            )
            if item is None:
                return SessionClaim(
                    status=MailboxClaimStatus.EMPTY,
                    mailbox=mailbox,
                    acceptance=acceptance,
                )
            if item["retry_at"] is not None and item["retry_at"] > server_now:
                retry_row = await connection.fetchrow(
                    """
                    UPDATE session_mailboxes
                       SET status='RETRY_WAIT',retry_at=$3,updated_at=clock_timestamp()
                     WHERE tenant_id=$1 AND session_id=$2
                     RETURNING *
                    """,
                    tenant_id,
                    session_id,
                    item["retry_at"],
                )
                assert retry_row is not None
                return SessionClaim(
                    status=MailboxClaimStatus.EMPTY,
                    mailbox=_mailbox_from_row(retry_row),
                    acceptance=acceptance,
                )
            inbound = await connection.fetchrow(
                """
                SELECT inbound_id,status,app_id,principal_id,config_version
                  FROM inbound_messages
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid
                 FOR UPDATE
                """,
                tenant_id,
                item["inbound_id"],
            )
            if inbound is None:
                return SessionClaim(
                    status=MailboxClaimStatus.EMPTY,
                    mailbox=mailbox,
                    acceptance=acceptance,
                )
            if inbound["status"] == "committed":
                resolved_mailbox = await self._resolve_mailbox_item(
                    connection,
                    tenant_id=tenant_id,
                    session_id=session_id,
                    sequence=sequence,
                    server_now=server_now,
                    expected_lease_owner=mailbox.lease_owner,
                    expected_lease_epoch=mailbox.lease_epoch,
                )
                return SessionClaim(
                    status=MailboxClaimStatus.EMPTY,
                    mailbox=resolved_mailbox,
                    acceptance=acceptance,
                )
            turn = await connection.fetchrow(
                """
                SELECT * FROM session_turns
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid
                 FOR UPDATE
                """,
                tenant_id,
                item["inbound_id"],
            )
            await connection.execute(
                """
                INSERT INTO sessions (tenant_id,session_id,app_id,principal_id)
                VALUES ($1,$2,$3,$4)
                ON CONFLICT (tenant_id,session_id) DO NOTHING
                """,
                tenant_id,
                session_id,
                inbound["app_id"],
                inbound["principal_id"],
            )
            session = await connection.fetchrow(
                """
                SELECT app_id,principal_id,version,next_sequence,
                       lease_owner,lease_expires_at,lease_epoch
                  FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                tenant_id,
                session_id,
            )
            assert session is not None
            if (
                session["lease_owner"] is not None
                and session["lease_expires_at"] is not None
                and session["lease_expires_at"] > server_now
            ):
                return SessionClaim(
                    status=MailboxClaimStatus.RUNNING,
                    mailbox=mailbox,
                    acceptance=acceptance,
                )
            epoch = max(mailbox.lease_epoch, int(session["lease_epoch"])) + 1
            attempt = int(item["attempt"]) + 1
            if turn is not None and turn["status"] == "committed":
                resolved_mailbox = await self._resolve_mailbox_item(
                    connection,
                    tenant_id=tenant_id,
                    session_id=session_id,
                    sequence=sequence,
                    server_now=server_now,
                    expected_lease_owner=mailbox.lease_owner,
                    expected_lease_epoch=mailbox.lease_epoch,
                )
                return SessionClaim(
                    status=MailboxClaimStatus.EMPTY,
                    mailbox=resolved_mailbox,
                    acceptance=acceptance,
                )
            turn_id = turn["turn_id"] if turn is not None else uuid4()
            if turn is None:
                await connection.execute(
                    """
                    INSERT INTO session_turns (
                        tenant_id,turn_id,session_id,inbound_id,config_version,
                        status,fencing_token,attempt
                    ) VALUES ($1,$2,$3,$4::uuid,$5,'processing',$6,$7)
                    """,
                    tenant_id,
                    turn_id,
                    session_id,
                    item["inbound_id"],
                    inbound["config_version"],
                    epoch,
                    attempt,
                )
            else:
                await connection.execute(
                    """
                    UPDATE session_turns
                       SET status='processing',fencing_token=$3,attempt=$4,
                           error_type=NULL,started_at=clock_timestamp()
                     WHERE tenant_id=$1 AND turn_id=$2
                    """,
                    tenant_id,
                    turn_id,
                    epoch,
                    attempt,
                )
            await connection.execute(
                """
                UPDATE sessions
                   SET lease_epoch=$3,lease_owner=$4,
                       lease_expires_at=clock_timestamp()+$5::interval,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                tenant_id,
                session_id,
                epoch,
                owner_id,
                lease_for,
            )
            await connection.execute(
                """
                UPDATE session_mailbox_items
                   SET attempt=$4
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                """,
                tenant_id,
                session_id,
                sequence,
                attempt,
            )
            claimed_row = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET status='RUNNING',processing_sequence=$3,
                       processing_inbound_id=$4::uuid,lease_owner=$5,
                       lease_epoch=$6,lease_expires_at=clock_timestamp()+$7::interval,
                       attempt=$8,retry_count=$9,priority=$10,retry_at=NULL,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                 RETURNING *
                """,
                tenant_id,
                session_id,
                sequence,
                item["inbound_id"],
                owner_id,
                epoch,
                lease_for,
                attempt,
                item["retry_count"],
                item["priority"],
            )
            assert claimed_row is not None
            await connection.execute(
                """
                UPDATE inbound_messages SET status='processing'
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid
                """,
                tenant_id,
                item["inbound_id"],
            )
            # Do not read state_json, envelope_json, or session_events while
            # the mailbox row is locked.  The claim transaction must commit
            # before the worker performs the potentially unbounded history
            # read.  ``version`` and ``next_sequence`` are the hydration
            # anchors; the worker verifies them after the short transaction.
            snapshot = SessionSnapshot(
                tenant_id=tenant_id,
                app_id=session["app_id"],
                session_id=session_id,
                principal_id=session["principal_id"],
                version=session["version"],
                next_sequence=session["next_sequence"],
                state={},
                events=(),
            )
            # Full inbound envelope hydration also happens after the Redis
            # ACK.  ``acceptance`` is retained for direct compatibility
            # callers that already supplied the authoritative value.
            acceptance_value = acceptance
            mailbox_value = _mailbox_from_row(claimed_row)
            execution_lease = SessionLease(
                tenant_id=tenant_id,
                session_id=session_id,
                turn_id=str(turn_id),
                inbound_id=str(item["inbound_id"]),
                worker_id=owner_id,
                fencing_token=epoch,
                expires_at=mailbox_value.lease_expires_at,
                attempt=attempt,
                snapshot=snapshot,
                snapshot_hydrated=False,
            )
            mailbox_lease = MailboxLease(
                tenant_id=tenant_id,
                session_id=session_id,
                inbound_id=str(item["inbound_id"]),
                sequence=sequence,
                owner_id=owner_id,
                epoch=epoch,
                expires_at=mailbox_value.lease_expires_at,
                attempt=attempt,
                retry_count=int(item["retry_count"]),
                priority=int(item["priority"]),
            )
            return SessionClaim(
                status=MailboxClaimStatus.CLAIMED,
                mailbox=mailbox_value,
                lease=mailbox_lease,
                acceptance=acceptance_value,
                execution_lease=execution_lease,
            )

    async def renew_mailbox(self, lease: MailboxLease, *, lease_for: timedelta) -> MailboxLease:
        return await self._mailbox_store.renew(lease, lease_for=lease_for)

    async def renew_mailboxes(
        self, leases: tuple[MailboxLease, ...], *, lease_for: timedelta
    ) -> tuple[MailboxLease, ...]:
        return await self._mailbox_store.renew_many(leases, lease_for=lease_for)

    async def commit_mailbox(self, lease: MailboxLease) -> SessionMailbox:
        return await self._mailbox_store.commit(lease)

    async def reschedule_mailbox(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
    ) -> SessionMailbox:
        return await self._mailbox_store.reschedule(lease, retry_at=retry_at, priority=priority)

    async def retry_mailbox(
        self,
        lease: MailboxLease,
        *,
        retry_at: datetime | None = None,
        priority: int | None = None,
    ) -> SessionMailbox:
        return await self._mailbox_store.retry(lease, retry_at=retry_at, priority=priority)

    async def recover_mailbox(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        return await self._mailbox_store.recover(tenant_id, session_id)

    async def reconcile_mailbox(self, tenant_id: str, session_id: str) -> SessionMailbox | None:
        return await self._mailbox_store.reconcile(tenant_id, session_id)

    async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int:
        return await self._mailbox_store.sweep_expired_leases(owner_id=owner_id, limit=limit)

    async def schedule_retries(self, *, owner_id: str, limit: int) -> int:
        return await self._mailbox_store.schedule_retries(owner_id=owner_id, limit=limit)

    async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int:
        return await self._mailbox_store.reconcile_sessions(owner_id=owner_id, limit=limit)

    async def accept_inbound(
        self,
        *,
        context: TenantContext,
        envelope: InboundEnvelope,
        trace_headers: dict[str, str],
    ) -> Acceptance:
        inbound_id = uuid4()
        provider_event_hash = _im_provider_event_hash(
            context.tenant_id,
            context.channel_binding_id,
            envelope.channel,
            envelope.external_message_id,
        )
        async with self._tenant_transaction(context.tenant_id) as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO inbound_messages (
                    tenant_id, inbound_id, binding_id, app_id, config_version,
                    channel, account_id, external_message_id, principal_id,
                    session_id, request_id, trace_id, envelope_json,
                    provider_event_hash
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14
                )
                ON CONFLICT (tenant_id, channel, account_id, external_message_id)
                    DO NOTHING
                RETURNING *
                """,
                context.tenant_id,
                inbound_id,
                context.channel_binding_id,
                context.app_id,
                context.config_version,
                envelope.channel.value,
                envelope.account_id,
                envelope.external_message_id,
                context.principal_id,
                context.session_id,
                context.request_id,
                context.trace_id,
                _dump(envelope.model_dump(mode="json")),
                provider_event_hash,
            )
            if row is None:
                existing = await connection.fetchrow(
                    """
                    UPDATE inbound_messages
                       SET delivery_count=delivery_count+1,
                           provider_event_hash=COALESCE(provider_event_hash,$5)
                     WHERE tenant_id=$1 AND channel=$2 AND account_id=$3
                       AND external_message_id=$4
                       AND binding_id=$6
                       AND (provider_event_hash IS NULL OR provider_event_hash=$5)
                    RETURNING *
                    """,
                    context.tenant_id,
                    envelope.channel.value,
                    envelope.account_id,
                    envelope.external_message_id,
                    provider_event_hash,
                    context.channel_binding_id,
                )
                assert existing is not None
                return self._acceptance(existing).model_copy(update={"duplicate": True})

            await connection.execute(
                """
                INSERT INTO channel_identities (
                    tenant_id, binding_id, external_user_hash, principal_id
                ) VALUES ($1,$2,$3,$4)
                ON CONFLICT (tenant_id, binding_id, external_user_hash)
                DO UPDATE SET last_seen_at=now()
                """,
                context.tenant_id,
                context.channel_binding_id,
                context.principal_id,
                context.principal_id,
            )
            await connection.execute(
                """
                INSERT INTO audit_logs (
                    tenant_id, channel, user_id, session_id, decision, trace_id,
                    config_version, idempotency_key, redaction_applied, metadata_json
                ) VALUES ($1,$2,$3,$4,'inbound_accepted',$5,$6,$7,true,'{}'::jsonb)
                """,
                context.tenant_id,
                envelope.channel.value,
                context.principal_id,
                context.session_id,
                context.trace_id,
                context.config_version,
                envelope.external_message_id,
            )
            await connection.execute(
                """
                INSERT INTO outbox_events (
                    tenant_id, aggregate_type, aggregate_id, event_type,
                    payload_json, trace_headers
                ) VALUES ($1,'inbound',$2::text,'inbound.accepted',$3::jsonb,$4::jsonb)
                """,
                context.tenant_id,
                str(inbound_id),
                _dump({"tenant_id": context.tenant_id, "inbound_id": str(inbound_id)}),
                _dump(trace_headers),
            )
            return self._acceptance(row)

    async def accept_inbound_v2(
        self,
        *,
        context: TenantContext,
        envelope: InboundEnvelope,
        trace_headers: dict[str, str],
        priority: int = 0,
        retry_at: datetime | None = None,
    ) -> Acceptance:
        """Accept an inbound and enqueue its mailbox item in one transaction.

        This deliberately does not emit ``inbound.accepted``.  The durable
        session-ready outbox is the sole wake-up for the v2 mailbox path.
        """

        if priority < 0 or isinstance(priority, bool):
            raise ValueError("mailbox priority must be non-negative")
        inbound_id = uuid4()
        provider_event_hash = _im_provider_event_hash(
            context.tenant_id,
            context.channel_binding_id,
            envelope.channel,
            envelope.external_message_id,
        )
        async with self._tenant_transaction(context.tenant_id) as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO inbound_messages (
                    tenant_id, inbound_id, binding_id, app_id, config_version,
                    channel, account_id, external_message_id, principal_id,
                    session_id, request_id, trace_id, envelope_json,
                    provider_event_hash
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14
                )
                ON CONFLICT (tenant_id, channel, account_id, external_message_id)
                    DO NOTHING
                RETURNING *
                """,
                context.tenant_id,
                inbound_id,
                context.channel_binding_id,
                context.app_id,
                context.config_version,
                envelope.channel.value,
                envelope.account_id,
                envelope.external_message_id,
                context.principal_id,
                context.session_id,
                context.request_id,
                context.trace_id,
                _dump(envelope.model_dump(mode="json")),
                provider_event_hash,
            )
            if row is None:
                existing = await connection.fetchrow(
                    """
                    UPDATE inbound_messages
                       SET delivery_count=delivery_count+1,
                           provider_event_hash=COALESCE(provider_event_hash,$5)
                     WHERE tenant_id=$1 AND channel=$2 AND account_id=$3
                       AND external_message_id=$4
                       AND binding_id=$6
                       AND (provider_event_hash IS NULL OR provider_event_hash=$5)
                    RETURNING *
                    """,
                    context.tenant_id,
                    envelope.channel.value,
                    envelope.account_id,
                    envelope.external_message_id,
                    provider_event_hash,
                    context.channel_binding_id,
                )
                assert existing is not None
                return self._acceptance(existing).model_copy(update={"duplicate": True})

            await connection.execute(
                """
                INSERT INTO channel_identities (
                    tenant_id, binding_id, external_user_hash, principal_id
                ) VALUES ($1,$2,$3,$4)
                ON CONFLICT (tenant_id, binding_id, external_user_hash)
                DO UPDATE SET last_seen_at=clock_timestamp()
                """,
                context.tenant_id,
                context.channel_binding_id,
                context.principal_id,
                context.principal_id,
            )
            await connection.execute(
                """
                INSERT INTO audit_logs (
                    tenant_id, channel, user_id, session_id, decision, trace_id,
                    config_version, idempotency_key, redaction_applied, metadata_json
                ) VALUES ($1,$2,$3,$4,'inbound_accepted',$5,$6,$7,true,'{}'::jsonb)
                """,
                context.tenant_id,
                envelope.channel.value,
                context.principal_id,
                context.session_id,
                context.trace_id,
                context.config_version,
                envelope.external_message_id,
            )
            await connection.execute(
                """
                INSERT INTO session_mailboxes (tenant_id,session_id)
                VALUES ($1,$2::text)
                ON CONFLICT (tenant_id,session_id) DO NOTHING
                """,
                context.tenant_id,
                context.session_id,
            )
            mailbox = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2::text
                 FOR UPDATE
                """,
                context.tenant_id,
                context.session_id,
            )
            assert mailbox is not None
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
                    # item's retry time must not replace the head timer.
                    next_status = MailboxStatus.RETRY_WAIT.value
                    next_retry_at = current_retry_at
                else:
                    # The current head is due.  A future retry time belongs to
                    # the newly appended item and is evaluated after the head
                    # is resolved; it must not block the head now.
                    next_status = MailboxStatus.QUEUED.value
                    next_retry_at = None
            elif previous_status == MailboxStatus.IDLE:
                if retry_at is not None and retry_at > server_now:
                    next_status = MailboxStatus.RETRY_WAIT.value
                    next_retry_at = retry_at
                else:
                    next_status = MailboxStatus.QUEUED.value
                    next_retry_at = None
            else:
                next_status = previous_status.value
                next_retry_at = current_retry_at
            ready = next_status == MailboxStatus.QUEUED.value and previous_status in (
                MailboxStatus.IDLE,
                MailboxStatus.RETRY_WAIT,
            )
            item = await connection.fetchrow(
                """
                INSERT INTO session_mailbox_items (
                    tenant_id,session_id,sequence,inbound_id,trace_id,priority,retry_at
                ) VALUES ($1,$2::text,$3,$4::uuid,$5::text,$6::integer,$7::timestamptz)
                RETURNING *
                """,
                context.tenant_id,
                context.session_id,
                int(mailbox["accepted_sequence"]) + 1,
                inbound_id,
                context.trace_id or str(inbound_id),
                priority,
                retry_at,
            )
            assert item is not None
            changed = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET accepted_sequence=accepted_sequence+1,
                       status=$3,retry_at=$4,
                       queue_generation=queue_generation+$5,
                       priority=greatest(priority,$6::integer),
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2::text
                 RETURNING *
                """,
                context.tenant_id,
                context.session_id,
                next_status,
                next_retry_at,
                1 if ready else 0,
                priority,
            )
            assert changed is not None
            if ready:
                await connection.execute(
                    """
                    INSERT INTO outbox_events (
                        tenant_id,aggregate_type,aggregate_id,event_type,payload_json,
                        trace_headers
                    ) VALUES (
                        $1,'session',$2::text,'session.ready.v2',
                        jsonb_build_object(
                            'generation',$3::bigint,
                            'priority',$4::integer,
                            'trace_id',$5::text,
                            'created_at',to_char(clock_timestamp() AT TIME ZONE 'UTC',
                                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                        ),$6::jsonb
                    ) ON CONFLICT DO NOTHING
                    """,
                    context.tenant_id,
                    context.session_id,
                    int(changed["queue_generation"]),
                    int(item["priority"]),
                    item["trace_id"],
                    _dump(trace_headers),
                )
            return self._acceptance(row)

    async def acquire(
        self,
        *,
        acceptance: Acceptance,
        worker_id: str,
        lease_for: timedelta,
    ) -> SessionLease | None:
        context = acceptance.context
        inbound_uuid = UUID(acceptance.inbound_id)
        async with self._tenant_transaction(context.tenant_id) as connection:
            prefetched_session: Mapping[str, Any] | None = None
            mailbox = await connection.fetchrow(
                """
                SELECT status,accepted_sequence,resolved_sequence,
                       lease_expires_at
                  FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                context.tenant_id,
                context.session_id,
            )
            if mailbox is not None and "status" not in mailbox:
                # Compatibility with lightweight repository fakes that model
                # the pre-mailbox v1 query sequence.
                prefetched_session = mailbox
                mailbox = None
            mailbox_now = None
            if (
                mailbox is not None
                and mailbox["status"] == MailboxStatus.RUNNING.value
                and mailbox["lease_expires_at"] is not None
            ):
                mailbox_now = await connection.fetchval("SELECT clock_timestamp()")
            if mailbox is not None and (
                int(mailbox["accepted_sequence"]) > int(mailbox["resolved_sequence"])
                or (
                    mailbox["status"] == MailboxStatus.RUNNING.value
                    and mailbox["lease_expires_at"] is not None
                    and mailbox_now is not None
                    and mailbox["lease_expires_at"] > mailbox_now
                )
            ):
                return None
            await connection.execute(
                """
                INSERT INTO sessions (tenant_id,session_id,app_id,principal_id)
                VALUES ($1,$2,$3,$4)
                ON CONFLICT (tenant_id,session_id) DO NOTHING
                """,
                context.tenant_id,
                context.session_id,
                context.app_id,
                context.principal_id,
            )
            session = prefetched_session or await connection.fetchrow(
                """
                SELECT * FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                context.tenant_id,
                context.session_id,
            )
            assert session is not None
            existing_turn = await connection.fetchrow(
                """
                SELECT * FROM session_turns
                 WHERE tenant_id=$1 AND inbound_id=$2
                 FOR UPDATE
                """,
                context.tenant_id,
                inbound_uuid,
            )
            if existing_turn and existing_turn["status"] == "committed":
                return None
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if (
                session["lease_owner"]
                and session["lease_expires_at"]
                and session["lease_expires_at"] > server_now
            ):
                return None

            current_inbound = await connection.fetchrow(
                """
                SELECT accepted_at, inbound_id FROM inbound_messages
                 WHERE tenant_id=$1 AND inbound_id=$2
                """,
                context.tenant_id,
                inbound_uuid,
            )
            assert current_inbound is not None
            earlier = await connection.fetchval(
                """
                SELECT 1
                  FROM inbound_messages older
                  LEFT JOIN session_turns turn
                    ON turn.tenant_id=older.tenant_id AND turn.inbound_id=older.inbound_id
                 WHERE older.tenant_id=$1 AND older.session_id=$2
                   AND (older.accepted_at, older.inbound_id) < ($3, $4)
                   AND coalesce(turn.status, '') <> 'committed'
                 LIMIT 1
                """,
                context.tenant_id,
                context.session_id,
                current_inbound["accepted_at"],
                inbound_uuid,
            )
            if earlier:
                return None

            epoch = int(session["lease_epoch"]) + 1
            expires = server_now + lease_for
            turn_id = existing_turn["turn_id"] if existing_turn else uuid4()
            attempt = int(existing_turn["attempt"]) + 1 if existing_turn else 1
            await connection.execute(
                """
                UPDATE sessions
                   SET lease_epoch=$3, lease_owner=$4,
                       lease_expires_at=clock_timestamp()+$5::interval,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                context.tenant_id,
                context.session_id,
                epoch,
                worker_id,
                lease_for,
            )
            if existing_turn:
                await connection.execute(
                    """
                    UPDATE session_turns
                       SET status='processing', fencing_token=$3, attempt=$4,
                           error_type=NULL, started_at=clock_timestamp()
                     WHERE tenant_id=$1 AND turn_id=$2
                    """,
                    context.tenant_id,
                    turn_id,
                    epoch,
                    attempt,
                )
            else:
                await connection.execute(
                    """
                    INSERT INTO session_turns (
                        tenant_id,turn_id,session_id,inbound_id,config_version,
                        status,fencing_token,attempt
                    ) VALUES ($1,$2,$3,$4,$5,'processing',$6,1)
                    """,
                    context.tenant_id,
                    turn_id,
                    context.session_id,
                    inbound_uuid,
                    context.config_version,
                    epoch,
                )
            await connection.execute(
                """
                UPDATE inbound_messages SET status='processing'
                 WHERE tenant_id=$1 AND inbound_id=$2
                """,
                context.tenant_id,
                inbound_uuid,
            )
            event_rows = await connection.fetch(
                """
                SELECT sequence,event_id,author,event_timestamp,event_json,state_delta
                  FROM session_events
                 WHERE tenant_id=$1 AND session_id=$2
                 ORDER BY sequence
                """,
                context.tenant_id,
                context.session_id,
            )
            events = tuple(
                SequencedEvent(
                    sequence=item["sequence"],
                    event_id=item["event_id"],
                    author=item["author"],
                    timestamp=item["event_timestamp"],
                    event=_json(item["event_json"]),
                    state_delta=_json(item["state_delta"]),
                )
                for item in event_rows
            )
            snapshot = SessionSnapshot(
                tenant_id=context.tenant_id,
                app_id=session["app_id"],
                session_id=context.session_id,
                principal_id=session["principal_id"],
                version=session["version"],
                next_sequence=session["next_sequence"],
                state=_json(session["state_json"]),
                events=events,
            )
            return SessionLease(
                tenant_id=context.tenant_id,
                session_id=context.session_id,
                turn_id=str(turn_id),
                inbound_id=acceptance.inbound_id,
                worker_id=worker_id,
                fencing_token=epoch,
                expires_at=expires,
                attempt=attempt,
                snapshot=snapshot,
            )

    async def renew(self, lease: SessionLease, *, lease_for: timedelta) -> SessionLease:
        async with self._tenant_transaction(lease.tenant_id) as connection:
            updated = await connection.fetchval(
                """
                UPDATE sessions
                   SET lease_expires_at=GREATEST(
                           clock_timestamp()+$5::interval,
                           lease_expires_at + interval '1 microsecond'
                       ),
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2 AND lease_owner=$3
                   AND lease_epoch=$4 AND lease_expires_at > clock_timestamp()
                RETURNING lease_expires_at
                """,
                lease.tenant_id,
                lease.session_id,
                lease.worker_id,
                lease.fencing_token,
                lease_for,
            )
        if updated is None:
            raise FencingConflict("session lease is no longer current")
        return lease.model_copy(update={"expires_at": updated})

    @observe_session_ready_lease_renewal
    async def renew_session_ready(
        self, lease: SessionLease, *, lease_for: timedelta
    ) -> SessionLease:
        if lease_for <= timedelta(0):
            raise ValueError("session lease must be positive")
        async with self._tenant_transaction(lease.tenant_id) as connection:
            updated = await connection.fetchval(
                """
                UPDATE session_mailboxes
                   SET lease_expires_at=greatest(
                           clock_timestamp()+$3::interval,
                           lease_expires_at+interval '1 microsecond'
                       ),
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                   AND status='RUNNING'
                   AND processing_inbound_id=$4::uuid
                   AND lease_owner=$5 AND lease_epoch=$6
                   AND lease_expires_at > clock_timestamp()
                 RETURNING lease_expires_at
                """,
                lease.tenant_id,
                lease.session_id,
                lease_for,
                lease.inbound_id,
                lease.worker_id,
                lease.fencing_token,
            )
            if updated is None:
                raise FencingConflict("session mailbox lease is no longer current")
            session_updated = await connection.fetchval(
                """
                UPDATE sessions
                   SET lease_expires_at=greatest(
                           clock_timestamp()+$3::interval,
                           lease_expires_at+interval '1 microsecond'
                       ),
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                   AND lease_owner=$4 AND lease_epoch=$5
                   AND lease_expires_at > clock_timestamp()
                 RETURNING lease_expires_at
                """,
                lease.tenant_id,
                lease.session_id,
                lease_for,
                lease.worker_id,
                lease.fencing_token,
            )
            if session_updated is None:
                raise FencingConflict("session lease is no longer current")
        return lease.model_copy(update={"expires_at": updated})

    @observe_session_ready_commit
    async def commit_session_ready(self, commit: TurnCommit) -> CommitResult:
        lease = commit.lease
        async with self._tenant_transaction(lease.tenant_id) as connection:
            mailbox = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if (
                mailbox is None
                or mailbox["status"] != MailboxStatus.RUNNING.value
                or mailbox["lease_owner"] != lease.worker_id
                or int(mailbox["lease_epoch"]) != lease.fencing_token
                or str(mailbox["processing_inbound_id"]) != lease.inbound_id
                or mailbox["lease_expires_at"] is None
                or mailbox["lease_expires_at"] <= server_now
            ):
                raise FencingConflict("stale worker cannot commit this session")
            sequence_number = int(mailbox["processing_sequence"])
            # Every v2 terminal transition uses this lock order:
            # session_mailboxes -> sessions -> session_turns -> mailbox item
            # -> inbound.  The mailbox serializes the v1/v2 scheduler switch;
            # taking session before turn then makes the remaining shared row
            # order deterministic for commit/retry/fail.
            session = await connection.fetchrow(
                """
                SELECT * FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            turn = await connection.fetchrow(
                """
                SELECT * FROM session_turns
                 WHERE tenant_id=$1 AND turn_id=$2::uuid
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.turn_id,
            )
            item = await connection.fetchrow(
                """
                SELECT * FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
                sequence_number,
            )
            inbound = await connection.fetchrow(
                """
                SELECT * FROM inbound_messages
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.inbound_id,
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if (
                item is None
                or inbound is None
                or turn is None
                or session is None
                or session["lease_owner"] != lease.worker_id
                or int(session["lease_epoch"]) != lease.fencing_token
                or session["lease_expires_at"] is None
                or session["lease_expires_at"] <= server_now
                or str(item["inbound_id"]) != lease.inbound_id
                or str(turn["inbound_id"]) != lease.inbound_id
                or turn["status"] != "processing"
                or int(turn["fencing_token"]) != lease.fencing_token
                or inbound["status"] != "processing"
            ):
                raise FencingConflict("stale worker cannot commit this session")

            if self._fault_stages is not None:
                await self._fault_stages.checkpoint(
                    FaultStageEvent(
                        stage=FaultStage.COMMIT_TXN_OPEN,
                        tenant_id=lease.tenant_id,
                        inbound_id=lease.inbound_id,
                        turn_id=lease.turn_id,
                        worker_id=lease.worker_id,
                        fencing_token=lease.fencing_token,
                    )
                )

            first = int(session["next_sequence"]) if commit.events else None
            sequence = int(session["next_sequence"])
            for event in commit.events:
                await connection.execute(
                    """
                    INSERT INTO session_events (
                        tenant_id,session_id,sequence,event_id,turn_id,author,
                        event_timestamp,event_json,state_delta
                    ) VALUES ($1,$2,$3,$4,$5::uuid,$6,$7,$8::jsonb,$9::jsonb)
                    """,
                    lease.tenant_id,
                    lease.session_id,
                    sequence,
                    event.event_id,
                    lease.turn_id,
                    event.author,
                    event.timestamp,
                    _dump(event.event),
                    _dump(event.state_delta),
                )
                sequence += 1

            outbound_id: str | None = None
            if commit.outbound is not None:
                outbound_id = commit.outbound.outbound_id
                await connection.execute(
                    """
                    INSERT INTO outbound_messages (
                        tenant_id,outbound_id,binding_id,session_id,channel,target_id,
                        in_reply_to,payload_json,trace_headers
                    ) VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)
                    """,
                    lease.tenant_id,
                    outbound_id,
                    commit.outbound.binding_id,
                    lease.session_id,
                    commit.outbound.channel.value,
                    commit.outbound.target_id,
                    commit.outbound.in_reply_to,
                    _dump(commit.outbound.model_dump(mode="json")),
                    _dump(commit.outbound.trace_headers),
                )
                await connection.execute(
                    """
                    INSERT INTO outbox_events (
                        tenant_id,aggregate_type,aggregate_id,event_type,
                        payload_json,trace_headers
                    ) VALUES ($1,'outbound',$2,$5,$3::jsonb,$4::jsonb)
                    """,
                    lease.tenant_id,
                    outbound_id,
                    _dump(commit.outbound.model_dump(mode="json")),
                    _dump(commit.outbound.trace_headers),
                    f"outbound.{commit.outbound.channel.value}.ready",
                )

            # Event and outbound inserts above are deliberately part of this
            # transaction, but they can take materially longer than the
            # initial fence check (large turns can contain many events).  The
            # final state write is therefore the last lease check as well as
            # the state transition.  ``clock_timestamp()`` is evaluated by
            # PostgreSQL at statement time, rather than using the stale
            # ``server_now`` captured before the writes.  A failed UPDATE
            # aborts the transaction, rolling back all event/outbound rows.
            updated_session = await connection.fetchrow(
                """
                UPDATE sessions
                   SET state_json=$3::jsonb,version=version+1,next_sequence=$4,
                       lease_owner=NULL,lease_expires_at=NULL,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                   AND lease_owner=$5 AND lease_epoch=$6
                   AND lease_expires_at > clock_timestamp()
                 RETURNING session_id
                """,
                lease.tenant_id,
                lease.session_id,
                _dump(commit.state),
                sequence,
                lease.worker_id,
                lease.fencing_token,
            )
            if updated_session is None:
                raise FencingConflict("session lease expired before commit")
            await connection.execute(
                """
                UPDATE session_turns
                   SET status='committed',committed_at=clock_timestamp(),error_type=NULL
                 WHERE tenant_id=$1 AND turn_id=$2::uuid AND status='processing'
                """,
                lease.tenant_id,
                lease.turn_id,
            )
            await connection.execute(
                """
                UPDATE inbound_messages SET status='committed'
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid AND status='processing'
                """,
                lease.tenant_id,
                lease.inbound_id,
            )
            await connection.execute(
                """
                UPDATE session_mailbox_items SET resolved_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                """,
                lease.tenant_id,
                lease.session_id,
                sequence_number,
            )
            # The initial fence timestamp may be stale after a large event
            # batch.  Re-read the database clock immediately before deciding
            # whether the next mailbox item is still in its retry window.
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            next_item = await connection.fetchrow(
                """
                SELECT inbound_id,trace_id,priority,retry_at
                  FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
                sequence_number + 1,
            )
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
            updated_mailbox = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET resolved_sequence=$3,processing_sequence=NULL,
                       processing_inbound_id=NULL,lease_owner=NULL,
                       lease_expires_at=NULL,status=$4,retry_at=$5,
                       queue_generation=queue_generation+$6,updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                   AND lease_owner=$7 AND lease_epoch=$8
                   AND lease_expires_at > clock_timestamp()
                 RETURNING *
                """,
                lease.tenant_id,
                lease.session_id,
                sequence_number,
                next_status.value,
                next_retry_at,
                generation_increment,
                lease.worker_id,
                lease.fencing_token,
            )
            if updated_mailbox is None:
                raise FencingConflict("mailbox lease expired before commit")
            if next_status == MailboxStatus.QUEUED and next_item is not None:
                await PostgresSessionMailboxStore._emit_ready_outbox(
                    connection, updated_mailbox, next_item
                )
            await connection.execute(
                """
                INSERT INTO outbox_events (
                    tenant_id,aggregate_type,aggregate_id,event_type,payload_json
                ) VALUES ($1,'turn',$2,'post_turn.ready',$3::jsonb)
                """,
                lease.tenant_id,
                lease.turn_id,
                _dump(
                    {
                        "tenant_id": lease.tenant_id,
                        "session_id": lease.session_id,
                        "turn_id": lease.turn_id,
                        "up_to_sequence": sequence - 1,
                    }
                ),
            )
            return CommitResult(
                turn_id=lease.turn_id,
                first_sequence=first,
                last_sequence=sequence - 1 if commit.events else None,
                outbound_id=outbound_id,
            )

    @observe_session_ready_retry
    async def retry_session_ready(
        self, lease: SessionLease, *, error_type: str, delay: timedelta
    ) -> None:
        if delay < timedelta(0):
            raise ValueError("retry delay must be non-negative")
        async with self._tenant_transaction(lease.tenant_id) as connection:
            mailbox = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if not self._valid_v2_mailbox(mailbox, lease, server_now):
                raise FencingConflict("stale worker cannot retry this session")
            sequence = int(mailbox["processing_sequence"])
            # Keep the same mailbox -> session -> turn order as commit and
            # fail.  The item and inbound rows are subordinate to the turn
            # and are locked only after these shared session rows.
            session = await connection.fetchrow(
                """
                SELECT lease_owner,lease_epoch,lease_expires_at FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            turn = await connection.fetchrow(
                """
                SELECT status,fencing_token FROM session_turns
                 WHERE tenant_id=$1 AND turn_id=$2::uuid FOR UPDATE
                """,
                lease.tenant_id,
                lease.turn_id,
            )
            item = await connection.fetchrow(
                """
                SELECT * FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
                sequence,
            )
            inbound = await connection.fetchrow(
                """
                SELECT status FROM inbound_messages
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid FOR UPDATE
                """,
                lease.tenant_id,
                lease.inbound_id,
            )
            if (
                item is None
                or inbound is None
                or turn is None
                or session is None
                or session["lease_owner"] != lease.worker_id
                or int(session["lease_epoch"]) != lease.fencing_token
                or session["lease_expires_at"] is None
                or session["lease_expires_at"] <= server_now
                or inbound["status"] != "processing"
                or turn["status"] != "processing"
                or int(turn["fencing_token"]) != lease.fencing_token
            ):
                raise FencingConflict("stale worker cannot retry this session")
            await connection.execute(
                """
                UPDATE session_turns SET status='failed',error_type=$3
                 WHERE tenant_id=$1 AND turn_id=$2::uuid AND status='processing'
                """,
                lease.tenant_id,
                lease.turn_id,
                error_type,
            )
            await connection.execute(
                """
                UPDATE inbound_messages SET status='accepted'
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid AND status='processing'
                """,
                lease.tenant_id,
                lease.inbound_id,
            )
            await connection.execute(
                """
                UPDATE session_mailbox_items
                   SET retry_count=retry_count+1,
                       retry_at=clock_timestamp()+$4::interval
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                """,
                lease.tenant_id,
                lease.session_id,
                sequence,
                delay,
            )
            await connection.execute(
                """
                UPDATE session_mailboxes
                   SET status='RETRY_WAIT',retry_at=clock_timestamp()+$3::interval,
                       processing_sequence=NULL,processing_inbound_id=NULL,
                       lease_owner=NULL,lease_expires_at=NULL,
                       retry_count=retry_count+1,updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                lease.tenant_id,
                lease.session_id,
                delay,
            )
            await connection.execute(
                """
                UPDATE sessions
                   SET lease_owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                   AND lease_owner=$3 AND lease_epoch=$4
                """,
                lease.tenant_id,
                lease.session_id,
                lease.worker_id,
                lease.fencing_token,
            )

    @observe_session_ready_failure
    async def fail_session_ready(self, lease: SessionLease, *, error_type: str) -> None:
        async with self._tenant_transaction(lease.tenant_id) as connection:
            mailbox = await connection.fetchrow(
                """
                SELECT * FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if not self._valid_v2_mailbox(mailbox, lease, server_now):
                raise FencingConflict("stale worker cannot fail this session")
            sequence = int(mailbox["processing_sequence"])
            # Keep the same mailbox -> session -> turn order as commit and
            # retry so a v1/v2 overlap cannot acquire shared rows in opposite
            # order.  Item and inbound rows are subordinate locks.
            session = await connection.fetchrow(
                """
                SELECT lease_owner,lease_epoch,lease_expires_at FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            turn = await connection.fetchrow(
                """
                SELECT status,fencing_token FROM session_turns
                 WHERE tenant_id=$1 AND turn_id=$2::uuid FOR UPDATE
                """,
                lease.tenant_id,
                lease.turn_id,
            )
            item = await connection.fetchrow(
                """
                SELECT * FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
                sequence,
            )
            inbound = await connection.fetchrow(
                """
                SELECT status FROM inbound_messages
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid FOR UPDATE
                """,
                lease.tenant_id,
                lease.inbound_id,
            )
            if (
                item is None
                or inbound is None
                or turn is None
                or session is None
                or session["lease_owner"] != lease.worker_id
                or int(session["lease_epoch"]) != lease.fencing_token
                or session["lease_expires_at"] is None
                or session["lease_expires_at"] <= server_now
                or inbound["status"] != "processing"
                or turn["status"] != "processing"
                or int(turn["fencing_token"]) != lease.fencing_token
            ):
                raise FencingConflict("stale worker cannot fail this session")
            await connection.execute(
                """
                UPDATE session_turns SET status='failed',error_type=$3
                 WHERE tenant_id=$1 AND turn_id=$2::uuid AND status='processing'
                """,
                lease.tenant_id,
                lease.turn_id,
                error_type,
            )
            await connection.execute(
                """
                UPDATE inbound_messages SET status='failed'
                 WHERE tenant_id=$1 AND inbound_id=$2::uuid AND status='processing'
                """,
                lease.tenant_id,
                lease.inbound_id,
            )
            await connection.execute(
                """
                UPDATE session_mailbox_items SET resolved_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                """,
                lease.tenant_id,
                lease.session_id,
                sequence,
            )
            next_item = await connection.fetchrow(
                """
                SELECT inbound_id,trace_id,priority,retry_at
                  FROM session_mailbox_items
                 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
                sequence + 1,
            )
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
            updated = await connection.fetchrow(
                """
                UPDATE session_mailboxes
                   SET resolved_sequence=$3,processing_sequence=NULL,
                       processing_inbound_id=NULL,lease_owner=NULL,
                       lease_expires_at=NULL,status=$4,retry_at=$5,
                       queue_generation=queue_generation+$6,updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2 RETURNING *
                """,
                lease.tenant_id,
                lease.session_id,
                sequence,
                next_status.value,
                next_retry_at,
                generation_increment,
            )
            assert updated is not None
            if next_status == MailboxStatus.QUEUED and next_item is not None:
                await PostgresSessionMailboxStore._emit_ready_outbox(connection, updated, next_item)
            await connection.execute(
                """
                UPDATE sessions
                   SET lease_owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                   AND lease_owner=$3 AND lease_epoch=$4
                """,
                lease.tenant_id,
                lease.session_id,
                lease.worker_id,
                lease.fencing_token,
            )

    async def _resolve_mailbox_item(
        self,
        connection: asyncpg.Connection,
        *,
        tenant_id: str,
        session_id: str,
        sequence: int,
        server_now: datetime,
        expected_lease_owner: str | None,
        expected_lease_epoch: int,
    ) -> SessionMailbox:
        """Repair a mailbox item whose durable inbound/turn already committed."""

        await connection.execute(
            """
            UPDATE session_mailbox_items SET resolved_at=coalesce(resolved_at,clock_timestamp())
             WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
            """,
            tenant_id,
            session_id,
            sequence,
        )
        await connection.execute(
            """
            -- Duplicate repair may observe a mailbox that expired while the
            -- paired session lease was renewed by another path.  Only clear
            -- the session lease represented by this mailbox fence; an
            -- unconditional update could evict a live replacement worker.
            UPDATE sessions
               SET lease_owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
             WHERE tenant_id=$1 AND session_id=$2
               AND lease_owner IS NOT DISTINCT FROM $3
               AND lease_epoch=$4
            """,
            tenant_id,
            session_id,
            expected_lease_owner,
            expected_lease_epoch,
        )
        next_item = await connection.fetchrow(
            """
            SELECT inbound_id,trace_id,priority,retry_at
              FROM session_mailbox_items
             WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
             FOR UPDATE
            """,
            tenant_id,
            session_id,
            sequence + 1,
        )
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
                   lease_expires_at=NULL,status=$4,retry_at=$5,
                   queue_generation=queue_generation+$6,updated_at=clock_timestamp()
             WHERE tenant_id=$1 AND session_id=$2 RETURNING *
            """,
            tenant_id,
            session_id,
            sequence,
            next_status.value,
            next_retry_at,
            generation_increment,
        )
        assert row is not None
        if next_status == MailboxStatus.QUEUED and next_item is not None:
            await PostgresSessionMailboxStore._emit_ready_outbox(connection, row, next_item)
        return _mailbox_from_row(row)

    @staticmethod
    def _valid_v2_mailbox(
        mailbox: Mapping[str, Any] | None,
        lease: SessionLease,
        server_now: datetime,
    ) -> bool:
        return bool(
            mailbox is not None
            and mailbox["status"] == MailboxStatus.RUNNING.value
            and mailbox["processing_sequence"] is not None
            and str(mailbox["processing_inbound_id"]) == lease.inbound_id
            and mailbox["lease_owner"] == lease.worker_id
            and int(mailbox["lease_epoch"]) == lease.fencing_token
            and mailbox["lease_expires_at"] is not None
            and mailbox["lease_expires_at"] > server_now
        )

    async def commit(self, commit: TurnCommit) -> CommitResult:
        lease = commit.lease
        async with self._tenant_transaction(lease.tenant_id) as connection:
            # v1 remains compatible with databases created before the
            # mailbox tables, but when a mailbox exists it is the first lock.
            # This is the same mailbox -> session -> turn order used by the
            # v2 terminal transitions and prevents a scheduler cutover from
            # creating a lock cycle.
            mailbox_or_session = await connection.fetchrow(
                """
                SELECT status,lease_owner,lease_epoch,lease_expires_at
                  FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            prefetched_session: Mapping[str, Any] | None = None
            legacy_prefetched = False
            if mailbox_or_session is not None and "status" not in mailbox_or_session:
                # Compatibility with old repository fakes that start with
                # the session row, and with a pre-mailbox v1 query sequence.
                prefetched_session = mailbox_or_session
                legacy_prefetched = True
            session = prefetched_session or await connection.fetchrow(
                """
                SELECT * FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            turn = await connection.fetchrow(
                """
                SELECT * FROM session_turns
                 WHERE tenant_id=$1 AND turn_id=$2
                 FOR UPDATE
                """,
                lease.tenant_id,
                UUID(lease.turn_id),
            )
            server_now = await connection.fetchval("SELECT clock_timestamp()")
            if (
                session is None
                or turn is None
                or session["lease_owner"] != lease.worker_id
                or session["lease_epoch"] != lease.fencing_token
                or session["lease_expires_at"] is None
                or session["lease_expires_at"] <= server_now
                or turn["fencing_token"] != lease.fencing_token
                or turn["status"] != "processing"
            ):
                raise FencingConflict("stale worker cannot commit this session")

            if self._fault_stages is not None:
                fault_event = FaultStageEvent(
                    stage=FaultStage.COMMIT_TXN_OPEN,
                    tenant_id=lease.tenant_id,
                    inbound_id=lease.inbound_id,
                    turn_id=lease.turn_id,
                    worker_id=lease.worker_id,
                    fencing_token=lease.fencing_token,
                )
                await self._fault_stages.checkpoint(fault_event)

            first = int(session["next_sequence"]) if commit.events else None
            sequence = int(session["next_sequence"])
            for event in commit.events:
                await connection.execute(
                    """
                    INSERT INTO session_events (
                        tenant_id,session_id,sequence,event_id,turn_id,author,
                        event_timestamp,event_json,state_delta
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)
                    """,
                    lease.tenant_id,
                    lease.session_id,
                    sequence,
                    event.event_id,
                    UUID(lease.turn_id),
                    event.author,
                    event.timestamp,
                    _dump(event.event),
                    _dump(event.state_delta),
                )
                sequence += 1

            outbound_id: str | None = None
            if commit.outbound:
                outbound_id = commit.outbound.outbound_id
                await connection.execute(
                    """
                    INSERT INTO outbound_messages (
                        tenant_id,outbound_id,binding_id,session_id,channel,target_id,
                        in_reply_to,payload_json,trace_headers
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)
                    """,
                    lease.tenant_id,
                    UUID(outbound_id),
                    commit.outbound.binding_id,
                    lease.session_id,
                    commit.outbound.channel.value,
                    commit.outbound.target_id,
                    commit.outbound.in_reply_to,
                    _dump(commit.outbound.model_dump(mode="json")),
                    _dump(commit.outbound.trace_headers),
                )
                await connection.execute(
                    """
                    INSERT INTO outbox_events (
                        tenant_id,aggregate_type,aggregate_id,event_type,payload_json,trace_headers
                    ) VALUES ($1,'outbound',$2,$5,$3::jsonb,$4::jsonb)
                    """,
                    lease.tenant_id,
                    outbound_id,
                    _dump(commit.outbound.model_dump(mode="json")),
                    _dump(commit.outbound.trace_headers),
                    f"outbound.{commit.outbound.channel.value}.ready",
                )

            updated_session = await connection.fetchrow(
                """
                UPDATE sessions
                   SET state_json=$3::jsonb, version=version+1, next_sequence=$4,
                        lease_owner=NULL, lease_expires_at=NULL, updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND session_id=$2
                   AND lease_owner=$5 AND lease_epoch=$6
                   AND lease_expires_at > clock_timestamp()
                 RETURNING session_id
                """,
                lease.tenant_id,
                lease.session_id,
                _dump(commit.state),
                sequence,
                lease.worker_id,
                lease.fencing_token,
            )
            if updated_session is None and not legacy_prefetched:
                raise FencingConflict("session lease expired or changed before commit")
            await connection.execute(
                """
                UPDATE session_turns
                   SET status='committed', committed_at=clock_timestamp()
                 WHERE tenant_id=$1 AND turn_id=$2
                """,
                lease.tenant_id,
                UUID(lease.turn_id),
            )
            await connection.execute(
                """
                UPDATE inbound_messages SET status='committed'
                 WHERE tenant_id=$1 AND inbound_id=$2
                """,
                lease.tenant_id,
                UUID(lease.inbound_id),
            )
            await connection.execute(
                """
                INSERT INTO outbox_events (
                    tenant_id,aggregate_type,aggregate_id,event_type,payload_json
                ) VALUES ($1,'turn',$2,'post_turn.ready',$3::jsonb)
                """,
                lease.tenant_id,
                lease.turn_id,
                _dump(
                    {
                        "tenant_id": lease.tenant_id,
                        "session_id": lease.session_id,
                        "turn_id": lease.turn_id,
                        "up_to_sequence": sequence - 1,
                    }
                ),
            )
            return CommitResult(
                turn_id=lease.turn_id,
                first_sequence=first,
                last_sequence=sequence - 1 if commit.events else None,
                outbound_id=outbound_id,
            )

    async def fail(self, lease: SessionLease, *, error_type: str) -> None:
        async with self._tenant_transaction(lease.tenant_id) as connection:
            # Keep legacy v1 failure on the same lock order as v1 commit and
            # v2: if the mailbox row exists, serialize on it first, then the
            # session and turn.  Missing mailbox rows are valid for legacy
            # data, so the subsequent conditional UPDATEs retain their old
            # best-effort semantics.
            await connection.fetchrow(
                """
                SELECT status,lease_owner,lease_epoch,lease_expires_at
                  FROM session_mailboxes
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            await connection.fetchrow(
                """
                SELECT lease_owner,lease_epoch,lease_expires_at
                  FROM sessions
                 WHERE tenant_id=$1 AND session_id=$2
                 FOR UPDATE
                """,
                lease.tenant_id,
                lease.session_id,
            )
            await connection.fetchrow(
                """
                SELECT status,fencing_token
                  FROM session_turns
                 WHERE tenant_id=$1 AND turn_id=$2
                 FOR UPDATE
                """,
                lease.tenant_id,
                UUID(lease.turn_id),
            )
            await connection.execute(
                """
                UPDATE session_turns
                   SET status='failed', error_type=$3
                 WHERE tenant_id=$1 AND turn_id=$2 AND fencing_token=$4
                   AND status='processing'
                """,
                lease.tenant_id,
                UUID(lease.turn_id),
                error_type,
                lease.fencing_token,
            )
            await connection.execute(
                """
                UPDATE inbound_messages SET status='accepted'
                 WHERE tenant_id=$1 AND inbound_id=$2
                   AND EXISTS (
                       SELECT 1 FROM sessions
                        WHERE tenant_id=$1 AND session_id=$3 AND lease_owner=$4
                          AND lease_epoch=$5
                   )
                """,
                lease.tenant_id,
                UUID(lease.inbound_id),
                lease.session_id,
                lease.worker_id,
                lease.fencing_token,
            )
            await connection.execute(
                """
                UPDATE sessions SET lease_owner=NULL, lease_expires_at=NULL, updated_at=now()
                 WHERE tenant_id=$1 AND session_id=$2 AND lease_owner=$3 AND lease_epoch=$4
                """,
                lease.tenant_id,
                lease.session_id,
                lease.worker_id,
                lease.fencing_token,
            )

    async def claim_outbox(
        self,
        *,
        event_type: str,
        owner_id: str,
        limit: int,
        lease_for: timedelta,
    ) -> tuple[OutboxRecord, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                "SELECT * FROM claim_outbox_events($1,$2,$3,$4)",
                event_type,
                owner_id,
                limit,
                round(lease_for.total_seconds()),
            )
        return tuple(
            OutboxRecord(
                outbox_id=str(row["outbox_id"]),
                tenant_id=row["tenant_id"],
                event_type=row["event_type"],
                aggregate_id=row["aggregate_id"],
                payload=_json(row["payload_json"]),
                trace_headers=_json(row["trace_headers"]),
                attempts=row["attempts"],
            )
            for row in rows
        )

    async def mark_outbox_published(self, tenant_id: str, outbox_id: str, *, owner_id: str) -> None:
        async with self._tenant_transaction(tenant_id) as connection:
            status = await connection.execute(
                """
                UPDATE outbox_events
                   SET published_at=now(), claimed_by=NULL, claim_expires_at=NULL
                 WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$3
                   AND published_at IS NULL
                """,
                tenant_id,
                UUID(outbox_id),
                owner_id,
            )
        if status != "UPDATE 1":
            raise FencingConflict("outbox claim is no longer current")

    async def release_outbox(
        self,
        tenant_id: str,
        outbox_id: str,
        *,
        owner_id: str,
        delay: timedelta,
        error_type: str,
    ) -> None:
        async with self._tenant_transaction(tenant_id) as connection:
            status = await connection.execute(
                """
                UPDATE outbox_events
                   SET claimed_by=NULL, claim_expires_at=NULL,
                       available_at=now()+$4::interval, last_error_type=$5
                 WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$3
                """,
                tenant_id,
                UUID(outbox_id),
                owner_id,
                delay,
                error_type,
            )
            if status != "UPDATE 1":
                raise FencingConflict("outbox claim is no longer current")

    async def dead_letter_outbox(
        self,
        record: OutboxRecord,
        *,
        owner_id: str,
        reason: str,
    ) -> None:
        """Atomically retire a claimed outbox event and retain it for review."""

        async with self._tenant_transaction(record.tenant_id) as connection:
            status = await connection.execute(
                """
                UPDATE outbox_events
                   SET published_at=now(), claimed_by=NULL, claim_expires_at=NULL,
                       last_error_type=$4
                 WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$3
                   AND published_at IS NULL
                """,
                record.tenant_id,
                UUID(record.outbox_id),
                owner_id,
                reason,
            )
            if status != "UPDATE 1":
                raise FencingConflict("outbox claim is no longer current")
            await connection.execute(
                """
                INSERT INTO dead_letters (
                    tenant_id,source_type,source_id,reason,payload_json
                ) VALUES ($1,$2,$3,$4,$5::jsonb)
                """,
                record.tenant_id,
                record.event_type,
                record.aggregate_id,
                reason,
                _dump(record.payload),
            )

    async def record_delivery(
        self, tenant_id: str, receipt: DeliveryReceipt, *, retrying: bool = False
    ) -> None:
        outbound_uuid = UUID(receipt.outbound_id)
        status = "pending" if retrying else receipt.status.value
        async with self._tenant_transaction(tenant_id) as connection:
            # Lock the parent row in the same statement that allocates the
            # sequence.  The old ``MAX(attempt_number)+1`` implementation
            # raced when two expired outbox claims were completing together.
            attempt = await connection.fetchval(
                """
                WITH locked_outbound AS (
                    SELECT outbound_id FROM outbound_messages
                     WHERE tenant_id=$1 AND outbound_id=$2
                     FOR UPDATE
                )
                SELECT coalesce(max(d.attempt_number),0)+1
                  FROM delivery_attempts AS d
                  JOIN locked_outbound AS o ON o.outbound_id=d.outbound_id
                 WHERE d.tenant_id=$1 AND d.outbound_id=$2
                """,
                tenant_id,
                outbound_uuid,
            )
            if attempt is None:
                raise LookupError("outbound message does not exist")
            await connection.execute(
                """
                INSERT INTO delivery_attempts (
                    tenant_id,outbound_id,attempt_number,status,provider_code,completed_at
                ) VALUES ($1,$2,$3,$4,$5,now())
                """,
                tenant_id,
                outbound_uuid,
                attempt,
                receipt.status.value,
                receipt.provider_code,
            )
            updated = await connection.execute(
                """
                UPDATE outbound_messages
                   SET status=$3, provider_message_id=$4, last_error_type=$5, updated_at=now()
                 WHERE tenant_id=$1 AND outbound_id=$2
                   AND status IN ('pending','sending','failed','ambiguous')
                """,
                tenant_id,
                outbound_uuid,
                status,
                receipt.provider_message_id,
                receipt.provider_code,
            )
            if updated != "UPDATE 1":
                raise FencingConflict("outbound delivery state is already terminal")
            await connection.execute(
                """
                INSERT INTO audit_logs (
                    tenant_id,decision,error_type,trace_id,redaction_applied,metadata_json
                )
                SELECT tenant_id,$3,$4::text,'delivery:'||outbound_id::text,true,
                       jsonb_build_object('outbound_id',outbound_id,'provider_code',$4::text)
                  FROM outbound_messages
                 WHERE tenant_id=$1 AND outbound_id=$2
                """,
                tenant_id,
                outbound_uuid,
                f"outbound_{receipt.status.value}",
                receipt.provider_code,
            )

    async def begin_delivery(self, record: OutboxRecord, *, owner_id: str) -> DeliveryAttempt:
        """Atomically claim a provider attempt after the outbox claim.

        A row in ``sending`` is intentionally not retried blindly: the prior
        provider request may have been accepted and its response lost.  The
        caller must route that condition to the existing ambiguous/manual
        replay path.
        """

        outbound_uuid = UUID(record.aggregate_id)
        async with self._tenant_transaction(record.tenant_id) as connection:
            outbox = await connection.fetchrow(
                """
                SELECT claimed_by,published_at FROM outbox_events
                 WHERE tenant_id=$1 AND outbox_id=$2 FOR UPDATE
                """,
                record.tenant_id,
                UUID(record.outbox_id),
            )
            if (
                outbox is None
                or outbox["claimed_by"] != owner_id
                or outbox["published_at"] is not None
            ):
                raise FencingConflict("outbox claim is no longer current")
            outbound = await connection.fetchrow(
                """
                SELECT status FROM outbound_messages
                 WHERE tenant_id=$1 AND outbound_id=$2 FOR UPDATE
                """,
                record.tenant_id,
                outbound_uuid,
            )
            if outbound is None:
                raise LookupError("outbound message does not exist")
            if outbound["status"] == "sending":
                latest_attempt = await connection.fetchval(
                    """
                    SELECT coalesce(max(attempt_number),0)
                      FROM delivery_attempts
                     WHERE tenant_id=$1 AND outbound_id=$2
                    """,
                    record.tenant_id,
                    outbound_uuid,
                )
                raise DeliveryInProgress(
                    "provider delivery is still unresolved",
                    attempt_number=int(latest_attempt or 0) or None,
                )
            if outbound["status"] == "delivered":
                raise FencingConflict("outbound message is already delivered")
            if outbound["status"] == "ambiguous":
                raise FencingConflict("ambiguous delivery requires manual replay")
            attempt = await connection.fetchval(
                """
                SELECT coalesce(max(attempt_number),0)+1
                  FROM delivery_attempts
                 WHERE tenant_id=$1 AND outbound_id=$2
                """,
                record.tenant_id,
                outbound_uuid,
            )
            await connection.execute(
                """
                INSERT INTO delivery_attempts (
                    tenant_id,outbound_id,attempt_number,status
                ) VALUES ($1,$2,$3,'sending')
                """,
                record.tenant_id,
                outbound_uuid,
                int(attempt),
            )
            updated = await connection.execute(
                """
                UPDATE outbound_messages
                   SET status='sending',last_error_type=NULL,updated_at=now()
                 WHERE tenant_id=$1 AND outbound_id=$2
                   AND status IN ('pending','failed','ambiguous')
                """,
                record.tenant_id,
                outbound_uuid,
            )
            if updated != "UPDATE 1":
                raise FencingConflict("outbound delivery state changed before begin")
        return DeliveryAttempt(
            tenant_id=record.tenant_id,
            outbound_id=record.aggregate_id,
            attempt_number=int(attempt),
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
        """Finish an attempt and terminally/retry-transition its outbox row.

        All updates are fenced by the current outbox claimant and attempt
        number.  A late response therefore cannot overwrite a newer attempt.
        """

        outbound_uuid = UUID(record.aggregate_id)
        if receipt.outbound_id != record.aggregate_id:
            raise ValueError("delivery receipt does not match outbox record")
        if retry_delay < timedelta(0):
            raise ValueError("retry delay must be non-negative")
        async with self._tenant_transaction(record.tenant_id) as connection:
            outbox = await connection.fetchrow(
                """
                SELECT claimed_by,published_at FROM outbox_events
                 WHERE tenant_id=$1 AND outbox_id=$2 FOR UPDATE
                """,
                record.tenant_id,
                UUID(record.outbox_id),
            )
            if (
                outbox is None
                or outbox["claimed_by"] != owner_id
                or outbox["published_at"] is not None
            ):
                raise FencingConflict("outbox claim is no longer current")
            attempt = await connection.fetchrow(
                """
                SELECT status FROM delivery_attempts
                 WHERE tenant_id=$1 AND outbound_id=$2 AND attempt_number=$3
                 FOR UPDATE
                """,
                record.tenant_id,
                outbound_uuid,
                attempt_number,
            )
            if attempt is None or attempt["status"] != "sending":
                raise FencingConflict("delivery attempt is no longer current")
            outbound_status = receipt.status.value
            attempt_update = await connection.execute(
                """
                UPDATE delivery_attempts
                   SET status=$4,provider_code=$5,retry_after_seconds=$6,
                       completed_at=now()
                 WHERE tenant_id=$1 AND outbound_id=$2 AND attempt_number=$3
                   AND status='sending'
                """,
                record.tenant_id,
                outbound_uuid,
                attempt_number,
                outbound_status,
                receipt.provider_code,
                receipt.retry_after_seconds,
            )
            if attempt_update != "UPDATE 1":
                raise FencingConflict("delivery attempt changed before finish")
            updated = await connection.execute(
                """
                UPDATE outbound_messages
                   SET status=$3,provider_message_id=$4,last_error_type=$5,updated_at=now()
                 WHERE tenant_id=$1 AND outbound_id=$2 AND status='sending'
                """,
                record.tenant_id,
                outbound_uuid,
                outbound_status,
                receipt.provider_message_id,
                receipt.provider_code,
            )
            if updated != "UPDATE 1":
                raise FencingConflict("outbound delivery state is no longer sending")
            if receipt.status.value == "delivered":
                outbox_update = await connection.execute(
                    """
                    UPDATE outbox_events
                       SET published_at=now(),claimed_by=NULL,claim_expires_at=NULL
                     WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$3
                       AND published_at IS NULL
                    """,
                    record.tenant_id,
                    UUID(record.outbox_id),
                    owner_id,
                )
                if outbox_update != "UPDATE 1":
                    raise FencingConflict("outbox claim was lost while finishing delivery")
            elif receipt.status.value == "failed" and receipt.retryable:
                outbox_update = await connection.execute(
                    """
                    UPDATE outbox_events
                       SET claimed_by=NULL,claim_expires_at=NULL,
                           available_at=now()+$4::interval,last_error_type=$5
                     WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$3
                       AND published_at IS NULL
                    """,
                    record.tenant_id,
                    UUID(record.outbox_id),
                    owner_id,
                    retry_delay,
                    receipt.provider_code,
                )
                if outbox_update != "UPDATE 1":
                    raise FencingConflict("outbox claim was lost while scheduling retry")
            else:
                outbox_update = await connection.execute(
                    """
                    UPDATE outbox_events
                       SET published_at=now(),claimed_by=NULL,claim_expires_at=NULL,
                           last_error_type=$3
                     WHERE tenant_id=$1 AND outbox_id=$2 AND claimed_by=$4
                       AND published_at IS NULL
                    """,
                    record.tenant_id,
                    UUID(record.outbox_id),
                    receipt.provider_code or receipt.status.value,
                    owner_id,
                )
                if outbox_update != "UPDATE 1":
                    raise FencingConflict("outbox claim was lost while closing delivery")
                await connection.execute(
                    """
                    INSERT INTO dead_letters (
                        tenant_id,source_type,source_id,reason,payload_json
                    ) VALUES ($1,'outbound',$2,$3,$4::jsonb)
                    """,
                    record.tenant_id,
                    record.aggregate_id,
                    receipt.status.value,
                    _dump(record.payload),
                )

    @staticmethod
    def _acceptance(row: Mapping[str, Any]) -> Acceptance:
        context = TenantContext(
            tenant_id=row["tenant_id"],
            app_id=row["app_id"],
            config_version=row["config_version"],
            channel_binding_id=row["binding_id"],
            principal_id=row["principal_id"],
            session_id=row["session_id"],
            request_id=row["request_id"],
            trace_id=row["trace_id"],
        )
        return Acceptance(
            inbound_id=str(row["inbound_id"]),
            context=context,
            envelope=InboundEnvelope.model_validate(_json(row["envelope_json"])),
            accepted_at=row["accepted_at"],
        )


class PostgresBindingLease:
    """Keep one fenced advisory lock per tenant-scoped WeCom connection."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._held: dict[tuple[str, str], tuple[WeComBindingLeaseGrant, asyncpg.Connection]] = {}
        self._lock = asyncio.Lock()

    async def acquire_binding(
        self, binding: ChannelBinding, owner_id: str
    ) -> WeComBindingLeaseGrant | None:
        key = (binding.tenant_id, binding.binding_id)
        owner_hash = _wecom_identifier_hash(
            "owner", binding.tenant_id, binding.binding_id, owner_id
        )
        advisory_key = _wecom_advisory_lock_key(binding.tenant_id, binding.binding_id)
        async with self._lock:
            held = self._held.get(key)
            if held is not None:
                return held[0] if held[0].owner_hash == owner_hash else None
            connection = await self._pool.acquire()
            try:
                acquired = await connection.fetchval(
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", advisory_key
                )
            except BaseException:
                await self._pool.release(connection)
                raise
            if not acquired:
                await self._pool.release(connection)
                return None
            try:
                async with connection.transaction():
                    await connection.execute(
                        "SELECT set_config('app.tenant_id', $1, true)", binding.tenant_id
                    )
                    previous = await connection.fetchrow(
                        """
                        SELECT epoch,released_at
                          FROM wecom_connection_state
                         WHERE tenant_id=$1 AND binding_id=$2
                         FOR UPDATE
                        """,
                        binding.tenant_id,
                        binding.binding_id,
                    )
                    previous_epoch = int(previous["epoch"]) if previous is not None else 0
                    event_type = (
                        "takeover"
                        if previous is not None and previous["released_at"] is None
                        else "acquired"
                    )
                    row = await connection.fetchrow(
                        """
                        INSERT INTO wecom_connection_state (
                            tenant_id,binding_id,owner_hash,epoch,phase,acquired_at,
                            authenticated_at,disconnected_at,released_at,
                            last_provider_event_hash,last_provider_event_at,updated_at
                        ) VALUES (
                            $1,$2,$3,$4,'acquired',clock_timestamp(),
                            NULL,NULL,NULL,NULL,NULL,clock_timestamp()
                        )
                        ON CONFLICT (tenant_id,binding_id) DO UPDATE
                           SET owner_hash=EXCLUDED.owner_hash,
                               epoch=EXCLUDED.epoch,
                               phase='acquired',
                               acquired_at=EXCLUDED.acquired_at,
                               authenticated_at=NULL,
                               disconnected_at=NULL,
                               released_at=NULL,
                               last_provider_event_hash=NULL,
                               last_provider_event_at=NULL,
                               updated_at=EXCLUDED.updated_at
                         WHERE wecom_connection_state.epoch=$5
                        RETURNING epoch,acquired_at
                        """,
                        binding.tenant_id,
                        binding.binding_id,
                        owner_hash,
                        previous_epoch + 1,
                        previous_epoch,
                    )
                    if row is None:
                        raise RuntimeError("WeCom connection epoch changed during acquisition")
                    grant = WeComBindingLeaseGrant(
                        tenant_id=binding.tenant_id,
                        binding_id=binding.binding_id,
                        owner_hash=owner_hash,
                        epoch=int(row["epoch"]),
                        acquired_at=row["acquired_at"],
                    )
                    await connection.execute(
                        """
                        INSERT INTO im_acceptance_evidence_events (
                            tenant_id,binding_id,channel,connection_epoch,event_type,
                            owner_hash
                        ) VALUES ($1,$2,'wecom_ai_bot',$3,$4,$5)
                        """,
                        grant.tenant_id,
                        grant.binding_id,
                        grant.epoch,
                        event_type,
                        grant.owner_hash,
                    )
            except BaseException:
                try:
                    await connection.execute(
                        "SELECT pg_advisory_unlock(hashtextextended($1, 0))", advisory_key
                    )
                finally:
                    await self._pool.release(connection)
                raise
            self._held[key] = (grant, connection)
            return grant

    async def mark_authenticated(self, grant: WeComBindingLeaseGrant) -> bool:
        return await self._record_fenced_event(grant, "authenticated")

    async def record_provider_event(
        self, grant: WeComBindingLeaseGrant, provider_event_id: str
    ) -> bool:
        provider_hash = _wecom_identifier_hash(
            "provider-event",
            grant.tenant_id,
            grant.binding_id,
            provider_event_id,
        )
        return await self._record_fenced_event(
            grant,
            "provider_event",
            provider_event_hash=provider_hash,
        )

    async def mark_disconnected(self, grant: WeComBindingLeaseGrant) -> bool:
        return await self._record_fenced_event(grant, "disconnected")

    async def release_binding(self, grant: WeComBindingLeaseGrant) -> None:
        key = (grant.tenant_id, grant.binding_id)
        async with self._lock:
            held = self._held.get(key)
            if held is None or held[0] != grant:
                return
            _, connection = self._held.pop(key)
            failure: BaseException | None = None
            try:
                await self._record_fenced_event_on_connection(connection, grant, "released")
            except BaseException as error:
                failure = error
            advisory_key = _wecom_advisory_lock_key(grant.tenant_id, grant.binding_id)
            try:
                await connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended($1, 0))", advisory_key
                )
            except BaseException as error:
                if failure is None:
                    failure = error
            finally:
                await self._pool.release(connection)
            if failure is not None:
                raise failure

    async def _record_fenced_event(
        self,
        grant: WeComBindingLeaseGrant,
        event_type: str,
        *,
        provider_event_hash: str | None = None,
    ) -> bool:
        key = (grant.tenant_id, grant.binding_id)
        async with self._lock:
            held = self._held.get(key)
            if held is None or held[0] != grant:
                return False
            return await self._record_fenced_event_on_connection(
                held[1],
                grant,
                event_type,
                provider_event_hash=provider_event_hash,
            )

    async def _record_fenced_event_on_connection(
        self,
        connection: asyncpg.Connection,
        grant: WeComBindingLeaseGrant,
        event_type: str,
        *,
        provider_event_hash: str | None = None,
    ) -> bool:
        async with connection.transaction():
            await connection.execute(
                "SELECT set_config('app.tenant_id', $1, true)", grant.tenant_id
            )
            row = await connection.fetchrow(
                """
                WITH fenced AS (
                    UPDATE wecom_connection_state
                       SET phase=CASE
                               WHEN $6='provider_event' THEN phase
                               ELSE $6
                           END,
                           authenticated_at=CASE
                               WHEN $6='authenticated' THEN
                                   COALESCE(authenticated_at,clock_timestamp())
                               ELSE authenticated_at
                           END,
                           disconnected_at=CASE
                               WHEN $6='disconnected' THEN
                                   COALESCE(disconnected_at,clock_timestamp())
                               ELSE disconnected_at
                           END,
                           released_at=CASE
                               WHEN $6='released' THEN
                                   COALESCE(released_at,clock_timestamp())
                               ELSE released_at
                           END,
                           last_provider_event_hash=CASE
                               WHEN $6='provider_event' THEN $5
                               ELSE last_provider_event_hash
                           END,
                           last_provider_event_at=CASE
                               WHEN $6='provider_event' THEN clock_timestamp()
                               ELSE last_provider_event_at
                           END,
                           updated_at=clock_timestamp()
                     WHERE tenant_id=$1 AND binding_id=$2
                       AND epoch=$3 AND owner_hash=$4
                       AND released_at IS NULL
                       AND (
                           ($6='authenticated' AND phase='acquired')
                           OR ($6='provider_event'
                               AND phase IN ('acquired','authenticated'))
                           OR ($6='disconnected'
                               AND phase IN ('acquired','authenticated'))
                           OR $6='released'
                       )
                    RETURNING tenant_id,binding_id,epoch,owner_hash
                )
                INSERT INTO im_acceptance_evidence_events (
                    tenant_id,binding_id,channel,connection_epoch,event_type,
                    owner_hash,provider_event_hash
                )
                SELECT tenant_id,binding_id,'wecom_ai_bot',epoch,$6,owner_hash,$5
                  FROM fenced
                RETURNING event_id
                """,
                grant.tenant_id,
                grant.binding_id,
                grant.epoch,
                grant.owner_hash,
                provider_event_hash,
                event_type,
            )
        return row is not None


def _wecom_identifier_hash(domain: str, tenant_id: str, binding_id: str, value: str) -> str:
    material = "\x00".join(("trpc-wecom-evidence-v1", domain, tenant_id, binding_id, value))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _wecom_advisory_lock_key(tenant_id: str, binding_id: str) -> str:
    """Return a PostgreSQL TEXT-safe identity for the binding lease lock."""

    return _wecom_identifier_hash("binding-lease-lock", tenant_id, binding_id, "")


__all__ = ["PostgresBindingLease", "PostgresRuntimeRepository"]

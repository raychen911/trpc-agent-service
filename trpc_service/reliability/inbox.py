"""Durable inbound message idempotency."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import aliased

from trpc_service.persistence import Database
from trpc_service.persistence.models import InboundMessageRow
from trpc_service.persistence.models import IMEventReceiptRow
from trpc_service.persistence.models import TenantConfigVersionRow
from trpc_service.persistence.models import TenantRow
from trpc_service.persistence.models import utcnow
from trpc_service.tenant import ResourcePolicy, TenantConfig
from trpc_service.telemetry.runtime import count


class DuplicatePayloadError(ValueError):
    pass


class MessageRecalledError(RuntimeError):
    pass


class InboxDisposition(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"


@dataclass(frozen=True)
class InboxReceipt:
    disposition: InboxDisposition
    inbound_message_id: str
    execution_id: str
    status: str


@dataclass(frozen=True)
class InboxWorkItem:
    inbound_message_id: str
    execution_id: str
    tenant_id: str
    partition_key: str
    payload: dict
    config_version: int
    storage_revision: int
    attempt: int
    request_id: str
    trace_id: str


def payload_hash(payload: dict) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class InboxRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    @property
    def database(self):
        return self._database

    def is_recalled(self, inbound_message_id: str) -> bool:
        with self._database.sessions() as session:
            row = session.get(InboundMessageRow, inbound_message_id)
            return row is not None and row.error_type == "message_recalled"

    def require_active(self, work, worker_id: str) -> None:
        with self._database.sessions() as session:
            row = session.get(InboundMessageRow, work.inbound_message_id)
            if row is not None and row.error_type == "message_recalled":
                raise MessageRecalledError("inbound message was recalled")
            if row is None or row.status != "processing" or row.lease_owner != worker_id or row.attempt != work.attempt:
                raise RuntimeError("inbound execution is no longer active")

    def accept(self, **values) -> InboxReceipt:
        with self._database.sessions.begin() as session:
            return self.accept_in_session(session, **values)

    def accept_in_session(self, session, **values) -> InboxReceipt:
        """Join the ingress transaction so receipt, reply context and inbox commit together."""
        payload = values.pop("payload")
        digest = payload_hash(values.pop("identity_payload", payload))
        key = (values["tenant_id"], values["channel_binding_id"], values["external_message_id"])
        inbound_id = uuid.uuid4().hex
        execution_id = uuid.uuid4().hex
        from trpc_service.persistence.models import TenantMaintenanceRow
        from trpc_service.management import ConfigConflict
        # Lock the tenant row so admission decisions are serialized across workers.
        session.get(TenantRow, key[0], with_for_update=True)
        maintenance = session.get(TenantMaintenanceRow, key[0])
        if maintenance and maintenance.enabled:
            raise ConfigConflict("tenant is in maintenance")
        # Idempotent retries must not consume a rate or queue slot.
        existing = session.scalar(select(InboundMessageRow).where(
            InboundMessageRow.tenant_id == key[0],
            InboundMessageRow.channel_binding_id == key[1],
            InboundMessageRow.external_message_id == key[2]))
        if existing is not None:
            if existing.payload_hash != digest:
                raise DuplicatePayloadError("external message id was reused with a different payload")
            return InboxReceipt(InboxDisposition.DUPLICATE, existing.inbound_message_id,
                                existing.execution_id, existing.status)
        policy = self._resource_policy(session, key[0])
        minute_ago = utcnow() - timedelta(minutes=1)
        recent = session.scalar(select(func.count()).select_from(InboundMessageRow).where(
            InboundMessageRow.tenant_id == key[0], InboundMessageRow.created_at >= minute_ago)) or 0
        if recent >= policy.requests_per_minute:
            count("tenant.admission.rejected", reason="rate_limit")
            raise ConfigConflict("tenant request rate limit exceeded")
        queued = session.scalar(select(func.count()).select_from(InboundMessageRow).where(
            InboundMessageRow.tenant_id == key[0],
            InboundMessageRow.status.in_(("pending", "retry", "processing")))) or 0
        if queued >= policy.max_queued_messages:
            count("tenant.admission.rejected", reason="queue_limit")
            raise ConfigConflict("tenant queue limit exceeded")
        row = InboundMessageRow(
            inbound_message_id=inbound_id, execution_id=execution_id,
            payload_hash=digest, payload_json=payload, status="pending", **values)
        try:
            with session.begin_nested():
                session.add(row)
                session.flush()
            count("tenant.admission.accepted")
            return InboxReceipt(InboxDisposition.ACCEPTED, inbound_id, execution_id, row.status)
        except IntegrityError:
            existing = session.scalar(select(InboundMessageRow).where(
                InboundMessageRow.tenant_id == key[0],
                InboundMessageRow.channel_binding_id == key[1],
                InboundMessageRow.external_message_id == key[2]))
            if existing is None:
                raise
            if existing.payload_hash != digest:
                raise DuplicatePayloadError("external message id was reused with a different payload")
            return InboxReceipt(InboxDisposition.DUPLICATE, existing.inbound_message_id,
                                existing.execution_id, existing.status)

    @staticmethod
    def _resource_policy(session, tenant_id: str) -> ResourcePolicy:
        tenant = session.get(TenantRow, tenant_id)
        if tenant is None:
            return ResourcePolicy()
        config = session.scalar(select(TenantConfigVersionRow).where(
            TenantConfigVersionRow.tenant_id == tenant_id,
            TenantConfigVersionRow.config_version == tenant.active_config_version))
        if config is None:
            return ResourcePolicy()
        try:
            return TenantConfig.model_validate(config.config_json).resource_policy
        except Exception:
            # An old or operator-created snapshot without the optional policy
            # keeps the safe defaults and remains processable.
            return ResourcePolicy()

    def claim(self, *, worker_id: str, lease_seconds: float, include_im: bool = False) -> InboxWorkItem | None:
        now = utcnow()
        earlier = aliased(InboundMessageRow)
        with self._database.sessions.begin() as session:
            candidates = session.scalars(
                select(InboundMessageRow).where(
                    True if include_im else ~select(IMEventReceiptRow.external_event_id).where(
                        IMEventReceiptRow.inbound_id == InboundMessageRow.inbound_message_id).exists(),
                    or_(
                        InboundMessageRow.status.in_(("pending", "retry")),
                        (InboundMessageRow.status == "processing") & (InboundMessageRow.lease_expires_at < now),
                    ),
                    or_(InboundMessageRow.next_retry_at.is_(None), InboundMessageRow.next_retry_at <= now),
                    ~select(earlier.inbound_message_id).where(
                        earlier.tenant_id == InboundMessageRow.tenant_id,
                        earlier.partition_key == InboundMessageRow.partition_key,
                        earlier.status.in_(("pending", "retry", "processing")),
                        or_(earlier.created_at < InboundMessageRow.created_at,
                            (earlier.created_at == InboundMessageRow.created_at) &
                            (earlier.inbound_message_id < InboundMessageRow.inbound_message_id)),
                    ).exists(),
                ).order_by(InboundMessageRow.created_at).limit(64).with_for_update(skip_locked=True)).all()
            selected = None
            selected_key = None
            for candidate in candidates:
                session.get(TenantRow, candidate.tenant_id, with_for_update=True)
                policy = self._resource_policy(session, candidate.tenant_id)
                active = session.scalar(select(func.count()).select_from(InboundMessageRow).where(
                    InboundMessageRow.tenant_id == candidate.tenant_id,
                    InboundMessageRow.status == "processing",
                    InboundMessageRow.lease_expires_at > now)) or 0
                if active >= policy.max_concurrent_runs:
                    count("tenant.admission.throttled", reason="concurrency")
                    continue
                queued = session.scalar(select(func.count()).select_from(InboundMessageRow).where(
                    InboundMessageRow.tenant_id == candidate.tenant_id,
                    InboundMessageRow.status.in_(("pending", "retry", "processing")))) or 0
                if queued > policy.max_queued_messages:
                    count("tenant.admission.throttled", reason="queue_limit")
                    continue
                key = (active, -policy.priority, candidate.created_at, candidate.inbound_message_id)
                if selected is None or key < selected_key:
                    selected, selected_key = candidate, key
            row = selected
            if row is None:
                return None
            row.status = "processing"
            row.lease_owner = worker_id
            row.lease_expires_at = now + timedelta(seconds=lease_seconds)
            row.attempt += 1
            count("tenant.admission.claimed")
            return InboxWorkItem(
                row.inbound_message_id,
                row.execution_id,
                row.tenant_id,
                row.partition_key,
                dict(row.payload_json),
                row.config_version,
                row.storage_revision,
                row.attempt,
                row.request_id,
                row.trace_id,
            )

    def mark_succeeded(self, inbound_message_id: str, worker_id: str) -> None:
        self.mark_terminal(inbound_message_id, worker_id, "succeeded")

    def renew(self, work: InboxWorkItem, worker_id: str, lease_seconds: float) -> bool:
        now = utcnow()
        with self._database.sessions.begin() as session:
            result = session.execute(update(InboundMessageRow).where(
                InboundMessageRow.inbound_message_id == work.inbound_message_id,
                InboundMessageRow.lease_owner == worker_id,
                InboundMessageRow.attempt == work.attempt,
                InboundMessageRow.status == "processing",
                InboundMessageRow.lease_expires_at > now,
            ).values(lease_expires_at=now + timedelta(seconds=lease_seconds)))
            if result.rowcount != 1:
                row = session.get(InboundMessageRow, work.inbound_message_id)
                if row is not None and row.attempt == work.attempt and row.status in {
                        "succeeded", "failed_final", "unknown_outcome"}:
                    return False
                raise RuntimeError("inbound lease lost")
            return True

    def mark_terminal(self, inbound_message_id: str, worker_id: str, status: str,
                      error_type: str | None = None) -> None:
        if status not in {"succeeded", "failed_final", "unknown_outcome"}:
            raise ValueError("invalid inbound terminal status")
        with self._database.sessions.begin() as session:
            row = session.get(InboundMessageRow, inbound_message_id, with_for_update=True)
            if row is None:
                raise LookupError("inbound message not found")
            if row.lease_owner != worker_id or row.status != "processing":
                raise RuntimeError("inbound message is not owned by this worker")
            from .execution import timezone_utc

            if row.lease_expires_at.replace(tzinfo=timezone_utc(row.lease_expires_at)) <= utcnow():
                raise RuntimeError("inbound lease expired")
            row.status = status
            row.error_type = error_type
            row.next_retry_at = None
            row.lease_owner = None
            row.lease_expires_at = None

    def retry(self, inbound_message_id: str, worker_id: str, error_type: str, delay_seconds: float,
              *, count_attempt: bool = True) -> None:
        with self._database.sessions.begin() as session:
            row = session.get(InboundMessageRow, inbound_message_id, with_for_update=True)
            if row is None or row.lease_owner != worker_id or row.status != "processing":
                raise RuntimeError("inbound message is not owned by this worker")
            row.status = "retry"
            if not count_attempt:
                row.attempt -= 1
            row.error_type = error_type
            row.next_retry_at = utcnow() + timedelta(seconds=delay_seconds)
            row.lease_owner = None
            row.lease_expires_at = None

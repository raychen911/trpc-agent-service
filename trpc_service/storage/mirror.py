"""Tenant-scoped dual-read/dual-write storage used during backend cutovers."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from trpc_service.storage.base import (
    AuditRecord,
    MemoryItem,
    SessionEvent,
    Summary,
    ToolExecution,
)
from trpc_service.storage.durable import OutboxRecord
from trpc_service.storage.factory import StorageBundle
from trpc_service.storage.locking import SessionLease
from trpc_service.storage.mailbox import MailboxRecord
from trpc_service.storage.session_mailbox import SessionMailboxLease


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _enqueue(callback, operation: str, payload: dict[str, Any]) -> None:
    callback(operation, _jsonable(payload))


class _MirrorSession:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue

    def acquire_session_lock(self, tenant_id: str, session_id: str, timeout: float):
        acquire = getattr(self.primary, "acquire_session_lock", None)
        if acquire is None:
            raise AttributeError("primary session backend has no distributed lock")
        return acquire(tenant_id, session_id, timeout)

    def release_session_lock(self, tenant_id: str, session_id: str, token: str):
        release = getattr(self.primary, "release_session_lock", None)
        if release is None:
            raise AttributeError("primary session backend has no distributed lock")
        release(tenant_id, session_id, token)

    def acquire_session_lease(self, tenant_id: str, session_id: str, timeout: float):
        acquire = getattr(self.primary, "acquire_session_lease", None)
        if acquire is None:
            raise AttributeError("primary session backend has no fencing lease")
        return acquire(tenant_id, session_id, timeout)

    def release_session_lease(self, tenant_id: str, session_id: str, lease: SessionLease):
        release = getattr(self.primary, "release_session_lease", None)
        if release is None:
            raise AttributeError("primary session backend has no fencing lease")
        release(tenant_id, session_id, lease)

    def renew_session_lease(self, lease: SessionLease):
        renew = getattr(self.primary, "renew_session_lease", None)
        if renew is None:
            raise AttributeError("primary session backend has no fencing lease")
        return renew(lease)

    def validate_session_lease(self, lease: SessionLease) -> None:
        validate = getattr(self.primary, "validate_session_lease", None)
        if validate is not None:
            validate(lease)

    def append_event(self, event: SessionEvent, fencing_token: int | None = None) -> int:
        seq = self.primary.append_event(event, fencing_token=fencing_token)
        try:
            secondary_seq = self.secondary.append_event(event)
        except Exception:
            _enqueue(self._enqueue, "mirror.session.append_event", {"event": asdict(event)})
            raise
        if secondary_seq != seq:
            raise RuntimeError("dual-write session sequence mismatch")
        return seq

    def load_events(self, tenant_id: str, session_id: str, after_seq: int = 0):
        try:
            events = self.primary.load_events(tenant_id, session_id, after_seq)
            return events or self.secondary.load_events(tenant_id, session_id, after_seq)
        except Exception:
            return self.secondary.load_events(tenant_id, session_id, after_seq)

    def load_state(self, tenant_id: str, session_id: str):
        try:
            state = self.primary.load_state(tenant_id, session_id)
            if state.state_version == 0 and state.latest_event_seq == 0:
                secondary = self.secondary.load_state(tenant_id, session_id)
                return secondary if secondary.state_version or secondary.latest_event_seq else state
            return state
        except Exception:
            return self.secondary.load_state(tenant_id, session_id)

    def compare_and_set_state(
        self,
        tenant_id,
        session_id,
        expected_version,
        state,
        fencing_token: int | None = None,
    ):
        updated = self.primary.compare_and_set_state(
            tenant_id,
            session_id,
            expected_version,
            state,
            fencing_token=fencing_token,
        )
        if updated:
            try:
                mirrored = self.secondary.compare_and_set_state(tenant_id, session_id, expected_version, state)
            except Exception:
                _enqueue(
                    self._enqueue,
                    "mirror.session.compare_and_set",
                    {
                        "tenant_id": tenant_id,
                        "session_id": session_id,
                        "expected_version": expected_version,
                        "state": state,
                    },
                )
                raise
            if not mirrored:
                current = self.secondary.load_state(tenant_id, session_id)
                if current.state != state:
                    _enqueue(
                        self._enqueue,
                        "mirror.session.compare_and_set",
                        {
                            "tenant_id": tenant_id,
                            "session_id": session_id,
                            "expected_version": expected_version,
                            "state": state,
                        },
                    )
                    raise RuntimeError("dual-write session state mismatch")
        return updated


class _MirrorMemory:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue

    def put(self, item: MemoryItem) -> None:
        self.primary.put(item)
        try:
            self.secondary.put(item)
        except Exception:
            _enqueue(self._enqueue, "mirror.memory.put", {"item": asdict(item)})
            raise

    def search(self, tenant_id, query, limit=5, scope_keys=None):
        try:
            items = self.primary.search(tenant_id, query, limit, scope_keys)
            return items or self.secondary.search(tenant_id, query, limit, scope_keys)
        except Exception:
            return self.secondary.search(tenant_id, query, limit, scope_keys)


class _MirrorSummary:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue

    def put(self, summary: Summary) -> None:
        self.primary.put(summary)
        try:
            self.secondary.put(summary)
        except Exception:
            _enqueue(self._enqueue, "mirror.summary.put", {"summary": asdict(summary)})
            raise

    def latest(self, tenant_id, session_id):
        try:
            summary = self.primary.latest(tenant_id, session_id)
            return summary or self.secondary.latest(tenant_id, session_id)
        except Exception:
            return self.secondary.latest(tenant_id, session_id)


class _MirrorAudit:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue

    def append(self, record: AuditRecord) -> None:
        self.primary.append(record)
        try:
            self.secondary.append(record)
        except Exception:
            _enqueue(self._enqueue, "mirror.audit.append", {"record": asdict(record)})
            raise

    def list_by_tenant(self, tenant_id, limit=100):
        try:
            records = self.primary.list_by_tenant(tenant_id, limit)
            return records or self.secondary.list_by_tenant(tenant_id, limit)
        except Exception:
            return self.secondary.list_by_tenant(tenant_id, limit)


class _MirrorIdempotency:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue

    def start(self, tenant_id, key, trace_id, lease_seconds=180):
        record = self.primary.start(tenant_id, key, trace_id, lease_seconds)
        try:
            self.secondary.start(tenant_id, key, trace_id, lease_seconds)
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.idempotency.start",
                {
                    "tenant_id": tenant_id,
                    "key": key,
                    "trace_id": trace_id,
                    "lease_seconds": lease_seconds,
                },
            )
            raise
        return record

    def complete(self, tenant_id, key, response_ref, result):
        record = self.primary.complete(tenant_id, key, response_ref, result)
        try:
            self.secondary.complete(tenant_id, key, response_ref, result)
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.idempotency.complete",
                {
                    "tenant_id": tenant_id,
                    "key": key,
                    "response_ref": response_ref,
                    "result": result,
                },
            )
            raise
        return record

    def fail(self, tenant_id, key, error_type):
        record = self.primary.fail(tenant_id, key, error_type)
        try:
            self.secondary.fail(tenant_id, key, error_type)
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.idempotency.fail",
                {"tenant_id": tenant_id, "key": key, "error_type": error_type},
            )
            raise
        return record

    def get(self, tenant_id, key):
        try:
            record = self.primary.get(tenant_id, key)
            return record or self.secondary.get(tenant_id, key)
        except Exception:
            return self.secondary.get(tenant_id, key)

    def claim_delivery(self, tenant_id, key):
        claimed = self.primary.claim_delivery(tenant_id, key)
        if claimed:
            try:
                self.secondary.claim_delivery(tenant_id, key)
            except Exception:
                _enqueue(
                    self._enqueue,
                    "mirror.idempotency.claim_delivery",
                    {"tenant_id": tenant_id, "key": key},
                )
                raise
        return claimed

    def release_delivery(self, tenant_id, key):
        self.primary.release_delivery(tenant_id, key)
        try:
            self.secondary.release_delivery(tenant_id, key)
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.idempotency.release_delivery",
                {"tenant_id": tenant_id, "key": key},
            )
            raise


class _MirrorMailbox:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue
        self._secondary_claims: dict[tuple[str, str, int], Any] = {}

    def enqueue(self, tenant_id, session_id, message_id, dedupe_key, payload):
        record = self.primary.enqueue(tenant_id, session_id, message_id, dedupe_key, payload)
        try:
            mirrored = self.secondary.enqueue(tenant_id, session_id, message_id, dedupe_key, payload)
            if mirrored.sequence != record.sequence:
                raise RuntimeError("dual-write mailbox sequence mismatch")
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.mailbox.enqueue",
                {
                    "tenant_id": tenant_id,
                    "session_id": session_id,
                    "message_id": message_id,
                    "dedupe_key": dedupe_key,
                    "payload": payload,
                },
            )
            raise
        return record

    def get(self, tenant_id, dedupe_key):
        try:
            return self.primary.get(tenant_id, dedupe_key) or self.secondary.get(tenant_id, dedupe_key)
        except Exception:
            return self.secondary.get(tenant_id, dedupe_key)

    def claim_next(self, tenant_id, session_id, owner, lease_seconds=None):
        record = self.primary.claim_next(tenant_id, session_id, owner, lease_seconds)
        if record is None:
            return None
        try:
            mirrored = self.secondary.claim_next(tenant_id, session_id, owner, lease_seconds)
            if mirrored is None or mirrored.sequence != record.sequence:
                raise RuntimeError("dual-write mailbox claim mismatch")
            self._secondary_claims[(tenant_id, session_id, record.sequence)] = mirrored
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.mailbox.claim_next",
                {
                    "tenant_id": tenant_id,
                    "session_id": session_id,
                    "owner": owner,
                    "lease_seconds": lease_seconds,
                },
            )
            raise
        return record

    def renew(self, record, lease_seconds=None):
        renewed = self.primary.renew(record, lease_seconds)
        secondary = self._secondary_claims.get((record.tenant_id, record.session_id, record.sequence))
        if secondary is not None:
            try:
                self.secondary.renew(secondary, lease_seconds)
            except Exception:
                _enqueue(
                    self._enqueue,
                    "mirror.mailbox.renew",
                    {"record": asdict(record), "lease_seconds": lease_seconds},
                )
                raise
        return renewed

    def complete(self, record):
        self.primary.complete(record)
        secondary = self._secondary_claims.pop((record.tenant_id, record.session_id, record.sequence), None)
        if secondary is not None:
            try:
                self.secondary.complete(secondary)
            except Exception:
                _enqueue(self._enqueue, "mirror.mailbox.complete", {"record": asdict(record)})
                raise

    def fail(self, record, error, retry_after_seconds=0):
        self.primary.fail(record, error, retry_after_seconds)
        secondary = self._secondary_claims.pop((record.tenant_id, record.session_id, record.sequence), None)
        if secondary is not None:
            try:
                self.secondary.fail(secondary, error, retry_after_seconds)
            except Exception:
                _enqueue(
                    self._enqueue,
                    "mirror.mailbox.fail",
                    {
                        "record": asdict(record),
                        "error": str(error),
                        "retry_after_seconds": retry_after_seconds,
                    },
                )
                raise

    def recover_expired(self, tenant_id=None, session_id=None):
        recovered = self.primary.recover_expired(tenant_id, session_id)
        try:
            self.secondary.recover_expired(tenant_id, session_id)
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.mailbox.recover_expired",
                {"tenant_id": tenant_id, "session_id": session_id},
            )
            raise
        return recovered


class _MirrorSessionMailboxV2:
    """Dual-write adapter for the session aggregate mailbox.

    The primary and secondary stores each issue their own lease epoch.  The
    mapping is kept locally so completion and retry can fence both stores with
    the lease returned by that store.
    """

    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue
        self._secondary_claims: dict[tuple[str, str, int], SessionMailboxLease] = {}

    def get(self, tenant_id, session_id):
        try:
            return self.primary.get(tenant_id, session_id) or self.secondary.get(
                tenant_id, session_id
            )
        except Exception:
            return self.secondary.get(tenant_id, session_id)

    def export_by_tenant(self, tenant_id):
        try:
            return self.primary.export_by_tenant(tenant_id)
        except Exception:
            return self.secondary.export_by_tenant(tenant_id)

    def restore_export(self, payload):
        result = self.primary.restore_export(payload)
        try:
            self.secondary.restore_export(payload)
        except Exception:
            self._record_failure("restore_export", (), {"payload": payload})
            raise
        return result

    def has_unresolved_message(self, tenant_id, session_id, message_id):
        primary = self.primary.has_unresolved_message(
            tenant_id, session_id, message_id
        )
        secondary = self.secondary.has_unresolved_message(
            tenant_id, session_id, message_id
        )
        if primary != secondary:
            raise RuntimeError("dual-write session mailbox message state mismatch")
        return primary

    def accept(self, *args, **kwargs):
        result = self.primary.accept(*args, **kwargs)
        try:
            mirrored = self.secondary.accept(*args, **kwargs)
            if (
                mirrored.accepted_sequence != result.accepted_sequence
                or mirrored.queue_generation != result.queue_generation
            ):
                raise RuntimeError("dual-write session mailbox accept mismatch")
        except Exception:
            self._record_failure("accept", args, kwargs)
            raise
        return result

    def claim(self, tenant_id, session_id, owner, lease_seconds):
        lease = self.primary.claim(tenant_id, session_id, owner, lease_seconds)
        try:
            mirrored = self.secondary.claim(tenant_id, session_id, owner, lease_seconds)
            if (lease is None) != (mirrored is None):
                raise RuntimeError("dual-write session mailbox claim mismatch")
            if lease is not None:
                if (
                    mirrored.sequence != lease.sequence
                    or mirrored.message_id != lease.message_id
                ):
                    raise RuntimeError("dual-write session mailbox lease mismatch")
                self._secondary_claims[
                    (tenant_id, session_id, lease.sequence)
                ] = mirrored
        except Exception:
            self._record_failure(
                "claim",
                (tenant_id, session_id, owner, lease_seconds),
                {},
            )
            raise
        return lease

    def claim_session(self, *args, **kwargs):
        result = self.primary.claim_session(*args, **kwargs)
        try:
            mirrored = self.secondary.claim_session(*args, **kwargs)
            if result.status != mirrored.status:
                raise RuntimeError("dual-write session mailbox claim status mismatch")
            if result.claimed:
                if (
                    result.lease is None
                    or mirrored.lease is None
                    or result.lease.sequence != mirrored.lease.sequence
                    or result.lease.message_id != mirrored.lease.message_id
                ):
                    raise RuntimeError("dual-write session mailbox claim mismatch")
                self._secondary_claims[
                    (
                        result.lease.tenant_id,
                        result.lease.session_id,
                        result.lease.sequence,
                    )
                ] = mirrored.lease
        except Exception:
            self._record_failure("claim_session", args, kwargs)
            raise
        return result

    def renew(self, lease, lease_seconds):
        renewed = self.primary.renew(lease, lease_seconds)
        secondary = self._secondary_claims.get(
            (lease.tenant_id, lease.session_id, lease.sequence)
        )
        if secondary is None:
            secondary = lease
        try:
            self.secondary.renew(secondary, lease_seconds)
        except Exception:
            self._record_failure(
                "renew",
                (),
                {"lease": asdict(lease), "lease_seconds": lease_seconds},
            )
            raise
        return renewed

    def commit(self, lease):
        result = self.primary.commit(lease)
        secondary = self._secondary_claims.pop(
            (lease.tenant_id, lease.session_id, lease.sequence), None
        ) or lease
        try:
            self.secondary.commit(secondary)
        except Exception:
            self._record_failure("commit", (), {"lease": asdict(lease)})
            raise
        return result

    def retry(self, lease, **kwargs):
        result = self.primary.retry(lease, **kwargs)
        secondary = self._secondary_claims.pop(
            (lease.tenant_id, lease.session_id, lease.sequence), None
        ) or lease
        try:
            self.secondary.retry(secondary, **kwargs)
        except Exception:
            payload = {"lease": asdict(lease), **kwargs}
            self._record_failure("retry", (), payload)
            raise
        return result

    def dead_letter(self, lease, error):
        result = self.primary.dead_letter(lease, error)
        secondary = self._secondary_claims.pop(
            (lease.tenant_id, lease.session_id, lease.sequence), None
        ) or lease
        try:
            self.secondary.dead_letter(secondary, error)
        except Exception:
            self._record_failure(
                "dead_letter",
                (),
                {"lease": asdict(lease), "error": error},
            )
            raise
        return result

    def recover(self, tenant_id, session_id):
        result = self.primary.recover(tenant_id, session_id)
        try:
            mirrored = self.secondary.recover(tenant_id, session_id)
            if (result is None) != (mirrored is None):
                raise RuntimeError("dual-write session mailbox recovery mismatch")
        except Exception:
            self._record_failure("recover", (tenant_id, session_id), {})
            raise
        return result

    def sweep_expired_leases(self, *, limit=100):
        return self._maintenance("sweep_expired_leases", limit=limit)

    def schedule_retries(self, *, limit=100):
        return self._maintenance("schedule_retries", limit=limit)

    def reconcile_sessions(self, *, limit=100):
        return self._maintenance("reconcile_sessions", limit=limit)

    def reconcile(self, tenant_id, session_id):
        result = self.primary.reconcile(tenant_id, session_id)
        try:
            mirrored = self.secondary.reconcile(tenant_id, session_id)
            if (result is None) != (mirrored is None):
                raise RuntimeError("dual-write session mailbox reconcile mismatch")
        except Exception:
            self._record_failure("reconcile", (tenant_id, session_id), {})
            raise
        return result

    def _maintenance(self, operation, **kwargs):
        result = getattr(self.primary, operation)(**kwargs)
        try:
            mirrored = getattr(self.secondary, operation)(**kwargs)
            if mirrored != result:
                raise RuntimeError(f"dual-write session mailbox {operation} mismatch")
        except Exception:
            self._record_failure(operation, (), kwargs)
            raise
        return result

    def _record_failure(self, operation, args, kwargs):
        _enqueue(
            self._enqueue,
            f"mirror.session_mailbox_v2.{operation}",
            {"args": args, "kwargs": kwargs},
        )


class _MirrorToolGovernance:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue

    def _write(self, operation, args, kwargs):
        result = getattr(self.primary, operation)(*args, **kwargs)
        try:
            getattr(self.secondary, operation)(*args, **kwargs)
        except Exception:
            _enqueue(
                self._enqueue,
                f"mirror.tool_governance.{operation}",
                {"args": args, "kwargs": kwargs},
            )
            raise
        return result

    def create_or_get(self, *args, **kwargs):
        return self._write("create_or_get", args, kwargs)

    def get(self, *args, **kwargs):
        try:
            return self.primary.get(*args, **kwargs) or self.secondary.get(*args, **kwargs)
        except Exception:
            return self.secondary.get(*args, **kwargs)

    def approve(self, *args, **kwargs):
        return self._write("approve", args, kwargs)

    def consume(self, *args, **kwargs):
        return self._write("consume", args, kwargs)

    def reserve_call(self, *args, **kwargs):
        return self._write("reserve_call", args, kwargs)

    def begin_execution(self, *args, **kwargs):
        return self._write("begin_execution", args, kwargs)

    def get_execution(self, *args, **kwargs):
        try:
            return self.primary.get_execution(*args, **kwargs) or self.secondary.get_execution(*args, **kwargs)
        except Exception:
            return self.secondary.get_execution(*args, **kwargs)

    def complete_execution(self, *args, **kwargs):
        return self._write("complete_execution", args, kwargs)

    def fail_execution(self, *args, **kwargs):
        return self._write("fail_execution", args, kwargs)

    def list_executions_by_tenant(self, *args, **kwargs):
        try:
            return self.primary.list_executions_by_tenant(*args, **kwargs)
        except Exception:
            return self.secondary.list_executions_by_tenant(*args, **kwargs)

    def restore_execution(self, record):
        return self._write("restore_execution", (record,), {})


class _MirrorInboxOutbox:
    """Keep durable ingress and egress records aligned during cutover."""

    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue
        self._secondary_claims: dict[tuple[str, str], OutboxRecord] = {}

    def accept_inbox(self, *args, **kwargs):
        result = self.primary.accept_inbox(*args, **kwargs)
        record, created = result
        if not created:
            return result
        try:
            secondary_kwargs = dict(kwargs)
            secondary_kwargs["message_id"] = record.message_id
            mirrored, mirrored_created = self.secondary.accept_inbox(*args, **secondary_kwargs)
            if mirrored.message_id != record.message_id or not mirrored_created:
                raise RuntimeError("dual-write inbox identity mismatch")
        except Exception:
            failure_kwargs = dict(kwargs)
            failure_kwargs["message_id"] = record.message_id
            self._record_failure("accept_inbox", args, failure_kwargs)
            raise
        return result

    def complete_inbox(self, *args, **kwargs):
        return self._write("complete_inbox", args, kwargs)

    def complete_inbox_and_enqueue_outbox(self, *args, **kwargs):
        result = self.primary.complete_inbox_and_enqueue_outbox(*args, **kwargs)
        try:
            mirrored = self.secondary.complete_inbox_and_enqueue_outbox(*args, **kwargs)
            if mirrored.event_id != result.event_id:
                raise RuntimeError("dual-write outbox identity mismatch")
        except Exception:
            self._record_failure("complete_inbox_and_enqueue_outbox", args, kwargs)
            raise
        return result

    def fail_inbox(self, *args, **kwargs):
        return self._write("fail_inbox", args, kwargs)

    def dead_inbox(self, *args, **kwargs):
        return self._write("dead_inbox", args, kwargs)

    def enqueue_outbox(self, *args, **kwargs):
        result = self.primary.enqueue_outbox(*args, **kwargs)
        try:
            mirrored = self.secondary.enqueue_outbox(*args, **kwargs)
            if mirrored.event_id != result.event_id:
                raise RuntimeError("dual-write outbox identity mismatch")
        except Exception:
            self._record_failure("enqueue_outbox", args, kwargs)
            raise
        return result

    def claim_outbox(self, *args, **kwargs):
        claimed = self.primary.claim_outbox(*args, **kwargs)
        if not claimed:
            return claimed
        try:
            mirrored = self.secondary.claim_outbox(*args, **kwargs)
            if [item.event_id for item in mirrored] != [item.event_id for item in claimed]:
                raise RuntimeError("dual-write outbox claim mismatch")
            for item in mirrored:
                self._secondary_claims[(item.tenant_id, item.event_id)] = item
        except Exception:
            self._record_failure("claim_outbox", args, kwargs)
            raise
        return claimed

    def complete_outbox(self, event_id, owner, tenant_id=None):
        self.primary.complete_outbox(event_id, owner, tenant_id=tenant_id)
        secondary = self._secondary_claims.pop((tenant_id, event_id), None) if tenant_id else None
        try:
            self.secondary.complete_outbox(event_id, owner, tenant_id=tenant_id)
        except Exception:
            self._record_failure("complete_outbox", (event_id, owner), {"tenant_id": tenant_id})
            raise
        return secondary

    def fail_outbox(self, event_id, owner, error, retry_after_seconds=30, tenant_id=None):
        self.primary.fail_outbox(event_id, owner, error, retry_after_seconds, tenant_id=tenant_id)
        self._secondary_claims.pop((tenant_id, event_id), None) if tenant_id else None
        try:
            self.secondary.fail_outbox(event_id, owner, error, retry_after_seconds, tenant_id=tenant_id)
        except Exception:
            self._record_failure(
                "fail_outbox",
                (event_id, owner, error, retry_after_seconds),
                {"tenant_id": tenant_id},
            )
            raise

    def replay_outbox(self, event_id, tenant_id):
        return self._write("replay_outbox", (event_id, tenant_id), {})

    def _write(self, operation, args, kwargs):
        result = getattr(self.primary, operation)(*args, **kwargs)
        try:
            getattr(self.secondary, operation)(*args, **kwargs)
        except Exception:
            self._record_failure(operation, args, kwargs)
            raise
        return result

    def _record_failure(self, operation, args, kwargs):
        _enqueue(
            self._enqueue,
            f"mirror.inbox_outbox.{operation}",
            {"args": args, "kwargs": kwargs},
        )


class _MirrorArtifact:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue

    def put(self, tenant_id, content, content_type="application/octet-stream"):
        result = self.primary.put(tenant_id, content, content_type)
        try:
            self.secondary.put_with_id(tenant_id, result.object_id, content, content_type)
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.artifact.put",
                {
                    "tenant_id": tenant_id,
                    "object_id": result.object_id,
                    "content_type": content_type,
                    "content_base64": base64.b64encode(content).decode("ascii"),
                },
            )
            raise
        return result

    def put_with_id(self, tenant_id, object_id, content, content_type="application/octet-stream"):
        result = self.primary.put_with_id(tenant_id, object_id, content, content_type)
        try:
            self.secondary.put_with_id(tenant_id, object_id, content, content_type)
        except Exception:
            _enqueue(
                self._enqueue,
                "mirror.artifact.put",
                {
                    "tenant_id": tenant_id,
                    "object_id": object_id,
                    "content_type": content_type,
                    "content_base64": base64.b64encode(content).decode("ascii"),
                },
            )
            raise
        return result

    def get(self, tenant_id, object_id):
        try:
            return self.primary.get(tenant_id, object_id)
        except Exception:
            return self.secondary.get(tenant_id, object_id)

    def list_by_tenant(self, tenant_id):
        try:
            objects = self.primary.list_by_tenant(tenant_id)
            return objects or self.secondary.list_by_tenant(tenant_id)
        except Exception:
            return self.secondary.list_by_tenant(tenant_id)


class _MirrorKnowledge:
    def __init__(self, primary, secondary, enqueue) -> None:
        self.primary = primary
        self.secondary = secondary
        self._enqueue = enqueue

    def upsert(self, chunk):
        self.primary.upsert(chunk)
        try:
            self.secondary.upsert(chunk)
        except Exception:
            _enqueue(self._enqueue, "mirror.knowledge.upsert", {"chunk": asdict(chunk)})
            raise

    def search(self, tenant_id, collection, query, limit=5):
        try:
            chunks = self.primary.search(tenant_id, collection, query, limit)
            return chunks or self.secondary.search(tenant_id, collection, query, limit)
        except Exception:
            return self.secondary.search(tenant_id, collection, query, limit)

    def list_by_tenant(self, tenant_id):
        try:
            chunks = self.primary.list_by_tenant(tenant_id)
            return chunks or self.secondary.list_by_tenant(tenant_id)
        except Exception:
            return self.secondary.list_by_tenant(tenant_id)


class MirroredStorageBundle:
    """Primary storage with synchronous replication to a migration target.

    Writes fail closed if the target cannot be updated. The caller can retry
    the message because all logical writes carry idempotency keys.
    """

    def __init__(self, primary: StorageBundle, secondary: StorageBundle) -> None:
        self.primary = primary
        self.secondary = secondary
        self.structured = primary.structured
        self.session = _MirrorSession(primary.session, secondary.session, self._enqueue)
        self.memory = _MirrorMemory(primary.memory, secondary.memory, self._enqueue)
        self.summary = _MirrorSummary(primary.summary, secondary.summary, self._enqueue)
        self.audit = _MirrorAudit(primary.audit, secondary.audit, self._enqueue)
        self.idempotency = _MirrorIdempotency(primary.idempotency, secondary.idempotency, self._enqueue)
        self.artifacts = _MirrorArtifact(primary.artifacts, secondary.artifacts, self._enqueue)
        self.objects = self.artifacts
        self.knowledge = _MirrorKnowledge(primary.knowledge, secondary.knowledge, self._enqueue)
        self.knowledge_collection = primary.knowledge_collection
        self.extra_backends: list[Any] = []
        self.compensation = primary.compensation
        self.inbox_outbox = (
            _MirrorInboxOutbox(primary.inbox_outbox, secondary.inbox_outbox, self._enqueue)
            if primary.inbox_outbox is not None and secondary.inbox_outbox is not None
            else primary.inbox_outbox or secondary.inbox_outbox
        )
        self.mailbox = (
            _MirrorMailbox(primary.mailbox, secondary.mailbox, self._enqueue)
            if primary.mailbox is not None and secondary.mailbox is not None
            else primary.mailbox or secondary.mailbox
        )
        self.session_mailbox_v2 = (
            _MirrorSessionMailboxV2(
                primary.session_mailbox_v2,
                secondary.session_mailbox_v2,
                self._enqueue,
            )
            if primary.session_mailbox_v2 is not None
            and secondary.session_mailbox_v2 is not None
            else primary.session_mailbox_v2 or secondary.session_mailbox_v2
        )
        self.tool_governance = (
            _MirrorToolGovernance(primary.tool_governance, secondary.tool_governance, self._enqueue)
            if primary.tool_governance is not None and secondary.tool_governance is not None
            else primary.tool_governance or secondary.tool_governance
        )

    def _enqueue(self, operation: str, payload: dict[str, Any]) -> None:
        """Persist a target-side outbox item in the primary backend."""

        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        task_id = f"mirror:{operation}:{digest}"
        self.primary.compensation.enqueue(
            "mirror",
            operation,
            payload,
            task_id=task_id,
        )

    def replay_mirror_task(self, task) -> None:
        """Apply one target-side outbox item idempotently."""

        operation = task.operation
        payload = task.payload
        if operation == "mirror.session.append_event":
            raw = dict(payload["event"])
            raw["created_at"] = datetime.fromisoformat(raw["created_at"])
            self.secondary.session.append_event(SessionEvent(**raw))
        elif operation == "mirror.session.compare_and_set":
            current = self.secondary.session.load_state(payload["tenant_id"], payload["session_id"])
            if current.state != payload["state"]:
                updated = self.secondary.session.compare_and_set_state(
                    payload["tenant_id"],
                    payload["session_id"],
                    int(payload["expected_version"]),
                    dict(payload["state"]),
                )
                if not updated:
                    current = self.secondary.session.load_state(payload["tenant_id"], payload["session_id"])
                    if current.state != payload["state"]:
                        raise RuntimeError("mirror target state conflict")
        elif operation == "mirror.memory.put":
            raw = dict(payload["item"])
            raw["created_at"] = datetime.fromisoformat(raw["created_at"])
            self.secondary.memory.put(MemoryItem(**raw))
        elif operation == "mirror.summary.put":
            raw = dict(payload["summary"])
            raw["created_at"] = datetime.fromisoformat(raw["created_at"])
            self.secondary.summary.put(Summary(**raw))
        elif operation == "mirror.audit.append":
            raw = dict(payload["record"])
            raw["created_at"] = datetime.fromisoformat(raw["created_at"])
            self.secondary.audit.append(AuditRecord(**raw))
        elif operation == "mirror.idempotency.start":
            self.secondary.idempotency.start(**payload)
        elif operation == "mirror.idempotency.complete":
            self.secondary.idempotency.complete(**payload)
        elif operation == "mirror.idempotency.fail":
            self.secondary.idempotency.fail(**payload)
        elif operation == "mirror.idempotency.claim_delivery":
            self.secondary.idempotency.claim_delivery(**payload)
        elif operation == "mirror.idempotency.release_delivery":
            self.secondary.idempotency.release_delivery(**payload)
        elif operation == "mirror.artifact.put":
            self.secondary.artifacts.put_with_id(
                payload["tenant_id"],
                payload["object_id"],
                base64.b64decode(payload["content_base64"]),
                payload["content_type"],
            )
        elif operation == "mirror.knowledge.upsert":
            from trpc_service.storage.vector_store import KnowledgeChunk

            self.secondary.knowledge.upsert(KnowledgeChunk(**payload["chunk"]))
        elif operation == "mirror.mailbox.enqueue":
            self.secondary.mailbox.enqueue(**payload)
        elif operation == "mirror.mailbox.claim_next":
            self.secondary.mailbox.claim_next(**payload)
        elif operation == "mirror.mailbox.renew":
            raw = dict(payload["record"])
            for key in ("created_at", "updated_at", "available_at", "lease_until"):
                if isinstance(raw.get(key), str):
                    raw[key] = datetime.fromisoformat(raw[key])
            self.secondary.mailbox.renew(MailboxRecord(**raw), payload.get("lease_seconds"))
        elif operation == "mirror.mailbox.complete":
            raw = dict(payload["record"])
            for key in ("created_at", "updated_at", "available_at", "lease_until"):
                if isinstance(raw.get(key), str):
                    raw[key] = datetime.fromisoformat(raw[key])
            self.secondary.mailbox.complete(MailboxRecord(**raw))
        elif operation == "mirror.mailbox.fail":
            raw = dict(payload["record"])
            for key in ("created_at", "updated_at", "available_at", "lease_until"):
                if isinstance(raw.get(key), str):
                    raw[key] = datetime.fromisoformat(raw[key])
            self.secondary.mailbox.fail(
                MailboxRecord(**raw),
                payload["error"],
                int(payload.get("retry_after_seconds", 0)),
            )
        elif operation == "mirror.mailbox.recover_expired":
            self.secondary.mailbox.recover_expired(**payload)
        elif operation.startswith("mirror.session_mailbox_v2."):
            operation_name = operation.removeprefix("mirror.session_mailbox_v2.")
            args = list(payload.get("args", []))
            kwargs = dict(payload.get("kwargs", {}))
            if operation_name in {"renew", "commit", "retry"}:
                raw = dict(kwargs.pop("lease"))
                raw["expires_at"] = datetime.fromisoformat(raw["expires_at"])
                lease = SessionMailboxLease(**raw)
                kwargs["lease"] = lease
            elif operation_name == "restore_export":
                kwargs["payload"] = payload["kwargs"]["payload"]
            getattr(self.secondary.session_mailbox_v2, operation_name)(
                *args,
                **kwargs,
            )
        elif operation.startswith("mirror.inbox_outbox."):
            operation_name = operation.removeprefix("mirror.inbox_outbox.")
            self.secondary.inbox_outbox.__getattribute__(operation_name)(
                *payload.get("args", []),
                **payload.get("kwargs", {}),
            )
        elif operation.startswith("mirror.tool_governance."):
            operation_name = operation.removeprefix("mirror.tool_governance.")
            args = list(payload.get("args", []))
            if operation_name == "restore_execution" and args:
                raw = dict(args[0])
                for key in ("started_at", "completed_at", "created_at", "updated_at"):
                    if isinstance(raw.get(key), str):
                        raw[key] = datetime.fromisoformat(raw[key])
                args[0] = ToolExecution(**raw)
            self.secondary.tool_governance.__getattribute__(operation_name)(
                *args,
                **payload.get("kwargs", {}),
            )
        else:
            raise ValueError(f"unsupported mirror operation: {operation}")

    def close(self) -> None:
        self.primary.close()
        self.secondary.close()


def mirrored_storage(primary_profile, secondary_profile, data_dir) -> MirroredStorageBundle:
    from trpc_service.storage.factory import create_storage

    return MirroredStorageBundle(
        create_storage(primary_profile, data_dir / "primary"),
        create_storage(secondary_profile, data_dir / "secondary"),
    )

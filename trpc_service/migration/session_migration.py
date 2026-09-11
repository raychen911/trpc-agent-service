"""Restartable per-tenant session migration with preserved IDs and revisions."""

from __future__ import annotations

import hashlib
import json
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta

from trpc_agent_sdk.events import Event

from trpc_service.persistence import Database
from trpc_service.persistence.models import MigrationCheckpointRow
from trpc_service.persistence.models import MigrationJobRow
from trpc_service.persistence.models import utcnow
from trpc_service.storage import ConflictError

from .models import MigrationState
from .models import VerificationReport


@dataclass(frozen=True)
class SessionRecord:
    key: str
    revision: int
    payload: dict

    @property
    def digest(self) -> str:
        payload = dict(self.payload)
        events = payload.get("events")
        if isinstance(events, list):
            payload["events"] = [
                {
                    "id": event.get("id"),
                    "author": event.get("author"),
                    "invocationId": event.get("invocationId"),
                    "content": event.get("content"),
                    "actions": event.get("actions"),
                    "customMetadata": event.get("customMetadata"),
                } if isinstance(event, dict) else event for event in events
            ]
        value = {"key": self.key, "revision": self.revision, "payload": payload}
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class SessionStore:
    """Minimal migration store protocol implementation used for local tests and adapters."""

    def __init__(self, records=()) -> None:
        self._records = {record.key: record for record in records}

    def scan_after(self, key: str | None, limit: int) -> list[SessionRecord]:
        keys = sorted(item for item in self._records if key is None or item > key)
        return [self._records[item] for item in keys[:limit]]

    def put(self, record: SessionRecord) -> None:
        existing = self._records.get(record.key)
        if existing is not None and existing.revision > record.revision:
            raise ConflictError("migration cannot overwrite a newer target revision")
        self._records[record.key] = record

    def records(self) -> dict[str, SessionRecord]:
        return dict(self._records)


async def snapshot_trpc_session(service, *, app_name: str, user_id: str, session_id: str) -> SessionRecord:
    """Read a portable session record through the public tRPC service contract."""

    session = await service.get_session(app_name=app_name, user_id=user_id, session_id=session_id)
    if session is None:
        raise LookupError("source tRPC session not found")
    events = [event.model_dump(mode="json", by_alias=True) for event in session.events]
    return SessionRecord(
        key=session.id,
        revision=len(events),
        payload={
            "app_name": session.app_name,
            "user_id": session.user_id,
            "session_id": session.id,
            "state": deepcopy(session.state),
            "events": events,
            "historical_events": [event.model_dump(mode="json", by_alias=True)
                                  for event in session.historical_events],
            "conversation_count": session.conversation_count,
        },
    )


async def restore_trpc_session(service, record: SessionRecord, *, target_writes_paused: bool = False,
                               expected_target_digest: str | None = None) -> None:
    """Restore an offline target; the caller must fence all other target writers.

    Public SDK writes do not offer CAS. This flag is a precondition, not a lock.
    An explicitly captured target digest permits a planned state-only replacement.
    Event count is a history length, NOT a monotonic state revision.
    """

    if not target_writes_paused:
        raise ConflictError("session migration requires target writes to be paused")

    payload = deepcopy(record.payload)
    session = await service.get_session(
        app_name=payload["app_name"],
        user_id=payload["user_id"],
        session_id=payload["session_id"],
    )
    events = [Event.model_validate(value) for value in payload["events"]]
    if len({event.id for event in events}) != len(events):
        raise ConflictError("source history contains duplicate event IDs")
    if session is not None:
        existing = await snapshot_trpc_session(service, app_name=payload["app_name"],
                                               user_id=payload["user_id"], session_id=payload["session_id"])
        if expected_target_digest is not None and existing.digest != expected_target_digest:
            raise ConflictError("target changed since the approved snapshot")
        # Compare contents and order, not only event IDs or counts, before ANY write.
        prefix = SessionRecord(record.key, existing.revision, {
            **payload, "state": existing.payload["state"], "events": payload["events"][:existing.revision],
            "historical_events": existing.payload["historical_events"],
            "conversation_count": existing.payload["conversation_count"],
        })
        if existing.revision > record.revision or existing.digest != prefix.digest:
            raise ConflictError("target history is newer or divergent")
        if (existing.revision == record.revision and session.state != payload["state"]
                and expected_target_digest is None):
            raise ConflictError("state-only conflict requires an approved target snapshot")
        session = session.model_copy(deep=True)
    elif expected_target_digest is not None:
        raise ConflictError("approved target snapshot no longer exists")
    else:
        session = await service.create_session(
            app_name=payload["app_name"],
            user_id=payload["user_id"],
            session_id=payload["session_id"],
            state=payload["state"],
        )
    existing_ids = {event.id for event in session.events}
    for event in events:
        if event.id not in existing_ids:
            await service.append_event(session, event)
            existing_ids.add(event.id)
    # append_event applies historical state deltas. Restore the final snapshot LAST.
    session = await service.get_session(app_name=payload["app_name"], user_id=payload["user_id"],
                                        session_id=payload["session_id"])
    session = session.model_copy(deep=True)
    historical = [Event.model_validate(value) for value in payload.get("historical_events", [])]
    if (session.state != payload["state"] or session.historical_events != historical or
            session.conversation_count != payload.get("conversation_count", session.conversation_count)):
        session.state = deepcopy(payload["state"])
        session.historical_events = historical
        session.conversation_count = payload.get("conversation_count", session.conversation_count)
        await service.update_session(session)


async def copy_and_verify_trpc_session(source, target, *, app_name: str, user_id: str,
                                       session_id: str, target_writes_paused: bool = False,
                                       expected_target_digest: str | None = None) -> SessionRecord:
    """Backfill one catalogued Redis/SQL session and verify its normalized hash."""

    source_record = await snapshot_trpc_session(
        source,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )
    await restore_trpc_session(target, source_record, target_writes_paused=target_writes_paused,
                               expected_target_digest=expected_target_digest)
    target_record = await snapshot_trpc_session(
        target,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )
    if source_record.digest != target_record.digest:
        raise ConflictError("tRPC session migration verification failed")
    return target_record


_TRANSITIONS = {
    MigrationState.PREPARING: {MigrationState.BACKFILLING, MigrationState.PAUSED, MigrationState.FAILED_FINAL},
    MigrationState.BACKFILLING: {MigrationState.DUAL_WRITE, MigrationState.PAUSED, MigrationState.FAILED_RETRYABLE},
    MigrationState.DUAL_WRITE: {MigrationState.VERIFYING, MigrationState.PAUSED, MigrationState.FAILED_RETRYABLE},
    MigrationState.VERIFYING: {MigrationState.SHADOW_READ, MigrationState.DUAL_WRITE, MigrationState.PAUSED},
    MigrationState.SHADOW_READ: {MigrationState.CUTOVER, MigrationState.VERIFYING, MigrationState.PAUSED},
    MigrationState.CUTOVER: {MigrationState.ROLLBACK_WINDOW},
    MigrationState.ROLLBACK_WINDOW: {MigrationState.COMPLETED, MigrationState.ROLLED_BACK},
    MigrationState.PAUSED: {
        MigrationState.PREPARING,
        MigrationState.BACKFILLING,
        MigrationState.DUAL_WRITE,
        MigrationState.VERIFYING,
        MigrationState.SHADOW_READ,
    },
}


class MigrationCoordinator:
    def __init__(self, database, job_id, source, target) -> None:
        self._database = database
        self.job_id = job_id
        self._source = source
        self._target = target
        self._paused_from: MigrationState | None = None
        self._last_verification: VerificationReport | None = None

    @classmethod
    def create(cls, database: Database, *, source, target, **values):
        job_id = uuid.uuid4().hex
        with database.sessions.begin() as session:
            session.add(
                MigrationJobRow(
                    migration_job_id=job_id,
                    status=MigrationState.PREPARING.value,
                    **values,
                ))
            session.add(MigrationCheckpointRow(migration_job_id=job_id, partition_id="default"))
        return cls(database, job_id, source, target)

    @classmethod
    def load(cls, database, job_id, *, source, target):
        with database.sessions() as session:
            if session.get(MigrationJobRow, job_id) is None:
                raise LookupError("migration job not found")
        return cls(database, job_id, source, target)

    @property
    def state(self) -> MigrationState:
        with self._database.sessions() as session:
            row = session.get(MigrationJobRow, self.job_id)
            assert row is not None
            return MigrationState(row.status)

    def _transition(self, target: MigrationState) -> None:
        current = self.state
        if target not in _TRANSITIONS.get(current, set()):
            raise ConflictError(f"invalid migration transition: {current.value} -> {target.value}")
        with self._database.sessions.begin() as session:
            row = session.get(MigrationJobRow, self.job_id)
            assert row is not None
            row.status = target.value

    def prepare(self) -> None:
        self._transition(MigrationState.BACKFILLING)

    def pause(self) -> None:
        self._paused_from = self.state
        self._transition(MigrationState.PAUSED)

    def resume(self) -> None:
        with self._database.sessions() as session:
            checkpoint = session.get(MigrationCheckpointRow, (self.job_id, "default"))
            assert checkpoint is not None
            resume_state = (
                MigrationState.BACKFILLING if checkpoint.last_source_key is not None else MigrationState.PREPARING)
        if self._paused_from is not None:
            resume_state = self._paused_from
        self._transition(resume_state)

    def backfill(self, batch_size: int) -> bool:
        if self.state is not MigrationState.BACKFILLING:
            raise ConflictError("backfill requires BACKFILLING state")
        with self._database.sessions.begin() as session:
            checkpoint = session.get(MigrationCheckpointRow, (self.job_id, "default"))
            assert checkpoint is not None
            records = self._source.scan_after(checkpoint.last_source_key, batch_size)
            for record in records:
                self._target.put(record)
                checkpoint.last_source_key = record.key
                checkpoint.last_source_revision = record.revision
                checkpoint.copied_count += 1
        return len(records) < batch_size

    def begin_dual_write(self) -> None:
        self._transition(MigrationState.DUAL_WRITE)

    def dual_write(self, record: SessionRecord) -> None:
        if self.state not in {MigrationState.DUAL_WRITE, MigrationState.VERIFYING, MigrationState.SHADOW_READ}:
            raise ConflictError("dual write is not active")
        self._source.put(record)
        self._target.put(record)

    def verify(self) -> VerificationReport:
        if self.state is MigrationState.DUAL_WRITE:
            self._transition(MigrationState.VERIFYING)
        elif self.state is not MigrationState.VERIFYING:
            raise ConflictError("verification requires DUAL_WRITE or VERIFYING state")
        source = self._source.records()
        target = self._target.records()
        differences = tuple(
            key for key in sorted(set(source) | set(target))
            if key not in source or key not in target or source[key].digest != target[key].digest
        )
        self._last_verification = VerificationReport(len(source), len(target), differences)
        with self._database.sessions.begin() as session:
            checkpoint = session.get(MigrationCheckpointRow, (self.job_id, "default"))
            assert checkpoint is not None
            checkpoint.verified_count = len(source) - len(differences)
        return self._last_verification

    def shadow_read(self) -> None:
        if self._last_verification is None or not self._last_verification.matches:
            raise ConflictError("migration verification must match before shadow read")
        self._transition(MigrationState.SHADOW_READ)

    def cutover(self, rollback_seconds: float) -> None:
        if self.state is not MigrationState.SHADOW_READ:
            raise ConflictError("cutover requires SHADOW_READ state")
        self._transition(MigrationState.CUTOVER)
        with self._database.sessions.begin() as session:
            row = session.get(MigrationJobRow, self.job_id)
            assert row is not None
            row.rollback_deadline = utcnow() + timedelta(seconds=rollback_seconds)
        self._transition(MigrationState.ROLLBACK_WINDOW)

    def rollback(self) -> None:
        self._transition(MigrationState.ROLLED_BACK)

    def complete(self) -> None:
        self._transition(MigrationState.COMPLETED)

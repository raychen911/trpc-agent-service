"""Tenant-scoped dual-read/dual-write storage used during backend cutovers."""

from __future__ import annotations

import base64
from dataclasses import asdict
from datetime import datetime
import hashlib
import json
from typing import Any

from trpc_service.storage.base import (
    AuditRecord,
    MemoryItem,
    SessionEvent,
    Summary,
)
from trpc_service.storage.factory import StorageBundle
from trpc_service.storage.locking import SessionLease


def _jsonable(value: Any) -> Any:
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
        self.inbox_outbox = primary.inbox_outbox or secondary.inbox_outbox

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

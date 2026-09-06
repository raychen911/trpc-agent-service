"""Structured-store export/import utility for tenant-scoped cutovers."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from trpc_service.database import (
    DatabaseMigrationError,
    database_status,
    downgrade_database,
    upgrade_database,
)
from trpc_service.migration_state import MigrationPhase, MigrationState, new_migration
from trpc_service.storage.base import (
    AuditRecord,
    IdempotencyStatus,
    MemoryItem,
    SessionEvent,
    SessionState,
    Summary,
    ToolExecution,
)
from trpc_service.storage.durable import InboxRecord, OutboxRecord, _json_object
from trpc_service.storage.factory import create_storage
from trpc_service.storage.mailbox import MailboxRecord
from trpc_service.storage.postgres_rls import (
    PostgresRLSError,
    apply_rls_migration_dsn,
    postgres_tenant_context,
)
from trpc_service.storage.tool_governance import ToolApproval, ToolBudget
from trpc_service.storage.vector_store import KnowledgeChunk
from trpc_service.tenant.models import StorageProfile


def _json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json(item) for item in value]
    return value


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _sessions(storage, tenant_id: str) -> list[str]:
    backend = getattr(storage.structured, "backend_name", "")
    if backend == "redis":
        session_prefix = storage.structured._key("session-events", tenant_id)
        sessions = set()
        for key in storage.structured.client.scan_iter(f"{session_prefix}:*"):
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            sessions.add(str(key)[len(session_prefix) + 1 :])
        return sorted(sessions)
    if backend in {"sql", "postgres"}:
        context = postgres_tenant_context(storage.structured, tenant_id) if backend == "postgres" else None
        if context is None:
            cur = storage.structured._conn.cursor()
            try:
                cur.execute("SELECT DISTINCT session_id FROM message_event WHERE tenant_id=?", (tenant_id,))
                return sorted(str(row[0]) for row in cur.fetchall())
            finally:
                cur.close()
        with context:
            cur = storage.structured._conn.cursor()
            try:
                cur.execute("SELECT DISTINCT session_id FROM message_event WHERE tenant_id=%s", (tenant_id,))
                return sorted(str(row[0]) for row in cur.fetchall())
            finally:
                cur.close()
    events = getattr(storage.structured.session, "_events", {})
    return sorted(session_id for current_tenant, session_id in events if current_tenant == tenant_id)


def _idempotency_records(storage, tenant_id: str) -> list[dict[str, Any]]:
    store = storage.idempotency
    backend = getattr(storage.structured, "backend_name", getattr(store, "backend_name", ""))
    records: list[dict[str, Any]] = []
    if backend == "redis":
        prefix = store._key("idempotency", tenant_id, "")
        for key in store.client.scan_iter(f"{prefix}*"):
            raw = store.client.get(key)
            if raw:
                records.append(json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw))
    elif backend in {"sql", "postgres"} and hasattr(store, "_conn"):
        context = postgres_tenant_context(store, tenant_id) if backend == "postgres" else None
        if context is None:
            cur = store._conn.cursor()
            try:
                cur.execute(
                    (
                        "SELECT tenant_id,key,status,response_ref,result_json,trace_id,created_at,updated_at "
                        "FROM idempotency WHERE tenant_id=?"
                    ),
                    (tenant_id,),
                )
                rows = cur.fetchall()
            finally:
                cur.close()
        else:
            with context:
                cur = store._conn.cursor()
                try:
                    cur.execute(
                        (
                            "SELECT tenant_id,key,status,response_ref,result_json,trace_id,created_at,updated_at "
                            "FROM idempotency WHERE tenant_id=%s"
                        ),
                        (tenant_id,),
                    )
                    rows = cur.fetchall()
                finally:
                    cur.close()
        for row in rows:
            result_json = row[4]
            records.append(
                {
                    "tenant_id": row[0],
                    "key": row[1],
                    "status": row[2],
                    "response_ref": row[3],
                    "result": _json_object(result_json) if result_json is not None else None,
                    "trace_id": row[5],
                    "created_at": _json(row[6]),
                    "updated_at": _json(row[7]),
                }
            )
    else:
        for (current_tenant, _), record in getattr(store, "_records", {}).items():
            if current_tenant == tenant_id:
                records.append(_json(asdict(record)))
    return sorted(records, key=lambda item: item["key"])


def _knowledge_chunks(storage, tenant_id: str) -> list[dict[str, Any]]:
    if hasattr(storage.knowledge, "list_by_tenant"):
        return _json([asdict(chunk) for chunk in storage.knowledge.list_by_tenant(tenant_id)])
    return []


def _artifacts(storage, tenant_id: str) -> list[dict[str, Any]]:
    if not hasattr(storage.artifacts, "list_by_tenant"):
        return []
    artifacts = []
    for item in storage.artifacts.list_by_tenant(tenant_id):
        content = storage.artifacts.get(tenant_id, item.object_id)
        artifacts.append(
            {
                "tenant_id": tenant_id,
                "object_id": item.object_id,
                "content_type": item.content_type,
                "size": item.size,
                "content_base64": base64.b64encode(content).decode("ascii"),
            }
        )
    return artifacts


def export_tenant(storage, tenant_id: str) -> dict[str, Any]:
    sessions = []
    for session_id in _sessions(storage, tenant_id):
        state = storage.session.load_state(tenant_id, session_id)
        events = storage.session.load_events(tenant_id, session_id)
        summary = storage.summary.latest(tenant_id, session_id)
        sessions.append(
            {
                "session_id": session_id,
                "state": _json(asdict(state)),
                "events": _json([asdict(event) for event in events]),
                "summary": _json(asdict(summary)) if summary else None,
            }
        )

    memories: list[dict[str, Any]] = []
    memory_store = storage.memory
    memory_backend = getattr(
        memory_store,
        "backend_name",
        "memory" if hasattr(memory_store, "_items") else "",
    )
    if memory_backend == "redis":
        for memory_id in memory_store.client.smembers(memory_store._key("memory-index", tenant_id)):
            memory_id = memory_id.decode() if isinstance(memory_id, bytes) else memory_id
            item = memory_store.client.get(memory_store._key("memory", tenant_id, memory_id))
            if item:
                if isinstance(item, bytes):
                    item = item.decode("utf-8")
                memories.append(json.loads(item))
    elif memory_backend in {"sql", "postgres"}:
        context = postgres_tenant_context(memory_store, tenant_id) if memory_backend == "postgres" else None
        if context is None:
            cur = memory_store._conn.cursor()
            try:
                cur.execute(
                    (
                        "SELECT tenant_id,memory_id,scope_key,content,version,metadata_json,created_at "
                        "FROM memory WHERE tenant_id=?"
                    ),
                    (tenant_id,),
                )
                rows = cur.fetchall()
            finally:
                cur.close()
        else:
            with context:
                cur = memory_store._conn.cursor()
                try:
                    cur.execute(
                        (
                            "SELECT tenant_id,memory_id,scope_key,content,version,metadata_json,created_at "
                            "FROM memory WHERE tenant_id=%s"
                        ),
                        (tenant_id,),
                    )
                    rows = cur.fetchall()
                finally:
                    cur.close()
        for row in rows:
            metadata = row[5] if isinstance(row[5], dict) else json.loads(row[5])
            memories.append(
                _json(asdict(MemoryItem(row[0], row[1], row[2], row[3], metadata, int(row[4]), row[6])))
            )
    elif memory_backend == "memory":
        items = getattr(memory_store, "_items", {}).values()
        for item in items:
            if item.tenant_id == tenant_id:
                memories.append(_json(asdict(item)))

    mailbox = []
    mailbox_store = getattr(storage, "mailbox", None)
    if mailbox_store is not None and hasattr(mailbox_store, "list_by_tenant"):
        mailbox = _json([asdict(item) for item in mailbox_store.list_by_tenant(tenant_id)])
    # Keep the optional mailbox export shape stable across backends. Redis
    # does not currently expose the session-mailbox-v2 adapter, while SQL
    # backends do; representing an absent adapter as an empty payload avoids
    # false checksum mismatches during cross-backend migration.
    session_mailbox_v2 = {"mailboxes": [], "items": []}
    session_mailbox_store = getattr(storage, "session_mailbox_v2", None)
    if session_mailbox_store is not None and hasattr(session_mailbox_store, "export_by_tenant"):
        session_mailbox_v2 = _json(session_mailbox_store.export_by_tenant(tenant_id))
    inbox = []
    outbox = []
    inbox_outbox = getattr(storage, "inbox_outbox", None)
    if inbox_outbox is not None:
        if hasattr(inbox_outbox, "list_inbox_by_tenant"):
            inbox = _json([asdict(item) for item in inbox_outbox.list_inbox_by_tenant(tenant_id)])
        if hasattr(inbox_outbox, "list_outbox_by_tenant"):
            outbox = _json([asdict(item) for item in inbox_outbox.list_outbox_by_tenant(tenant_id)])
    approvals = []
    budgets = []
    executions = []
    governance = getattr(storage, "tool_governance", None)
    if governance is not None:
        if hasattr(governance, "list_by_tenant"):
            approvals = _json([asdict(item) for item in governance.list_by_tenant(tenant_id)])
        if hasattr(governance, "list_budgets_by_tenant"):
            budgets = _json([asdict(item) for item in governance.list_budgets_by_tenant(tenant_id)])
        if hasattr(governance, "list_executions_by_tenant"):
            executions = _json(
                [asdict(item) for item in governance.list_executions_by_tenant(tenant_id)]
            )

    payload = {
        "version": 3,
        "tenant_id": tenant_id,
        "sessions": sessions,
        "memories": memories,
        "audit": _json([asdict(item) for item in storage.audit.list_by_tenant(tenant_id)]),
        "idempotency": _idempotency_records(storage, tenant_id),
        "knowledge": _knowledge_chunks(storage, tenant_id),
        "artifacts": _artifacts(storage, tenant_id),
        "mailbox": mailbox,
        "session_mailbox_v2": session_mailbox_v2,
        "inbox": inbox,
        "outbox": outbox,
        "tool_approvals": approvals,
        "tool_budgets": budgets,
        "tool_executions": executions,
    }
    payload["checksums"] = _checksums(payload)
    return payload


def import_tenant(storage, payload: dict[str, Any]) -> None:
    tenant_id = payload["tenant_id"]
    for session in payload.get("sessions", []):
        for raw in session.get("events", []):
            event = SessionEvent(**{**raw, "created_at": _dt(raw["created_at"])})
            storage.session.append_event(event)
        state = session.get("state")
        if state:
            parsed = SessionState(**{**state, "state": dict(state.get("state", {}))})
            current = storage.session.load_state(tenant_id, parsed.session_id)
            if current.state_version == 0:
                restore = getattr(storage.session, "restore_state", None)
                if restore is not None:
                    restore(
                        tenant_id,
                        parsed.session_id,
                        parsed.state,
                        parsed.state_version,
                    )
                else:
                    if not storage.session.compare_and_set_state(tenant_id, parsed.session_id, 0, parsed.state):
                        raise RuntimeError(f"unable to restore session state: {tenant_id}/{parsed.session_id}")
        if session.get("summary"):
            raw_summary = session["summary"]
            storage.summary.put(Summary(**{**raw_summary, "created_at": _dt(raw_summary["created_at"])}))
    for raw in payload.get("memories", []):
        storage.memory.put(MemoryItem(**{**raw, "created_at": _dt(raw["created_at"])}))
    for raw in payload.get("audit", []):
        record = AuditRecord(**{**raw, "created_at": _dt(raw["created_at"])})
        try:
            storage.audit.append(record)
        except Exception:
            existing = storage.audit.list_by_tenant(tenant_id, limit=10_000)
            if not any(item.audit_id == record.audit_id for item in existing):
                raise
    for raw in payload.get("idempotency", []):
        _import_idempotency(storage, raw)
    for raw in payload.get("knowledge", []):
        storage.knowledge.upsert(KnowledgeChunk(**raw))
    for raw in payload.get("artifacts", []):
        content = base64.b64decode(raw["content_base64"])
        put_with_id = getattr(storage.artifacts, "put_with_id", None)
        if put_with_id:
            put_with_id(tenant_id, raw["object_id"], content, raw.get("content_type", "application/octet-stream"))
        else:
            storage.artifacts.put(tenant_id, content, raw.get("content_type", "application/octet-stream"))
    mailbox = getattr(storage, "mailbox", None)
    if mailbox is not None and hasattr(mailbox, "restore"):
        for raw in payload.get("mailbox", []):
            for key in ("created_at", "updated_at", "available_at", "lease_until"):
                if isinstance(raw.get(key), str):
                    raw[key] = _dt(raw[key])
            mailbox.restore(MailboxRecord(**raw))
    session_mailbox_store = getattr(storage, "session_mailbox_v2", None)
    if session_mailbox_store is not None and hasattr(session_mailbox_store, "restore_export"):
        session_mailbox_store.restore_export(payload.get("session_mailbox_v2", {}))
    inbox_outbox = getattr(storage, "inbox_outbox", None)
    if inbox_outbox is not None:
        if hasattr(inbox_outbox, "restore_inbox"):
            for raw in payload.get("inbox", []):
                item = dict(raw)
                for key in ("lease_until", "created_at", "updated_at"):
                    if isinstance(item.get(key), str):
                        item[key] = _dt(item[key])
                inbox_outbox.restore_inbox(InboxRecord(**item))
        if hasattr(inbox_outbox, "restore_outbox"):
            for raw in payload.get("outbox", []):
                item = dict(raw)
                for key in ("available_at", "locked_until", "created_at", "updated_at"):
                    if isinstance(item.get(key), str):
                        item[key] = _dt(item[key])
                inbox_outbox.restore_outbox(OutboxRecord(**item))
    governance = getattr(storage, "tool_governance", None)
    if governance is not None:
        if hasattr(governance, "restore_approval"):
            for raw in payload.get("tool_approvals", []):
                for key in ("created_at", "updated_at", "expires_at", "consumed_at"):
                    if isinstance(raw.get(key), str):
                        raw[key] = _dt(raw[key])
                governance.restore_approval(ToolApproval(**raw))
        if hasattr(governance, "restore_budget"):
            for raw in payload.get("tool_budgets", []):
                for key in ("created_at", "updated_at"):
                    if isinstance(raw.get(key), str):
                        raw[key] = _dt(raw[key])
                governance.restore_budget(ToolBudget(**raw))
        if hasattr(governance, "restore_execution"):
            for raw in payload.get("tool_executions", []):
                for key in ("started_at", "completed_at", "created_at", "updated_at"):
                    if isinstance(raw.get(key), str):
                        raw[key] = _dt(raw[key])
                governance.restore_execution(ToolExecution(**raw))


def _import_idempotency(storage, raw: dict[str, Any]) -> None:
    tenant_id = raw["tenant_id"]
    key = raw["key"]
    if storage.idempotency.get(tenant_id, key):
        return
    storage.idempotency.start(tenant_id, key, raw.get("trace_id") or "migration")
    status = raw.get("status")
    if status == IdempotencyStatus.COMPLETED.value:
        storage.idempotency.complete(tenant_id, key, raw.get("response_ref") or "", raw.get("result") or {})
    elif status == IdempotencyStatus.FAILED.value:
        error_type = (raw.get("result") or {}).get("error_type", "migrated_failure")
        storage.idempotency.fail(tenant_id, key, error_type)


def verify_tenant(storage, payload: dict[str, Any]) -> dict[str, Any]:
    tenant_id = payload["tenant_id"]
    exported = export_tenant(storage, tenant_id)
    expected = _counts(payload)
    actual = _counts(exported)
    expected_checksums = payload.get("checksums") or _checksums(payload)
    actual_checksums = exported.get("checksums") or _checksums(exported)
    checksum_ok = expected_checksums == actual_checksums
    return {
        "tenant_id": tenant_id,
        "ok": expected == actual and checksum_ok,
        "expected": expected,
        "actual": actual,
        "checksums": {
            "ok": checksum_ok,
            "expected": expected_checksums,
            "actual": actual_checksums,
        },
    }


def cutover_plan(payload: dict[str, Any]) -> dict[str, Any]:
    state = new_migration(
        migration_id=str(payload.get("migration_id", f"{payload['tenant_id']}:cutover")),
        tenant_id=payload["tenant_id"],
        source_backend=str(payload.get("source_backend", "unknown")),
        target_backend=str(payload.get("target_backend", "unknown")),
    )
    return {
        "tenant_id": payload["tenant_id"],
        "migration": state.to_dict(),
        "steps": [
            phase.value
            for phase in MigrationPhase
            if phase not in {MigrationPhase.ROLLED_BACK, MigrationPhase.FAILED}
        ],
        "counts": _counts(payload),
    }


def execute_migration(
    tenant_id: str,
    source_backend: str,
    target_backend: str,
    *,
    source_url: str = "",
    target_url: str = "",
    source_sql_dsn: str = "",
    target_sql_dsn: str = "",
    state_path: str | Path,
    snapshot_path: str | Path,
    data_dir: str | Path = "data/migrations",
    inject_failure_phase: str | MigrationPhase | None = None,
) -> dict[str, Any]:
    """Run a resumable backfill, shadow-read, dual-write, cutover and verify.

    The state file is also consumed by TenantStorageManager, so future tenant
    requests use the mirror during ``dual_write`` and the target after
    ``cutover``. A failed verification, target preflight, or source-stability
    check automatically records failure and rollback.

    The persisted manifest intentionally contains no URLs or credentials. It
    binds a state file to one tenant and backend pair, while the snapshot
    fingerprint prevents a resumed process from silently changing its
    backfill baseline.
    """

    state_file = Path(state_path)
    snapshot_file = Path(snapshot_path)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    snapshot_file.parent.mkdir(parents=True, exist_ok=True)
    manifest = _migration_manifest(tenant_id, source_backend, target_backend)
    state = _load_or_create_migration_state(state_file, manifest)
    if isinstance(inject_failure_phase, MigrationPhase):
        failure_phase = inject_failure_phase
    elif inject_failure_phase is not None and str(inject_failure_phase).strip():
        failure_phase = MigrationPhase(str(inject_failure_phase).strip())
    else:
        failure_phase = None

    def maybe_inject_failure(phase: MigrationPhase) -> None:
        if failure_phase == phase:
            raise RuntimeError(f"deterministic migration fault injected at {phase.value}")

    def persist() -> None:
        state_file.write_text(
            json.dumps(state.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    source = create_storage(
        _profile(source_backend, source_url, source_sql_dsn),
        Path(data_dir) / "source",
    )
    target = create_storage(
        _profile(target_backend, target_url, target_sql_dsn),
        Path(data_dir) / "target",
    )
    try:
        persist()
        if state.phase == MigrationPhase.PREPARE:
            target_preflight = _target_tenant_is_empty(target, tenant_id)
            state.metadata["target_empty_preflight"] = target_preflight
            state.transition(MigrationPhase.BACKFILL, actor="migration-runner")
            persist()

        if state.phase == MigrationPhase.BACKFILL:
            maybe_inject_failure(MigrationPhase.BACKFILL)
            snapshot = export_tenant(source, tenant_id)
            fingerprint = _snapshot_fingerprint(snapshot)
            stored_fingerprint = state.metadata.get("snapshot_fingerprint")
            if stored_fingerprint and stored_fingerprint != fingerprint:
                raise RuntimeError("source snapshot differs from the immutable migration baseline")
            snapshot_file.write_text(
                json.dumps(snapshot, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            state.metadata.update(
                {
                    "snapshot_fingerprint": fingerprint,
                    "snapshot_counts": _counts(snapshot),
                }
            )
            import_tenant(target, snapshot)
            state.transition(MigrationPhase.SHADOW_READ, actor="migration-runner")
            persist()

        snapshot = _load_snapshot(snapshot_file, tenant_id, state)
        if state.phase == MigrationPhase.SHADOW_READ:
            maybe_inject_failure(MigrationPhase.SHADOW_READ)
            shadow = verify_tenant(target, snapshot)
            if not shadow["ok"]:
                raise RuntimeError(f"shadow-read mismatch: {shadow}")
            state.metadata["shadow_read"] = shadow
            state.transition(MigrationPhase.DUAL_WRITE, actor="migration-runner")
            persist()

        if state.phase == MigrationPhase.DUAL_WRITE:
            maybe_inject_failure(MigrationPhase.DUAL_WRITE)
            # Re-read the source at the end of dual-write. Import is
            # idempotent, and the persisted fingerprint becomes the exact
            # cutover baseline used by the final verification below.
            catch_up = export_tenant(source, tenant_id)
            import_tenant(target, catch_up)
            dual_write = verify_tenant(target, catch_up)
            if not dual_write["ok"]:
                raise RuntimeError(f"dual-write mismatch: {dual_write}")
            state.metadata.update(
                {
                    "dual_write": dual_write,
                    "cutover_snapshot_fingerprint": _snapshot_fingerprint(catch_up),
                    "cutover_snapshot_counts": _counts(catch_up),
                }
            )
            state.transition(MigrationPhase.CUTOVER, actor="migration-runner")
            state.metadata["cutover_backend"] = target_backend
            persist()

        if state.phase == MigrationPhase.CUTOVER:
            maybe_inject_failure(MigrationPhase.CUTOVER)
            state.transition(MigrationPhase.VERIFY, actor="migration-runner")
            persist()

        if state.phase == MigrationPhase.VERIFY:
            maybe_inject_failure(MigrationPhase.VERIFY)
            cutover_snapshot = export_tenant(source, tenant_id)
            expected_fingerprint = state.metadata.get("cutover_snapshot_fingerprint")
            if expected_fingerprint != _snapshot_fingerprint(cutover_snapshot):
                raise RuntimeError("source changed after the observed cutover baseline")
            verification = verify_tenant(target, cutover_snapshot)
            state.metadata["verification"] = verification
            if not verification["ok"]:
                raise RuntimeError(f"cutover verification mismatch: {verification}")
            state.transition(MigrationPhase.CLEANUP, actor="migration-runner")
            persist()
        if state.phase != MigrationPhase.CLEANUP:
            raise RuntimeError(f"migration stopped in unexpected phase: {state.phase.value}")
        return state.to_dict()
    except Exception as exc:
        state.fail(str(exc), actor="migration-runner")
        persist()
        state.rollback("automatic rollback after migration failure", actor="migration-runner")
        persist()
        raise
    finally:
        source.close()
        target.close()


def _migration_manifest(tenant_id: str, source_backend: str, target_backend: str) -> dict[str, Any]:
    """Return the credential-free identity persisted with a migration state."""

    payload = {
        "version": 1,
        "tenant_id": str(tenant_id),
        "source_backend": str(source_backend).lower(),
        "target_backend": str(target_backend).lower(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {**payload, "fingerprint": hashlib.sha256(encoded).hexdigest()}


def _load_or_create_migration_state(state_file: Path, manifest: dict[str, Any]) -> MigrationState:
    migration_id = f"{manifest['tenant_id']}:{manifest['source_backend']}:{manifest['target_backend']}"
    if not state_file.exists():
        return new_migration(
            migration_id,
            manifest["tenant_id"],
            manifest["source_backend"],
            manifest["target_backend"],
            metadata={"manifest": manifest},
        )
    try:
        state = MigrationState.from_dict(json.loads(state_file.read_text(encoding="utf-8")))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise RuntimeError(f"migration state is unreadable: {state_file}") from exc
    stored_manifest = state.metadata.get("manifest")
    if stored_manifest != manifest or state.migration_id != migration_id:
        raise RuntimeError("migration manifest is immutable and does not match this run")
    if state.phase in {MigrationPhase.CLEANUP, MigrationPhase.ROLLED_BACK, MigrationPhase.FAILED}:
        raise RuntimeError(f"migration state is terminal: {state.phase.value}; use a new state file")
    return state


def _snapshot_fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"tenant_id": payload.get("tenant_id"), "checksums": payload.get("checksums") or _checksums(payload)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_snapshot(snapshot_file: Path, tenant_id: str, state: MigrationState) -> dict[str, Any]:
    try:
        snapshot = json.loads(snapshot_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError("migration snapshot is required to resume after backfill") from exc
    if snapshot.get("tenant_id") != tenant_id:
        raise RuntimeError("migration snapshot tenant does not match the state manifest")
    if state.metadata.get("snapshot_fingerprint") != _snapshot_fingerprint(snapshot):
        raise RuntimeError("migration snapshot fingerprint does not match persisted state")
    return snapshot


def _target_tenant_is_empty(storage, tenant_id: str) -> dict[str, int]:
    counts = _counts(export_tenant(storage, tenant_id))
    nonempty = {name: count for name, count in counts.items() if count}
    if nonempty:
        raise RuntimeError(f"target tenant is not empty before backfill: {nonempty}")
    return counts


def _counts(payload: dict[str, Any]) -> dict[str, int]:
    return {
        "sessions": len(payload.get("sessions", [])),
        "events": sum(len(session.get("events", [])) for session in payload.get("sessions", [])),
        "memories": len(payload.get("memories", [])),
        "audit": len(payload.get("audit", [])),
        "idempotency": len(payload.get("idempotency", [])),
        "knowledge": len(payload.get("knowledge", [])),
        "artifacts": len(payload.get("artifacts", [])),
        "mailbox": len(payload.get("mailbox", [])),
        "session_mailboxes": len(
            payload.get("session_mailbox_v2", {}).get("mailboxes", [])
        ),
        "session_mailbox_items": len(
            payload.get("session_mailbox_v2", {}).get("items", [])
        ),
        "inbox": len(payload.get("inbox", [])),
        "outbox": len(payload.get("outbox", [])),
        "tool_approvals": len(payload.get("tool_approvals", [])),
        "tool_budgets": len(payload.get("tool_budgets", [])),
        "tool_executions": len(payload.get("tool_executions", [])),
    }


def _checksums(payload: dict[str, Any]) -> dict[str, str]:
    """Hash logical records so migration verification catches silent loss.

    Generated target-only fields such as summary version and retry attempt are
    intentionally excluded; logical content, event ordering, and artifact
    bytes remain part of the checksum.
    """
    sections: dict[str, Any] = {
        "sessions": payload.get("sessions", []),
        "memories": payload.get("memories", []),
        "audit": payload.get("audit", []),
        "idempotency": payload.get("idempotency", []),
        "knowledge": payload.get("knowledge", []),
        "artifacts": payload.get("artifacts", []),
        "mailbox": payload.get("mailbox", []),
        "session_mailbox_v2": payload.get("session_mailbox_v2", {}),
        "inbox": payload.get("inbox", []),
        "outbox": payload.get("outbox", []),
        "tool_approvals": payload.get("tool_approvals", []),
        "tool_budgets": payload.get("tool_budgets", []),
        "tool_executions": payload.get("tool_executions", []),
    }
    checksums: dict[str, str] = {}
    for name, value in sections.items():
        normalized = _normalize_checksum_value(value)
        encoded = json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        checksums[name] = hashlib.sha256(encoded).hexdigest()
    return checksums


def _normalize_checksum_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _normalize_checksum_value(item)
            for key, item in sorted(value.items())
            if key
            not in {
                "summary_version",
                "attempt",
                "path",
                "created_at",
                "updated_at",
            }
        }
    if isinstance(value, list):
        normalized = [_normalize_checksum_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        )
    return value


def _profile(backend: str, url: str, sql_dsn: str) -> StorageProfile:
    normalized = backend.lower()
    # Keep the migration source profile faithful to the selected backend.
    # Redis migrations must include Redis knowledge/artifact records instead
    # of silently reading local filesystem/vector stores.
    if normalized == "redis":
        knowledge_backend = "redis"
        artifact_backend = "redis"
    elif normalized in {"postgres", "postgresql"}:
        knowledge_backend = "postgres"
        artifact_backend = "object"
    else:
        knowledge_backend = "vector"
        artifact_backend = "object"
    return StorageProfile(
        session_backend=backend,
        memory_backend=backend,
        summary_backend=backend,
        audit_backend=backend,
        knowledge_backend=knowledge_backend,
        artifact_backend=artifact_backend,
        redis_url=url if backend == "redis" else "",
        sql_dsn=sql_dsn if backend in {"postgres", "postgresql"} else "",
    )


def ensure_postgres_schema(dsn: str) -> None:
    """Upgrade PostgreSQL to the latest versioned schema."""

    if not dsn.strip():
        raise PostgresRLSError("PostgreSQL schema migration requires a SQL DSN")
    try:
        upgrade_database(dsn)
    except DatabaseMigrationError as exc:
        raise PostgresRLSError(str(exc)) from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Export/import tenant structured state")
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export")
    export.add_argument("--tenant", required=True)
    export.add_argument("--output", required=True)
    export.add_argument("--backend", choices=["redis", "postgres", "sql"], required=True)
    export.add_argument("--redis-url", default="")
    export.add_argument("--sql-dsn", default="")
    export.add_argument("--data-dir", default="data")
    imp = sub.add_parser("import")
    imp.add_argument("--input", required=True)
    imp.add_argument("--backend", choices=["redis", "postgres", "sql"], required=True)
    imp.add_argument("--redis-url", default="")
    imp.add_argument("--sql-dsn", default="")
    imp.add_argument("--data-dir", default="data")
    verify = sub.add_parser("verify")
    verify.add_argument("--input", required=True)
    verify.add_argument("--backend", choices=["redis", "postgres", "sql"], required=True)
    verify.add_argument("--redis-url", default="")
    verify.add_argument("--sql-dsn", default="")
    verify.add_argument("--data-dir", default="data")
    plan = sub.add_parser("cutover-plan")
    plan.add_argument("--input", required=True)
    migration = sub.add_parser("migration-state")
    migration.add_argument("--mode", choices=["new", "transition"], required=True)
    migration.add_argument("--output", required=True)
    migration.add_argument("--input", default="")
    migration.add_argument("--migration-id", default="")
    migration.add_argument("--tenant", default="")
    migration.add_argument("--source-backend", default="")
    migration.add_argument("--target-backend", default="")
    migration.add_argument("--phase", default="")
    migration.add_argument("--actor", default="operator")
    migration.add_argument("--reason", default="")
    run_migration = sub.add_parser("migration-run")
    run_migration.add_argument("--tenant", required=True)
    run_migration.add_argument("--source-backend", choices=["memory", "redis", "postgres", "sql"], required=True)
    run_migration.add_argument("--target-backend", choices=["memory", "redis", "postgres", "sql"], required=True)
    run_migration.add_argument("--source-url", default="")
    run_migration.add_argument("--target-url", default="")
    run_migration.add_argument("--source-sql-dsn", default="")
    run_migration.add_argument("--target-sql-dsn", default="")
    run_migration.add_argument("--state-file", required=True)
    run_migration.add_argument("--snapshot", required=True)
    run_migration.add_argument("--data-dir", default="data/migrations")
    run_migration.add_argument(
        "--inject-failure-phase",
        choices=[phase.value for phase in MigrationPhase if phase not in {
            MigrationPhase.PREPARE,
            MigrationPhase.CLEANUP,
            MigrationPhase.FAILED,
            MigrationPhase.ROLLED_BACK,
        }],
        default="",
        help="deterministically fail after entering a phase; intended for rollback acceptance tests",
    )
    rls = sub.add_parser("rls")
    rls.add_argument("--sql-dsn", default="")
    rls.add_argument("--app-role", default="")
    rls.add_argument("--admin-role", default="")
    schema = sub.add_parser("schema")
    schema.add_argument("--backend", choices=["postgres", "sql", "redis"], required=True)
    schema.add_argument("--redis-url", default="")
    schema.add_argument("--sql-dsn", default="")
    database = sub.add_parser("db", help="manage the versioned PostgreSQL schema")
    database_sub = database.add_subparsers(dest="db_command", required=True)
    db_upgrade = database_sub.add_parser("upgrade")
    db_upgrade.add_argument("--sql-dsn", default="")
    db_upgrade.add_argument("--revision", default="head")
    db_current = database_sub.add_parser("current")
    db_current.add_argument("--sql-dsn", default="")
    db_check = database_sub.add_parser("check")
    db_check.add_argument("--sql-dsn", default="")
    db_downgrade = database_sub.add_parser("downgrade")
    db_downgrade.add_argument("--sql-dsn", default="")
    db_downgrade.add_argument("--revision", default="-1")
    db_downgrade.add_argument("--allow-destructive", action="store_true")
    args = parser.parse_args()
    if args.command == "db":
        try:
            if args.db_command == "upgrade":
                result = upgrade_database(args.sql_dsn, args.revision)
            elif args.db_command == "downgrade":
                result = downgrade_database(
                    args.sql_dsn,
                    args.revision,
                    allow_destructive=args.allow_destructive,
                )
            else:
                result = database_status(args.sql_dsn)
        except DatabaseMigrationError as exc:
            parser.error(str(exc))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if args.db_command == "check" and result["status"] != "pass":
            raise SystemExit(1)
        return
    if args.command == "migration-run":
        try:
            result = execute_migration(
                args.tenant,
                args.source_backend,
                args.target_backend,
                source_url=args.source_url,
                target_url=args.target_url,
                source_sql_dsn=args.source_sql_dsn,
                target_sql_dsn=args.target_sql_dsn,
                state_path=args.state_file,
                snapshot_path=args.snapshot,
                data_dir=args.data_dir,
                inject_failure_phase=args.inject_failure_phase,
            )
        except Exception as exc:
            parser.error(str(exc))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "migration-state":
        if args.mode == "new":
            if not all((args.migration_id, args.tenant, args.source_backend, args.target_backend)):
                parser.error(
                    "migration-state new requires --migration-id, --tenant, "
                    "--source-backend, --target-backend"
                )
            state = new_migration(
                args.migration_id,
                args.tenant,
                args.source_backend,
                args.target_backend,
            )
        else:
            if not args.input or not args.phase:
                parser.error("migration-state transition requires --input and --phase")
            state = MigrationState.from_dict(
                json.loads(Path(args.input).read_text(encoding="utf-8"))
            )
            if args.phase == MigrationPhase.FAILED.value:
                state.fail(args.reason or "migration failed", actor=args.actor)
            elif args.phase == MigrationPhase.ROLLED_BACK.value:
                state.rollback(args.reason or "operator rollback", actor=args.actor)
            else:
                state.transition(args.phase, actor=args.actor)
        Path(args.output).write_text(
            json.dumps(state.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(state.to_dict(), ensure_ascii=False, indent=2))
        return
    if args.command == "cutover-plan":
        print(
            json.dumps(
                cutover_plan(json.loads(Path(args.input).read_text(encoding="utf-8"))), ensure_ascii=False, indent=2
            )
        )
        return
    if args.command == "rls":
        dsn = args.sql_dsn or os.getenv("POSTGRES_SCHEMA_DSN", "") or os.getenv("POSTGRES_DSN", "")
        apply_rls_migration_dsn(
            dsn,
            app_role=args.app_role or None,
            admin_role=args.admin_role or None,
        )
        print(json.dumps({"status": "ready", "backend": "postgres", "rls": "enabled"}, ensure_ascii=False))
        return
    if args.command == "schema" and args.backend == "postgres":
        dsn = args.sql_dsn or os.getenv("POSTGRES_SCHEMA_DSN", "") or os.getenv("POSTGRES_DSN", "")
        ensure_postgres_schema(dsn)
        print(json.dumps({"status": "ready", "backend": "postgres"}, ensure_ascii=False))
        return
    storage = create_storage(
        _profile(args.backend, args.redis_url, args.sql_dsn),
        Path(args.data_dir),
    )
    try:
        if args.command == "schema":
            print(json.dumps({"status": "ready", "backend": args.backend}, ensure_ascii=False))
            return
        if args.command == "export":
            Path(args.output).write_text(
                json.dumps(export_tenant(storage, args.tenant), ensure_ascii=False, indent=2), encoding="utf-8"
            )
        elif args.command == "verify":
            print(
                json.dumps(
                    verify_tenant(storage, json.loads(Path(args.input).read_text(encoding="utf-8"))),
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            import_tenant(storage, json.loads(Path(args.input).read_text(encoding="utf-8")))
    finally:
        storage.close()


if __name__ == "__main__":
    main()

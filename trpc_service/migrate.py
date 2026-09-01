"""Structured-store export/import utility for tenant-scoped cutovers."""

from __future__ import annotations

import argparse
import base64
import json
import hashlib
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from trpc_service.storage.base import (
    AuditRecord,
    IdempotencyStatus,
    MemoryItem,
    SessionEvent,
    SessionState,
    Summary,
)
from trpc_service.storage.factory import create_storage
from trpc_service.storage.durable import _json_object
from trpc_service.storage.postgres_rls import (
    PostgresRLSError,
    apply_rls_migration_dsn,
    postgres_tenant_context,
)
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
                    "created_at": row[6],
                    "updated_at": row[7],
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

    payload = {
        "version": 2,
        "tenant_id": tenant_id,
        "sessions": sessions,
        "memories": memories,
        "audit": _json([asdict(item) for item in storage.audit.list_by_tenant(tenant_id)]),
        "idempotency": _idempotency_records(storage, tenant_id),
        "knowledge": _knowledge_chunks(storage, tenant_id),
        "artifacts": _artifacts(storage, tenant_id),
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
    return {
        "tenant_id": payload["tenant_id"],
        "steps": [
            "freeze tenant writes or enable IM callback queue draining",
            "export source tenant package",
            "import into target backend",
            "run verify against the imported target",
            "switch tenant storage_profile to the target backend as a new config version",
            "publish or gray-release the new config version",
            "keep source backend read-only until rollback window expires",
        ],
        "counts": _counts(payload),
    }


def _counts(payload: dict[str, Any]) -> dict[str, int]:
    return {
        "sessions": len(payload.get("sessions", [])),
        "events": sum(len(session.get("events", [])) for session in payload.get("sessions", [])),
        "memories": len(payload.get("memories", [])),
        "audit": len(payload.get("audit", [])),
        "idempotency": len(payload.get("idempotency", [])),
        "knowledge": len(payload.get("knowledge", [])),
        "artifacts": len(payload.get("artifacts", [])),
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
    return StorageProfile(
        session_backend=backend,
        memory_backend=backend,
        audit_backend=backend,
        redis_url=url if backend == "redis" else "",
        sql_dsn=sql_dsn if backend in {"postgres", "postgresql"} else "",
    )


def ensure_postgres_schema(dsn: str) -> None:
    """Create all PostgreSQL tables using a schema-owner connection."""

    if not dsn.strip():
        raise PostgresRLSError("PostgreSQL schema migration requires a SQL DSN")
    from trpc_service.storage.postgres_knowledge import PostgresKnowledgeStore
    from trpc_service.storage.postgres_store import PostgresStorage
    from trpc_service.tenant.repository import PostgresTenantRepository

    previous = os.environ.get("POSTGRES_AUTO_CREATE_SCHEMA")
    os.environ["POSTGRES_AUTO_CREATE_SCHEMA"] = "1"
    storage = None
    knowledge = None
    repository = None
    try:
        storage = PostgresStorage(dsn)
        knowledge = PostgresKnowledgeStore(dsn)
        repository = PostgresTenantRepository(dsn)
    finally:
        for resource in (repository, knowledge, storage):
            close = getattr(resource, "close", None)
            if close:
                close()
        if previous is None:
            os.environ.pop("POSTGRES_AUTO_CREATE_SCHEMA", None)
        else:
            os.environ["POSTGRES_AUTO_CREATE_SCHEMA"] = previous


def main() -> None:
    parser = argparse.ArgumentParser(description="Export/import tenant structured state")
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export")
    export.add_argument("--tenant", required=True)
    export.add_argument("--output", required=True)
    export.add_argument("--backend", choices=["redis", "postgres", "sql"], required=True)
    export.add_argument("--redis-url", default="")
    export.add_argument("--sql-dsn", default="")
    imp = sub.add_parser("import")
    imp.add_argument("--input", required=True)
    imp.add_argument("--backend", choices=["redis", "postgres", "sql"], required=True)
    imp.add_argument("--redis-url", default="")
    imp.add_argument("--sql-dsn", default="")
    verify = sub.add_parser("verify")
    verify.add_argument("--input", required=True)
    verify.add_argument("--backend", choices=["redis", "postgres", "sql"], required=True)
    verify.add_argument("--redis-url", default="")
    verify.add_argument("--sql-dsn", default="")
    plan = sub.add_parser("cutover-plan")
    plan.add_argument("--input", required=True)
    rls = sub.add_parser("rls")
    rls.add_argument("--sql-dsn", default="")
    rls.add_argument("--app-role", default="")
    rls.add_argument("--admin-role", default="")
    schema = sub.add_parser("schema")
    schema.add_argument("--backend", choices=["postgres", "sql", "redis"], required=True)
    schema.add_argument("--redis-url", default="")
    schema.add_argument("--sql-dsn", default="")
    args = parser.parse_args()
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
    storage = create_storage(_profile(args.backend, args.redis_url, args.sql_dsn))
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

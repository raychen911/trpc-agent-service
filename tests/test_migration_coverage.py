from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from trpc_service.database import DatabaseMigrationError
from trpc_service.migrate import (
    _artifacts,
    _checksums,
    _counts,
    _dt,
    _idempotency_records,
    _import_idempotency,
    _json,
    _knowledge_chunks,
    _load_or_create_migration_state,
    _load_snapshot,
    _migration_manifest,
    _normalize_checksum_value,
    _profile,
    _sessions,
    _snapshot_fingerprint,
    _target_tenant_is_empty,
    cutover_plan,
    ensure_postgres_schema,
    execute_migration,
    export_tenant,
    import_tenant,
    verify_tenant,
)
from trpc_service.migration_state import MigrationPhase
from trpc_service.storage.base import (
    AuditRecord,
    MemoryItem,
    SessionEvent,
    Summary,
    ToolExecution,
)
from trpc_service.storage.durable import InboxRecord, OutboxRecord
from trpc_service.storage.factory import create_storage
from trpc_service.storage.postgres_rls import PostgresRLSError
from trpc_service.storage.session_mailbox import (
    SessionMailbox,
    SessionMailboxItem,
    SessionMailboxStatus,
)
from trpc_service.storage.tool_governance import ToolApproval, ToolBudget
from trpc_service.storage.vector_store import KnowledgeChunk
from trpc_service.tenant.models import StorageProfile


def payload():
    return {
        "tenant_id": "tenant-a",
        "sessions": [{"session_id": "s", "events": [{"event_id": "e"}], "state": {}}],
        "memories": [{"memory_id": "m"}],
        "audit": [{"audit_id": "a"}],
        "idempotency": [{"key": "i"}],
        "knowledge": [{"chunk_id": "k"}],
        "artifacts": [{"object_id": "o"}],
        "mailbox": [{"message_id": "mb"}],
        "session_mailbox_v2": {"mailboxes": [{"session_id": "s"}], "items": [{"message_id": "x"}]},
        "inbox": [{"inbox_id": "in"}],
        "outbox": [{"event_id": "out"}],
        "tool_approvals": [{"approval_id": "ap"}],
        "tool_budgets": [{"request_id": "r"}],
        "tool_executions": [{"call_key": "c"}],
    }


def test_migration_identity_checksums_and_profiles():
    value = payload()
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    assert _json({"when": timestamp, "nested": [timestamp]})["when"].startswith("2026-")
    assert _dt(timestamp.isoformat()) == timestamp
    counts = _counts(value)
    assert counts["events"] == 1 and counts["session_mailboxes"] == 1
    checksums = _checksums(value)
    assert set(checksums) == {
        "sessions",
        "memories",
        "audit",
        "idempotency",
        "knowledge",
        "artifacts",
        "mailbox",
        "session_mailbox_v2",
        "inbox",
        "outbox",
        "tool_approvals",
        "tool_budgets",
        "tool_executions",
    }
    normalized = _normalize_checksum_value(
        {"updated_at": "ignored", "items": [{"attempt": 1, "value": 2}, {"value": 1}]}
    )
    assert normalized == {"items": [{"value": 1}, {"value": 2}]}
    assert _snapshot_fingerprint(value) == _snapshot_fingerprint({**value, "checksums": checksums})
    manifest = _migration_manifest("tenant-a", "SQL", "PostgreSQL")
    assert manifest["source_backend"] == "sql"
    assert len(manifest["fingerprint"]) == 64
    assert _profile("redis", "redis://localhost", "") .redis_url.startswith("redis://")
    assert _profile("postgresql", "", "dsn").knowledge_backend == "postgres"
    plan = cutover_plan({"tenant_id": "tenant-a", "source_backend": "memory", "target_backend": "sql"})
    assert plan["migration"]["phase"] == MigrationPhase.PREPARE.value
    assert "failed" not in plan["steps"] and "rolled_back" not in plan["steps"]


def test_migration_state_and_snapshot_fingerprints_are_immutable(tmp_path):
    manifest = _migration_manifest("tenant-a", "memory", "sql")
    state_file = tmp_path / "migration.json"
    state = _load_or_create_migration_state(state_file, manifest)
    assert state.metadata["manifest"] == manifest
    state_file.write_text(json.dumps(state.to_dict()), encoding="utf-8")
    loaded = _load_or_create_migration_state(state_file, manifest)
    assert loaded.migration_id == state.migration_id
    with pytest.raises(RuntimeError, match="manifest"):
        _load_or_create_migration_state(state_file, {**manifest, "target_backend": "redis"})
    state_file.write_text("{broken", encoding="utf-8")
    with pytest.raises(RuntimeError, match="unreadable"):
        _load_or_create_migration_state(state_file, manifest)

    snapshot = payload()
    snapshot_file = tmp_path / "snapshot.json"
    snapshot_file.write_text(json.dumps(snapshot), encoding="utf-8")
    state.metadata["snapshot_fingerprint"] = _snapshot_fingerprint(snapshot)
    assert _load_snapshot(snapshot_file, "tenant-a", state)["tenant_id"] == "tenant-a"
    with pytest.raises(RuntimeError, match="tenant"):
        _load_snapshot(snapshot_file, "tenant-b", state)
    state.metadata["snapshot_fingerprint"] = "wrong"
    with pytest.raises(RuntimeError, match="fingerprint"):
        _load_snapshot(snapshot_file, "tenant-a", state)
    snapshot_file.unlink()
    with pytest.raises(RuntimeError, match="required"):
        _load_snapshot(snapshot_file, "tenant-a", state)


def test_empty_target_guard_and_postgres_dsn_validation(tmp_path):
    profile = StorageProfile(
        session_backend="sql",
        memory_backend="sql",
        summary_backend="sql",
        audit_backend="sql",
        knowledge_backend="vector",
        artifact_backend="object",
    )
    storage = create_storage(profile, tmp_path / "target")
    try:
        assert _target_tenant_is_empty(storage, "tenant-a")["events"] == 0
        storage.memory.put(__import__("trpc_service.storage.base", fromlist=["MemoryItem"]).MemoryItem(
            "tenant-a", "m", "session", "content"
        ))
        with pytest.raises(RuntimeError, match="not empty"):
            _target_tenant_is_empty(storage, "tenant-a")
    finally:
        storage.close()
    with pytest.raises(PostgresRLSError, match="SQL DSN"):
        ensure_postgres_schema(" ")


def test_migration_state_rejects_terminal_and_malformed_records(tmp_path):
    manifest = _migration_manifest("tenant-a", "memory", "sql")
    state_file = tmp_path / "migration.json"
    state = _load_or_create_migration_state(state_file, manifest)
    state.transition(MigrationPhase.BACKFILL, actor="test")
    state.transition(MigrationPhase.SHADOW_READ, actor="test")
    state.transition(MigrationPhase.DUAL_WRITE, actor="test")
    state.transition(MigrationPhase.CUTOVER, actor="test")
    state.transition(MigrationPhase.VERIFY, actor="test")
    state.transition(MigrationPhase.CLEANUP, actor="test")
    state_file.write_text(json.dumps(state.to_dict()), encoding="utf-8")
    with pytest.raises(RuntimeError, match="terminal"):
        _load_or_create_migration_state(state_file, manifest)
    state_file.write_text("[]", encoding="utf-8")
    with pytest.raises(RuntimeError, match="unreadable"):
        _load_or_create_migration_state(state_file, manifest)


@pytest.mark.parametrize("phase", [
    MigrationPhase.BACKFILL,
    MigrationPhase.SHADOW_READ,
    MigrationPhase.DUAL_WRITE,
    MigrationPhase.CUTOVER,
])
def test_migration_injected_phase_is_persisted_and_rolled_back(tmp_path, phase):
    with pytest.raises(RuntimeError, match="deterministic migration fault"):
        from trpc_service.migrate import execute_migration

        execute_migration(
            "tenant-a",
            "sql",
            "sql",
            state_path=tmp_path / "migration.json",
            snapshot_path=tmp_path / "snapshot.json",
            data_dir=tmp_path / "data",
            inject_failure_phase=phase,
        )
    state = json.loads((tmp_path / "migration.json").read_text(encoding="utf-8"))
    assert state["phase"] == MigrationPhase.ROLLED_BACK.value
    assert ("failed", "rolled_back") in {
        (item["from"], item["to"]) for item in state["history"]
    }


def test_execute_migration_completes_empty_memory_cutover(tmp_path):
    result = execute_migration(
        "tenant-a",
        "memory",
        "memory",
        state_path=tmp_path / "migration.json",
        snapshot_path=tmp_path / "snapshot.json",
        data_dir=tmp_path / "data",
    )
    assert result["phase"] == MigrationPhase.CLEANUP.value
    assert result["metadata"]["verification"]["ok"] is True
    assert json.loads((tmp_path / "snapshot.json").read_text(encoding="utf-8"))["tenant_id"] == "tenant-a"


def _rich_storage(root):
    storage = create_storage(_profile("memory", "", ""), root)
    storage.session.append_event(
        SessionEvent(
            "tenant-a", "session-a", "event-a", "user_message", {"text": "hello"}, "trace-a", "idem-a"
        )
    )
    storage.session.restore_state("tenant-a", "session-a", {"answer": "world"}, 2)
    storage.summary.put(Summary("tenant-a", "session-a", "summary", 1))
    storage.memory.put(MemoryItem("tenant-a", "memory-a", "session-a", "remember this", {"kind": "fact"}))
    storage.audit.append(AuditRecord("audit-a", "tenant-a", "allow", "trace-a", channel="web"))
    storage.idempotency.start("tenant-a", "idem-complete", "trace-a")
    storage.idempotency.complete("tenant-a", "idem-complete", "response-a", {"ok": True})
    storage.idempotency.start("tenant-a", "idem-failed", "trace-a")
    storage.idempotency.fail("tenant-a", "idem-failed", "provider_error")
    storage.knowledge.upsert(KnowledgeChunk("tenant-a", "default", "chunk-a", "hello world", {"source": "test"}))
    storage.artifacts.put_with_id("tenant-a", "object-a", b"artifact", "text/plain")
    storage.mailbox.enqueue("tenant-a", "session-a", "message-a", "dedupe-a", {"text": "hello"})

    storage.session_mailbox_v2.restore_export(
        {
            "mailboxes": [
                asdict(
                    SessionMailbox(
                        "tenant-a", "session-a", status=SessionMailboxStatus.QUEUED,
                        accepted_sequence=1, queue_generation=1,
                    )
                )
            ],
            "items": [
                asdict(SessionMailboxItem("tenant-a", "session-a", 1, "message-a", "trace-a"))
            ],
        }
    )
    storage.inbox_outbox.restore_inbox(
        InboxRecord("message-a", "tenant-a", "inbox-a", "session-a", {"text": "hello"})
    )
    storage.inbox_outbox.restore_outbox(
        OutboxRecord("outbox-a", "tenant-a", "reply", "session-a", {"text": "ok"})
    )
    storage.tool_governance.restore_approval(
        ToolApproval("tenant-a", "approval-a", "session-a", "request-a", "send", "args-hash")
    )
    storage.tool_governance.restore_budget(ToolBudget("tenant-a", "request-a"))
    storage.tool_governance.restore_execution(
        ToolExecution("tenant-a", "execution-a", "request-a", "session-a", "send", "call-a", "args-hash")
    )
    return storage


def test_export_import_roundtrip_restores_every_optional_store(tmp_path):
    source = _rich_storage(tmp_path / "source")
    target = _rich_storage(tmp_path / "seed")
    try:
        payload = export_tenant(source, "tenant-a")
        target.close()
        target = create_storage(_profile("memory", "", ""), tmp_path / "target")
        import_tenant(target, payload)
        result = verify_tenant(target, payload)
        assert result["ok"] is True
        assert result["actual"] == result["expected"]
        assert target.artifacts.get("tenant-a", "object-a") == b"artifact"
    finally:
        source.close()
        target.close()


def test_import_is_idempotent_for_audit_and_idempotency_states(tmp_path):
    storage = create_storage(_profile("memory", "", ""), tmp_path / "target")
    try:
        raw = {
            "tenant_id": "tenant-a", "key": "processing", "status": "processing", "trace_id": "trace"
        }
        _import_idempotency(storage, raw)
        assert storage.idempotency.get("tenant-a", "processing").status.value == "processing"
        _import_idempotency(storage, {**raw, "key": "completed", "status": "completed", "result": {"ok": 1}})
        _import_idempotency(storage, {**raw, "key": "failed", "status": "failed", "result": {"error_type": "bad"}})
        assert storage.idempotency.get("tenant-a", "completed").result == {"ok": 1}
        assert storage.idempotency.get("tenant-a", "failed").result == {"error_type": "bad"}

        record = AuditRecord("audit-a", "tenant-a", "allow", "trace")

        class DuplicateAudit:
            def __init__(self):
                self.records = []

            def append(self, value):
                if any(item.audit_id == value.audit_id for item in self.records):
                    raise RuntimeError("duplicate audit")
                self.records.append(value)

            def list_by_tenant(self, tenant_id, limit=100):
                return [item for item in self.records if item.tenant_id == tenant_id][:limit]

        audit = DuplicateAudit()
        raw_record = {**asdict(record), "created_at": record.created_at.isoformat()}
        import_tenant(SimpleNamespace(audit=audit), {"tenant_id": "tenant-a", "audit": [raw_record]})
        import_tenant(SimpleNamespace(audit=audit), {"tenant_id": "tenant-a", "audit": [raw_record]})
        assert len(audit.records) == 1
    finally:
        storage.close()


def test_import_artifact_fallback_and_missing_optional_adapters():
    calls = []

    class PutOnlyArtifacts:
        def put(self, tenant_id, content, content_type):
            calls.append((tenant_id, content, content_type))

    import_tenant(
        SimpleNamespace(artifacts=PutOnlyArtifacts()),
        {
            "tenant_id": "tenant-a",
            "artifacts": [{"object_id": "object-a", "content_base64": "Ymlu", "content_type": "text/plain"}],
        },
    )
    assert calls == [("tenant-a", b"bin", "text/plain")]
    assert _knowledge_chunks(SimpleNamespace(knowledge=object()), "tenant-a") == []
    assert _artifacts(SimpleNamespace(artifacts=object()), "tenant-a") == []


class _FakeRedis:
    def __init__(self, values):
        self.values = values

    def scan_iter(self, pattern):
        prefix = pattern.removesuffix("*")
        return [key for key in self.values if key.startswith(prefix)]

    def get(self, key):
        return self.values[key]


def test_export_helpers_cover_redis_and_sql_backends():
    redis_client = _FakeRedis({
        "session-events:tenant-a:session-b": b"unused",
        "idempotency:tenant-a:key-b": b'{"tenant_id":"tenant-a","key":"key-b","status":"completed"}',
    })
    structured = SimpleNamespace(
        backend_name="redis",
        client=redis_client,
        _key=lambda *parts: ":".join(str(part) for part in parts if part != ""),
    )
    redis_store = SimpleNamespace(client=redis_client, _key=structured._key)
    redis_storage = SimpleNamespace(structured=structured, idempotency=redis_store)
    assert _sessions(redis_storage, "tenant-a") == ["session-b"]
    assert _idempotency_records(redis_storage, "tenant-a")[0]["key"] == "key-b"

    class Cursor:
        def __init__(self, rows):
            self.rows = rows
            self.query = ""

        def execute(self, query, params):
            self.query = str(query)

        def fetchall(self):
            return self.rows

        def close(self):
            pass

    class Connection:
        def __init__(self, rows):
            self.rows = rows

        def cursor(self):
            return Cursor(self.rows)

    sql_structured = SimpleNamespace(backend_name="sql", _conn=Connection([("session-a",)]))
    sql_store = SimpleNamespace(
        backend_name="sql",
        _conn=Connection([("tenant-a", "key-a", "completed", "ref", '{"ok":true}', "trace", None, None)]),
    )
    sql_storage = SimpleNamespace(structured=sql_structured, idempotency=sql_store)
    assert _sessions(sql_storage, "tenant-a") == ["session-a"]
    assert _idempotency_records(sql_storage, "tenant-a")[0]["result"] == {"ok": True}


def test_verify_reports_checksum_mismatch_and_schema_wraps_database_error():
    storage = create_storage(_profile("memory", "", ""), "data")
    try:
        payload_value = {"tenant_id": "tenant-a", "checksums": {"audit": "wrong"}}
        result = verify_tenant(storage, payload_value)
        assert result["ok"] is False
        with patch("trpc_service.migrate.upgrade_database", side_effect=DatabaseMigrationError("db unavailable")):
            with pytest.raises(PostgresRLSError, match="db unavailable"):
                ensure_postgres_schema("postgresql://db")
    finally:
        storage.close()

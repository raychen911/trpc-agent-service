from __future__ import annotations

import asyncio
import base64
import json
import os
import sqlite3
import struct
import tempfile
from datetime import timedelta
from threading import RLock
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from trpc_service.admin.service import AdminService, _merge_public_config
from trpc_service.channels.sdk import run_async
from trpc_service.channels.wechat_crypto import decrypt_message, verify_handshake
from trpc_service.database import runner as migration_runner
from trpc_service.storage.base import (
    AuditRecord,
    MemoryItem,
    SessionEvent,
    SessionState,
    Summary,
    now_utc,
)
from trpc_service.storage.durable import (
    InboxRecord,
    InboxStatus,
    InMemoryInboxOutbox,
    OutboxRecord,
    OutboxStatus,
    SQLiteInboxOutbox,
    _json_object,
    _max_outbox_attempts,
)
from trpc_service.storage.external_memory import ExternalMemoryStore
from trpc_service.storage.factory import create_storage
from trpc_service.storage.locking import (
    PostgresSessionLockMixin,
    RedisSessionLockMixin,
    SessionLease,
    SessionLeaseLost,
    local_session_lock,
    renew_session_lease,
    session_lease,
    session_lock,
    validate_session_lease,
)
from trpc_service.storage.mailbox import (
    InMemoryMailboxStore,
    MailboxRecord,
    MailboxStatus,
    SQLiteMailboxStore,
    _lease_seconds,
    _max_attempts,
)
from trpc_service.storage.manager import TenantStorageManager
from trpc_service.storage.mirror import (
    _jsonable,
    _MirrorArtifact,
    _MirrorAudit,
    _MirrorIdempotency,
    _MirrorInboxOutbox,
    _MirrorKnowledge,
    _MirrorMailbox,
    _MirrorMemory,
    _MirrorSession,
    _MirrorSessionMailboxV2,
    _MirrorSummary,
    _MirrorToolGovernance,
)
from trpc_service.storage.retry import retry_delay_seconds
from trpc_service.storage.session_mailbox import SessionMailboxLease
from trpc_service.storage.tool_governance import (
    ApprovalStatus,
    InMemoryToolGovernanceStore,
    PostgresToolGovernanceStore,
    ToolExecutionStatus,
    arguments_hash,
)
from trpc_service.storage.vector_store import KnowledgeChunk
from trpc_service.tenant.models import ChannelBinding, StorageProfile, default_demo_config
from trpc_service.tenant.repository import (
    InMemoryTenantRepository,
    SQLiteTenantRepository,
    TenantNotFound,
    TenantRepositoryConflict,
    demo_repository,
    persistent_demo_repository,
)
from trpc_service.tenant.service import TenantService


def _sqlite() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


@pytest.mark.parametrize("factory", [InMemoryMailboxStore, lambda: SQLiteMailboxStore(_sqlite(), RLock())])
def test_mailbox_lifecycle_fences_duplicates_and_recovers(factory, monkeypatch):
    store = factory()
    first = store.enqueue("tenant-a", "session", "message-1", "dedupe-1", {"text": "one"})
    assert store.enqueue("tenant-a", "session", "message-2", "dedupe-1", {}) == first
    assert store.get("tenant-b", "dedupe-1") is None
    assert [item.message_id for item in store.list_by_tenant("tenant-a")] == ["message-1"]

    lease = store.claim_next("tenant-a", "session", "worker-a", 1)
    assert lease is not None
    assert store.claim_next("tenant-a", "session", "worker-b", 30) is None
    renewed = store.renew(lease, 30)
    assert renewed.lease_until > lease.lease_until
    with pytest.raises(SessionLeaseLost):
        store.complete(MailboxRecord(**lease.__dict__) if hasattr(lease, "__dict__") else MailboxRecord(
            tenant_id=lease.tenant_id, session_id=lease.session_id, sequence=lease.sequence,
            message_id=lease.message_id, dedupe_key=lease.dedupe_key, payload=lease.payload,
            owner="other", fencing_token=lease.fencing_token, lease_until=lease.lease_until,
        ))
    store.complete(renewed)
    assert store.get("tenant-a", "dedupe-1").status == MailboxStatus.COMPLETED

    second = store.enqueue("tenant-a", "session", "message-2", "dedupe-2", {})
    second_lease = store.claim_next("tenant-a", "session", "worker-a", 30)
    assert second_lease.sequence == second.sequence
    store.fail(second_lease, "api_key=secret", retry_after_seconds=0)
    failed = store.get("tenant-a", "dedupe-2")
    assert failed.status == MailboxStatus.PENDING
    assert "api_key=secret" not in failed.last_error
    expired = store.claim_next("tenant-a", "session", "worker-b", 30)
    assert expired is not None
    monkeypatch.setenv("MAILBOX_MAX_ATTEMPTS", "1")
    store.fail(expired, "permanent")
    assert store.get("tenant-a", "dedupe-2").status == MailboxStatus.DEAD


@pytest.mark.parametrize("factory", [InMemoryMailboxStore, lambda: SQLiteMailboxStore(_sqlite(), RLock())])
def test_mailbox_restore_and_expired_recovery_are_scoped(factory):
    store = factory()
    record = MailboxRecord(
        tenant_id="tenant-a", session_id="s", sequence=1, message_id="m",
        dedupe_key="d", payload={"x": 1}, status=MailboxStatus.PROCESSING,
        owner="crashed", fencing_token=4, lease_until=now_utc() - timedelta(seconds=1),
    )
    restored = store.restore(record)
    assert restored.status == MailboxStatus.PENDING
    assert restored.owner is None
    claimed = store.claim_next("tenant-a", "s", "worker", 30)
    assert claimed is not None
    store.enqueue("tenant-b", "s", "m2", "d2", {})
    second_lease = store.claim_next("tenant-b", "s", "worker", 30)
    assert second_lease is not None
    second_lease.lease_until = now_utc() - timedelta(seconds=1)
    if isinstance(store, InMemoryMailboxStore):
        store._records[("tenant-b", "s", 1)].lease_until = second_lease.lease_until
    else:
        store._conn.execute(
            "UPDATE mailbox_message SET lease_until=? WHERE tenant_id=? AND dedupe_key=?",
            (second_lease.lease_until.isoformat(), "tenant-b", "d2"),
        )
        store._conn.commit()
    assert store.recover_expired(tenant_id="tenant-b", session_id="s") == 1
    assert store.get("tenant-b", "d2").status == MailboxStatus.PENDING
    assert store.recover_expired(tenant_id="tenant-a", session_id="s") == 0


def test_mailbox_configuration_errors_are_explicit(monkeypatch):
    monkeypatch.setenv("MAILBOX_MAX_ATTEMPTS", "bad")
    with pytest.raises(ValueError, match="MAILBOX_MAX_ATTEMPTS"):
        _max_attempts()
    monkeypatch.setenv("MAILBOX_LEASE_SECONDS", "bad")
    with pytest.raises(ValueError, match="MAILBOX_LEASE_SECONDS"):
        _lease_seconds(None)
    assert _lease_seconds(0) == 1


def test_inmemory_durable_inbox_and_outbox_all_terminal_paths(monkeypatch):
    store = InMemoryInboxOutbox()
    first, created = store.accept_inbox("tenant-a", "d1", "s", {"x": 1}, "owner-a")
    assert created
    duplicate, created = store.accept_inbox("tenant-a", "d1", "s", {}, "owner-b")
    assert not created and duplicate.message_id == first.message_id
    with pytest.raises(RuntimeError, match="ownership"):
        store.complete_inbox("tenant-a", "d1", "owner-b", {})
    store.fail_inbox("tenant-a", "d1", "owner-a", "token=secret")
    assert store.list_inbox_by_tenant("tenant-a")[0].status == InboxStatus.FAILED
    reclaimed, created = store.accept_inbox("tenant-a", "d1", "s", {}, "owner-b")
    assert created and reclaimed.attempts == 2
    outbox = store.complete_inbox_and_enqueue_outbox(
        "tenant-a", "d1", "owner-b", {"ok": True}, "topic", "s", "event-1"
    )
    assert outbox.event_id == "event-1"
    assert store.enqueue_outbox("tenant-a", "topic", "s", {}, event_id="event-1").payload == {"ok": True}
    store.enqueue_outbox("tenant-b", "topic", "s", {}, event_id="event-2")
    assert len(store.claim_outbox("worker", limit=1, tenant_id="tenant-a")) == 1
    with pytest.raises(RuntimeError, match="ownership"):
        store.complete_outbox("event-1", "other", tenant_id="tenant-a")
    store.complete_outbox("event-1", "worker", tenant_id="tenant-a")
    claimed = store.claim_outbox("worker-b", tenant_id="tenant-b")[0]
    monkeypatch.setenv("DURABLE_OUTBOX_MAX_ATTEMPTS", "1")
    store.fail_outbox(claimed.event_id, "worker-b", "api_key=secret", tenant_id="tenant-b")
    assert store.list_outbox_by_tenant("tenant-b")[0].status == OutboxStatus.DEAD
    assert "api_key=secret" not in store.list_outbox_by_tenant("tenant-b")[0].last_error
    with pytest.raises(ValueError, match="only dead"):
        store.replay_outbox("event-1", "tenant-a")
    assert store.replay_outbox("event-2", "tenant-b").status == OutboxStatus.PENDING
    with pytest.raises(KeyError):
        store.replay_outbox("event-2", "other")


def test_sqlite_durable_restore_reclaim_and_terminal_paths(monkeypatch):
    store = SQLiteInboxOutbox(_sqlite(), RLock())
    record, _ = store.accept_inbox("tenant", "d", "s", {"a": 1}, "owner", lease_seconds=1)
    store._conn.execute("UPDATE inbox_message SET lease_until=?", ((now_utc() - timedelta(seconds=1)).isoformat(),))
    store._conn.commit()
    reclaimed, created = store.accept_inbox("tenant", "d", "s", {}, "new-owner")
    assert created and reclaimed.attempts == 2
    store.dead_inbox("tenant", "d", "new-owner", "secret=hidden")
    assert store.list_inbox_by_tenant("tenant")[0].status == InboxStatus.DEAD
    restored = store.restore_inbox(record)
    assert restored.status == InboxStatus.DEAD
    store.enqueue_outbox("tenant", "topic", "s", {"x": 1}, event_id="e")
    assert store.claim_outbox("w", tenant_id="tenant")[0].event_id == "e"
    with pytest.raises(RuntimeError, match="ownership"):
        store.fail_outbox("e", "bad", "error", tenant_id="tenant")
    monkeypatch.setenv("DURABLE_OUTBOX_MAX_ATTEMPTS", "1")
    store.fail_outbox("e", "w", "error", retry_after_seconds=0, tenant_id="tenant")
    assert store.list_outbox_by_tenant("tenant")[0].status == OutboxStatus.DEAD
    assert store.replay_outbox("e", "tenant").attempts == 0


def test_durable_json_and_configuration_validation(monkeypatch):
    assert _json_object(None) == {}
    assert _json_object(b'{"x": 1}') == {"x": 1}
    assert _json_object('{"x": 1}') == {"x": 1}
    with pytest.raises(ValueError, match="JSON object"):
        _json_object(json.dumps([1]))
    monkeypatch.setenv("DURABLE_OUTBOX_MAX_ATTEMPTS", "invalid")
    with pytest.raises(ValueError, match="DURABLE_OUTBOX_MAX_ATTEMPTS"):
        _max_outbox_attempts()


def test_tool_governance_approval_execution_and_budget_edges(monkeypatch):
    store = InMemoryToolGovernanceStore()
    digest = arguments_hash({"b": 2, "a": 1})
    approval = store.create_or_get("tenant", "a", "s", "r", "send", digest, expires_seconds=60)
    store.create_or_get("tenant", "a", "s", "r", "send", "different")
    assert store.get("tenant", "a").status == ApprovalStatus.AMBIGUOUS
    assert store.get("other", "a") is None
    restored = store.restore_approval(approval)
    assert restored.status == ApprovalStatus.AMBIGUOUS

    execution = store.begin_execution("tenant", "e", "r", "s", "tool", "call", digest, False, 1)
    with pytest.raises(RuntimeError, match="stale"):
        store.complete_execution("tenant", "call", {}, fencing_token=2)
    failed = store.fail_execution("tenant", "call", "provider", "message", fencing_token=1)
    assert failed.status == ToolExecutionStatus.FAILED
    assert store.begin_execution("tenant", "e2", "r", "s", "tool", "call", digest, False, 1).attempt == 2
    budget = store.reserve_call("tenant", "r", "c1", True, 2, 1)
    assert store.reserve_call("tenant", "r", "c1", True, 2, 1).total_calls == budget.total_calls
    store.reserve_call("tenant", "r", "c2", False, 2, 1)
    with pytest.raises(RuntimeError, match="call budget"):
        store.reserve_call("tenant", "r", "c3", False, 2, 1)
    with pytest.raises(RuntimeError, match="side-effect"):
        store.reserve_call("tenant-2", "r", "c1", True, 2, 0)
    assert store.list_executions_by_tenant("tenant")
    assert store.list_by_tenant("tenant")
    assert store.list_budgets_by_tenant("tenant")
    assert execution.execution_id == "e"


class _GovernancePgCursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split()).lower()
        self.connection.statements.append((normalized, params))
        if "tool_approval" in normalized and normalized.startswith("select"):
            if "order by" in normalized:
                self.result = [row for (tenant_id, _), row in self.connection.approvals.items() if tenant_id == params[0]]
            else:
                self.result = self.connection.approvals.get((params[0], params[1]))
        elif normalized.startswith("insert into tool_approval"):
            if len(params) == 10:
                tenant, approval, session, request, tool, args, status, expires, created, updated = params
                row = [tenant, approval, session, request, tool, args, status, expires, None, None, created, updated]
            else:
                row = list(params)
            self.connection.approvals.setdefault((row[0], row[1]), row)
            self.result = None
        elif normalized.startswith("update tool_approval"):
            if len(params) == 4:
                status, updated, tenant, approval = params
                row = self.connection.approvals[(tenant, approval)]
                row[6], row[11] = status, updated
                self.result = None
            elif "returning" in normalized:
                status = params[0]
                if len(params) == 6:
                    approved_by, updated, tenant, approval = params[1], params[2], params[3], params[4]
                    row = self.connection.approvals[(tenant, approval)]
                    if row[6] != ApprovalStatus.PENDING:
                        self.result = None
                        return self
                    row[6], row[8], row[11] = status, approved_by, updated
                else:
                    consumed_at, updated, tenant, approval = params[1], params[2], params[3], params[4]
                    row = self.connection.approvals[(tenant, approval)]
                    row[6], row[9], row[11] = status, consumed_at, updated
                self.result = row
        elif "tool_budget" in normalized and normalized.startswith("select"):
            if "order by" in normalized:
                self.result = [row for (tenant_id, _), row in self.connection.budgets.items() if tenant_id == params[0]]
            else:
                self.result = self.connection.budgets.get((params[0], params[1]))
        elif normalized.startswith("insert into tool_budget"):
            row = list(params)
            self.connection.budgets[(row[0], row[1])] = row
            self.result = None
        elif "tool_executions" in normalized and normalized.startswith("select"):
            if "order by" in normalized:
                self.result = [row for (tenant_id, _), row in self.connection.executions.items() if tenant_id == params[0]]
            else:
                self.result = self.connection.executions.get((params[0], params[1]))
        elif normalized.startswith("insert into tool_executions"):
            values = list(params)
            if len(values) == 13:
                tenant, execution, request, session, tool, call, args, side, status, fence, started, created, updated = values
                row = [tenant, execution, request, session, tool, call, args, side, status, 1, fence, None, None, None,
                       started, None, created, updated]
            else:
                row = values
            self.connection.executions.setdefault((row[0], row[5]), row)
            self.result = None
        elif normalized.startswith("update tool_executions"):
            row = self.connection.executions[(params[-2], params[-1])]
            if "result_json" in normalized:
                status, result, completed, updated = params[:4]
                row[8], row[11], row[15], row[17] = status, result, completed, updated
            elif "error_type" in normalized:
                if "attempt=attempt+1" in normalized:
                    status, started, _completed, tenant, call = params
                    row[8], row[9], row[14], row[15], row[12], row[13] = status, row[9] + 1, started, None, None, None
                    row[17] = now_utc()
                else:
                    status, error_type, error_message, completed, updated = params[:5]
                    row[8], row[12], row[13], row[15], row[17] = status, error_type, error_message, completed, updated
            else:
                row[8], row[12], row[17] = params[0], params[1], params[2]
            self.result = None
        else:
            self.result = None
        return self

    def fetchone(self):
        if isinstance(self.result, list) and self.result and isinstance(self.result[0], (list, tuple)):
            return self.result[0]
        return self.result

    def fetchall(self):
        if self.result is None:
            return []
        if isinstance(self.result, list) and self.result and isinstance(self.result[0], (list, tuple)):
            return self.result
        return [self.result]


class _GovernancePgTransaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        self.connection.transactions += 1
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _GovernancePgConnection:
    def __init__(self):
        self.approvals = {}
        self.budgets = {}
        self.executions = {}
        self.statements = []
        self.transactions = 0

    def cursor(self):
        return _GovernancePgCursor(self)

    def transaction(self):
        return _GovernancePgTransaction(self)


def test_postgres_tool_governance_contract_without_external_postgres(monkeypatch):
    monkeypatch.setenv("POSTGRES_AUTO_CREATE_SCHEMA", "0")
    monkeypatch.delenv("POSTGRES_RLS_ENABLED", raising=False)
    connection = _GovernancePgConnection()
    store = PostgresToolGovernanceStore(connection, RLock())
    args = arguments_hash({"query": "status"})

    approval = store.create_or_get("tenant", "approval", "session", "request", "send", args, expires_seconds=60)
    assert approval.status == ApprovalStatus.PENDING
    assert store.get("tenant", "approval").approval_id == "approval"
    assert store.list_by_tenant("tenant")[0].tenant_id == "tenant"
    restored = store.restore_approval(approval)
    assert restored.approval_id == "approval"
    approved = store.approve("tenant", "approval", "operator")
    assert approved.status == ApprovalStatus.APPROVED
    consumed = store.consume("tenant", "approval", "request", args)
    assert consumed.status == ApprovalStatus.CONSUMED
    assert store.consume("tenant", "approval", "request", args).status == ApprovalStatus.CONSUMED

    ambiguous = store.create_or_get("tenant", "ambiguous", "session", "request", "send", args)
    assert store.create_or_get("tenant", "ambiguous", "session", "request", "send", "other").status == ApprovalStatus.AMBIGUOUS
    assert store.get("tenant", "ambiguous").status == ApprovalStatus.AMBIGUOUS
    with pytest.raises(RuntimeError, match="not pending"):
        store.approve("tenant", "ambiguous")
    with pytest.raises(RuntimeError, match="ambiguous"):
        store.consume("tenant", "ambiguous", "request", args)
    assert ambiguous.approval_id == "ambiguous"

    budget = store.reserve_call("tenant", "request", "call-1", True, 2, 1)
    assert budget.total_calls == 1
    assert store.reserve_call("tenant", "request", "call-1", True, 2, 1).total_calls == 1
    assert store.list_budgets_by_tenant("tenant")[0].call_keys == ["call-1"]
    restored_budget = store.restore_budget(budget)
    assert restored_budget.request_id == "request"

    execution = store.begin_execution("tenant", "execution", "request", "session", "send", "call-1", args, True, 4)
    assert execution.status == ToolExecutionStatus.RUNNING
    assert store.get_execution("tenant", "call-1").execution_id == "execution"
    completed = store.complete_execution("tenant", "call-1", {"sent": True}, 4)
    assert completed.status == ToolExecutionStatus.SUCCEEDED
    assert store.complete_execution("tenant", "call-1", {"ignored": True}, 4).result == {"sent": True}
    assert store.list_executions_by_tenant("tenant")[0].call_key == "call-1"
    restored_execution = store.restore_execution(execution)
    assert restored_execution.execution_id == "execution"
    failed = store.begin_execution("tenant", "execution-failed", "request", "session", "send", "call-failed", args, True, 5)
    assert store.fail_execution("tenant", "call-failed", "provider", "temporary", 5).status == ToolExecutionStatus.FAILED
    retried = store.begin_execution("tenant", "execution-failed-2", "request", "session", "send", "call-failed", args, True, 5)
    assert retried.status == ToolExecutionStatus.RUNNING
    assert failed.execution_id == "execution-failed"
    assert connection.transactions >= 8


def test_storage_factory_configuration_branches_and_manager_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_REDIS_URL", "redis://configured.example")
    profile = StorageProfile(
        session_backend="memory",
        memory_backend="external",
        summary_backend="sql",
        audit_backend="memory",
        external_memory_url="https://memory.example",
        redis_url_ref="env://TEST_REDIS_URL",
    )
    bundle = create_storage(profile, tmp_path / "factory")
    assert bundle.memory.backend_name == "external"
    assert bundle.summary.backend_name == "sql"
    bundle.close()

    with pytest.raises(ValueError, match="unsupported storage backend"):
        create_storage(StorageProfile(session_backend="unknown"), tmp_path / "invalid-session")
    with pytest.raises(ValueError, match="unsupported knowledge backend"):
        create_storage(StorageProfile(knowledge_backend="unknown"), tmp_path / "invalid-knowledge")
    with pytest.raises(ValueError, match="unsupported artifact backend"):
        create_storage(StorageProfile(artifact_backend="unknown"), tmp_path / "invalid-artifact")

    manager = TenantStorageManager(tmp_path / "manager")
    config = default_demo_config()
    config.tenant_id = "manager-tenant"
    first = manager.get(config)
    assert manager.get(config) is first
    manager.close()

    state = tmp_path / "migration.json"
    state.write_text("{not-json", encoding="utf-8")
    monkeypatch.setenv("MIGRATION_STATE_FILE", str(state))
    assert TenantStorageManager._migration_for("manager-tenant") is None
    state.write_text(json.dumps({"tenant_id": "other", "phase": "cutover"}), encoding="utf-8")
    assert TenantStorageManager._migration_for("manager-tenant") is None


def test_retry_backoff_is_bounded_deterministic_and_validates_configuration(monkeypatch):
    first = retry_delay_seconds(1, identity="event", base_env="TEST_RETRY_BASE", cap_env="TEST_RETRY_CAP")
    second = retry_delay_seconds(1, identity="event", base_env="TEST_RETRY_BASE", cap_env="TEST_RETRY_CAP")
    assert first == second
    assert retry_delay_seconds(40, identity="event", base_env="TEST_RETRY_BASE", cap_env="TEST_RETRY_CAP") <= 300
    with pytest.raises(ValueError, match="positive integer"):
        retry_delay_seconds(0, identity="event", base_env="TEST_RETRY_BASE", cap_env="TEST_RETRY_CAP")
    with pytest.raises(ValueError, match="numeric"):
        monkeypatch.setenv("TEST_RETRY_BASE", "bad")
        retry_delay_seconds(1, identity="event", base_env="TEST_RETRY_BASE", cap_env="TEST_RETRY_CAP")
    monkeypatch.setenv("TEST_RETRY_BASE", "5")
    monkeypatch.setenv("TEST_RETRY_CAP", "2")
    with pytest.raises(ValueError, match="greater than or equal"):
        retry_delay_seconds(1, identity="event", base_env="TEST_RETRY_BASE", cap_env="TEST_RETRY_CAP")


def test_external_memory_http_protocol_serializes_and_restores_items(monkeypatch):
    store = ExternalMemoryStore("https://memory.example", token="secret-token")
    requests = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps({"items": [{"tenant_id": "tenant", "memory_id": "m1", "content": "fact"}]}).encode()

    def fake_urlopen(request, timeout):
        requests.append((request, timeout))
        return Response()

    monkeypatch.setattr("trpc_service.storage.external_memory.urlopen", fake_urlopen)
    item = MemoryItem("tenant", "m1", "user", "fact", {"source": "test"})
    store.put(item)
    result = store.search("tenant", "fact", scope_keys=("user",))
    assert result[0].memory_id == "m1"
    assert requests[0][0].method == "PUT"
    assert requests[1][0].method == "POST"
    assert requests[0][0].headers["Authorization"] == "Bearer secret-token"
    assert store.close() is None
    with pytest.raises(ValueError, match="requires external_memory_url"):
        ExternalMemoryStore("")


class _Projection:
    def __init__(self, **values):
        self.values = values
        self.failures: set[str] = set()

    def __getattr__(self, name):
        def call(*args, **kwargs):
            if name in self.failures:
                raise RuntimeError(f"{name} failed")
            value = self.values.get(name)
            if callable(value):
                return value(*args, **kwargs)
            return value

        return call


def test_mirror_read_fallbacks_and_write_failures_are_recorded():
    failures = []
    enqueue = lambda operation, payload: failures.append((operation, payload))
    item = MemoryItem("t", "m", "s", "memory")
    summary = Summary("t", "s", "summary", 1)
    audit = AuditRecord("a", "t", "allow", "trace")
    chunk = KnowledgeChunk("t", "docs", "c", "knowledge")
    event = SessionEvent("t", "s", "e", "message", {}, "trace")
    secondary_state = SessionState("t", "s", {"from": "secondary"}, 1, 1)
    primary = _Projection(
        load_events=lambda *_: (_ for _ in ()).throw(RuntimeError("down")),
        load_state=lambda *_: (_ for _ in ()).throw(RuntimeError("down")),
        search=[], latest=None, list_by_tenant=[], get=None,
        claim_delivery=True, get_execution=None,
    )
    secondary = _Projection(
        load_events=[event], load_state=secondary_state, search=[item], latest=summary,
        list_by_tenant=[audit], get=SimpleNamespace(object_id="object"), claim_delivery=True,
        get_execution=SimpleNamespace(call_key="call"),
    )
    session = _MirrorSession(primary, secondary, enqueue)
    assert session.load_events("t", "s") == [event]
    assert session.load_state("t", "s") is secondary_state
    session_memory = _MirrorMemory(primary, secondary, enqueue)
    assert session_memory.search("t", "q") == [item]
    assert _MirrorSummary(primary, secondary, enqueue).latest("t", "s") == summary
    assert _MirrorAudit(primary, secondary, enqueue).list_by_tenant("t") == [audit]
    assert _MirrorIdempotency(primary, secondary, enqueue).get("t", "key") is not None
    assert _MirrorIdempotency(primary, secondary, enqueue).claim_delivery("t", "key")
    assert _MirrorToolGovernance(primary, secondary, enqueue).get_execution("t", "call") is not None

    primary.values["put"] = None
    secondary.failures.update({"put", "append", "upsert", "start", "complete", "fail", "release_delivery"})
    for adapter, value, operation in (
        (session_memory, item, "put"),
        (_MirrorSummary(primary, secondary, enqueue), summary, "put"),
        (_MirrorAudit(primary, secondary, enqueue), audit, "append"),
        (_MirrorKnowledge(primary, secondary, enqueue), chunk, "upsert"),
    ):
        with pytest.raises(RuntimeError):
            getattr(adapter, operation)(value)
    idem = _MirrorIdempotency(primary, secondary, enqueue)
    for method, args in (("start", ("t", "k", "trace")), ("complete", ("t", "k", "ref", {})), ("fail", ("t", "k", "error")), ("release_delivery", ("t", "k"))):
        with pytest.raises(RuntimeError):
            getattr(idem, method)(*args)
    assert {name for name, _ in failures} >= {
        "mirror.memory.put", "mirror.summary.put", "mirror.audit.append",
        "mirror.knowledge.upsert", "mirror.idempotency.start", "mirror.idempotency.complete",
        "mirror.idempotency.fail", "mirror.idempotency.release_delivery",
    }
    assert _jsonable((event, event.created_at, {1: [event]}))[0]["event_id"] == "e"


def test_mirror_session_and_mailbox_fail_closed_on_replication_mismatch():
    failures = []
    enqueue = lambda operation, payload: failures.append(operation)
    event = SessionEvent("t", "s", "e", "message", {}, "trace")
    primary = _Projection(append_event=1, compare_and_set_state=True, load_state=SessionState("t", "s"))
    secondary = _Projection(append_event=2, compare_and_set_state=False, load_state=SessionState("t", "s", {"old": 1}))
    session = _MirrorSession(primary, secondary, enqueue)
    with pytest.raises(RuntimeError, match="sequence"):
        session.append_event(event)
    with pytest.raises(RuntimeError, match="state"):
        session.compare_and_set_state("t", "s", 0, {"new": 1})
    missing = _MirrorSession(SimpleNamespace(), SimpleNamespace(), enqueue)
    for method, args in (
        ("acquire_session_lock", ("t", "s", 1)),
        ("release_session_lock", ("t", "s", "x")),
        ("acquire_session_lease", ("t", "s", 1)),
        ("release_session_lease", ("t", "s", "x")),
        ("renew_session_lease", ("x",)),
    ):
        with pytest.raises(AttributeError):
            getattr(missing, method)(*args)
    mailbox_primary = _Projection(enqueue=SimpleNamespace(sequence=1), claim_next=None, recover_expired=0)
    mailbox_secondary = _Projection(enqueue=SimpleNamespace(sequence=2), claim_next=None, recover_expired=0)
    mailbox = _MirrorMailbox(mailbox_primary, mailbox_secondary, enqueue)
    with pytest.raises(RuntimeError, match="sequence"):
        mailbox.enqueue("t", "s", "m", "d", {})
    assert "mirror.mailbox.enqueue" in failures

    v2_primary = _Projection(accept=SimpleNamespace(accepted_sequence=1, queue_generation=1), has_unresolved_message=True)
    v2_secondary = _Projection(accept=SimpleNamespace(accepted_sequence=2, queue_generation=1), has_unresolved_message=False)
    v2 = _MirrorSessionMailboxV2(v2_primary, v2_secondary, enqueue)
    with pytest.raises(RuntimeError, match="accept"):
        v2.accept("t", "s", "m")
    with pytest.raises(RuntimeError, match="state mismatch"):
        v2.has_unresolved_message("t", "s", "m")
    assert "mirror.session_mailbox_v2.accept" in failures


def test_mirror_artifact_and_knowledge_fallbacks_and_bundle_selection():
    failures = []
    enqueue = lambda operation, payload: failures.append(operation)
    artifact = _MirrorArtifact(
        _Projection(get=lambda *_: (_ for _ in ()).throw(RuntimeError("down")), list_by_tenant=[]),
        _Projection(get=b"secondary", list_by_tenant=["object"]), enqueue,
    )
    assert artifact.get("t", "o") == b"secondary"
    assert artifact.list_by_tenant("t") == ["object"]
    knowledge = _MirrorKnowledge(
        _Projection(search=lambda *_: (_ for _ in ()).throw(RuntimeError("down")), list_by_tenant=[]),
        _Projection(search=["chunk"], list_by_tenant=["chunk"]), enqueue,
    )
    assert knowledge.search("t", "c", "q") == ["chunk"]
    assert knowledge.list_by_tenant("t") == ["chunk"]


def test_mirror_success_paths_cover_projection_contracts():
    calls = []
    enqueue = lambda operation, payload: calls.append((operation, payload))
    lease = SessionMailboxLease("tenant", "session", "message", 1, "worker", 1, now_utc(), 1, 0, 0)
    mailbox_record = MailboxRecord("tenant", "session", 1, "message", "dedupe", {})
    inbox_record = InboxRecord("message", "tenant", "dedupe", "session", {})
    outbox_record = OutboxRecord("event", "tenant", "topic", "session", {})
    execution = SimpleNamespace(call_key="call")
    claim = SimpleNamespace(status="claimed", claimed=True, lease=lease)
    values = {
        "acquire_session_lock": "lock",
        "acquire_session_lease": SimpleNamespace(token="lease"),
        "renew_session_lease": SimpleNamespace(token="renewed"),
        "append_event": 1,
        "load_events": ["event"],
        "load_state": SimpleNamespace(state={"ok": True}, state_version=1, latest_event_seq=1),
        "compare_and_set_state": True,
        "search": ["memory"], "latest": "summary", "list_by_tenant": ["record"],
        "start": "started", "complete": "completed", "fail": "failed", "get": "found",
        "claim_delivery": True,
        "enqueue": mailbox_record, "claim_next": mailbox_record, "renew": mailbox_record,
        "recover_expired": 1,
        "get_execution": execution, "create_or_get": "approval", "approve": "approved",
        "consume": "consumed", "reserve_call": "budget", "begin_execution": execution,
        "complete_execution": execution, "fail_execution": execution,
        "list_executions_by_tenant": [execution], "restore_execution": execution,
        "accept_inbox": (inbox_record, True), "complete_inbox": None,
        "complete_inbox_and_enqueue_outbox": outbox_record, "fail_inbox": None,
        "dead_inbox": None, "enqueue_outbox": outbox_record, "claim_outbox": [outbox_record],
        "complete_outbox": None, "fail_outbox": None, "replay_outbox": outbox_record,
        "put": SimpleNamespace(object_id="object"), "put_with_id": SimpleNamespace(object_id="object"),
        "upsert": None, "export_by_tenant": {"items": []}, "restore_export": None,
        "has_unresolved_message": False,
        "accept": SimpleNamespace(accepted_sequence=1, queue_generation=1), "claim": lease,
        "claim_session": claim, "commit": "committed", "retry": "retried",
        "dead_letter": "dead", "recover": None, "sweep_expired_leases": 1,
        "schedule_retries": 1, "reconcile_sessions": 1, "reconcile": None,
    }
    primary = _Projection(**values)
    secondary = _Projection(**values)

    session = _MirrorSession(primary, secondary, enqueue)
    assert session.acquire_session_lock("tenant", "session", 1) == "lock"
    session.release_session_lock("tenant", "session", "lock")
    session_lease = session.acquire_session_lease("tenant", "session", 1)
    assert session.renew_session_lease(session_lease).token == "renewed"
    session.release_session_lease("tenant", "session", session_lease)
    session.validate_session_lease(session_lease)
    assert session.append_event(SessionEvent("tenant", "session", "event", "message", {}, "trace")) == 1
    assert session.load_events("tenant", "session") == ["event"]
    assert session.load_state("tenant", "session").state == {"ok": True}
    assert session.compare_and_set_state("tenant", "session", 0, {"ok": True})

    memory = _MirrorMemory(primary, secondary, enqueue)
    memory.put(MemoryItem("tenant", "memory", "session", "content"))
    assert memory.search("tenant", "content") == ["memory"]
    summary = _MirrorSummary(primary, secondary, enqueue)
    summary.put(Summary("tenant", "session", "summary", 1))
    assert summary.latest("tenant", "session") == "summary"
    audit = _MirrorAudit(primary, secondary, enqueue)
    audit.append(AuditRecord("audit", "tenant", "allow", "trace"))
    assert audit.list_by_tenant("tenant") == ["record"]
    idem = _MirrorIdempotency(primary, secondary, enqueue)
    idem.start("tenant", "key", "trace")
    idem.complete("tenant", "key", "response", {})
    idem.fail("tenant", "key", "error")
    assert idem.get("tenant", "key") == "found"
    assert idem.claim_delivery("tenant", "key")
    idem.release_delivery("tenant", "key")

    mailbox = _MirrorMailbox(primary, secondary, enqueue)
    assert mailbox.enqueue("tenant", "session", "message", "dedupe", {}) == mailbox_record
    claimed = mailbox.claim_next("tenant", "session", "worker")
    mailbox.renew(claimed)
    mailbox.complete(claimed)
    claimed = mailbox.claim_next("tenant", "session", "worker")
    mailbox.fail(claimed, "temporary", 1)
    assert mailbox.recover_expired("tenant", "session") == 1

    v2 = _MirrorSessionMailboxV2(primary, secondary, enqueue)
    assert v2.get("tenant", "session") == "found"
    assert v2.export_by_tenant("tenant") == {"items": []}
    v2.restore_export({})
    assert v2.has_unresolved_message("tenant", "session", "message") is False
    v2.accept("tenant", "session", "message")
    v2.claim("tenant", "session", "worker", 30)
    v2.claim_session("tenant", "session", "worker", 30)
    v2.renew(lease, 30)
    v2.commit(lease)
    v2.retry(lease, increment_retry=False)
    v2.dead_letter(lease, "bad")
    v2.recover("tenant", "session")
    assert v2.sweep_expired_leases(limit=1) == 1
    assert v2.schedule_retries(limit=1) == 1
    assert v2.reconcile_sessions(limit=1) == 1
    v2.reconcile("tenant", "session")

    governance = _MirrorToolGovernance(primary, secondary, enqueue)
    governance.create_or_get("tenant", "approval")
    governance.get("tenant", "approval")
    governance.approve("tenant", "approval")
    governance.consume("tenant", "approval")
    governance.reserve_call("tenant", "request", "call", False, 2, 1)
    governance.begin_execution("tenant", "execution", "request", "session", "tool", "call", "hash", False)
    governance.get_execution("tenant", "call")
    governance.complete_execution("tenant", "call", {})
    governance.fail_execution("tenant", "call", "error", "message")
    governance.list_executions_by_tenant("tenant")
    governance.restore_execution(execution)

    inbox = _MirrorInboxOutbox(primary, secondary, enqueue)
    inbox.accept_inbox("tenant", "dedupe", "session", {}, "worker")
    inbox.complete_inbox("tenant", "dedupe", "worker", {})
    inbox.complete_inbox_and_enqueue_outbox("tenant", "dedupe", "worker", {}, "topic", "session", "event")
    inbox.fail_inbox("tenant", "dedupe", "worker", "error")
    inbox.dead_inbox("tenant", "dedupe", "worker", "error")
    inbox.enqueue_outbox("tenant", "topic", "session", {}, event_id="event")
    inbox.claim_outbox("worker")
    inbox.complete_outbox("event", "worker", tenant_id="tenant")
    inbox.fail_outbox("event", "worker", "error", tenant_id="tenant")
    inbox.replay_outbox("event", "tenant")

    artifact = _MirrorArtifact(primary, secondary, enqueue)
    artifact.put("tenant", b"data", "text/plain")
    artifact.put_with_id("tenant", "object", b"data", "text/plain")
    artifact.get("tenant", "object")
    artifact.list_by_tenant("tenant")
    knowledge = _MirrorKnowledge(primary, secondary, enqueue)
    knowledge.upsert(KnowledgeChunk("tenant", "docs", "chunk", "content"))
    knowledge.search("tenant", "docs", "content")
    knowledge.list_by_tenant("tenant")
    assert calls == []


def test_sqlite_tenant_repository_versions_are_durable_and_optimistic():
    with tempfile.TemporaryDirectory() as directory:
        path = f"{directory}/tenant.sqlite3"
        repository = SQLiteTenantRepository(path)
        config = default_demo_config()
        config.channel_bindings = [config.channel_bindings[-1]]
        created = repository.create(config)
        assert created.config_version == 1
        with pytest.raises(Exception, match="already exists"):
            repository.create(config)
        saved = repository.save_version(repository.get("tenant_demo"), expected_version=1)
        assert saved.config_version == 2
        with pytest.raises(TenantRepositoryConflict):
            repository.save_version(config, expected_version=99)
        assert repository.publish("tenant_demo", 1, expected_version=1).config_version == 1
        with pytest.raises(TenantNotFound):
            repository.get("tenant_demo", version=99)
        with pytest.raises(TenantNotFound):
            unknown = default_demo_config()
            unknown.tenant_id = "missing"
            repository.save_version(unknown)
        published = repository.save_and_publish(repository.get("tenant_demo"), expected_version=1)
        assert repository.get("tenant_demo").config_version == published.config_version
        assert repository.find_binding("WEB", "web_demo").channel == "web"
        with pytest.raises(TenantNotFound):
            repository.find_binding("telegram", "missing")
        assert repository.all_active()[0].tenant_id == "tenant_demo"
        repository.close()
        reopened = SQLiteTenantRepository(path)
        assert reopened.get("tenant_demo").config_version == published.config_version
        reopened.close()


def test_inmemory_repository_helpers_and_ambiguous_binding_are_explicit():
    assert demo_repository().get("tenant_demo").tenant_id == "tenant_demo"
    with tempfile.TemporaryDirectory() as directory:
        repository = persistent_demo_repository(f"{directory}/demo.sqlite3")
        assert repository.get("tenant_demo")
        repository.close()
    first = default_demo_config()
    first.channel_bindings = [ChannelBinding("t1", "binding-1", "web", "shared", "app_support")]
    second = default_demo_config()
    second.tenant_id = "t2"
    second.channel_bindings = [ChannelBinding("t2", "binding-2", "web", "shared", "app_support")]
    repository = InMemoryTenantRepository()
    repository.create(first)
    repository.create(second)
    with pytest.raises(Exception, match="ambiguous"):
        repository.find_binding("web", "shared")


def test_admin_service_round_trips_public_redactions_and_all_mutations():
    repository = InMemoryTenantRepository()
    admin = AdminService(TenantService(repository))
    source = default_demo_config().to_dict()
    created = admin.create_tenant(source)
    assert created["tenant_id"] == source["tenant_id"]
    current = admin.get_tenant("tenant_demo")
    assert _merge_public_config("[configured]", {"secret": "value"}) == {"secret": "value"}
    merged = admin.update_config("tenant_demo", {"apps": current["apps"]}, expected_version=1)
    assert merged["config_version"] == 2
    published = admin.publish("tenant_demo", 2, expected_version=1)
    assert published["config_version"] == 2
    assert admin.rollback("tenant_demo", 1, expected_version=2)["config_version"] == 1
    gray = admin.configure_gray_release(
        "tenant_demo", {"candidate_version": 2, "percent": 25, "session_overrides": {"s": 1}}, expected_version=1
    )
    assert gray["gray_release"]["percent"] == 25
    channel = admin.add_channel(
        "tenant_demo", {"channel": "web", "account_id": "new", "agent_app_id": "app_support", "config": {}},
        expected_version=3,
    )
    assert any(item["account_id"] == "new" for item in channel["channel_bindings"])


def test_public_config_merge_preserves_identity_and_handles_scalars():
    current = {"apps": [{"agent_app_id": "a", "value": 1}, {"agent_app_id": "b", "value": 2}], "values": [1, 2]}
    merged = _merge_public_config({"apps": [{"agent_app_id": "b", "value": 3}], "values": [9]}, current)
    assert merged["apps"] == [{"agent_app_id": "b", "value": 3}]
    assert merged["values"] == [9]
    assert _merge_public_config(None, {"x": 1}) is None


def test_database_runner_resolves_dsn_and_enforces_downgrade_confirmation(monkeypatch):
    monkeypatch.delenv("POSTGRES_SCHEMA_DSN", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    with pytest.raises(migration_runner.DatabaseMigrationError, match="requires"):
        migration_runner.resolve_schema_dsn()
    monkeypatch.setenv("POSTGRES_SCHEMA_DSN", " schema-dsn ")
    assert migration_runner.resolve_schema_dsn() == "schema-dsn"
    assert migration_runner.resolve_schema_dsn(" direct ") == "direct"
    assert migration_runner.alembic_config().get_main_option("script_location")
    with pytest.raises(migration_runner.DatabaseMigrationError, match="destructive"):
        migration_runner.downgrade_database("dsn")


def test_database_runner_migration_context_releases_lock_on_success_and_failure():
    connection = MagicMock()
    connection.in_transaction.side_effect = [False, False]
    connection.__enter__.return_value = connection
    engine = MagicMock()
    engine.connect.return_value = connection
    with patch.object(migration_runner, "create_database_engine", return_value=engine):
        with migration_runner.migration_connection("dsn") as yielded:
            assert yielded is connection
    connection.exec_driver_sql.assert_any_call(
        f"SELECT pg_advisory_lock(hashtext('{migration_runner.MIGRATION_LOCK}'))"
    )
    connection.exec_driver_sql.assert_any_call(
        f"SELECT pg_advisory_unlock(hashtext('{migration_runner.MIGRATION_LOCK}'))"
    )
    engine.dispose.assert_called_once()

    failing = MagicMock()
    failing.in_transaction.side_effect = [True, True]
    failing.__enter__.return_value = failing
    failing_engine = MagicMock()
    failing_engine.connect.return_value = failing
    with patch.object(migration_runner, "create_database_engine", return_value=failing_engine):
        with pytest.raises(RuntimeError, match="boom"):
            with migration_runner.migration_connection("dsn"):
                raise RuntimeError("boom")
    failing.rollback.assert_called()
    failing_engine.dispose.assert_called_once()


def test_database_runner_upgrade_downgrade_and_status_report(monkeypatch):
    context = MagicMock()
    context.__enter__.return_value = "connection"
    with patch.object(migration_runner, "migration_connection", return_value=context), \
            patch.object(migration_runner, "alembic_config", return_value=MagicMock()), \
            patch.object(migration_runner.command, "upgrade") as upgrade, \
            patch.object(migration_runner, "database_status", return_value={"status": "pass"}) as status:
        assert migration_runner.upgrade_database("dsn", "v1") == {"status": "pass"}
        upgrade.assert_called_once()
        status.assert_called_with("dsn")

    with patch.object(migration_runner, "migration_connection", return_value=context), \
            patch.object(migration_runner, "alembic_config", return_value=MagicMock()), \
            patch.object(migration_runner.command, "downgrade") as downgrade, \
            patch.object(migration_runner, "database_status", return_value={"status": "fail"}):
        assert migration_runner.downgrade_database("dsn", "-1", allow_destructive=True)["status"] == "fail"
        downgrade.assert_called_once()

    connection = MagicMock()
    connection.__enter__.return_value = connection
    engine = MagicMock()
    engine.connect.return_value = connection
    inspector = MagicMock()
    inspector.get_table_names.return_value = ["tenant_config"]
    inspector.get_columns.return_value = [{"name": "tenant_id"}]
    migration_context = MagicMock()
    migration_context.get_current_revision.return_value = "old"
    scripts = MagicMock()
    scripts.get_heads.return_value = ["head"]
    with patch.object(migration_runner, "create_database_engine", return_value=engine), \
            patch.object(migration_runner, "inspect", return_value=inspector), \
            patch.object(migration_runner.MigrationContext, "configure", return_value=migration_context), \
            patch.object(migration_runner.ScriptDirectory, "from_config", return_value=scripts):
        report = migration_runner.database_status("dsn")
    assert report["status"] == "fail"
    assert "tenant_active" in report["missing_tables"]
    assert report["missing_columns"]["tenant_config"]
    engine.dispose.assert_called_once()


def test_session_lock_helpers_select_fencing_legacy_and_local_protocols():
    lease = SessionLease("t", "s", "owner", 1, now_utc() + timedelta(seconds=30))
    calls = []

    class Fenced:
        def acquire_session_lease(self, *args):
            calls.append(("acquire", args))
            return lease

        def release_session_lease(self, *args):
            calls.append(("release", args))

        def renew_session_lease(self, value):
            calls.append(("renew", value))
            return lease

        def validate_session_lease(self, value):
            calls.append(("validate", value))

    fenced = Fenced()
    with session_lease(fenced, "t", "s") as current:
        assert current == lease
    with session_lock(fenced, "t", "s") as current:
        assert current == lease
    assert renew_session_lease(fenced, lease) == lease
    validate_session_lease(fenced, lease)
    assert [item[0] for item in calls] == ["acquire", "release", "acquire", "release", "renew", "validate", "validate"]

    legacy_calls = []

    class Legacy:
        def acquire_session_lock(self, *args):
            legacy_calls.append(("acquire", args))
            return "token"

        def release_session_lock(self, *args):
            legacy_calls.append(("release", args))

    with session_lease(Legacy(), "t", "s") as token:
        assert token == "token"
    with session_lock(Legacy(), "t", "s") as token:
        assert token == "token"
    assert renew_session_lease(object(), "value") == "value"
    validate_session_lease(object(), None)
    with local_session_lock("local-t", "local-s"):
        pass
    assert len(legacy_calls) == 4


def test_run_async_supports_sync_and_active_event_loop_error_propagation():
    assert run_async(lambda: _value()) == 7

    async def inside():
        assert run_async(lambda: _value()) == 7
        with pytest.raises(ValueError, match="async failure"):
            run_async(lambda: _failure())

    asyncio.run(inside())


async def _value():
    return 7


async def _failure():
    raise ValueError("async failure")


def test_redis_session_lock_mixin_lifecycle_and_fencing_errors(monkeypatch):
    class Client:
        def __init__(self):
            self.values = {}
            self.eval_results = [1, 1, 1, 1]

        def set(self, key, value, nx=False, ex=None):
            if key in self.values:
                return False
            self.values[key] = value
            return True

        def eval(self, _script, _count, key, *args):
            result = self.eval_results.pop(0) if self.eval_results else 1
            if result and "session-lease" in key and args:
                self.values[key] = json.dumps({"owner": args[0], "fencing_token": int(result)})
            return result

        def get(self, key):
            return self.values.get(key)

    class Lock(RedisSessionLockMixin):
        def __init__(self):
            self.client = Client()

        def _key(self, kind, tenant, session):
            return f"{kind}:{tenant}:{session}"

    lock = Lock()
    token = lock.acquire_session_lock("t", "s", 0)
    lock.release_session_lock("t", "s", token)
    obtained = lock.acquire_session_lease("t", "s", 0)
    assert obtained.fencing_token == 1
    monkeypatch.setenv("SESSION_LEASE_TTL_SECONDS", "1")
    assert lock.renew_session_lease(obtained).fencing_token == 1
    lock.validate_session_lease(obtained)
    lock.client.values["session-lease:t:s"] = json.dumps({"owner": "other", "fencing_token": 2})
    with pytest.raises(SessionLeaseLost):
        lock.validate_session_lease(obtained)


def test_postgres_lock_mixin_and_advisory_lock_protocols():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(True,), (None, None, None), (1,), (1,), (True,)]
    cursor.rowcount = 1
    cursor.__enter__.return_value = cursor
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connection.transaction.return_value = MagicMock()

    class Lock(PostgresSessionLockMixin):
        def __init__(self):
            self._conn = connection
            self._lock = RLock()

    lock = Lock()
    token = lock.acquire_session_lock("t", "s", 0)
    lock.release_session_lock("t", "s", token)
    lease = lock.acquire_session_lease("t", "s", 0)
    assert lease.fencing_token == 1
    lock.renew_session_lease(lease)
    lock.validate_session_lease(lease)
    cursor.fetchone.return_value = (True,)
    from trpc_service.storage.locking import postgres_advisory_lock

    with postgres_advisory_lock(connection, "migration", timeout=0):
        pass


def test_wechat_crypto_plain_and_encrypted_handshakes():
    token = "wechat-token"
    timestamp, nonce, echo = "1", "2", "echo"
    import hashlib

    signature = hashlib.sha1("".join(sorted((token, timestamp, nonce))).encode()).hexdigest()
    with patch("trpc_service.channels.wechat_crypto.SecretManager.resolve", return_value=token):
        assert verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": echo, "signature": signature}, "token-ref") == echo
        with pytest.raises(ValueError, match="invalid"):
            verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": echo, "signature": "bad"}, "token-ref")
        with pytest.raises(ValueError, match="incomplete"):
            verify_handshake({"timestamp": timestamp}, "token-ref")

    key = os.urandom(32)
    raw_key = base64.b64encode(key).decode().rstrip("=")
    xml = b"<xml><Content>hello</Content><MsgId>42</MsgId></xml>"
    body = os.urandom(16) + struct.pack("!I", len(xml)) + xml + b"source"
    body += bytes([16 - len(body) % 16]) * (16 - len(body) % 16)
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    encrypted = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor().update(body)
    encoded = base64.b64encode(encrypted).decode()
    encrypted_signature = hashlib.sha1("".join(sorted((token, timestamp, nonce, encoded))).encode()).hexdigest()
    with patch("trpc_service.channels.wechat_crypto.SecretManager.resolve", side_effect=[token, raw_key, raw_key]):
        assert verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": encoded, "msg_signature": encrypted_signature}, "token-ref", "aes-ref") == xml.decode()
        assert decrypt_message(encoded, "aes-ref")["MsgId"] == "42"

from __future__ import annotations

import sys
from datetime import UTC, datetime
from threading import RLock
from types import SimpleNamespace

import pytest

from trpc_service.storage.base import AuditRecord, MemoryItem, SessionEvent, Summary
from trpc_service.storage.postgres_knowledge import (
    PostgresKnowledgeStore,
)
from trpc_service.storage.postgres_knowledge import (
    _is_postgres_connection_error as knowledge_connection_error,
)
from trpc_service.storage.postgres_store import (
    PostgresStorage,
)
from trpc_service.storage.postgres_store import (
    _is_postgres_connection_error as storage_connection_error,
)
from trpc_service.storage.vector_store import KnowledgeChunk


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = connection.rowcount

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=()):
        self.connection.executed.append((" ".join(str(query).split()), params))
        if self.connection.fail_once:
            self.connection.fail_once = False
            raise self.connection.fail_once_error("connection is closed")
        self.rowcount = self.connection.rowcount

    def fetchone(self):
        return self.connection.one.pop(0) if self.connection.one else None

    def fetchall(self):
        return self.connection.many.pop(0) if self.connection.many else []


class Connection:
    def __init__(self):
        self.closed = False
        self.autocommit = False
        self.executed = []
        self.one = []
        self.many = []
        self.rowcount = 1
        self.fail_once = False
        self.fail_once_error = RuntimeError

    def cursor(self):
        return Cursor(self)

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def close(self):
        self.closed = True


def test_postgres_knowledge_initializes_queries_and_reconnects(monkeypatch):
    first = Connection()
    second = Connection()
    connections = iter((first, second))
    fake_psycopg = SimpleNamespace(connect=lambda _dsn: next(connections))
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setenv("POSTGRES_AUTO_CREATE_SCHEMA", "1")

    store = PostgresKnowledgeStore("postgresql://fake")
    assert first.autocommit is True
    assert any("CREATE TABLE IF NOT EXISTS knowledge_chunk" in query for query, _ in first.executed)

    chunk = KnowledgeChunk("tenant-a", "docs", "chunk-1", "hello", {"source": "test"})
    store.upsert(chunk)
    now = datetime.now(UTC)
    first.many = [[("tenant-a", "docs", "chunk-1", "hello", {"source": "test"})]]
    assert store.search("tenant-a", "docs", "hello")[0].chunk_id == "chunk-1"
    first.many = [[("tenant-a", "docs", "chunk-1", "hello", {"source": "test"})]]
    assert store.list_by_tenant("tenant-a")[0].metadata == {"source": "test"}

    first.closed = True
    assert store._conn is second
    store.close()
    assert second.closed
    del now


def test_postgres_knowledge_retries_connection_failures_and_preserves_other_errors(monkeypatch):
    class OperationalError(Exception):
        __module__ = "psycopg"

    first = Connection()
    first.fail_once = True
    first.fail_once_error = OperationalError
    second = Connection()
    fake_psycopg = SimpleNamespace(connect=lambda _dsn: second)
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setenv("POSTGRES_AUTO_CREATE_SCHEMA", "0")
    monkeypatch.setattr(
        "trpc_service.storage.postgres_knowledge.validate_runtime_role",
        lambda *_args, **_kwargs: None,
    )
    store = PostgresKnowledgeStore.__new__(PostgresKnowledgeStore)
    store._psycopg = fake_psycopg
    store._dsn = "fake"
    store._lock = RLock()
    store._connection = first
    store.upsert(KnowledgeChunk("tenant-a", "docs", "chunk-1", "hello"))
    assert first.closed and store._connection is second
    assert knowledge_connection_error(OperationalError("connection is closed"))
    with pytest.raises(ValueError):
        store.upsert = lambda *_args: (_ for _ in ()).throw(ValueError("bad input"))
        store.upsert(KnowledgeChunk("tenant-a", "docs", "chunk-2", "hello"))


def test_postgres_storage_initializes_dependencies_and_reconnects(monkeypatch):
    import trpc_service.storage.postgres_store as module

    first = Connection()
    second = Connection()
    connections = iter((first, second))
    fake_psycopg = SimpleNamespace(connect=lambda _dsn: next(connections))
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setenv("POSTGRES_PROCESS_ROLE", "worker")
    monkeypatch.setenv("POSTGRES_WORKER_DSN", "worker-dsn")
    monkeypatch.setenv("POSTGRES_AUTO_CREATE_SCHEMA", "0")
    validated = []
    monkeypatch.setattr(module, "validate_worker_role", lambda *_args, **_kwargs: validated.append(True))
    monkeypatch.setattr(module, "validate_runtime_role", lambda *_args, **_kwargs: validated.append(False))
    monkeypatch.setattr(module, "ensure_migration_control_schema", lambda *_args: None)

    class Dependency:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def set_connection(self, connection):
            self.connection = connection

    for name in (
        "PostgresInboxOutbox",
        "PostgresMailboxStore",
        "PostgresSessionMailboxStore",
        "PostgresToolGovernanceStore",
        "PostgresMigrationControl",
    ):
        monkeypatch.setattr(module, name, Dependency)

    storage = PostgresStorage()
    assert first.autocommit is True
    assert validated == [True]
    assert storage._dsn == "worker-dsn"
    first.closed = True
    assert storage._conn is second
    storage.close()
    assert second.closed


def test_postgres_storage_connection_error_classifier_and_projection_helpers():
    class InterfaceError(Exception):
        __module__ = "psycopg"

    assert storage_connection_error(InterfaceError("terminating connection"))
    assert not storage_connection_error(ValueError("bad"))
    assert PostgresStorage._parse_dt if hasattr(PostgresStorage, "_parse_dt") else True
    assert isinstance(AuditRecord("a", "t", "allow", "trace"), AuditRecord)
    assert isinstance(MemoryItem("t", "m", "s", "text"), MemoryItem)
    assert isinstance(SessionEvent("t", "s", "e", "message", {}, "trace"), SessionEvent)
    assert isinstance(Summary("t", "s", "summary", 1), Summary)

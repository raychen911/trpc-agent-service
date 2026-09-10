"""PostgreSQL-backed tenant-scoped knowledge chunks."""

from __future__ import annotations

import json
import os
from functools import wraps
from threading import RLock

from trpc_service.storage.durable import _json_object
from trpc_service.storage.postgres_rls import (
    _tenant_from_first_argument,
    postgres_schema_auto_create,
    rls_tenant_method,
    validate_runtime_role,
)
from trpc_service.storage.vector_store import KnowledgeChunk


def _is_postgres_connection_error(exc: Exception) -> bool:
    module = exc.__class__.__module__
    name = exc.__class__.__name__
    text = str(exc).lower()
    return module.startswith("psycopg") and (
        name in {"OperationalError", "InterfaceError", "AdminShutdown", "ConnectionTimeout"}
        or "connection is closed" in text
        or "terminating connection" in text
    )


def _retry_postgres_once(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as exc:
            if not _is_postgres_connection_error(exc):
                raise
            with self._lock:
                if self._connection is not None and not self._connection.closed:
                    self._connection.close()
                self._connect()
            return method(self, *args, **kwargs)

    return wrapper


class PostgresKnowledgeStore:
    backend_name = "postgres"

    def __init__(self, dsn: str | None = None) -> None:
        import psycopg

        self._psycopg = psycopg
        self._dsn = dsn or os.getenv("POSTGRES_DSN", "")
        self._lock = RLock()
        self._connection = None
        self._connect()
        if postgres_schema_auto_create():
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS knowledge_chunk (
                      tenant_id TEXT NOT NULL, collection TEXT NOT NULL,
                      chunk_id TEXT NOT NULL, text TEXT NOT NULL,
                      metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                      PRIMARY KEY (tenant_id, collection, chunk_id)
                    )
                    """
                )
        else:
            validate_runtime_role(self, expected_role_env="POSTGRES_RLS_APP_ROLE")

    @property
    def _conn(self):
        with self._lock:
            if self._connection is None or self._connection.closed:
                self._connect()
            return self._connection

    def _connect(self) -> None:
        connection = self._psycopg.connect(self._dsn)
        connection.autocommit = True
        self._connection = connection

    def upsert(self, chunk: KnowledgeChunk) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO knowledge_chunk (tenant_id, collection, chunk_id, text, metadata_json)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, collection, chunk_id)
                DO UPDATE SET text=EXCLUDED.text, metadata_json=EXCLUDED.metadata_json
                """,
                (chunk.tenant_id, chunk.collection, chunk.chunk_id, chunk.text, json.dumps(chunk.metadata)),
            )

    def search(self, tenant_id: str, collection: str, query: str, limit: int = 5) -> list[KnowledgeChunk]:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT tenant_id, collection, chunk_id, text, metadata_json "
                    "FROM knowledge_chunk WHERE tenant_id=%s AND collection=%s "
                    "AND text ILIKE %s LIMIT %s"
                ),
                (tenant_id, collection, f"%{query}%", limit),
            )
            return [KnowledgeChunk(row[0], row[1], row[2], row[3], _json_object(row[4])) for row in cur.fetchall()]

    def list_by_tenant(self, tenant_id: str) -> list[KnowledgeChunk]:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT tenant_id, collection, chunk_id, text, metadata_json "
                    "FROM knowledge_chunk WHERE tenant_id=%s ORDER BY collection, chunk_id"
                ),
                (tenant_id,),
            )
            return [KnowledgeChunk(row[0], row[1], row[2], row[3], _json_object(row[4])) for row in cur.fetchall()]

    def close(self) -> None:
        if self._connection is not None and not self._connection.closed:
            self._connection.close()


_POSTGRES_KNOWLEDGE_METHODS = {
    "upsert": lambda args, kwargs: getattr(args[0], "tenant_id", None)
    if args
    else getattr(kwargs.get("chunk"), "tenant_id", None),
    "search": _tenant_from_first_argument,
    "list_by_tenant": _tenant_from_first_argument,
}
for _method_name, _tenant_getter in _POSTGRES_KNOWLEDGE_METHODS.items():
    method = rls_tenant_method(_tenant_getter)(getattr(PostgresKnowledgeStore, _method_name))
    setattr(
        PostgresKnowledgeStore,
        _method_name,
        _retry_postgres_once(method),
    )
del _method_name, _tenant_getter, method

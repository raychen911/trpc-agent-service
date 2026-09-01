"""PostgreSQL storage adapter used for production structured state."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from functools import wraps
from threading import RLock
from typing import Any

from trpc_service.storage.base import (
    AuditRecord,
    IdempotencyRecord,
    IdempotencyStatus,
    MemoryItem,
    SessionEvent,
    SessionState,
    Summary,
    now_utc,
)
from trpc_service.storage.compensation import _task_from_dict
from trpc_service.storage.locking import (
    PostgresSessionLockMixin,
    SessionLeaseLost,
    postgres_advisory_lock,
)
from trpc_service.storage.durable import PostgresInboxOutbox, _json_object
from trpc_service.storage.postgres_rls import (
    _tenant_from_event,
    _tenant_from_first_argument,
    _tenant_from_optional_keyword,
    _tenant_from_value,
    postgres_schema_auto_create,
    rls_tenant_method,
    validate_runtime_role,
)
from trpc_service.security.secrets import redact_secret_text


def _dt(value: datetime) -> str:
    return value.isoformat()


def _parse_dt(value: datetime | str) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(value)


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


class PostgresStorage(PostgresSessionLockMixin):
    backend_name = "postgres"

    def __init__(self, dsn: str | None = None) -> None:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("PostgreSQL backend requires psycopg[binary]") from exc
        self._psycopg = psycopg
        self._dsn = dsn or os.getenv("POSTGRES_DSN", "")
        self._lock = RLock()
        self._connection = None
        self._connect()
        if postgres_schema_auto_create():
            self._init_schema()
        else:
            validate_runtime_role(self, expected_role_env="POSTGRES_RLS_APP_ROLE")
        self.session = self
        self.memory = self
        self.summary = self
        self.audit = self
        self.idempotency = self
        self.compensation = _PostgresCompensationStore(self)
        self.inbox_outbox = PostgresInboxOutbox(
            self._conn,
            self._lock,
            connection_provider=lambda: self._conn,
        )

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
        inbox_outbox = getattr(self, "inbox_outbox", None)
        if inbox_outbox is not None:
            inbox_outbox.set_connection(connection)

    def _init_schema(self) -> None:
        with postgres_advisory_lock(self._conn, "trpc-agent-schema-v1"):
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                CREATE TABLE IF NOT EXISTS session_state (
                  tenant_id TEXT NOT NULL, session_id TEXT NOT NULL,
                  state_version INTEGER NOT NULL DEFAULT 0,
                  latest_event_seq INTEGER NOT NULL DEFAULT 0,
                  state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                  PRIMARY KEY (tenant_id, session_id)
                );
                CREATE TABLE IF NOT EXISTS message_event (
                  tenant_id TEXT NOT NULL, event_id TEXT NOT NULL,
                  session_id TEXT NOT NULL, seq INTEGER NOT NULL,
                  idempotency_key TEXT, event_type TEXT NOT NULL,
                  payload_json JSONB NOT NULL, trace_id TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, event_id),
                  UNIQUE (tenant_id, session_id, seq),
                  UNIQUE (tenant_id, session_id, idempotency_key, event_type)
                );
                CREATE TABLE IF NOT EXISTS memory (
                  tenant_id TEXT NOT NULL, memory_id TEXT NOT NULL,
                  scope_key TEXT NOT NULL, content TEXT NOT NULL,
                  version INTEGER NOT NULL, metadata_json JSONB NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, memory_id)
                );
                CREATE TABLE IF NOT EXISTS summary (
                  tenant_id TEXT NOT NULL, session_id TEXT NOT NULL,
                  summary_version INTEGER NOT NULL, source_event_seq INTEGER NOT NULL,
                  content TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, session_id, summary_version)
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                  audit_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
                  channel TEXT, user_id TEXT, session_id TEXT, agent_name TEXT,
                  tool_name TEXT, decision TEXT NOT NULL, latency_ms INTEGER,
                  error_type TEXT, token_usage INTEGER, cost DOUBLE PRECISION,
                  trace_id TEXT NOT NULL, metadata_json JSONB NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS idempotency (
                  tenant_id TEXT NOT NULL, key TEXT NOT NULL, status TEXT NOT NULL,
                  response_ref TEXT, result_json JSONB, trace_id TEXT,
                  attempt INTEGER NOT NULL DEFAULT 1,
                  created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, key)
                );
                CREATE TABLE IF NOT EXISTS compensation_task (
                  task_id TEXT PRIMARY KEY,
                  tenant_id TEXT NOT NULL,
                  operation TEXT NOT NULL,
                  payload_json JSONB NOT NULL,
                  status TEXT NOT NULL,
                  attempt INTEGER NOT NULL DEFAULT 0,
                  available_at TIMESTAMPTZ NOT NULL,
                  last_error TEXT,
                  created_at TIMESTAMPTZ NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL
                );
                CREATE TABLE IF NOT EXISTS session_fence (
                  tenant_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  fencing_token BIGINT NOT NULL DEFAULT 0,
                  PRIMARY KEY (tenant_id, session_id)
                );
                CREATE TABLE IF NOT EXISTS session_lease (
                  tenant_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  owner TEXT NOT NULL,
                  fencing_token BIGINT NOT NULL,
                  expires_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, session_id)
                );
                    """
                )
                cur.execute("ALTER TABLE idempotency ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1")

    def append_event(self, event: SessionEvent, fencing_token: int | None = None) -> int:
        with self._lock, self._conn.transaction():
            with self._conn.cursor() as cur:
                self._assert_fencing(cur, event.tenant_id, event.session_id, fencing_token)
                cur.execute(
                    (
                        "SELECT seq FROM message_event WHERE tenant_id=%s AND session_id=%s "
                        "AND idempotency_key=%s AND event_type=%s"
                    ),
                    (event.tenant_id, event.session_id, event.idempotency_key, event.event_type),
                )
                duplicate = cur.fetchone()
                if duplicate:
                    return int(duplicate[0])
                cur.execute(
                    "INSERT INTO session_state (tenant_id,session_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (event.tenant_id, event.session_id),
                )
                cur.execute(
                    "SELECT latest_event_seq FROM session_state WHERE tenant_id=%s AND session_id=%s FOR UPDATE",
                    (event.tenant_id, event.session_id),
                )
                seq = int(cur.fetchone()[0]) + 1
                cur.execute(
                    "INSERT INTO message_event VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        event.tenant_id,
                        event.event_id,
                        event.session_id,
                        seq,
                        event.idempotency_key,
                        event.event_type,
                        json.dumps(event.payload),
                        event.trace_id,
                        event.created_at,
                    ),
                )
                cur.execute(
                    (
                        "INSERT INTO session_state (tenant_id, session_id, latest_event_seq) "
                        "VALUES (%s,%s,%s) ON CONFLICT (tenant_id,session_id) "
                        "DO UPDATE SET latest_event_seq=EXCLUDED.latest_event_seq"
                    ),
                    (event.tenant_id, event.session_id, seq),
                )
                return seq

    def load_events(self, tenant_id: str, session_id: str, after_seq: int = 0) -> list[SessionEvent]:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT tenant_id,event_id,session_id,seq,idempotency_key,event_type,"
                    "payload_json,trace_id,created_at FROM message_event "
                    "WHERE tenant_id=%s AND session_id=%s AND seq>%s ORDER BY seq"
                ),
                (tenant_id, session_id, after_seq),
            )
            return [
                SessionEvent(
                    row[0],
                    row[2],
                    row[1],
                    row[5],
                    _json_object(row[6]),
                    row[7],
                    row[4],
                    row[3],
                    _parse_dt(row[8]),
                )
                for row in cur.fetchall()
            ]

    def load_state(self, tenant_id: str, session_id: str) -> SessionState:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT state_version,latest_event_seq,state_json FROM session_state "
                    "WHERE tenant_id=%s AND session_id=%s"
                ),
                (tenant_id, session_id),
            )
            row = cur.fetchone()
        if row is None:
            return SessionState(tenant_id, session_id)
        return SessionState(tenant_id, session_id, _json_object(row[2]), int(row[0]), int(row[1]))

    def compare_and_set_state(
        self,
        tenant_id: str,
        session_id: str,
        expected_version: int,
        state: dict[str, Any],
        fencing_token: int | None = None,
    ) -> bool:
        with self._lock, self._conn.transaction():
            with self._conn.cursor() as cur:
                self._assert_fencing(cur, tenant_id, session_id, fencing_token)
                cur.execute(
                    (
                        "UPDATE session_state SET state_version=%s,state_json=%s "
                        "WHERE tenant_id=%s AND session_id=%s AND state_version=%s"
                    ),
                    (expected_version + 1, json.dumps(state), tenant_id, session_id, expected_version),
                )
                return cur.rowcount == 1

    @staticmethod
    def _assert_fencing(cur, tenant_id: str, session_id: str, fencing_token: int | None) -> None:
        if not fencing_token:
            return
        cur.execute(
            """
            SELECT 1 FROM session_lease
            WHERE tenant_id=%s AND session_id=%s AND fencing_token=%s
              AND expires_at > CURRENT_TIMESTAMP
            FOR UPDATE
            """,
            (tenant_id, session_id, int(fencing_token)),
        )
        if cur.fetchone() is None:
            raise SessionLeaseLost(f"session fencing token rejected: {tenant_id}/{session_id}")

    def restore_state(
        self,
        tenant_id: str,
        session_id: str,
        state: dict[str, Any],
        state_version: int,
    ) -> None:
        with self._lock, self._conn.transaction():
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO session_state (
                      tenant_id, session_id, state_version, latest_event_seq, state_json
                    ) VALUES (
                      %s, %s, %s,
                      COALESCE((
                        SELECT MAX(seq) FROM message_event
                        WHERE tenant_id = %s AND session_id = %s
                      ), 0),
                      %s
                    )
                    ON CONFLICT (tenant_id, session_id) DO UPDATE SET
                      state_version = EXCLUDED.state_version,
                      state_json = EXCLUDED.state_json
                    """,
                    (
                        tenant_id,
                        session_id,
                        int(state_version),
                        tenant_id,
                        session_id,
                        json.dumps(state),
                    ),
                )

    def put(self, value: MemoryItem | Summary) -> None:
        with self._lock, self._conn.transaction():
            with self._conn.cursor() as cur:
                if isinstance(value, Summary):
                    cur.execute(
                        """
                        INSERT INTO session_state (tenant_id, session_id)
                        VALUES (%s, %s)
                        ON CONFLICT (tenant_id, session_id) DO NOTHING
                        """,
                        (value.tenant_id, value.session_id),
                    )
                    cur.execute(
                        """
                        SELECT state_version
                        FROM session_state
                        WHERE tenant_id=%s AND session_id=%s
                        FOR UPDATE
                        """,
                        (value.tenant_id, value.session_id),
                    )
                    cur.execute(
                        (
                            "SELECT COALESCE(MAX(summary_version),0),"
                            "COALESCE(MAX(source_event_seq),0) FROM summary "
                            "WHERE tenant_id=%s AND session_id=%s"
                        ),
                        (value.tenant_id, value.session_id),
                    )
                    version, source = cur.fetchone()
                    if int(source) > value.source_event_seq:
                        return
                    cur.execute(
                        "INSERT INTO summary VALUES (%s,%s,%s,%s,%s,%s)",
                        (
                            value.tenant_id,
                            value.session_id,
                            int(version) + 1,
                            value.source_event_seq,
                            value.content,
                            value.created_at,
                        ),
                    )
                else:
                    cur.execute(
                        (
                            "INSERT INTO memory VALUES (%s,%s,%s,%s,%s,%s,%s) "
                            "ON CONFLICT (tenant_id,memory_id) DO UPDATE SET "
                            "content=EXCLUDED.content,version=EXCLUDED.version,"
                            "metadata_json=EXCLUDED.metadata_json"
                        ),
                        (
                            value.tenant_id,
                            value.memory_id,
                            value.scope_key,
                            value.content,
                            value.version,
                            json.dumps(value.metadata),
                            value.created_at,
                        ),
                    )

    def search(
        self,
        tenant_id: str,
        query: str,
        limit: int = 5,
        scope_keys: tuple[str, ...] | None = None,
    ) -> list[MemoryItem]:
        with self._conn.cursor() as cur:
            if scope_keys is None:
                cur.execute(
                    (
                        "SELECT tenant_id,memory_id,scope_key,content,version,metadata_json,created_at "
                        "FROM memory WHERE tenant_id=%s AND content ILIKE %s "
                        "ORDER BY created_at DESC LIMIT %s"
                    ),
                    (tenant_id, f"%{query}%", limit),
                )
            else:
                cur.execute(
                    (
                        "SELECT tenant_id,memory_id,scope_key,content,version,metadata_json,created_at "
                        "FROM memory WHERE tenant_id=%s AND content ILIKE %s AND scope_key = ANY(%s) "
                        "ORDER BY created_at DESC LIMIT %s"
                    ),
                    (tenant_id, f"%{query}%", list(scope_keys), limit),
                )
            return [
                MemoryItem(
                    tenant_id=row[0],
                    memory_id=row[1],
                    scope_key=row[2],
                    content=row[3],
                    metadata=_json_object(row[5]),
                    version=int(row[4]),
                    created_at=_parse_dt(row[6]),
                )
                for row in cur.fetchall()
            ]

    def latest(self, tenant_id: str, session_id: str) -> Summary | None:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT tenant_id,session_id,content,source_event_seq,summary_version,created_at "
                    "FROM summary WHERE tenant_id=%s AND session_id=%s "
                    "ORDER BY summary_version DESC LIMIT 1"
                ),
                (tenant_id, session_id),
            )
            row = cur.fetchone()
        return Summary(row[0], row[1], row[2], int(row[3]), int(row[4]), _parse_dt(row[5])) if row else None

    def append(self, record: AuditRecord) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "INSERT INTO audit_log VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (audit_id) DO NOTHING"
                ),
                (
                    record.audit_id,
                    record.tenant_id,
                    record.channel,
                    record.user_id,
                    record.session_id,
                    record.agent_name,
                    record.tool_name,
                    record.decision,
                    record.latency_ms,
                    record.error_type,
                    record.token_usage,
                    record.cost,
                    record.trace_id,
                    json.dumps(record.metadata),
                    record.created_at,
                ),
            )

    def list_by_tenant(self, tenant_id: str, limit: int = 100) -> list[AuditRecord]:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT audit_id,tenant_id,channel,user_id,session_id,agent_name,tool_name,"
                    "decision,latency_ms,error_type,token_usage,cost,trace_id,metadata_json,created_at "
                    "FROM audit_log WHERE tenant_id=%s ORDER BY created_at DESC LIMIT %s"
                ),
                (tenant_id, limit),
            )
            return [
                AuditRecord(
                    audit_id=row[0],
                    tenant_id=row[1],
                    decision=row[7],
                    trace_id=row[12],
                    channel=row[2],
                    user_id=row[3],
                    session_id=row[4],
                    agent_name=row[5],
                    tool_name=row[6],
                    latency_ms=row[8],
                    error_type=row[9],
                    token_usage=row[10],
                    cost=row[11],
                    metadata=_json_object(row[13]),
                    created_at=_parse_dt(row[14]),
                )
                for row in cur.fetchall()
            ]

    def start(
        self,
        tenant_id: str,
        key: str,
        trace_id: str,
        lease_seconds: int = 180,
    ) -> IdempotencyRecord:
        now = now_utc()
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                (
                    "INSERT INTO idempotency (tenant_id,key,status,trace_id,created_at,updated_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                ),
                (tenant_id, key, IdempotencyStatus.PROCESSING.value, trace_id, now, now),
            )
            cur.execute(
                "SELECT status, updated_at, attempt FROM idempotency WHERE tenant_id=%s AND key=%s FOR UPDATE",
                (tenant_id, key),
            )
            status, updated_at, attempt = cur.fetchone()
            if status == IdempotencyStatus.FAILED.value or (
                status == IdempotencyStatus.PROCESSING.value
                and (now - _parse_dt(updated_at)).total_seconds() >= lease_seconds
            ):
                cur.execute(
                    (
                        "UPDATE idempotency SET status=%s, trace_id=%s, response_ref=NULL, "
                        "result_json=NULL, attempt=%s, updated_at=%s "
                        "WHERE tenant_id=%s AND key=%s"
                    ),
                    (
                        IdempotencyStatus.PROCESSING.value,
                        trace_id,
                        int(attempt) + 1,
                        now,
                        tenant_id,
                        key,
                    ),
                )
        record = self.get(tenant_id, key)
        return record  # type: ignore[return-value]

    def complete(self, tenant_id: str, key: str, response_ref: str, result: dict) -> IdempotencyRecord:
        return self._update_idempotency(tenant_id, key, IdempotencyStatus.COMPLETED, response_ref, result)

    def fail(self, tenant_id: str, key: str, error_type: str) -> IdempotencyRecord:
        return self._update_idempotency(tenant_id, key, IdempotencyStatus.FAILED, None, {"error_type": error_type})

    def get(self, tenant_id: str, key: str) -> IdempotencyRecord | None:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT tenant_id,key,status,response_ref,result_json,trace_id,attempt,created_at,updated_at "
                    "FROM idempotency WHERE tenant_id=%s AND key=%s"
                ),
                (tenant_id, key),
            )
            row = cur.fetchone()
        if not row:
            return None
        return IdempotencyRecord(
            row[0],
            row[1],
            IdempotencyStatus(row[2]),
            row[3],
            _json_object(row[4]) if row[4] is not None else None,
            row[5],
            int(row[6]),
            _parse_dt(row[7]),
            _parse_dt(row[8]),
        )

    def _update_idempotency(
        self, tenant_id: str, key: str, status: IdempotencyStatus, response_ref: str | None, result: dict
    ) -> IdempotencyRecord:
        with self._conn.cursor() as cur:
            cur.execute(
                (
                    "UPDATE idempotency SET status=%s,response_ref=%s,result_json=%s,updated_at=%s "
                    "WHERE tenant_id=%s AND key=%s"
                ),
                (status.value, response_ref, json.dumps(result), now_utc(), tenant_id, key),
            )
        record = self.get(tenant_id, key)
        if record is None:
            raise KeyError(f"idempotency record missing: {tenant_id}/{key}")
        return record

    def claim_delivery(self, tenant_id: str, key: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE idempotency
                SET result_json = COALESCE(result_json, '{}'::jsonb) || '{"delivery_claimed": true}'::jsonb,
                    updated_at = %s
                WHERE tenant_id = %s
                  AND key = %s
                  AND status = %s
                  AND COALESCE((result_json->>'delivered')::boolean, false) = false
                  AND COALESCE((result_json->>'delivery_claimed')::boolean, false) = false
                """,
                (now_utc(), tenant_id, key, IdempotencyStatus.COMPLETED.value),
            )
            return cur.rowcount == 1

    def release_delivery(self, tenant_id: str, key: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE idempotency
                SET result_json = COALESCE(result_json, '{}'::jsonb) - 'delivery_claimed',
                    updated_at = %s
                WHERE tenant_id = %s AND key = %s
                """,
                (now_utc(), tenant_id, key),
            )

    def _compensation_enqueue(self, tenant_id, operation, payload, task_id=None):
        from trpc_service.storage.base import CompensationTask

        task = CompensationTask(
            task_id=task_id or os.urandom(16).hex(),
            tenant_id=tenant_id,
            operation=operation,
            payload=dict(payload),
        )
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO compensation_task (
                  task_id, tenant_id, operation, payload_json, status, attempt,
                  available_at, last_error, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (task_id) DO NOTHING
                """,
                (
                    task.task_id,
                    task.tenant_id,
                    task.operation,
                    json.dumps(task.payload, ensure_ascii=False, sort_keys=True),
                    task.status,
                    task.attempt,
                    task.available_at,
                    task.last_error,
                    task.created_at,
                    task.updated_at,
                ),
            )
            cur.execute("SELECT * FROM compensation_task WHERE task_id=%s", (task.task_id,))
            row = cur.fetchone()
        return self._compensation_from_row(row)

    def _compensation_claim(self, limit=10, tenant_id=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            if tenant_id:
                cur.execute(
                    """
                    SELECT * FROM compensation_task
                    WHERE tenant_id = %s
                      AND status = 'pending' AND available_at <= %s
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (tenant_id, now_utc(), int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM compensation_task
                    WHERE status = 'pending' AND available_at <= %s
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (now_utc(), int(limit)),
                )
            rows = cur.fetchall()
            result = []
            for row in rows:
                task_id = row[0]
                cur.execute(
                    """
                    UPDATE compensation_task
                    SET status='processing', attempt=attempt + 1, updated_at=%s
                    WHERE task_id=%s AND status='pending'
                    RETURNING *
                    """,
                    (now_utc(), task_id),
                )
                current = cur.fetchone()
                if current is not None:
                    result.append(self._compensation_from_row(current))
            return result

    def _compensation_complete(self, task_id, tenant_id=None):
        with self._conn.cursor() as cur:
            if tenant_id:
                cur.execute(
                    """
                    UPDATE compensation_task
                    SET status='completed', updated_at=%s
                    WHERE task_id=%s AND tenant_id=%s
                    """,
                    (now_utc(), task_id, tenant_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE compensation_task
                    SET status='completed', updated_at=%s
                    WHERE task_id=%s
                    """,
                    (now_utc(), task_id),
                )

    def _compensation_fail(self, task_id, error, retry_after_seconds=30, tenant_id=None):
        now = now_utc()
        with self._conn.cursor() as cur:
            query = (
                """
                UPDATE compensation_task
                SET status=CASE WHEN attempt >= %s THEN 'dead' ELSE 'pending' END,
                    last_error=%s, available_at=%s, updated_at=%s
                WHERE task_id=%s AND tenant_id=%s
                """
                if tenant_id
                else
                """
                UPDATE compensation_task
                SET status=CASE WHEN attempt >= %s THEN 'dead' ELSE 'pending' END,
                    last_error=%s, available_at=%s, updated_at=%s
                WHERE task_id=%s
                """
            )
            params = (
                (
                    int(os.getenv("COMPENSATION_MAX_ATTEMPTS", "10")),
                    redact_secret_text(str(error))[:1000],
                    now + timedelta(seconds=retry_after_seconds),
                    now,
                    task_id,
                    tenant_id,
                )
                if tenant_id
                else
                (
                    int(os.getenv("COMPENSATION_MAX_ATTEMPTS", "10")),
                    redact_secret_text(str(error))[:1000],
                    now + timedelta(seconds=retry_after_seconds),
                    now,
                    task_id,
                )
            )
            cur.execute(query, params)

    @staticmethod
    def _compensation_from_row(row):
        payload = _json_object(row[3])
        data = {
            "task_id": row[0],
            "tenant_id": row[1],
            "operation": row[2],
            "payload": payload,
            "status": row[4],
            "attempt": int(row[5]),
            "available_at": row[6],
            "last_error": row[7],
            "created_at": row[8],
            "updated_at": row[9],
        }
        return _task_from_dict(data)

    def close(self) -> None:
        if self._connection is not None and not self._connection.closed:
            self._connection.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class _PostgresCompensationStore:
    backend_name = "postgres"

    def __init__(self, storage: PostgresStorage) -> None:
        self.storage = storage

    def enqueue(self, tenant_id, operation, payload, task_id=None):
        return self.storage._compensation_enqueue(tenant_id, operation, payload, task_id)

    def claim(self, limit=10, tenant_id=None):
        return self.storage._compensation_claim(limit, tenant_id=tenant_id)

    def complete(self, task_id, tenant_id=None):
        return self.storage._compensation_complete(task_id, tenant_id=tenant_id)

    def fail(self, task_id, error, retry_after_seconds=30, tenant_id=None):
        return self.storage._compensation_fail(
            task_id,
            error,
            retry_after_seconds,
            tenant_id=tenant_id,
        )


_POSTGRES_TENANT_METHODS = {
    "acquire_session_lock": _tenant_from_first_argument,
    "release_session_lock": _tenant_from_first_argument,
    "acquire_session_lease": _tenant_from_first_argument,
    "release_session_lease": _tenant_from_first_argument,
    "renew_session_lease": lambda args, kwargs: getattr(args[0], "tenant_id", None)
    if args
    else getattr(kwargs.get("lease"), "tenant_id", None),
    "validate_session_lease": lambda args, kwargs: getattr(args[0], "tenant_id", None)
    if args
    else getattr(kwargs.get("lease"), "tenant_id", None),
    "append_event": _tenant_from_event,
    "load_events": _tenant_from_first_argument,
    "load_state": _tenant_from_first_argument,
    "compare_and_set_state": _tenant_from_first_argument,
    "restore_state": _tenant_from_first_argument,
    "put": _tenant_from_value,
    "search": _tenant_from_first_argument,
    "latest": _tenant_from_first_argument,
    "append": lambda args, kwargs: getattr(args[0], "tenant_id", None)
    if args
    else getattr(kwargs.get("record"), "tenant_id", None),
    "list_by_tenant": _tenant_from_first_argument,
    "start": _tenant_from_first_argument,
    "complete": _tenant_from_first_argument,
    "fail": _tenant_from_first_argument,
    "get": _tenant_from_first_argument,
    "_update_idempotency": _tenant_from_first_argument,
    "claim_delivery": _tenant_from_first_argument,
    "release_delivery": _tenant_from_first_argument,
    "_compensation_enqueue": _tenant_from_first_argument,
    "_compensation_claim": _tenant_from_optional_keyword(1),
    "_compensation_complete": _tenant_from_optional_keyword(1),
    "_compensation_fail": _tenant_from_optional_keyword(3),
}
for _method_name, _tenant_getter in _POSTGRES_TENANT_METHODS.items():
    method = rls_tenant_method(_tenant_getter)(getattr(PostgresStorage, _method_name))
    setattr(
        PostgresStorage,
        _method_name,
        _retry_postgres_once(method),
    )
del _method_name, _tenant_getter, method

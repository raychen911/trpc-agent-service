"""SQLite-backed storage adapter.

The implementation intentionally uses the Python standard library so the demo
can run without a database server. It preserves the production shape from the
design: tenant-scoped tables, append-only events, idempotency records, summaries,
memory, and audit logs.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from time import monotonic, sleep
from uuid import uuid4

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
from trpc_service.storage.durable import SQLiteInboxOutbox
from trpc_service.storage.locking import SessionLease, SessionLeaseLost
from trpc_service.security.secrets import redact_secret_text


def _dt(value: datetime) -> str:
    return value.isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


class SQLiteStorage:
    backend_name = "sql"

    def __init__(self, path: str | Path = "data/trpc_service.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._conn = sqlite3.connect(
            self.path,
            check_same_thread=False,
            timeout=float(__import__("os").getenv("SQLITE_BUSY_TIMEOUT_SECONDS", "30")),
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 30000")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._init_schema()
        self.session = self
        self.memory = self
        self.summary = self
        self.audit = self
        self.idempotency = self
        self.compensation = _SQLiteCompensationStore(self)
        self.inbox_outbox = SQLiteInboxOutbox(self._conn, self._lock)

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS session_state (
              tenant_id TEXT NOT NULL,
              session_id TEXT NOT NULL,
              state_version INTEGER NOT NULL,
              latest_event_seq INTEGER NOT NULL,
              state_json TEXT NOT NULL,
              PRIMARY KEY (tenant_id, session_id)
            );

            CREATE TABLE IF NOT EXISTS message_event (
              tenant_id TEXT NOT NULL,
              event_id TEXT NOT NULL,
              session_id TEXT NOT NULL,
              seq INTEGER NOT NULL,
              idempotency_key TEXT,
              event_type TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              trace_id TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, event_id),
              UNIQUE (tenant_id, session_id, seq),
              UNIQUE (tenant_id, session_id, idempotency_key, event_type)
            );

            CREATE TABLE IF NOT EXISTS memory (
              tenant_id TEXT NOT NULL,
              memory_id TEXT NOT NULL,
              scope_key TEXT NOT NULL,
              content TEXT NOT NULL,
              version INTEGER NOT NULL,
              metadata_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, memory_id)
            );

            CREATE TABLE IF NOT EXISTS summary (
              tenant_id TEXT NOT NULL,
              session_id TEXT NOT NULL,
              summary_version INTEGER NOT NULL,
              source_event_seq INTEGER NOT NULL,
              content TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, session_id, summary_version)
            );

            CREATE TABLE IF NOT EXISTS audit_log (
              audit_id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              channel TEXT,
              user_id TEXT,
              session_id TEXT,
              agent_name TEXT,
              tool_name TEXT,
              decision TEXT NOT NULL,
              latency_ms INTEGER,
              error_type TEXT,
              token_usage INTEGER,
              cost REAL,
              trace_id TEXT NOT NULL,
              metadata_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS idempotency (
              tenant_id TEXT NOT NULL,
              key TEXT NOT NULL,
              status TEXT NOT NULL,
              response_ref TEXT,
              result_json TEXT,
              trace_id TEXT,
              attempt INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, key)
            );

            CREATE TABLE IF NOT EXISTS compensation_task (
              task_id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              operation TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              status TEXT NOT NULL,
              attempt INTEGER NOT NULL DEFAULT 0,
              available_at TEXT NOT NULL,
              last_error TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS session_lock (
              tenant_id TEXT NOT NULL,
              session_id TEXT NOT NULL,
              token TEXT NOT NULL,
              fencing_token INTEGER NOT NULL DEFAULT 0,
              expires_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, session_id)
            );
            CREATE TABLE IF NOT EXISTS session_fence (
              tenant_id TEXT NOT NULL,
              session_id TEXT NOT NULL,
              fencing_token INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (tenant_id, session_id)
            );
            """
        )
        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(idempotency)").fetchall()}
        if "attempt" not in columns:
            self._conn.execute("ALTER TABLE idempotency ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1")
        lock_columns = {
            row["name"] for row in self._conn.execute("PRAGMA table_info(session_lock)").fetchall()
        }
        if "fencing_token" not in lock_columns:
            self._conn.execute(
                "ALTER TABLE session_lock ADD COLUMN fencing_token INTEGER NOT NULL DEFAULT 0"
            )
        self._conn.commit()

    def append_event(self, event: SessionEvent, fencing_token: int | None = None) -> int:
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            self._assert_fencing(event.tenant_id, event.session_id, fencing_token)
            if event.idempotency_key:
                existing = self._conn.execute(
                    (
                        "SELECT seq FROM message_event WHERE tenant_id = ? AND session_id = ? "
                        "AND idempotency_key = ? AND event_type = ?"
                    ),
                    (event.tenant_id, event.session_id, event.idempotency_key, event.event_type),
                ).fetchone()
                if existing:
                    return int(existing["seq"])
            row = self._conn.execute(
                """
                SELECT COALESCE(MAX(seq), 0) AS max_seq
                FROM message_event
                WHERE tenant_id = ? AND session_id = ?
                """,
                (event.tenant_id, event.session_id),
            ).fetchone()
            next_seq = int(row["max_seq"]) + 1
            self._conn.execute(
                """
                INSERT INTO message_event (
                  tenant_id, event_id, session_id, seq, idempotency_key,
                  event_type, payload_json, trace_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.tenant_id,
                    event.event_id,
                    event.session_id,
                    next_seq,
                    event.idempotency_key,
                    event.event_type,
                    json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
                    event.trace_id,
                    _dt(event.created_at),
                ),
            )
            self._conn.execute(
                """
                INSERT INTO session_state (
                  tenant_id, session_id, state_version, latest_event_seq, state_json
                ) VALUES (?, ?, 0, ?, '{}')
                ON CONFLICT(tenant_id, session_id) DO UPDATE
                SET latest_event_seq = excluded.latest_event_seq
                """,
                (event.tenant_id, event.session_id, next_seq),
            )
            return next_seq

    def load_events(self, tenant_id: str, session_id: str, after_seq: int = 0) -> list[SessionEvent]:
        rows = self._conn.execute(
            """
            SELECT * FROM message_event
            WHERE tenant_id = ? AND session_id = ? AND seq > ?
            ORDER BY seq ASC
            """,
            (tenant_id, session_id, after_seq),
        ).fetchall()
        return [
            SessionEvent(
                tenant_id=row["tenant_id"],
                session_id=row["session_id"],
                event_id=row["event_id"],
                event_type=row["event_type"],
                payload=json.loads(row["payload_json"]),
                trace_id=row["trace_id"],
                idempotency_key=row["idempotency_key"],
                seq=int(row["seq"]),
                created_at=_parse_dt(row["created_at"]),
            )
            for row in rows
        ]

    def load_state(self, tenant_id: str, session_id: str) -> SessionState:
        row = self._conn.execute(
            "SELECT * FROM session_state WHERE tenant_id = ? AND session_id = ?",
            (tenant_id, session_id),
        ).fetchone()
        if row is None:
            return SessionState(tenant_id=tenant_id, session_id=session_id)
        return SessionState(
            tenant_id=tenant_id,
            session_id=session_id,
            state=json.loads(row["state_json"]),
            state_version=int(row["state_version"]),
            latest_event_seq=int(row["latest_event_seq"]),
        )

    def compare_and_set_state(
        self,
        tenant_id: str,
        session_id: str,
        expected_version: int,
        state: dict,
        fencing_token: int | None = None,
    ) -> bool:
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            self._assert_fencing(tenant_id, session_id, fencing_token)
            cursor = self._conn.execute(
                """
                UPDATE session_state
                SET state_version = ?, state_json = ?
                WHERE tenant_id = ? AND session_id = ? AND state_version = ?
                """,
                (
                    expected_version + 1,
                    json.dumps(state, ensure_ascii=False, sort_keys=True),
                    tenant_id,
                    session_id,
                    expected_version,
                ),
            )
            return cursor.rowcount == 1

    def _assert_fencing(self, tenant_id: str, session_id: str, fencing_token: int | None) -> None:
        if not fencing_token:
            return
        row = self._conn.execute(
            """
            SELECT 1 FROM session_lock
            WHERE tenant_id=? AND session_id=? AND fencing_token=? AND expires_at > ?
            """,
            (tenant_id, session_id, int(fencing_token), _dt(now_utc())),
        ).fetchone()
        if row is None:
            raise SessionLeaseLost(f"session fencing token rejected: {tenant_id}/{session_id}")

    def restore_state(
        self,
        tenant_id: str,
        session_id: str,
        state: dict,
        state_version: int,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO session_state (
                  tenant_id, session_id, state_version, latest_event_seq, state_json
                ) VALUES (?, ?, ?, COALESCE((
                  SELECT MAX(seq) FROM message_event
                  WHERE tenant_id = ? AND session_id = ?
                ), 0), ?)
                ON CONFLICT(tenant_id, session_id) DO UPDATE SET
                  state_version = excluded.state_version,
                  state_json = excluded.state_json
                """,
                (
                    tenant_id,
                    session_id,
                    int(state_version),
                    tenant_id,
                    session_id,
                    json.dumps(state, ensure_ascii=False, sort_keys=True),
                ),
            )

    def search(
        self,
        tenant_id: str,
        query: str,
        limit: int = 5,
        scope_keys: tuple[str, ...] | None = None,
    ) -> list[MemoryItem]:
        like = f"%{query}%"
        params: list[object] = [tenant_id, like, like]
        scope_sql = ""
        if scope_keys is not None:
            if not scope_keys:
                return []
            placeholders = ",".join("?" for _ in scope_keys)
            scope_sql = f" AND scope_key IN ({placeholders})"
            params.extend(scope_keys)
        params.append(limit)
        rows = self._conn.execute(
            f"""
            SELECT * FROM memory
            WHERE tenant_id = ? AND (? = '%%' OR content LIKE ?)
            {scope_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [
            MemoryItem(
                tenant_id=row["tenant_id"],
                memory_id=row["memory_id"],
                scope_key=row["scope_key"],
                content=row["content"],
                version=int(row["version"]),
                metadata=json.loads(row["metadata_json"]),
                created_at=_parse_dt(row["created_at"]),
            )
            for row in rows
        ]

    def put_summary(self, summary: Summary) -> None:
        self.put(summary)

    def latest(self, tenant_id: str, session_id: str) -> Summary | None:
        row = self._conn.execute(
            """
            SELECT * FROM summary
            WHERE tenant_id = ? AND session_id = ?
            ORDER BY summary_version DESC
            LIMIT 1
            """,
            (tenant_id, session_id),
        ).fetchone()
        if row is None:
            return None
        return Summary(
            tenant_id=row["tenant_id"],
            session_id=row["session_id"],
            content=row["content"],
            source_event_seq=int(row["source_event_seq"]),
            summary_version=int(row["summary_version"]),
            created_at=_parse_dt(row["created_at"]),
        )

    def put(self, value: MemoryItem | Summary) -> None:  # type: ignore[override]
        if isinstance(value, Summary):
            self._put_summary(value)
        else:
            self._put_memory(value)

    def _put_memory(self, item: MemoryItem) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO memory (
                  tenant_id, memory_id, scope_key, content, version, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, memory_id) DO UPDATE
                SET content = excluded.content,
                    version = excluded.version,
                    metadata_json = excluded.metadata_json
                """,
                (
                    item.tenant_id,
                    item.memory_id,
                    item.scope_key,
                    item.content,
                    item.version,
                    json.dumps(item.metadata, ensure_ascii=False, sort_keys=True),
                    _dt(item.created_at),
                ),
            )

    def _put_summary(self, summary: Summary) -> None:
        with self._lock, self._conn:
            current = self.latest(summary.tenant_id, summary.session_id)
            if current and current.source_event_seq > summary.source_event_seq:
                return
            next_version = 1 if current is None else current.summary_version + 1
            self._conn.execute(
                """
                INSERT INTO summary (
                  tenant_id, session_id, summary_version, source_event_seq, content, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    summary.tenant_id,
                    summary.session_id,
                    next_version,
                    summary.source_event_seq,
                    summary.content,
                    _dt(summary.created_at),
                ),
            )

    def append(self, record: AuditRecord) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO audit_log (
                  audit_id, tenant_id, channel, user_id, session_id, agent_name,
                  tool_name, decision, latency_ms, error_type, token_usage, cost,
                  trace_id, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
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
                    json.dumps(record.metadata, ensure_ascii=False, sort_keys=True),
                    _dt(record.created_at),
                ),
            )

    def list_by_tenant(self, tenant_id: str, limit: int = 100) -> list[AuditRecord]:
        rows = self._conn.execute(
            """
            SELECT * FROM audit_log
            WHERE tenant_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (tenant_id, limit),
        ).fetchall()
        return [
            AuditRecord(
                audit_id=row["audit_id"],
                tenant_id=row["tenant_id"],
                channel=row["channel"],
                user_id=row["user_id"],
                session_id=row["session_id"],
                agent_name=row["agent_name"],
                tool_name=row["tool_name"],
                decision=row["decision"],
                latency_ms=row["latency_ms"],
                error_type=row["error_type"],
                token_usage=row["token_usage"],
                cost=row["cost"],
                trace_id=row["trace_id"],
                metadata=json.loads(row["metadata_json"]),
                created_at=_parse_dt(row["created_at"]),
            )
            for row in rows
        ]

    def start(
        self,
        tenant_id: str,
        key: str,
        trace_id: str,
        lease_seconds: int = 180,
    ) -> IdempotencyRecord:
        with self._lock, self._conn:
            current = self.get(tenant_id, key)
            if current:
                if current.status == IdempotencyStatus.FAILED or (
                    current.status == IdempotencyStatus.PROCESSING
                    and (now_utc() - current.updated_at).total_seconds() >= lease_seconds
                ):
                    now = now_utc()
                    self._conn.execute(
                        """
                        UPDATE idempotency
                        SET status = ?, trace_id = ?, response_ref = NULL,
                            result_json = NULL,
                            attempt = attempt + 1, updated_at = ?
                        WHERE tenant_id = ? AND key = ?
                        """,
                        (
                            IdempotencyStatus.PROCESSING.value,
                            trace_id,
                            _dt(now),
                            tenant_id,
                            key,
                        ),
                    )
                    current = self.get(tenant_id, key)
                return current
            record = IdempotencyRecord(
                tenant_id=tenant_id,
                key=key,
                status=IdempotencyStatus.PROCESSING,
                trace_id=trace_id,
            )
            self._conn.execute(
                """
                INSERT INTO idempotency (
                  tenant_id, key, status, response_ref, result_json, trace_id,
                  attempt, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    key,
                    record.status.value,
                    record.response_ref,
                    None,
                    trace_id,
                    1,
                    _dt(record.created_at),
                    _dt(record.updated_at),
                ),
            )
            return record

    def complete(self, tenant_id: str, key: str, response_ref: str, result: dict) -> IdempotencyRecord:
        return self._update_idempotency(tenant_id, key, IdempotencyStatus.COMPLETED, response_ref, result)

    def fail(self, tenant_id: str, key: str, error_type: str) -> IdempotencyRecord:
        return self._update_idempotency(tenant_id, key, IdempotencyStatus.FAILED, None, {"error_type": error_type})

    def get(self, tenant_id: str, key: str) -> IdempotencyRecord | None:
        row = self._conn.execute(
            "SELECT * FROM idempotency WHERE tenant_id = ? AND key = ?",
            (tenant_id, key),
        ).fetchone()
        if row is None:
            return None
        result_json = row["result_json"]
        return IdempotencyRecord(
            tenant_id=row["tenant_id"],
            key=row["key"],
            status=IdempotencyStatus(row["status"]),
            response_ref=row["response_ref"],
            result=json.loads(result_json) if result_json else None,
            trace_id=row["trace_id"],
            attempt=int(row["attempt"]) if "attempt" in row.keys() else 1,
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )

    def _update_idempotency(
        self,
        tenant_id: str,
        key: str,
        status: IdempotencyStatus,
        response_ref: str | None,
        result: dict,
    ) -> IdempotencyRecord:
        with self._lock, self._conn:
            now = now_utc()
            self._conn.execute(
                """
                UPDATE idempotency
                SET status = ?, response_ref = ?, result_json = ?, updated_at = ?
                WHERE tenant_id = ? AND key = ?
                """,
                (
                    status.value,
                    response_ref,
                    json.dumps(result, ensure_ascii=False, sort_keys=True),
                    _dt(now),
                    tenant_id,
                    key,
                ),
            )
            record = self.get(tenant_id, key)
            if record is None:
                raise KeyError(f"idempotency record missing: {tenant_id}/{key}")
            return record

    def claim_delivery(self, tenant_id: str, key: str) -> bool:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE idempotency
                SET result_json = json_set(COALESCE(NULLIF(result_json, ''), '{}'), '$.delivery_claimed', 1),
                    updated_at = ?
                WHERE tenant_id = ?
                  AND key = ?
                  AND status = ?
                  AND COALESCE(json_extract(result_json, '$.delivered'), 0) NOT IN (1, 'true')
                  AND COALESCE(json_extract(result_json, '$.delivery_claimed'), 0) NOT IN (1, 'true')
                """,
                (_dt(now_utc()), tenant_id, key, IdempotencyStatus.COMPLETED.value),
            )
            return cursor.rowcount == 1

    def release_delivery(self, tenant_id: str, key: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE idempotency
                SET result_json = json_remove(COALESCE(NULLIF(result_json, ''), '{}'), '$.delivery_claimed'),
                    updated_at = ?
                WHERE tenant_id = ? AND key = ?
                """,
                (_dt(now_utc()), tenant_id, key),
            )

    def _compensation_enqueue(self, tenant_id, operation, payload, task_id=None):
        from trpc_service.storage.base import CompensationTask
        from uuid import uuid4

        task = CompensationTask(
            task_id=task_id or str(uuid4()),
            tenant_id=tenant_id,
            operation=operation,
            payload=dict(payload),
        )
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO compensation_task (
                  task_id, tenant_id, operation, payload_json, status, attempt,
                  available_at, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.task_id,
                    task.tenant_id,
                    task.operation,
                    json.dumps(task.payload, ensure_ascii=False, sort_keys=True),
                    task.status,
                    task.attempt,
                    _dt(task.available_at),
                    task.last_error,
                    _dt(task.created_at),
                    _dt(task.updated_at),
                ),
            )
            row = self._conn.execute(
                "SELECT * FROM compensation_task WHERE task_id = ?",
                (task.task_id,),
            ).fetchone()
        data = dict(row)
        data["payload"] = json.loads(data.pop("payload_json"))
        return _task_from_dict(data)

    def _compensation_claim(self, limit=10, tenant_id=None):
        result = []
        with self._lock, self._conn:
            if tenant_id is not None:
                rows = self._conn.execute(
                    """
                    SELECT * FROM compensation_task
                    WHERE tenant_id = ? AND status = 'pending' AND available_at <= ?
                    ORDER BY created_at
                    LIMIT ?
                    """,
                    (tenant_id, _dt(now_utc()), int(limit)),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT * FROM compensation_task
                    WHERE status = 'pending' AND available_at <= ?
                    ORDER BY created_at
                    LIMIT ?
                    """,
                    (_dt(now_utc()), int(limit)),
                ).fetchall()
            for row in rows:
                changed = self._conn.execute(
                    """
                    UPDATE compensation_task
                    SET status = 'processing', attempt = attempt + 1, updated_at = ?
                    WHERE task_id = ? AND status = 'pending'
                    """,
                    (_dt(now_utc()), row["task_id"]),
                ).rowcount
                if not changed:
                    continue
                current = self._conn.execute(
                    "SELECT * FROM compensation_task WHERE task_id = ?",
                    (row["task_id"],),
                ).fetchone()
                data = dict(current)
                data["payload"] = json.loads(data.pop("payload_json"))
                result.append(_task_from_dict(data))
        return result

    def _compensation_complete(self, task_id, tenant_id=None):
        with self._lock, self._conn:
            query = (
                """
                UPDATE compensation_task
                SET status = 'completed', updated_at = ?
                WHERE task_id = ?
                """
                + (" AND tenant_id = ?" if tenant_id is not None else "")
            )
            params = (_dt(now_utc()), task_id, tenant_id) if tenant_id is not None else (_dt(now_utc()), task_id)
            self._conn.execute(query, params)

    def _compensation_fail(self, task_id, error, retry_after_seconds=30, tenant_id=None):
        from datetime import timedelta

        now = now_utc()
        with self._lock, self._conn:
            query = (
                """
                UPDATE compensation_task
                SET status = CASE
                               WHEN attempt >= ? THEN 'dead'
                               ELSE 'pending'
                             END,
                    last_error = ?, available_at = ?, updated_at = ?
                WHERE task_id = ?
                """
                + (" AND tenant_id = ?" if tenant_id is not None else "")
            )
            params = (
                (
                    int(os.getenv("COMPENSATION_MAX_ATTEMPTS", "10")),
                    redact_secret_text(str(error))[:1000],
                    _dt(now + timedelta(seconds=retry_after_seconds)),
                    _dt(now),
                    task_id,
                    tenant_id,
                )
                if tenant_id is not None
                else (
                    int(os.getenv("COMPENSATION_MAX_ATTEMPTS", "10")),
                    redact_secret_text(str(error))[:1000],
                    _dt(now + timedelta(seconds=retry_after_seconds)),
                    _dt(now),
                    task_id,
                )
            )
            self._conn.execute(query, params)

    def export_schema(self) -> str:
        rows = self._conn.execute("SELECT sql FROM sqlite_master WHERE type = 'table' ORDER BY name").fetchall()
        return "\n\n".join(row["sql"] for row in rows if row["sql"])

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def acquire_session_lock(self, tenant_id: str, session_id: str, timeout: float) -> str:
        token = str(uuid4())
        deadline = monotonic() + max(0.0, timeout)
        ttl = float(__import__("os").getenv("SQLITE_SESSION_LOCK_TTL_SECONDS", "120"))
        while True:
            now = now_utc()
            expires_at = now.timestamp() + ttl
            try:
                with self._lock, self._conn:
                    self._conn.execute("BEGIN IMMEDIATE")
                    self._conn.execute(
                        "DELETE FROM session_lock WHERE expires_at <= ?",
                        (_dt(now),),
                    )
                    self._conn.execute(
                        """
                        INSERT INTO session_lock (tenant_id, session_id, token, expires_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            tenant_id,
                            session_id,
                            token,
                            datetime.fromtimestamp(expires_at, tz=now.tzinfo).isoformat(),
                        ),
                    )
                return token
            except sqlite3.IntegrityError:
                if monotonic() >= deadline:
                    raise TimeoutError(f"session lock timeout: {tenant_id}:{session_id}")
                sleep(0.05)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                if monotonic() >= deadline:
                    raise TimeoutError(f"session lock timeout: {tenant_id}:{session_id}") from exc
                sleep(0.05)

    def release_session_lock(self, tenant_id: str, session_id: str, token: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                DELETE FROM session_lock
                WHERE tenant_id = ? AND session_id = ? AND token = ?
                """,
                (tenant_id, session_id, token),
            )

    def acquire_session_lease(self, tenant_id: str, session_id: str, timeout: float) -> SessionLease:
        owner = str(uuid4())
        deadline = monotonic() + max(0.0, timeout)
        ttl = max(1.0, float(__import__("os").getenv("SESSION_LEASE_TTL_SECONDS", "120")))
        while True:
            now = now_utc()
            expires_at = now + timedelta(seconds=ttl)
            try:
                with self._lock, self._conn:
                    self._conn.execute("BEGIN IMMEDIATE")
                    self._conn.execute(
                        "DELETE FROM session_lock WHERE expires_at <= ?",
                        (_dt(now),),
                    )
                    current = self._conn.execute(
                        "SELECT 1 FROM session_lock WHERE tenant_id=? AND session_id=?",
                        (tenant_id, session_id),
                    ).fetchone()
                    if current is not None:
                        raise sqlite3.IntegrityError("session lease held")
                    self._conn.execute(
                        """
                        INSERT INTO session_fence (tenant_id, session_id, fencing_token)
                        VALUES (?, ?, 1)
                        ON CONFLICT(tenant_id, session_id) DO UPDATE
                        SET fencing_token = session_fence.fencing_token + 1
                        """,
                        (tenant_id, session_id),
                    )
                    fence = int(
                        self._conn.execute(
                            "SELECT fencing_token FROM session_fence WHERE tenant_id=? AND session_id=?",
                            (tenant_id, session_id),
                        ).fetchone()["fencing_token"]
                    )
                    self._conn.execute(
                        """
                        INSERT INTO session_lock (tenant_id, session_id, token, fencing_token, expires_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (tenant_id, session_id, owner, fence, _dt(expires_at)),
                    )
                return SessionLease(tenant_id, session_id, owner, fence, expires_at)
            except sqlite3.IntegrityError:
                if monotonic() >= deadline:
                    raise TimeoutError(f"session lease timeout: {tenant_id}:{session_id}")
                sleep(0.05)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                if monotonic() >= deadline:
                    raise TimeoutError(f"session lease timeout: {tenant_id}:{session_id}") from exc
                sleep(0.05)

    def release_session_lease(self, tenant_id: str, session_id: str, lease: SessionLease) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                DELETE FROM session_lock
                WHERE tenant_id=? AND session_id=? AND token=? AND fencing_token=?
                """,
                (tenant_id, session_id, lease.owner, lease.fencing_token),
            )

    def renew_session_lease(self, lease: SessionLease) -> SessionLease:
        ttl = max(1.0, float(__import__("os").getenv("SESSION_LEASE_TTL_SECONDS", "120")))
        expires_at = now_utc() + timedelta(seconds=ttl)
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE session_lock SET expires_at=?
                WHERE tenant_id=? AND session_id=? AND token=? AND fencing_token=?
                """,
                (_dt(expires_at), lease.tenant_id, lease.session_id, lease.owner, lease.fencing_token),
            )
            if cursor.rowcount != 1:
                raise SessionLeaseLost(f"session lease lost: {lease.tenant_id}/{lease.session_id}")
        return SessionLease(
            lease.tenant_id,
            lease.session_id,
            lease.owner,
            lease.fencing_token,
            expires_at,
        )

    def validate_session_lease(self, lease: SessionLease) -> None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1 FROM session_lock
                WHERE tenant_id=? AND session_id=? AND token=? AND fencing_token=?
                  AND expires_at > ?
                """,
                (
                    lease.tenant_id,
                    lease.session_id,
                    lease.owner,
                    lease.fencing_token,
                    _dt(now_utc()),
                ),
            ).fetchone()
            if row is None:
                raise SessionLeaseLost(f"session fencing token rejected: {lease.tenant_id}/{lease.session_id}")

    def __enter__(self) -> "SQLiteStorage":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


class _SQLiteCompensationStore:
    backend_name = "sql"

    def __init__(self, storage: SQLiteStorage) -> None:
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

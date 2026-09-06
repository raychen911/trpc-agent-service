"""Durable tool approvals, ambiguity tracking, and request budgets."""

from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import RLock
from typing import Any

from trpc_service.storage.base import ToolExecution, now_utc
from trpc_service.storage.locking import postgres_advisory_lock
from trpc_service.storage.postgres_rls import (
    _tenant_from_first_argument,
    _tenant_from_value,
    postgres_schema_auto_create,
    rls_tenant_method,
)


class ApprovalStatus:
    PENDING = "pending"
    APPROVED = "approved"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    AMBIGUOUS = "ambiguous"


class ToolExecutionStatus:
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


@dataclass(slots=True)
class ToolApproval:
    tenant_id: str
    approval_id: str
    session_id: str
    request_id: str
    tool_name: str
    arguments_hash: str
    status: str = ApprovalStatus.PENDING
    expires_at: datetime = field(default_factory=lambda: now_utc() + timedelta(seconds=600))
    approved_by: str | None = None
    consumed_at: datetime | None = None
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class ToolBudget:
    tenant_id: str
    request_id: str
    total_calls: int = 0
    side_effect_calls: int = 0
    max_calls: int = 16
    max_side_effect_calls: int = 4
    call_keys: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


def arguments_hash(arguments: dict[str, Any]) -> str:
    encoded = json.dumps(arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _approval_ttl() -> int:
    try:
        return max(1, int(os.getenv("TOOL_APPROVAL_TTL_SECONDS", "600")))
    except ValueError as exc:
        raise ValueError("TOOL_APPROVAL_TTL_SECONDS must be an integer") from exc


class InMemoryToolGovernanceStore:
    backend_name = "memory"

    def __init__(self) -> None:
        self._approvals: dict[tuple[str, str], ToolApproval] = {}
        self._budgets: dict[tuple[str, str], ToolBudget] = {}
        self._executions: dict[tuple[str, str], ToolExecution] = {}
        self._lock = RLock()

    def begin_execution(
        self,
        tenant_id: str,
        execution_id: str,
        request_id: str,
        session_id: str,
        tool_name: str,
        call_key: str,
        args_hash: str,
        side_effect: bool,
        fencing_token: int | None = None,
    ) -> ToolExecution:
        with self._lock:
            key = (tenant_id, call_key)
            current = self._executions.get(key)
            now = now_utc()
            if current is not None:
                if current.arguments_hash != args_hash or current.tool_name != tool_name:
                    current.status = ToolExecutionStatus.AMBIGUOUS
                    current.error_type = "execution_identity_conflict"
                    current.updated_at = now
                elif (
                    current.fencing_token is not None
                    and fencing_token is not None
                    and current.fencing_token != fencing_token
                ):
                    raise RuntimeError("tool execution fencing token is stale")
                elif current.status == ToolExecutionStatus.FAILED:
                    current.status = ToolExecutionStatus.RUNNING
                    current.attempt += 1
                    current.started_at = now
                    current.completed_at = None
                    current.error_type = None
                    current.error_message = None
                    current.updated_at = now
                return deepcopy(current)
            record = ToolExecution(
                tenant_id=tenant_id,
                execution_id=execution_id,
                request_id=request_id,
                session_id=session_id,
                tool_name=tool_name,
                call_key=call_key,
                arguments_hash=args_hash,
                side_effect=bool(side_effect),
                fencing_token=fencing_token,
                started_at=now,
                created_at=now,
                updated_at=now,
            )
            self._executions[key] = record
            return deepcopy(record)

    def get_execution(self, tenant_id: str, call_key: str) -> ToolExecution | None:
        with self._lock:
            record = self._executions.get((tenant_id, call_key))
            return deepcopy(record) if record else None

    def complete_execution(self, tenant_id: str, call_key: str, result: dict[str, Any],
                           fencing_token: int | None = None) -> ToolExecution:
        with self._lock:
            record = self._required_execution(tenant_id, call_key)
            self._assert_execution_fence(record, fencing_token)
            if record.status == ToolExecutionStatus.SUCCEEDED:
                return deepcopy(record)
            if record.status == ToolExecutionStatus.AMBIGUOUS:
                raise RuntimeError("tool execution is ambiguous")
            now = now_utc()
            record.status = ToolExecutionStatus.SUCCEEDED
            record.result = deepcopy(result)
            record.completed_at = now
            record.updated_at = now
            return deepcopy(record)

    def fail_execution(self, tenant_id: str, call_key: str, error_type: str,
                       error_message: str, fencing_token: int | None = None) -> ToolExecution:
        with self._lock:
            record = self._required_execution(tenant_id, call_key)
            self._assert_execution_fence(record, fencing_token)
            if record.status == ToolExecutionStatus.SUCCEEDED:
                return deepcopy(record)
            now = now_utc()
            record.status = ToolExecutionStatus.FAILED
            record.error_type = str(error_type)[:200]
            record.error_message = str(error_message)[:1000]
            record.completed_at = now
            record.updated_at = now
            return deepcopy(record)

    def list_executions_by_tenant(self, tenant_id: str) -> list[ToolExecution]:
        with self._lock:
            return sorted(
                (
                    deepcopy(record)
                    for (current_tenant, _), record in self._executions.items()
                    if current_tenant == tenant_id
                ),
                key=lambda item: item.created_at,
            )

    def restore_execution(self, record: ToolExecution) -> ToolExecution:
        with self._lock:
            current = self._executions.setdefault(
                (record.tenant_id, record.call_key), deepcopy(record)
            )
            return deepcopy(current)

    @staticmethod
    def _assert_execution_fence(record: ToolExecution, fencing_token: int | None) -> None:
        if (
            record.fencing_token is not None
            and fencing_token is not None
            and record.fencing_token != fencing_token
        ):
            raise RuntimeError("tool execution fencing token is stale")

    def _required_execution(self, tenant_id: str, call_key: str) -> ToolExecution:
        record = self._executions.get((tenant_id, call_key))
        if record is None:
            raise KeyError(f"tool execution not found: {tenant_id}/{call_key}")
        return record

    def create_or_get(
        self,
        tenant_id: str,
        approval_id: str,
        session_id: str,
        request_id: str,
        tool_name: str,
        args_hash: str,
        expires_seconds: int | None = None,
    ) -> ToolApproval:
        with self._lock:
            key = (tenant_id, approval_id)
            current = self._approvals.get(key)
            now = now_utc()
            if current is not None:
                self._expire(current, now)
                if current.arguments_hash != args_hash:
                    current.status = ApprovalStatus.AMBIGUOUS
                    current.updated_at = now
                return deepcopy(current)
            record = ToolApproval(
                tenant_id=tenant_id,
                approval_id=approval_id,
                session_id=session_id,
                request_id=request_id,
                tool_name=tool_name,
                arguments_hash=args_hash,
                expires_at=now + timedelta(seconds=expires_seconds or _approval_ttl()),
            )
            self._approvals[key] = record
            return deepcopy(record)

    def get(self, tenant_id: str, approval_id: str) -> ToolApproval | None:
        with self._lock:
            record = self._approvals.get((tenant_id, approval_id))
            if record:
                self._expire(record, now_utc())
                return deepcopy(record)
            return None

    def list_by_tenant(self, tenant_id: str) -> list[ToolApproval]:
        with self._lock:
            result = []
            for (current_tenant, _), record in self._approvals.items():
                if current_tenant == tenant_id:
                    self._expire(record, now_utc())
                    result.append(deepcopy(record))
            return sorted(result, key=lambda item: item.created_at)

    def list_budgets_by_tenant(self, tenant_id: str) -> list[ToolBudget]:
        with self._lock:
            return sorted(
                (
                    deepcopy(record)
                    for (current_tenant, _), record in self._budgets.items()
                    if current_tenant == tenant_id
                ),
                key=lambda item: item.created_at,
            )

    def restore_approval(self, record: ToolApproval) -> ToolApproval:
        with self._lock:
            current = self._approvals.get((record.tenant_id, record.approval_id))
            if current is None:
                self._approvals[(record.tenant_id, record.approval_id)] = deepcopy(record)
                current = self._approvals[(record.tenant_id, record.approval_id)]
            return deepcopy(current)

    def restore_budget(self, record: ToolBudget) -> ToolBudget:
        with self._lock:
            current = self._budgets.get((record.tenant_id, record.request_id))
            if current is None:
                self._budgets[(record.tenant_id, record.request_id)] = deepcopy(record)
                current = self._budgets[(record.tenant_id, record.request_id)]
            return deepcopy(current)

    def approve(self, tenant_id: str, approval_id: str, approved_by: str = "user") -> ToolApproval:
        with self._lock:
            record = self._required(tenant_id, approval_id)
            self._expire(record, now_utc())
            if record.status != ApprovalStatus.PENDING:
                raise RuntimeError(f"approval is not pending: {record.status}")
            record.status = ApprovalStatus.APPROVED
            record.approved_by = approved_by
            record.updated_at = now_utc()
            return deepcopy(record)

    def consume(
        self,
        tenant_id: str,
        approval_id: str,
        request_id: str,
        args_hash: str,
    ) -> ToolApproval:
        with self._lock:
            record = self._required(tenant_id, approval_id)
            now = now_utc()
            self._expire(record, now)
            if record.arguments_hash != args_hash:
                record.status = ApprovalStatus.AMBIGUOUS
                record.updated_at = now
                raise RuntimeError("approval arguments are ambiguous")
            if record.status == ApprovalStatus.CONSUMED:
                return deepcopy(record)
            if record.status not in {ApprovalStatus.PENDING, ApprovalStatus.APPROVED}:
                raise RuntimeError(f"approval cannot be consumed: {record.status}")
            record.status = ApprovalStatus.CONSUMED
            record.consumed_at = now
            record.updated_at = now
            return deepcopy(record)

    def reserve_call(
        self,
        tenant_id: str,
        request_id: str,
        call_key: str,
        side_effect: bool,
        max_calls: int,
        max_side_effect_calls: int,
    ) -> ToolBudget:
        with self._lock:
            budget = self._budgets.setdefault(
                (tenant_id, request_id),
                ToolBudget(
                    tenant_id=tenant_id,
                    request_id=request_id,
                    max_calls=max(1, int(max_calls)),
                    max_side_effect_calls=max(0, int(max_side_effect_calls)),
                ),
            )
            if call_key not in budget.call_keys:
                if budget.total_calls + 1 > budget.max_calls:
                    raise RuntimeError("tool call budget exceeded")
                if side_effect and budget.side_effect_calls + 1 > budget.max_side_effect_calls:
                    raise RuntimeError("side-effect tool call budget exceeded")
                budget.call_keys.append(call_key)
                budget.total_calls += 1
                budget.side_effect_calls += int(side_effect)
                budget.updated_at = now_utc()
            return deepcopy(budget)

    @staticmethod
    def _expire(record: ToolApproval, now: datetime) -> None:
        if record.status in {ApprovalStatus.PENDING, ApprovalStatus.APPROVED} and record.expires_at <= now:
            record.status = ApprovalStatus.EXPIRED
            record.updated_at = now

    def _required(self, tenant_id: str, approval_id: str) -> ToolApproval:
        record = self._approvals.get((tenant_id, approval_id))
        if record is None:
            raise KeyError(f"approval not found: {tenant_id}/{approval_id}")
        return record


class SQLiteToolGovernanceStore(InMemoryToolGovernanceStore):
    backend_name = "sql"

    def __init__(self, connection, lock: RLock) -> None:
        self._conn = connection
        self._lock = lock
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tool_approval (
                  tenant_id TEXT NOT NULL,
                  approval_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  request_id TEXT NOT NULL,
                  tool_name TEXT NOT NULL,
                  arguments_hash TEXT NOT NULL,
                  status TEXT NOT NULL,
                  expires_at TEXT NOT NULL,
                  approved_by TEXT,
                  consumed_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, approval_id)
                );
                CREATE TABLE IF NOT EXISTS tool_budget (
                  tenant_id TEXT NOT NULL,
                  request_id TEXT NOT NULL,
                  total_calls INTEGER NOT NULL DEFAULT 0,
                  side_effect_calls INTEGER NOT NULL DEFAULT 0,
                  max_calls INTEGER NOT NULL,
                  max_side_effect_calls INTEGER NOT NULL,
                  call_keys_json TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, request_id)
                );
                CREATE TABLE IF NOT EXISTS tool_executions (
                  tenant_id TEXT NOT NULL,
                  execution_id TEXT NOT NULL,
                  request_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  tool_name TEXT NOT NULL,
                  call_key TEXT NOT NULL,
                  arguments_hash TEXT NOT NULL,
                  side_effect INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL,
                  attempt INTEGER NOT NULL DEFAULT 1,
                  fencing_token INTEGER,
                  result_json TEXT,
                  error_type TEXT,
                  error_message TEXT,
                  started_at TEXT NOT NULL,
                  completed_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, execution_id),
                  UNIQUE (tenant_id, call_key)
                );
                """
            )

    def begin_execution(self, tenant_id, execution_id, request_id, session_id, tool_name, call_key,
                        args_hash, side_effect, fencing_token=None):
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM tool_executions WHERE tenant_id=? AND call_key=?",
                (tenant_id, call_key),
            ).fetchone()
            now = now_utc()
            if row is not None:
                current = self._from_execution(row)
                if current.arguments_hash != args_hash or current.tool_name != tool_name:
                    self._conn.execute(
                        "UPDATE tool_executions SET status=?, error_type=?, updated_at=? "
                        "WHERE tenant_id=? AND call_key=?",
                        (ToolExecutionStatus.AMBIGUOUS, "execution_identity_conflict",
                         _iso(now), tenant_id, call_key),
                    )
                elif (
                    current.fencing_token is not None
                    and fencing_token is not None
                    and current.fencing_token != fencing_token
                ):
                    raise RuntimeError("tool execution fencing token is stale")
                elif current.status == ToolExecutionStatus.FAILED:
                    self._conn.execute(
                        "UPDATE tool_executions SET status=?, attempt=attempt+1, started_at=?, "
                        "completed_at=NULL, error_type=NULL, error_message=NULL, updated_at=? "
                        "WHERE tenant_id=? AND call_key=?",
                        (ToolExecutionStatus.RUNNING, _iso(now), _iso(now), tenant_id, call_key),
                    )
                return self._from_execution(
                    self._conn.execute(
                        "SELECT * FROM tool_executions WHERE tenant_id=? AND call_key=?",
                        (tenant_id, call_key),
                    ).fetchone()
                )
            self._conn.execute(
                """
                INSERT INTO tool_executions (
                  tenant_id, execution_id, request_id, session_id, tool_name, call_key,
                  arguments_hash, side_effect, status, attempt, fencing_token,
                  started_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                """,
                (tenant_id, execution_id, request_id, session_id, tool_name, call_key,
                 args_hash, int(bool(side_effect)), ToolExecutionStatus.RUNNING,
                 fencing_token, _iso(now), _iso(now), _iso(now)),
            )
            return self._from_execution(
                self._conn.execute(
                    "SELECT * FROM tool_executions WHERE tenant_id=? AND call_key=?",
                    (tenant_id, call_key),
                ).fetchone()
            )

    def get_execution(self, tenant_id, call_key):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tool_executions WHERE tenant_id=? AND call_key=?",
                (tenant_id, call_key),
            ).fetchone()
            return self._from_execution(row) if row else None

    def complete_execution(self, tenant_id, call_key, result, fencing_token=None):
        with self._lock, self._conn:
            record = self.get_execution(tenant_id, call_key)
            if record is None:
                raise KeyError(f"tool execution not found: {tenant_id}/{call_key}")
            self._assert_execution_fence(record, fencing_token)
            if record.status == ToolExecutionStatus.SUCCEEDED:
                return record
            if record.status == ToolExecutionStatus.AMBIGUOUS:
                raise RuntimeError("tool execution is ambiguous")
            now = now_utc()
            self._conn.execute(
                "UPDATE tool_executions SET status=?, result_json=?, completed_at=?, updated_at=? "
                "WHERE tenant_id=? AND call_key=?",
                (ToolExecutionStatus.SUCCEEDED, json.dumps(result, default=str), _iso(now), _iso(now),
                 tenant_id, call_key),
            )
            return self.get_execution(tenant_id, call_key)

    def fail_execution(self, tenant_id, call_key, error_type, error_message, fencing_token=None):
        with self._lock, self._conn:
            record = self.get_execution(tenant_id, call_key)
            if record is None:
                raise KeyError(f"tool execution not found: {tenant_id}/{call_key}")
            self._assert_execution_fence(record, fencing_token)
            if record.status == ToolExecutionStatus.SUCCEEDED:
                return record
            now = now_utc()
            self._conn.execute(
                "UPDATE tool_executions SET status=?, error_type=?, error_message=?, completed_at=?, updated_at=? "
                "WHERE tenant_id=? AND call_key=?",
                (ToolExecutionStatus.FAILED, str(error_type)[:200], str(error_message)[:1000],
                 _iso(now), _iso(now), tenant_id, call_key),
            )
            return self.get_execution(tenant_id, call_key)

    def list_executions_by_tenant(self, tenant_id):
        with self._lock:
            return [
                self._from_execution(row)
                for row in self._conn.execute(
                    "SELECT * FROM tool_executions WHERE tenant_id=? ORDER BY created_at",
                    (tenant_id,),
                ).fetchall()
            ]

    def restore_execution(self, record):
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO tool_executions (
                  tenant_id, execution_id, request_id, session_id, tool_name, call_key,
                  arguments_hash, side_effect, status, attempt, fencing_token, result_json,
                  error_type, error_message, started_at, completed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (record.tenant_id, record.execution_id, record.request_id, record.session_id,
                 record.tool_name, record.call_key, record.arguments_hash, int(record.side_effect),
                 record.status, record.attempt, record.fencing_token,
                 json.dumps(record.result, default=str) if record.result is not None else None,
                 record.error_type, record.error_message, _iso(record.started_at),
                 _iso(record.completed_at), _iso(record.created_at), _iso(record.updated_at)),
            )
            return self.get_execution(record.tenant_id, record.call_key)

    def create_or_get(self, tenant_id, approval_id, session_id, request_id, tool_name, args_hash, expires_seconds=None):
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT * FROM tool_approval WHERE tenant_id=? AND approval_id=?",
                (tenant_id, approval_id),
            ).fetchone()
            if row is None:
                now = now_utc()
                self._conn.execute(
                    """
                    INSERT INTO tool_approval (
                      tenant_id, approval_id, session_id, request_id, tool_name, arguments_hash,
                      status, expires_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tenant_id, approval_id, session_id, request_id, tool_name, args_hash,
                        ApprovalStatus.PENDING,
                        _iso(now + timedelta(seconds=expires_seconds or _approval_ttl())),
                        _iso(now), _iso(now),
                    ),
                )
            else:
                current = self._from_approval(row)
                status = (
                    ApprovalStatus.AMBIGUOUS
                    if current.arguments_hash != args_hash
                    else current.status
                )
                if status != current.status:
                    self._conn.execute(
                        "UPDATE tool_approval SET status=?, updated_at=? WHERE tenant_id=? AND approval_id=?",
                        (status, _iso(now_utc()), tenant_id, approval_id),
                    )
            return self._from_approval(
                self._conn.execute(
                    "SELECT * FROM tool_approval WHERE tenant_id=? AND approval_id=?",
                    (tenant_id, approval_id),
                ).fetchone()
            )

    def get(self, tenant_id, approval_id):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tool_approval WHERE tenant_id=? AND approval_id=?",
                (tenant_id, approval_id),
            ).fetchone()
            if row is None:
                return None
            record = self._from_approval(row)
            if record.status in {ApprovalStatus.PENDING, ApprovalStatus.APPROVED} and record.expires_at <= now_utc():
                self._conn.execute(
                    "UPDATE tool_approval SET status=?, updated_at=? WHERE tenant_id=? AND approval_id=?",
                    (ApprovalStatus.EXPIRED, _iso(now_utc()), tenant_id, approval_id),
                )
                record.status = ApprovalStatus.EXPIRED
            return record

    def list_by_tenant(self, tenant_id):
        return [
            self.get(tenant_id, row["approval_id"])
            for row in self._conn.execute(
                "SELECT approval_id FROM tool_approval WHERE tenant_id=? ORDER BY created_at",
                (tenant_id,),
            ).fetchall()
        ]

    def list_budgets_by_tenant(self, tenant_id):
        with self._lock:
            return [
                self._from_budget(row)
                for row in self._conn.execute(
                    "SELECT * FROM tool_budget WHERE tenant_id=? ORDER BY created_at",
                    (tenant_id,),
                ).fetchall()
            ]

    def restore_approval(self, record):
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO tool_approval (
                  tenant_id, approval_id, session_id, request_id, tool_name, arguments_hash,
                  status, expires_at, approved_by, consumed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.tenant_id, record.approval_id, record.session_id, record.request_id,
                    record.tool_name, record.arguments_hash, record.status, _iso(record.expires_at),
                    record.approved_by, _iso(record.consumed_at), _iso(record.created_at), _iso(record.updated_at),
                ),
            )
            return self.get(record.tenant_id, record.approval_id)

    def restore_budget(self, record):
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO tool_budget (
                  tenant_id, request_id, total_calls, side_effect_calls, max_calls,
                  max_side_effect_calls, call_keys_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.tenant_id, record.request_id, record.total_calls, record.side_effect_calls,
                    record.max_calls, record.max_side_effect_calls, json.dumps(record.call_keys),
                    _iso(record.created_at), _iso(record.updated_at),
                ),
            )
            return self._from_budget(
                self._conn.execute(
                    "SELECT * FROM tool_budget WHERE tenant_id=? AND request_id=?",
                    (record.tenant_id, record.request_id),
                ).fetchone()
            )

    def approve(self, tenant_id, approval_id, approved_by="user"):
        with self._lock, self._conn:
            record = self.get(tenant_id, approval_id)
            if record is None:
                raise KeyError(f"approval not found: {tenant_id}/{approval_id}")
            if record.status != ApprovalStatus.PENDING:
                raise RuntimeError(f"approval is not pending: {record.status}")
            self._conn.execute(
                "UPDATE tool_approval SET status=?, approved_by=?, updated_at=? WHERE tenant_id=? AND approval_id=?",
                (ApprovalStatus.APPROVED, approved_by, _iso(now_utc()), tenant_id, approval_id),
            )
            return self.get(tenant_id, approval_id)

    def consume(self, tenant_id, approval_id, request_id, args_hash):
        with self._lock, self._conn:
            record = self.get(tenant_id, approval_id)
            if record is None:
                raise KeyError(f"approval not found: {tenant_id}/{approval_id}")
            if record.arguments_hash != args_hash:
                self._set_status(tenant_id, approval_id, ApprovalStatus.AMBIGUOUS)
                raise RuntimeError("approval arguments are ambiguous")
            if record.status == ApprovalStatus.CONSUMED:
                return record
            if record.status not in {ApprovalStatus.PENDING, ApprovalStatus.APPROVED}:
                raise RuntimeError(f"approval cannot be consumed: {record.status}")
            now = now_utc()
            self._conn.execute(
                """
                UPDATE tool_approval SET status=?, consumed_at=?, updated_at=?
                WHERE tenant_id=? AND approval_id=?
                """,
                (ApprovalStatus.CONSUMED, _iso(now), _iso(now), tenant_id, approval_id),
            )
            return self.get(tenant_id, approval_id)

    def reserve_call(self, tenant_id, request_id, call_key, side_effect, max_calls, max_side_effect_calls):
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT * FROM tool_budget WHERE tenant_id=? AND request_id=?",
                (tenant_id, request_id),
            ).fetchone()
            if row is None:
                now = now_utc()
                budget = ToolBudget(
                    tenant_id, request_id, 0, 0, max(1, int(max_calls)),
                    max(0, int(max_side_effect_calls)), [], now, now,
                )
            else:
                budget = self._from_budget(row)
            if call_key not in budget.call_keys:
                if budget.total_calls + 1 > budget.max_calls:
                    raise RuntimeError("tool call budget exceeded")
                if side_effect and budget.side_effect_calls + 1 > budget.max_side_effect_calls:
                    raise RuntimeError("side-effect tool call budget exceeded")
                budget.call_keys.append(call_key)
                budget.total_calls += 1
                budget.side_effect_calls += int(side_effect)
                budget.updated_at = now_utc()
            self._conn.execute(
                """
                INSERT INTO tool_budget (
                  tenant_id, request_id, total_calls, side_effect_calls, max_calls,
                  max_side_effect_calls, call_keys_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (tenant_id, request_id) DO UPDATE SET
                  total_calls=excluded.total_calls,
                  side_effect_calls=excluded.side_effect_calls,
                  call_keys_json=excluded.call_keys_json,
                  updated_at=excluded.updated_at
                """,
                (
                    budget.tenant_id, budget.request_id, budget.total_calls,
                    budget.side_effect_calls, budget.max_calls,
                    budget.max_side_effect_calls, json.dumps(budget.call_keys),
                    _iso(budget.created_at), _iso(budget.updated_at),
                ),
            )
            return deepcopy(budget)

    def _set_status(self, tenant_id, approval_id, status):
        self._conn.execute(
            "UPDATE tool_approval SET status=?, updated_at=? WHERE tenant_id=? AND approval_id=?",
            (status, _iso(now_utc()), tenant_id, approval_id),
        )

    @staticmethod
    def _from_approval(row):
        return ToolApproval(
            tenant_id=row["tenant_id"], approval_id=row["approval_id"], session_id=row["session_id"],
            request_id=row["request_id"], tool_name=row["tool_name"], arguments_hash=row["arguments_hash"],
            status=row["status"], expires_at=_parse(row["expires_at"]),
            approved_by=row["approved_by"], consumed_at=_parse(row["consumed_at"]),
            created_at=_parse(row["created_at"]), updated_at=_parse(row["updated_at"]),
        )

    @staticmethod
    def _from_budget(row):
        return ToolBudget(
            tenant_id=row["tenant_id"], request_id=row["request_id"],
            total_calls=int(row["total_calls"]), side_effect_calls=int(row["side_effect_calls"]),
            max_calls=int(row["max_calls"]), max_side_effect_calls=int(row["max_side_effect_calls"]),
            call_keys=list(json.loads(row["call_keys_json"])),
            created_at=_parse(row["created_at"]), updated_at=_parse(row["updated_at"]),
        )

    @staticmethod
    def _from_execution(row):
        return ToolExecution(
            tenant_id=row["tenant_id"],
            execution_id=row["execution_id"],
            request_id=row["request_id"],
            session_id=row["session_id"],
            tool_name=row["tool_name"],
            call_key=row["call_key"],
            arguments_hash=row["arguments_hash"],
            side_effect=bool(row["side_effect"]),
            status=row["status"],
            attempt=int(row["attempt"]),
            fencing_token=row["fencing_token"],
            result=json.loads(row["result_json"]) if row["result_json"] else None,
            error_type=row["error_type"],
            error_message=row["error_message"],
            started_at=_parse(row["started_at"]),
            completed_at=_parse(row["completed_at"]),
            created_at=_parse(row["created_at"]),
            updated_at=_parse(row["updated_at"]),
        )


class PostgresToolGovernanceStore(SQLiteToolGovernanceStore):
    backend_name = "postgres"

    def __init__(self, connection, lock: RLock, connection_provider=None) -> None:
        self._connection_provider = connection_provider
        self._conn = connection
        self._lock = lock
        if postgres_schema_auto_create():
            self._init_schema()

    def _init_schema(self):
        with self._lock, postgres_advisory_lock(self._conn, "trpc-agent-tool-schema-v1"):
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                CREATE TABLE IF NOT EXISTS tool_approval (
                  tenant_id TEXT NOT NULL, approval_id TEXT NOT NULL, session_id TEXT NOT NULL,
                  request_id TEXT NOT NULL, tool_name TEXT NOT NULL, arguments_hash TEXT NOT NULL,
                  status TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL, approved_by TEXT,
                  consumed_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, approval_id)
                );
                CREATE TABLE IF NOT EXISTS tool_budget (
                  tenant_id TEXT NOT NULL, request_id TEXT NOT NULL, total_calls INTEGER NOT NULL DEFAULT 0,
                  side_effect_calls INTEGER NOT NULL DEFAULT 0, max_calls INTEGER NOT NULL,
                  max_side_effect_calls INTEGER NOT NULL, call_keys_json JSONB NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, request_id)
                );
                CREATE TABLE IF NOT EXISTS tool_executions (
                  tenant_id TEXT NOT NULL,
                  execution_id TEXT NOT NULL,
                  request_id TEXT NOT NULL,
                  session_id TEXT NOT NULL,
                  tool_name TEXT NOT NULL,
                  call_key TEXT NOT NULL,
                  arguments_hash TEXT NOT NULL,
                  side_effect BOOLEAN NOT NULL DEFAULT FALSE,
                  status TEXT NOT NULL,
                  attempt INTEGER NOT NULL DEFAULT 1,
                  fencing_token BIGINT,
                  result_json JSONB,
                  error_type TEXT,
                  error_message TEXT,
                  started_at TIMESTAMPTZ NOT NULL,
                  completed_at TIMESTAMPTZ,
                  created_at TIMESTAMPTZ NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY (tenant_id, execution_id),
                  UNIQUE (tenant_id, call_key)
                );
                    """
                )

    def set_connection(self, connection):
        self._conn = connection

    def create_or_get(self, tenant_id, approval_id, session_id, request_id, tool_name, args_hash, expires_seconds=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            now = now_utc()
            cur.execute(
                """
                INSERT INTO tool_approval (
                  tenant_id, approval_id, session_id, request_id, tool_name, arguments_hash,
                  status, expires_at, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id, approval_id) DO NOTHING
                """,
                (
                    tenant_id, approval_id, session_id, request_id, tool_name, args_hash,
                    ApprovalStatus.PENDING, now + timedelta(seconds=expires_seconds or _approval_ttl()),
                    now, now,
                ),
            )
            cur.execute(
                "SELECT * FROM tool_approval WHERE tenant_id=%s AND approval_id=%s FOR UPDATE",
                (tenant_id, approval_id),
            )
            row = cur.fetchone()
            if row[5] != args_hash:
                cur.execute(
                    "UPDATE tool_approval SET status=%s, updated_at=%s WHERE tenant_id=%s AND approval_id=%s",
                    (ApprovalStatus.AMBIGUOUS, now, tenant_id, approval_id),
                )
                row = (*row[:6], ApprovalStatus.AMBIGUOUS, *row[7:])
            return self._from_pg_approval(row)

    def get(self, tenant_id, approval_id):
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tool_approval WHERE tenant_id=%s AND approval_id=%s",
                (tenant_id, approval_id),
            )
            row = cur.fetchone()
            if row is None:
                return None
            record = self._from_pg_approval(row)
            if record.status in {ApprovalStatus.PENDING, ApprovalStatus.APPROVED} and record.expires_at <= now_utc():
                with self._conn.cursor() as update:
                    update.execute(
                        "UPDATE tool_approval SET status=%s, updated_at=%s WHERE tenant_id=%s AND approval_id=%s",
                        (ApprovalStatus.EXPIRED, now_utc(), tenant_id, approval_id),
                    )
                record.status = ApprovalStatus.EXPIRED
            return record

    def list_by_tenant(self, tenant_id):
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tool_approval WHERE tenant_id=%s ORDER BY created_at",
                (tenant_id,),
            )
            return [self._from_pg_approval(row) for row in cur.fetchall()]

    def list_budgets_by_tenant(self, tenant_id):
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tool_budget WHERE tenant_id=%s ORDER BY created_at",
                (tenant_id,),
            )
            return [self._from_pg_budget(row) for row in cur.fetchall()]

    def restore_approval(self, record):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tool_approval (
                  tenant_id, approval_id, session_id, request_id, tool_name, arguments_hash,
                  status, expires_at, approved_by, consumed_at, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id, approval_id) DO NOTHING
                """,
                (
                    record.tenant_id, record.approval_id, record.session_id, record.request_id,
                    record.tool_name, record.arguments_hash, record.status, record.expires_at,
                    record.approved_by, record.consumed_at, record.created_at, record.updated_at,
                ),
            )
            cur.execute(
                "SELECT * FROM tool_approval WHERE tenant_id=%s AND approval_id=%s",
                (record.tenant_id, record.approval_id),
            )
            return self._from_pg_approval(cur.fetchone())

    def restore_budget(self, record):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tool_budget (
                  tenant_id, request_id, total_calls, side_effect_calls, max_calls,
                  max_side_effect_calls, call_keys_json, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id, request_id) DO NOTHING
                """,
                (
                    record.tenant_id, record.request_id, record.total_calls, record.side_effect_calls,
                    record.max_calls, record.max_side_effect_calls, json.dumps(record.call_keys),
                    record.created_at, record.updated_at,
                ),
            )
            cur.execute(
                "SELECT * FROM tool_budget WHERE tenant_id=%s AND request_id=%s",
                (record.tenant_id, record.request_id),
            )
            return self._from_pg_budget(cur.fetchone())

    def approve(self, tenant_id, approval_id, approved_by="user"):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tool_approval SET status=%s, approved_by=%s, updated_at=%s
                WHERE tenant_id=%s AND approval_id=%s AND status=%s AND expires_at>CURRENT_TIMESTAMP
                RETURNING *
                """,
                (ApprovalStatus.APPROVED, approved_by, now_utc(), tenant_id, approval_id, ApprovalStatus.PENDING),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("approval is not pending")
            return self._from_pg_approval(row)

    def consume(self, tenant_id, approval_id, request_id, args_hash):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tool_approval WHERE tenant_id=%s AND approval_id=%s FOR UPDATE",
                (tenant_id, approval_id),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(f"approval not found: {tenant_id}/{approval_id}")
            record = self._from_pg_approval(row)
            if record.arguments_hash != args_hash:
                cur.execute(
                    "UPDATE tool_approval SET status=%s, updated_at=%s WHERE tenant_id=%s AND approval_id=%s",
                    (ApprovalStatus.AMBIGUOUS, now_utc(), tenant_id, approval_id),
                )
                raise RuntimeError("approval arguments are ambiguous")
            if record.status == ApprovalStatus.CONSUMED:
                return record
            if record.status not in {ApprovalStatus.PENDING, ApprovalStatus.APPROVED} or record.expires_at <= now_utc():
                raise RuntimeError(f"approval cannot be consumed: {record.status}")
            now = now_utc()
            cur.execute(
                """
                UPDATE tool_approval SET status=%s, consumed_at=%s, updated_at=%s
                WHERE tenant_id=%s AND approval_id=%s
                RETURNING *
                """,
                (ApprovalStatus.CONSUMED, now, now, tenant_id, approval_id),
            )
            return self._from_pg_approval(cur.fetchone())

    def reserve_call(self, tenant_id, request_id, call_key, side_effect, max_calls, max_side_effect_calls):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tool_budget WHERE tenant_id=%s AND request_id=%s FOR UPDATE",
                (tenant_id, request_id),
            )
            row = cur.fetchone()
            if row is None:
                budget = ToolBudget(
                    tenant_id, request_id, 0, 0, max(1, int(max_calls)),
                    max(0, int(max_side_effect_calls)), [], now_utc(), now_utc(),
                )
            else:
                budget = self._from_pg_budget(row)
            if call_key not in budget.call_keys:
                if budget.total_calls + 1 > budget.max_calls:
                    raise RuntimeError("tool call budget exceeded")
                if side_effect and budget.side_effect_calls + 1 > budget.max_side_effect_calls:
                    raise RuntimeError("side-effect tool call budget exceeded")
                budget.call_keys.append(call_key)
                budget.total_calls += 1
                budget.side_effect_calls += int(side_effect)
                budget.updated_at = now_utc()
            cur.execute(
                """
                INSERT INTO tool_budget (
                  tenant_id, request_id, total_calls, side_effect_calls, max_calls,
                  max_side_effect_calls, call_keys_json, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id, request_id) DO UPDATE SET
                  total_calls=EXCLUDED.total_calls, side_effect_calls=EXCLUDED.side_effect_calls,
                  call_keys_json=EXCLUDED.call_keys_json, updated_at=EXCLUDED.updated_at
                """,
                (
                    budget.tenant_id, budget.request_id, budget.total_calls, budget.side_effect_calls,
                    budget.max_calls, budget.max_side_effect_calls, json.dumps(budget.call_keys),
                    budget.created_at, budget.updated_at,
                ),
            )
            return budget

    def begin_execution(self, tenant_id, execution_id, request_id, session_id, tool_name, call_key,
                        args_hash, side_effect, fencing_token=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            now = now_utc()
            cur.execute(
                "SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s FOR UPDATE",
                (tenant_id, call_key),
            )
            row = cur.fetchone()
            if row is not None:
                current = self._from_pg_execution(row)
                if current.arguments_hash != args_hash or current.tool_name != tool_name:
                    cur.execute(
                        "UPDATE tool_executions SET status=%s,error_type=%s,updated_at=%s "
                        "WHERE tenant_id=%s AND call_key=%s",
                        (ToolExecutionStatus.AMBIGUOUS, "execution_identity_conflict",
                         now, tenant_id, call_key),
                    )
                elif (
                    current.fencing_token is not None
                    and fencing_token is not None
                    and current.fencing_token != fencing_token
                ):
                    raise RuntimeError("tool execution fencing token is stale")
                elif current.status == ToolExecutionStatus.FAILED:
                    cur.execute(
                        "UPDATE tool_executions SET status=%s,attempt=attempt+1,started_at=%s,"
                        "completed_at=NULL,error_type=NULL,error_message=NULL,updated_at=%s "
                        "WHERE tenant_id=%s AND call_key=%s",
                        (ToolExecutionStatus.RUNNING, now, now, tenant_id, call_key),
                    )
                cur.execute(
                    "SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s",
                    (tenant_id, call_key),
                )
                return self._from_pg_execution(cur.fetchone())
            cur.execute(
                """
                INSERT INTO tool_executions (
                  tenant_id,execution_id,request_id,session_id,tool_name,call_key,arguments_hash,
                  side_effect,status,attempt,fencing_token,started_at,created_at,updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s,%s)
                """,
                (tenant_id, execution_id, request_id, session_id, tool_name, call_key, args_hash,
                 bool(side_effect), ToolExecutionStatus.RUNNING, fencing_token, now, now, now),
            )
            cur.execute(
                "SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s",
                (tenant_id, call_key),
            )
            return self._from_pg_execution(cur.fetchone())

    def get_execution(self, tenant_id, call_key):
        with self._lock, self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s",
                (tenant_id, call_key),
            )
            row = cur.fetchone()
            return self._from_pg_execution(row) if row else None

    def complete_execution(self, tenant_id, call_key, result, fencing_token=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s FOR UPDATE",
                (tenant_id, call_key),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(f"tool execution not found: {tenant_id}/{call_key}")
            record = self._from_pg_execution(row)
            self._assert_execution_fence(record, fencing_token)
            if record.status == ToolExecutionStatus.SUCCEEDED:
                return record
            if record.status == ToolExecutionStatus.AMBIGUOUS:
                raise RuntimeError("tool execution is ambiguous")
            now = now_utc()
            cur.execute(
                "UPDATE tool_executions SET status=%s,result_json=%s,completed_at=%s,updated_at=%s "
                "WHERE tenant_id=%s AND call_key=%s",
                (ToolExecutionStatus.SUCCEEDED, json.dumps(result, default=str), now, now,
                 tenant_id, call_key),
            )
            cur.execute("SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s",
                        (tenant_id, call_key))
            return self._from_pg_execution(cur.fetchone())

    def fail_execution(self, tenant_id, call_key, error_type, error_message, fencing_token=None):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s FOR UPDATE",
                (tenant_id, call_key),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(f"tool execution not found: {tenant_id}/{call_key}")
            record = self._from_pg_execution(row)
            self._assert_execution_fence(record, fencing_token)
            if record.status == ToolExecutionStatus.SUCCEEDED:
                return record
            now = now_utc()
            cur.execute(
                "UPDATE tool_executions SET status=%s,error_type=%s,error_message=%s,"
                "completed_at=%s,updated_at=%s WHERE tenant_id=%s AND call_key=%s",
                (ToolExecutionStatus.FAILED, str(error_type)[:200], str(error_message)[:1000],
                 now, now, tenant_id, call_key),
            )
            cur.execute("SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s",
                        (tenant_id, call_key))
            return self._from_pg_execution(cur.fetchone())

    def list_executions_by_tenant(self, tenant_id):
        with self._lock, self._conn.cursor() as cur:
            cur.execute("SELECT * FROM tool_executions WHERE tenant_id=%s ORDER BY created_at",
                        (tenant_id,))
            return [self._from_pg_execution(row) for row in cur.fetchall()]

    def restore_execution(self, record):
        with self._lock, self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tool_executions (
                  tenant_id,execution_id,request_id,session_id,tool_name,call_key,arguments_hash,
                  side_effect,status,attempt,fencing_token,result_json,error_type,error_message,
                  started_at,completed_at,created_at,updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id,call_key) DO NOTHING
                """,
                (record.tenant_id, record.execution_id, record.request_id, record.session_id,
                 record.tool_name, record.call_key, record.arguments_hash, record.side_effect,
                 record.status, record.attempt, record.fencing_token,
                 json.dumps(record.result, default=str) if record.result is not None else None,
                 record.error_type, record.error_message, record.started_at, record.completed_at,
                 record.created_at, record.updated_at),
            )
            cur.execute("SELECT * FROM tool_executions WHERE tenant_id=%s AND call_key=%s",
                        (record.tenant_id, record.call_key))
            return self._from_pg_execution(cur.fetchone())

    @staticmethod
    def _from_pg_approval(row):
        return ToolApproval(
            tenant_id=row[0], approval_id=row[1], session_id=row[2], request_id=row[3],
            tool_name=row[4], arguments_hash=row[5], status=row[6],
            expires_at=row[7], approved_by=row[8], consumed_at=row[9],
            created_at=row[10], updated_at=row[11],
        )

    @staticmethod
    def _from_pg_budget(row):
        keys = row[6] if isinstance(row[6], list) else json.loads(row[6])
        return ToolBudget(
            tenant_id=row[0], request_id=row[1], total_calls=int(row[2]),
            side_effect_calls=int(row[3]), max_calls=int(row[4]),
            max_side_effect_calls=int(row[5]), call_keys=list(keys),
            created_at=row[7], updated_at=row[8],
        )

    @staticmethod
    def _from_pg_execution(row):
        return ToolExecution(
            tenant_id=row[0], execution_id=row[1], request_id=row[2], session_id=row[3],
            tool_name=row[4], call_key=row[5], arguments_hash=row[6], side_effect=bool(row[7]),
            status=row[8], attempt=int(row[9]), fencing_token=row[10],
            result=_json_result(row[11]), error_type=row[12], error_message=row[13],
            started_at=row[14], completed_at=row[15], created_at=row[16], updated_at=row[17],
        )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _parse(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _json_result(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value) if isinstance(value, str) else dict(value)


_POSTGRES_TOOL_METHODS = {
    "create_or_get": _tenant_from_first_argument,
    "get": _tenant_from_first_argument,
    "list_by_tenant": _tenant_from_first_argument,
    "list_budgets_by_tenant": _tenant_from_first_argument,
    "approve": _tenant_from_first_argument,
    "consume": _tenant_from_first_argument,
    "reserve_call": _tenant_from_first_argument,
    "restore_approval": _tenant_from_value,
    "restore_budget": _tenant_from_value,
    "begin_execution": _tenant_from_first_argument,
    "get_execution": _tenant_from_first_argument,
    "complete_execution": _tenant_from_first_argument,
    "fail_execution": _tenant_from_first_argument,
    "list_executions_by_tenant": _tenant_from_first_argument,
    "restore_execution": _tenant_from_value,
}
for _method_name, _tenant_getter in _POSTGRES_TOOL_METHODS.items():
    setattr(
        PostgresToolGovernanceStore,
        _method_name,
        rls_tenant_method(_tenant_getter)(getattr(PostgresToolGovernanceStore, _method_name)),
    )
del _method_name, _tenant_getter

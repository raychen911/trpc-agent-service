"""One-time approval records for dangerous Tool calls."""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from typing import Any
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from pydantic import BaseModel

from trpc_service._compat import StrEnum


class ApprovalState(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"
    USED = "used"


class ApprovalError(PermissionError):
    """Raised for a missing, mismatched, expired or reused approval."""


class ToolApproval(BaseModel):
    approval_id: str
    tenant_id: str
    user_id: str
    session_id: str
    tool_name: str
    arguments_sha256: str
    state: ApprovalState = ApprovalState.PENDING
    token_hash: str = ""
    expires_at: datetime
    decided_by: str = ""


class InMemoryApprovalStore:
    """Reference approval state machine; tokens are returned once and stored hashed."""

    def __init__(self) -> None:
        self._records: dict[str, ToolApproval] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def arguments_hash(arguments_json: str) -> str:
        value = json.loads(arguments_json)
        canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    async def create(self,
                     tenant_id: str,
                     user_id: str,
                     session_id: str,
                     tool_name: str,
                     arguments_sha256: str,
                     ttl_seconds: int = 600) -> ToolApproval:
        approval_id = secrets.token_urlsafe(18)
        record = ToolApproval(
            approval_id=approval_id,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            tool_name=tool_name,
            arguments_sha256=arguments_sha256,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
        )
        async with self._lock:
            self._records[approval_id] = record
        return record.model_copy(deep=True)

    async def decide(self, approval_id: str, *, approve: bool, actor: str) -> tuple[ToolApproval, str]:
        async with self._lock:
            record = self._records.get(approval_id)
            if record is None or record.state != ApprovalState.PENDING:
                raise ApprovalError("approval is not pending")
            if record.expires_at <= datetime.now(timezone.utc):
                record.state = ApprovalState.EXPIRED
                raise ApprovalError("approval expired")
            record.decided_by = actor
            if not approve:
                record.state = ApprovalState.DENIED
                return record.model_copy(deep=True), ""
            token = secrets.token_urlsafe(32)
            record.token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
            record.state = ApprovalState.APPROVED
            return record.model_copy(deep=True), token

    async def consume(self, approval_id: str, token: str, *, tenant_id: str, user_id: str, session_id: str,
                      tool_name: str, arguments_sha256: str) -> ToolApproval:
        async with self._lock:
            record = self._records.get(approval_id)
            if record is None or record.state != ApprovalState.APPROVED:
                raise ApprovalError("approval is not usable")
            if record.expires_at <= datetime.now(timezone.utc):
                record.state = ApprovalState.EXPIRED
                raise ApprovalError("approval expired")
            expected = (record.tenant_id, record.user_id, record.session_id, record.tool_name, record.arguments_sha256)
            supplied = (tenant_id, user_id, session_id, tool_name, arguments_sha256)
            if expected != supplied:
                raise ApprovalError("approval scope mismatch")
            token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
            if not secrets.compare_digest(token_hash, record.token_hash):
                raise ApprovalError("approval token is invalid")
            record.state = ApprovalState.USED
            return record.model_copy(deep=True)


class PostgresApprovalStore:
    """Transactional one-time approval consumption shared by all Workers."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @staticmethod
    def arguments_hash(arguments_json: str) -> str:
        return InMemoryApprovalStore.arguments_hash(arguments_json)

    async def create(self,
                     tenant_id: str,
                     user_id: str,
                     session_id: str,
                     tool_name: str,
                     arguments_sha256: str,
                     ttl_seconds: int = 600) -> ToolApproval:
        record = ToolApproval(approval_id=secrets.token_urlsafe(18),
                              tenant_id=tenant_id,
                              user_id=user_id,
                              session_id=session_id,
                              tool_name=tool_name,
                              arguments_sha256=arguments_sha256,
                              expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds))
        await self._pool.execute(
            """
            INSERT INTO tool_approval
                (approval_id,tenant_id,request_id,user_id,session_id,tool_name,
                 arguments_sha256,status,expires_at)
            VALUES ($1,$2,'',$3,$4,$5,$6,'pending',$7)
            """, record.approval_id, tenant_id, user_id, session_id, tool_name, arguments_sha256, record.expires_at)
        return record

    async def decide(self, approval_id: str, *, approve: bool, actor: str) -> tuple[ToolApproval, str]:
        token = secrets.token_urlsafe(32) if approve else ""
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest() if token else None
        row = await self._pool.fetchrow(
            """
            UPDATE tool_approval
               SET status=CASE WHEN $2 THEN 'approved' ELSE 'denied' END,
                   confirmation_hash=$3,decided_by=$4,decided_at=now()
             WHERE approval_id=$1 AND status='pending' AND expires_at>now()
         RETURNING *
            """, approval_id, approve, token_hash, actor)
        if row is None:
            raise ApprovalError("approval is not pending or has expired")
        return self._from_row(row), token

    async def consume(self, approval_id: str, token: str, *, tenant_id: str, user_id: str, session_id: str,
                      tool_name: str, arguments_sha256: str) -> ToolApproval:
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        row = await self._pool.fetchrow(
            """
            UPDATE tool_approval SET status='used'
             WHERE approval_id=$1 AND status='approved' AND expires_at>now()
               AND tenant_id=$2 AND user_id=$3 AND session_id=$4
               AND tool_name=$5 AND arguments_sha256=$6
               AND confirmation_hash=$7
         RETURNING *
            """, approval_id, tenant_id, user_id, session_id, tool_name, arguments_sha256, token_hash)
        if row is None:
            raise ApprovalError("approval token is invalid, expired, mismatched or used")
        return self._from_row(row)

    @staticmethod
    def _from_row(row: Any) -> ToolApproval:
        return ToolApproval(approval_id=row["approval_id"],
                            tenant_id=row["tenant_id"],
                            user_id=row["user_id"],
                            session_id=row["session_id"],
                            tool_name=row["tool_name"],
                            arguments_sha256=row["arguments_sha256"],
                            state=row["status"],
                            token_hash=row["confirmation_hash"] or "",
                            expires_at=row["expires_at"],
                            decided_by=row["decided_by"] or "")

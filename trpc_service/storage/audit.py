"""SQL audit writer with a deny-by-construction body boundary."""

from __future__ import annotations

import uuid
import re
from dataclasses import dataclass
from dataclasses import field

from trpc_service.persistence import Database
from trpc_service.persistence.models import AuditLogRow
from sqlalchemy import select


_SENSITIVE_KEYS = {
    "api_key",
    "authorization",
    "credential",
    "password",
    "prompt",
    "response",
    "secret",
    "token",
    "tool_args",
    "tool_result",
}


@dataclass(frozen=True)
class AuditRecord:
    tenant_id: str
    decision: str
    channel: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    agent_name: str | None = None
    tool_name: str | None = None
    latency_ms: float | None = None
    error_type: str | None = None
    cost: float | None = None
    trace_id: str | None = None
    request_id: str | None = None
    config_version: int | None = None
    policy_version: int | None = None
    metadata: dict = field(default_factory=dict)


class AuditWriter:
    def __init__(self, database: Database) -> None:
        self._database = database

    def write(self, record: AuditRecord) -> str:
        lowered = {key.lower() for key in record.metadata}
        if any(any(marker in key for marker in _SENSITIVE_KEYS) for key in lowered):
            raise ValueError("sensitive bodies and credentials are forbidden in audit metadata")
        # Metadata is an identifier-only schema, not an arbitrary JSON body.
        allowed = {"execution_id", "operation_id", "binding_id", "profile_id", "migration_id"}
        if any(key not in allowed or not isinstance(value, str) or
               re.fullmatch(r"[A-Za-z0-9_:-]{1,128}", value) is None
               for key, value in record.metadata.items()):
            raise ValueError("audit metadata must contain only approved scalar identifiers")
        audit_id = uuid.uuid4().hex
        with self._database.sessions.begin() as session:
            session.add(
                AuditLogRow(
                    audit_id=audit_id,
                    tenant_id=record.tenant_id,
                    channel=record.channel,
                    user_id=record.user_id,
                    session_id=record.session_id,
                    agent_name=record.agent_name,
                    tool_name=record.tool_name,
                    decision=record.decision,
                    latency_ms=record.latency_ms,
                    error_type=record.error_type,
                    cost=record.cost,
                    trace_id=record.trace_id,
                    request_id=record.request_id,
                    config_version=record.config_version,
                    policy_version=record.policy_version,
                    redacted=True,
                    metadata_json=record.metadata,
                ))
        return audit_id

    def get(self, audit_id: str, *, tenant_id: str) -> AuditRecord:
        with self._database.sessions() as session:
            row = session.scalar(select(AuditLogRow).where(
                AuditLogRow.audit_id == audit_id, AuditLogRow.tenant_id == tenant_id))
            if row is None:
                raise LookupError("audit record not found")
            return AuditRecord(
                tenant_id=row.tenant_id,
                decision=row.decision,
                channel=row.channel,
                user_id=row.user_id,
                session_id=row.session_id,
                agent_name=row.agent_name,
                tool_name=row.tool_name,
                latency_ms=row.latency_ms,
                error_type=row.error_type,
                cost=row.cost,
                trace_id=row.trace_id,
                request_id=row.request_id,
                config_version=row.config_version,
                policy_version=row.policy_version,
                metadata=dict(row.metadata_json),
            )

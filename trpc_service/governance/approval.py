"""Stage 6A2 approval domain: vocabulary, immutable records, digests.

Pure domain module — no SQLAlchemy/SDK imports.  Tool arguments never appear
in these read models except as a SHA-256 digest; the restricted execution args
live only in the PostgreSQL row and are fetched via a dedicated repository
call for the approved execution path.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, Mapping

ApprovalDecision = Literal["approve", "reject"]

APPROVAL_STATES = frozenset({"pending", "executing", "completed", "rejected", "failed"})


class ApprovalAction(StrEnum):
    """What the caller should do after claiming an approval decision."""

    EXECUTE = "execute"
    REPLAY = "replay"
    IN_PROGRESS = "in_progress"
    CONFLICT = "conflict"
    NOT_AVAILABLE = "not_available"


@dataclass(frozen=True)
class ApprovalClaim:
    action: ApprovalAction
    approval_id: uuid.UUID | None
    state: str | None
    response_text: str | None


@dataclass(frozen=True)
class ApprovalRequest:
    approval_id: uuid.UUID
    tenant_id: str
    app_id: str
    config_version: int
    channel: str
    user_id: str
    session_id: str
    receipt_id: uuid.UUID
    function_call_id: str
    tool_name: str
    args_digest: str
    state: str
    decision: str | None
    decision_message_id: str | None
    response_text: str | None


class OrphanTerminationAction(StrEnum):
    """Outcome of the Stage 6D admin disposition of a stale ``executing``
    approval.  Only the two terminal actions succeed; everything else is a
    rejection the caller must not be able to turn into an execution — this
    surface NEVER runs the approved tool, it only books the failed terminal."""

    TERMINATED = "terminated"
    ALREADY_TERMINATED = "already_terminated"
    ACTIVE = "active"
    PENDING = "pending"
    NOT_AVAILABLE = "not_available"
    INCONSISTENT = "inconsistent"


@dataclass(frozen=True)
class OrphanTermination:
    action: OrphanTerminationAction
    approval_id: uuid.UUID | None
    state: str | None


@dataclass(frozen=True)
class OrphanedApproval:
    """One stale-executing candidate — opaque metadata only (digest, never
    args, never response text, never the raw user identity)."""

    approval_id: uuid.UUID
    tenant_id: str
    session_id: str
    function_call_id: str
    tool_name: str
    args_digest: str
    decision: str
    state: str
    decided_at: Any
    age_seconds: int


@dataclass(frozen=True)
class ApprovalAuditEvent:
    audit_id: uuid.UUID
    approval_id: uuid.UUID
    tenant_id: str
    event_type: str
    decision: str | None
    message_id: str | None
    args_digest: str
    occurred_at: Any


def compute_args_digest(tool_args: Mapping[str, Any]) -> str:
    """Canonical SHA-256 over JSON-serialized args (key order normalized)."""
    canonical = json.dumps(
        dict(tool_args),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


APPROVAL_REJECTED_RESULT: dict[str, str] = {"status": "rejected"}


def pending_reply_for(approval_id: uuid.UUID) -> str:
    """Fixed public pending text: opaque id + strict commands, no args."""
    return ("This action needs your approval. Reply exactly "
            f"\"/approve {approval_id}\" to run it, or \"/reject {approval_id}\" to deny. "
            "The tool has NOT been executed yet.")


__all__ = [
    "APPROVAL_REJECTED_RESULT",
    "APPROVAL_STATES",
    "ApprovalAction",
    "ApprovalAuditEvent",
    "ApprovalClaim",
    "ApprovalDecision",
    "ApprovalRequest",
    "OrphanTermination",
    "OrphanTerminationAction",
    "OrphanedApproval",
    "compute_args_digest",
    "pending_reply_for",
]

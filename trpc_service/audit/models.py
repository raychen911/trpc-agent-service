"""Immutable execution-audit event model and safe trace extraction.

Only fixed vocabularies exist here: event type, outcome, category and error
code are literals validated against the same enumerations the database
CHECK constraints enforce.  An event can never carry message body, reply
text, tool arguments, external identity or exception detail — the fields are
the type system, and ``extra="forbid"`` makes smuggling impossible.

Pairing rules (mirrored by PostgreSQL CHECKs in migration 0006):

===========================  =====================================
event_type                   outcome / extra constraints
===========================  =====================================
``content_decision``         allow | blocked; category required
``agent_result``             success | error; error_code on error
``tool_decision``            allow | deny_blocked | review_pending;
                             tool_name required
``delivery_result``          delivered | failed; error_code on failed
===========================  =====================================
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from typing import Literal

from opentelemetry import trace
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_validator,
    model_validator,
)

from trpc_service.transport.models import WorkerErrorCode

CATEGORY_TOKENS: frozenset[str] = frozenset({"none", "credential", "private_key", "credential_dsn"})

ContentCategoryToken = Literal["none", "credential", "private_key", "credential_dsn"]
EventOutcome = Literal[
    "allow",
    "blocked",
    "success",
    "error",
    "deny_blocked",
    "review_pending",
    "delivered",
    "failed",
]
EventType = Literal["content_decision", "agent_result", "tool_decision", "delivery_result"]

_TENANT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_TRACE_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")

# outcome vocabulary per event type — fixed, nothing else can pair.
_OUTCOME_BY_TYPE: dict[str, frozenset[str]] = {
    "content_decision": frozenset({"allow", "blocked"}),
    "agent_result": frozenset({"success", "error"}),
    "tool_decision": frozenset({"allow", "deny_blocked", "review_pending"}),
    "delivery_result": frozenset({"delivered", "failed"}),
}
# error_code is only meaningful on these two terminal failure pairs.
_ERROR_CODE_ALLOWED: frozenset[tuple[str, str]] = frozenset({
    ("agent_result", "error"),
    ("delivery_result", "failed"),
})


class ExecutionAuditEvent(BaseModel):
    """One append-only governance fact about a receipt's execution."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    audit_id: uuid.UUID
    tenant_id: str
    receipt_id: uuid.UUID | None
    request_id: uuid.UUID
    config_version: StrictInt = Field(ge=1)
    trace_id: str | None
    event_type: EventType
    outcome: EventOutcome
    category: ContentCategoryToken | None
    tool_name: str | None
    error_code: str | None
    latency_ms: StrictInt | None = Field(default=None, ge=0)
    occurred_at: AwareDatetime

    @field_validator("tenant_id")
    @classmethod
    def _check_tenant(cls, v: str) -> str:
        if _TENANT_ID_PATTERN.fullmatch(v) is None:
            raise ValueError("invalid tenant ID format")
        return v

    @field_validator("trace_id")
    @classmethod
    def _check_trace(cls, v: str | None) -> str | None:
        if v is not None and _TRACE_ID_PATTERN.fullmatch(v) is None:
            raise ValueError("trace_id must be 32 lowercase hex characters")
        return v

    @field_validator("error_code")
    @classmethod
    def _check_error_code(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            WorkerErrorCode(v)
        except ValueError:
            raise ValueError("error_code must be a known WorkerErrorCode") from None
        return v

    @model_validator(mode="after")
    def _check_pairings(self) -> "ExecutionAuditEvent":
        et, oc = self.event_type, self.outcome
        if self.receipt_id is None and et != "delivery_result":
            raise ValueError("only delivery_result may omit receipt_id")
        if oc not in _OUTCOME_BY_TYPE[et]:
            raise ValueError(f"outcome {oc!r} is invalid for event_type {et!r}")
        if et == "content_decision":
            if self.category is None:
                raise ValueError("content_decision requires a category")
        else:
            if self.category is not None:
                raise ValueError("only content_decision may carry a category")
        if et == "tool_decision":
            if self.tool_name is None or not self.tool_name.strip():
                raise ValueError("tool_decision requires a tool_name")
            if self.tool_name != self.tool_name.strip():
                raise ValueError("tool_name must already be normalized")
            if len(self.tool_name) > 200:
                raise ValueError("tool_name must be at most 200 characters")
        elif self.tool_name is not None:
            raise ValueError("only tool_decision may carry a tool_name")
        if self.error_code is not None and (et, oc) not in _ERROR_CODE_ALLOWED:
            raise ValueError("error_code is only allowed on agent_result/error or delivery_result/failed")
        if (et, oc) in _ERROR_CODE_ALLOWED and self.error_code is None:
            raise ValueError("failure outcomes require an error_code")
        return self


def delivery_event_factory(task) -> Callable[[WorkerErrorCode | None], ExecutionAuditEvent]:
    """Build ``delivery_result`` events for one request (receipt_id NULL).

    Used by the Gateway ingress boundary AFTER the message reached its
    terminal state; ``None`` error code yields the delivered outcome.  The
    task only needs ``tenant_id``, ``request_id`` and ``config_version``
    (WorkerTask and WorkerApprovalTask both qualify).
    """

    def _make(error_code: WorkerErrorCode | None) -> ExecutionAuditEvent:
        import uuid as _uuid
        from datetime import datetime, timezone

        code = error_code.value if error_code is not None else None
        return ExecutionAuditEvent(
            audit_id=_uuid.uuid4(),
            tenant_id=task.tenant_id,
            receipt_id=None,
            request_id=task.request_id,
            config_version=task.config_version,
            trace_id=current_trace_id(),
            event_type="delivery_result",
            outcome="failed" if code is not None else "delivered",
            category=None,
            tool_name=None,
            error_code=code,
            latency_ms=None,
            occurred_at=datetime.now(timezone.utc),
        )

    return _make


def current_trace_id() -> str | None:
    """Return the active OTel trace id as 32 lowercase hex, or None.

    Never raises: no tracer configured, a non-recording span, or an invalid
    span context all degrade to ``None`` so audit enrichment cannot break a
    user-facing request path.
    """
    try:
        ctx = trace.get_current_span().get_span_context()
    except Exception:
        return None
    if ctx is None or not ctx.is_valid:
        return None
    return format(ctx.trace_id, "032x")


__all__ = [
    "ExecutionAuditEvent",
    "current_trace_id",
    "delivery_event_factory",
]

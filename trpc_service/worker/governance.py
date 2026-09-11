"""Centralized content-governance + execution-audit boundary (Stage 6B2 Task 1).

One small seam, per the design rule "audit wiring goes through a small
service/repository boundary, never scattered into SDK adapters":

- :class:`ContentGovernance` applies the tenant's ``content_policy`` to an
  input or an output text and returns the fixed public replacement when the
  action blocks.  ``input_action``/``output_action`` are consumed HERE, not
  by the detector.
- :class:`ExecutionRecorder` accumulates the immutable ``ExecutionAuditEvent``
  facts for one WorkerTask execution with a fixed identity (tenant, receipt,
  request, config version) and a safely extracted trace id.  The recorded
  tuple is written by ``MessageReceiptRepository.complete()/fail()`` (or the
  approval pause/finalize transactions) INSIDE the same transaction as the
  receipt terminal state, so a required audit can never be missing while the
  receipt claims success.

Only fixed enumerations ever enter an event: bodies, replies, tool args,
external identities and exception details have no field here at all.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from trpc_service.audit.models import ExecutionAuditEvent, current_trace_id
from trpc_service.governance.content_policy import (
    CONTENT_OUTPUT_BLOCKED_TEXT,
    ContentPolicy,
    ContentPolicyConfig,
    ContentPolicyDecision,
)
from trpc_service.transport.models import WorkerApprovalTask, WorkerErrorCode, WorkerTask


class ContentGovernance:
    """Tenant content policy enforcement seam (input and output)."""

    def __init__(self, config: ContentPolicyConfig) -> None:
        self._config = config
        self._policy = ContentPolicy(config)

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    @property
    def output_enforced(self) -> bool:
        # Output enforcement requires buffering, so the check (and its audit
        # fact) only runs when the action actually blocks.  output_action
        # "allow" keeps live token streaming with no output inspection.
        return self._config.enabled and self._config.output_action == "block"

    def inspect_input(self, text: str) -> tuple[bool, ContentPolicyDecision]:
        """Return ``(blocked, decision)`` for an inbound user message."""
        if not self._config.enabled:
            return False, ContentPolicyDecision(allowed=True, category="none")
        decision = self._policy.inspect(text)
        blocked = (not decision.allowed) and self._config.input_action == "block"
        return blocked, decision

    def inspect_output(self, text: str) -> tuple[bool, ContentPolicyDecision]:
        """Return ``(blocked, decision)`` for the fully buffered reply."""
        decision = self._policy.inspect(text)
        blocked = not decision.allowed
        return blocked, decision

    def safe_output_text(self) -> str:
        return CONTENT_OUTPUT_BLOCKED_TEXT


class ExecutionRecorder:
    """Builds the audit trail bound to one task/receipt execution."""

    def __init__(self, task: WorkerTask | WorkerApprovalTask, receipt_id: uuid.UUID | None) -> None:
        self._task = task
        self._receipt_id = receipt_id
        self._events: list[ExecutionAuditEvent] = []

    def add(
        self,
        event_type: str,
        outcome: str,
        *,
        category: str | None = None,
        tool_name: str | None = None,
        error_code: WorkerErrorCode | str | None = None,
        latency_ms: int | None = None,
    ) -> ExecutionAuditEvent:
        event = self.derive(
            event_type,
            outcome,
            category=category,
            tool_name=tool_name,
            error_code=error_code,
            latency_ms=latency_ms,
        )
        self._events.append(event)
        return event

    def derive(
        self,
        event_type: str,
        outcome: str,
        *,
        category: str | None = None,
        tool_name: str | None = None,
        error_code: WorkerErrorCode | str | None = None,
        latency_ms: int | None = None,
    ) -> ExecutionAuditEvent:
        """Build a same-identity event WITHOUT appending (the tuple handed to
        a terminal transaction may be extended at commit time)."""
        code = error_code.value if isinstance(error_code, WorkerErrorCode) else error_code
        return ExecutionAuditEvent(
            audit_id=uuid.uuid4(),
            tenant_id=self._task.tenant_id,
            receipt_id=self._receipt_id,
            request_id=self._task.request_id,
            config_version=self._task.config_version,
            trace_id=current_trace_id(),
            event_type=event_type,
            outcome=outcome,
            category=category,
            tool_name=tool_name,
            error_code=code,
            latency_ms=latency_ms,
            occurred_at=datetime.now(timezone.utc),
        )

    def record_input_decision(self, governance: ContentGovernance, text: str) -> bool:
        """Inspect the input, append the fixed decision fact, return block."""
        if not governance.enabled:
            return False
        blocked, decision = governance.inspect_input(text)
        self.add("content_decision", "blocked" if blocked else "allow", category=decision.category)
        return blocked

    def snapshot(self) -> tuple[ExecutionAuditEvent, ...]:
        return tuple(self._events)


# ---------------------------------------------------------------------------
# Shared Stage 6C seams (WorkerService and ToolApprovalService use the exact
# same budget gate and accounting; no logic drift between the two entrypoints)
# ---------------------------------------------------------------------------


async def budget_block_reason(task, config, usage_repository, pricing):
    """Pre-model UTC-day budget check.

    Returns a fixed ``WorkerErrorCode`` when the request must not reach the
    model, else ``None``.  Decisive unknown handling: with a COST budget,
    an unknown accumulated cost or an unknown price for the tenant profile
    blocks (fail-closed); a TOKEN budget only blocks on proven exceedance.
    A usage-backend failure blocks fail-closed as repository-unavailable.
    """
    from datetime import datetime, timezone

    from trpc_service.transport.models import WorkerErrorCode

    limits = config.governance.limits
    if limits is None or usage_repository is None:
        return None
    if limits.daily_total_tokens is None and limits.daily_cost_microunits is None:
        return None
    if limits.daily_cost_microunits is not None:
        price = pricing.price_for(config.app.model_profile) if pricing is not None else None
        if price is None:
            return WorkerErrorCode.USAGE_BUDGET_EXCEEDED
    try:
        agg = await usage_repository.get_daily(task.tenant_id, datetime.now(timezone.utc).date())
    except Exception:
        return WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE
    if limits.daily_total_tokens is not None:
        totals = agg.tokens_or_none()
        if None not in totals and (totals[0] + totals[1]) >= limits.daily_total_tokens:
            return WorkerErrorCode.USAGE_BUDGET_EXCEEDED
    if limits.daily_cost_microunits is not None:
        cost = agg.cost_microunits_or_none()
        if cost is None or cost >= limits.daily_cost_microunits:
            return WorkerErrorCode.USAGE_BUDGET_EXCEEDED
    return None


async def record_usage(task, config, usage_acc, usage_repository, pricing, metrics) -> None:
    """Best-effort atomic accumulation after the turn.  A failed usage write
    is surfaced with one fixed log event; budgets are enforced on committed
    facts only (the same no-fabrication rule that keeps unknown usage NULL).
    """
    import logging
    from datetime import datetime, timezone

    from trpc_service.usage.models import UsageIncrement

    logger = logging.getLogger(__name__)
    if usage_repository is None or usage_acc is None:
        return None
    if not usage_acc.usage_seen:
        # No LLM response of this execution ever reached the accounting
        # seam: nothing to record (an input-blocked request must not make
        # the tenant-day unknown).
        return None
    input_tokens, output_tokens = usage_acc.snapshot()
    profile = config.app.model_profile
    cost = None
    if input_tokens is not None and output_tokens is not None and pricing is not None:
        cost = pricing.cost_microunits(profile, input_tokens, output_tokens)
    increment = UsageIncrement.for_request(
        usage_date=datetime.now(timezone.utc).date(),
        tenant_id=task.tenant_id,
        model_profile=profile,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_microunits=cost,
    )
    increment = increment.model_copy(
        update={
            "request_id": task.request_id,
            # WorkerTask deliberately does not carry receipt authority.  The
            # fact remains correlated by request/configuration until receipt data
            # is joined by the audit read model.
            "config_version": task.config_version,
            "occurred_at": datetime.now(timezone.utc),
        })
    try:
        await usage_repository.add_usage(increment)
    except Exception:
        logger.warning("worker usage record failed")
    if metrics is not None:
        if input_tokens is not None:
            metrics.record_counter("trpc.tokens", value=input_tokens, operation="llm_input", result="ok")
        if output_tokens is not None:
            metrics.record_counter("trpc.tokens", value=output_tokens, operation="llm_output", result="ok")
        if cost is not None:
            metrics.record_counter("trpc.cost.microunits", value=cost, operation="llm", result="known")
    return None


__all__ = [
    "ContentGovernance",
    "ExecutionRecorder",
    "budget_block_reason",
    "record_usage",
]

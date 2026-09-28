"""Tenant governance adapters applied at stable Agent pipeline boundaries."""

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from fnmatch import fnmatchcase
import logging
from time import perf_counter
from typing import Protocol, cast

from trpc_service.agent.approval import ApprovalRequestSnapshot, ApprovalService
from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentExecutionClaim,
    AgentExecutionRequest,
    AgentRunResult,
    AgentRuntimeConfig,
    AgentToolCall,
    AgentToolResult,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.agent.ports import (
    AgentContextBuilder,
    AgentOutputFilter,
    AgentPolicyEngine,
    AgentToolInvoker,
)
from trpc_service.agent.usage import UsageReader
from trpc_service.agent.ledger import ToolLedger, ToolLedgerStatus
from trpc_service.log import SensitiveDataRedactor
from trpc_service.metrics import PlatformTelemetry

logger = logging.getLogger(__name__)


class AgentAuditRecorder(Protocol):
    """Provider-neutral sink for immutable runtime audit decisions."""

    async def record(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        *,
        action: str,
        decision: str,
        reason_code: str,
        latency_ms: float,
        error_type: str | None = None,
        tool_name: str | None = None,
        cost_amount: float = 0,
        details: Mapping[str, object] | None = None,
    ) -> None:
        ...


class _SpanLike(Protocol):
    """Small span surface shared by OpenTelemetry and the local no-op."""

    def set_attribute(self, name: str, value: object) -> object:
        ...


class _NoopSpan:
    """Keep the governance adapter usable without an exporter in unit tests."""

    def set_attribute(self, name: str, value: object) -> None:
        del name, value


def _mapping(value: object, field: str) -> Mapping[str, object]:
    """Fail closed when flexible tenant configuration has an invalid shape."""

    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _string_set(value: object, field: str) -> frozenset[str]:
    """Validate a configured identity or capability list without coercion."""

    if value is None:
        return frozenset()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{field} must be an array of strings")
    if any(not isinstance(item, str) or item.strip() == "" for item in value):
        raise ValueError(f"{field} must contain non-empty strings")
    return frozenset(item.strip() for item in value)


def _redact_pii(config: AgentRuntimeConfig) -> bool:
    policy = _mapping(config.policy, "policy")
    governance = _mapping(policy.get("governance", {}), "governance policy")
    configured = governance.get("redact_pii", False)
    if not isinstance(configured, bool):
        raise ValueError("redact_pii must be a boolean")
    return configured


class TenantGovernancePolicy(AgentPolicyEngine):
    """Enforce tenant identity policy before invoking the model or Tools."""

    def __init__(
        self,
        *,
        telemetry: PlatformTelemetry | None = None,
        audit: AgentAuditRecorder | None = None,
        usage_reader: UsageReader | None = None,
    ) -> None:
        self._telemetry = telemetry
        self._audit = audit
        self._usage_reader = usage_reader

    async def evaluate(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
    ) -> PolicyDecision:
        """Return a stable decision and reason code suitable for audit."""

        started = perf_counter()
        decision = await self._evaluate(request, config)
        reason_code = str(decision.attributes.get("reason_code", "POLICY_UNKNOWN"))
        if self._telemetry is not None:
            self._telemetry.record_governance(decision.action.value, reason_code)
        if self._audit is not None:
            await self._audit.record(
                request,
                config,
                action="agent.policy.evaluate",
                decision=decision.action.value,
                reason_code=reason_code,
                latency_ms=(perf_counter() - started) * 1000,
                details={"reason": decision.reason or ""},
            )
        return decision

    async def _evaluate(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
    ) -> PolicyDecision:
        """Evaluate configuration separately from metrics and audit side effects."""

        try:
            policy = _mapping(config.policy, "policy")
            governance = _mapping(policy.get("governance", {}), "governance policy")
            limits = _mapping(policy.get("limits", {}), "governance limits")
            denied = _string_set(
                governance.get("denied_principals"),
                "denied_principals",
            )
            allowed = _string_set(
                governance.get("allowed_principals"),
                "allowed_principals",
            )
            max_input_chars = limits.get("max_input_chars")
            if (max_input_chars is not None
                    and (isinstance(max_input_chars, bool) or not isinstance(max_input_chars, int)
                         or max_input_chars < 1)):
                raise ValueError("max_input_chars must be a positive integer")
        except ValueError as error:
            return PolicyDecision(
                action=PolicyAction.DENY,
                reason=str(error),
                attributes={"reason_code": "POLICY_INVALID"},
            )
        if request.incoming.principal_id in denied:
            return PolicyDecision(
                action=PolicyAction.DENY,
                reason="principal is denied by tenant policy",
                attributes={"reason_code": "PRINCIPAL_DENIED"},
            )
        if allowed and request.incoming.principal_id not in allowed:
            return PolicyDecision(
                action=PolicyAction.DENY,
                reason="principal is not allowed by tenant policy",
                attributes={"reason_code": "PRINCIPAL_NOT_ALLOWED"},
            )
        if (max_input_chars is not None and len(request.incoming.text or "") > max_input_chars):
            return PolicyDecision(
                action=PolicyAction.DENY,
                reason="input exceeds the tenant character budget",
                attributes={"reason_code": "INPUT_BUDGET_EXCEEDED"},
            )
        ledger_limits = sorted(set(limits) - {"max_input_chars", "daily_tokens", "daily_calls"})
        if ledger_limits:
            return PolicyDecision(
                action=PolicyAction.DENY,
                reason="configured limits are not supported",
                attributes={
                    "reason_code": "BUDGET_LIMIT_UNSUPPORTED",
                    "limits": ledger_limits,
                },
            )
        budget_fields = ("daily_tokens", "daily_calls")
        for field in budget_fields:
            value = limits.get(field)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)
                                      or value < 1):
                return PolicyDecision(
                    action=PolicyAction.DENY,
                    reason=f"{field} must be a positive integer",
                    attributes={"reason_code": "POLICY_INVALID"},
                )
        if any(limits.get(field) is not None for field in budget_fields):
            if self._usage_reader is None:
                return PolicyDecision(
                    action=PolicyAction.REVIEW,
                    reason="configured limits require the durable usage ledger",
                    attributes={"reason_code": "BUDGET_LEDGER_REQUIRED"},
                )
            now = datetime.now(timezone.utc)
            daily_calls = limits.get("daily_calls")
            daily_tokens = limits.get("daily_tokens")
            daily_token_limit = (daily_tokens if isinstance(daily_tokens, int)
                                 and not isinstance(daily_tokens, bool) else None)
            context_window = config.model.get("context_window_tokens")
            maximum_output = config.model.get("max_output_tokens")
            if (daily_token_limit is not None
                    and (isinstance(context_window, bool) or not isinstance(context_window, int)
                         or isinstance(maximum_output, bool) or not isinstance(maximum_output, int)
                         or context_window < maximum_output or daily_token_limit < context_window)):
                return PolicyDecision(
                    action=PolicyAction.DENY,
                    reason=("daily_tokens requires context_window_tokens >= max_output_tokens "
                            "and daily_tokens >= context_window_tokens"),
                    attributes={"reason_code": "POLICY_INVALID"},
                )
            denial_code = await self._usage_reader.reserve(
                request,
                config,
                since=now.replace(hour=0, minute=0, second=0, microsecond=0),
                daily_calls=daily_calls if isinstance(daily_calls, int) else None,
                daily_tokens=daily_token_limit,
            )
            if denial_code == "DAILY_CALL_BUDGET_EXCEEDED":
                return PolicyDecision(
                    action=PolicyAction.DENY,
                    reason="tenant daily model call budget is exhausted",
                    attributes={"reason_code": denial_code},
                )
            if denial_code == "DAILY_TOKEN_BUDGET_EXCEEDED":
                return PolicyDecision(
                    action=PolicyAction.DENY,
                    reason="tenant daily token budget is exhausted",
                    attributes={"reason_code": denial_code},
                )
        return PolicyDecision(
            action=PolicyAction.ALLOW,
            attributes={"reason_code": "POLICY_ALLOWED"},
        )


class GovernanceOutputFilter(AgentOutputFilter):
    """Redact secrets and tenant-selected PII before durable output commit."""

    def __init__(self, redactor: SensitiveDataRedactor) -> None:
        self._redactor = redactor

    async def apply(
        self,
        context: AgentExecutionContext,
        result: AgentRunResult,
    ) -> AgentRunResult:
        """Return a redacted copy so the Runner result remains immutable."""

        redact_pii = _redact_pii(context.config)
        replies = tuple(
            replace(
                reply,
                text=(None if reply.text is None else self._redactor.redact_text(
                    reply.text,
                    redact_pii=redact_pii,
                )),
                attributes=self._redactor.redact_mapping(
                    reply.attributes,
                    redact_pii=redact_pii,
                ),
            ) for reply in result.replies)
        events = tuple(
            replace(
                event,
                payload=self._redactor.redact_mapping(
                    event.payload,
                    redact_pii=redact_pii,
                ),
            ) for event in result.events)
        return replace(result, replies=replies, events=events)


class GovernanceContextBuilder(AgentContextBuilder):
    """Sanitize untrusted model input after shared context has been loaded."""

    def __init__(
        self,
        delegate: AgentContextBuilder,
        redactor: SensitiveDataRedactor,
    ) -> None:
        self._delegate = delegate
        self._redactor = redactor

    async def build(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        policy: PolicyDecision,
        claim: AgentExecutionClaim,
    ) -> AgentExecutionContext:
        """Return an immutable context whose model-visible input is redacted."""

        context = await self._delegate.build(request, config, policy, claim)
        redact_pii = _redact_pii(config)
        # The storage builder may have enriched the immutable request. Sanitize
        # that authoritative result instead of using the pre-storage input.
        incoming = context.request.incoming
        sanitized = replace(
            incoming,
            text=(None if incoming.text is None else self._redactor.redact_text(
                incoming.text,
                redact_pii=redact_pii,
            )),
            attributes=self._redactor.redact_mapping(
                incoming.attributes,
                redact_pii=redact_pii,
            ),
        )
        trusted_attributes = dict(context.attributes)
        approval_id = incoming.attributes.get("approval_id")
        if (incoming.attributes.get("approval_verified") is True
                and incoming.attributes.get("approval_decision") == "approve"
                and isinstance(approval_id, str)):
            # Channel adapters create this evidence only after a durable scope
            # check. Free-form model arguments and user text never reach here.
            trusted_attributes["approval_id"] = approval_id
        return replace(
            context,
            request=replace(context.request, incoming=sanitized),
            attributes=trusted_attributes,
        )


class ToolApprovalRequired(PermissionError):
    """Raised when a high-risk Tool needs an out-of-band approval workflow."""

    def __init__(
        self,
        message: str,
        *,
        approval: ApprovalRequestSnapshot | None = None,
    ) -> None:
        super().__init__(message)
        self.approval = approval


class GovernedToolInvoker(AgentToolInvoker):
    """Enforce tenant grants for every executable Agent capability."""

    def __init__(
        self,
        delegate: AgentToolInvoker,
        *,
        telemetry: PlatformTelemetry | None = None,
        audit: AgentAuditRecorder | None = None,
        approvals: ApprovalService | None = None,
        ledger: ToolLedger | None = None,
    ) -> None:
        self._delegate = delegate
        self._telemetry = telemetry
        self._audit = audit
        self._approvals = approvals
        self._ledger = ledger

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        """Enforce grants, approval and ledger rules before delegation."""

        started = perf_counter()
        reason_code = "TOOL_COMPLETED"
        decision = "allow"
        error_type: str | None = None
        with self._span(context, call) as span:
            try:
                permissions = _mapping(context.config.tools, "capability permissions")
                try:
                    risk_level, legacy = self._authorize(permissions, call)
                except PermissionError:
                    reason_code = ("TOOL_NOT_ALLOWLISTED" if permissions.get("grants") is None else
                                   "CAPABILITY_NOT_GRANTED")
                    decision = "deny"
                    raise
                except ValueError:
                    reason_code = ("TOOL_POLICY_INVALID" if permissions.get("grants") is None else
                                   "CAPABILITY_POLICY_INVALID")
                    decision = "deny"
                    raise
                approvals = self._approvals
                if risk_level >= 2:
                    # Approval evidence must come from a trusted workflow, never
                    # from model-generated arguments or free-form user text.
                    reason_code = ("TOOL_APPROVAL_REQUIRED"
                                   if legacy else "CAPABILITY_APPROVAL_REQUIRED")
                    decision = "review"
                    if approvals is None:
                        raise ToolApprovalRequired(
                            f"capability requires trusted approval before execution: {call.name}")
                    if "approval_id" not in context.attributes:
                        approval = await approvals.request(
                            context,
                            call,
                            risk_level=risk_level,
                        )
                        raise ToolApprovalRequired(
                            f"capability requires trusted approval before execution: {call.name}",
                            approval=approval,
                        )
                    claimed_approval = await approvals.claim_execution(context, call)
                    reason_code = "CAPABILITY_APPROVED"
                    decision = "allow"
                else:
                    claimed_approval = None
                ledger_claim = None
                try:
                    ledger_claim = (None if self._ledger is None else await self._ledger.prepare(
                        context, call))
                    if ledger_claim is not None and not ledger_claim.should_execute:
                        if (ledger_claim.status is ToolLedgerStatus.SUCCEEDED
                                and ledger_claim.result is not None):
                            result = ledger_claim.result
                        else:
                            raise RuntimeError(
                                f"Tool call is quarantined in state {ledger_claim.status.value}")
                    else:
                        result = await self._delegate.invoke(context, call)
                        if self._ledger is not None:
                            await self._ledger.complete(context, call, result)
                except Exception as tool_error:
                    if (self._ledger is not None and ledger_claim is not None
                            and ledger_claim.should_execute):
                        # A non-trivial capability may have applied a provider
                        # side effect before failing to acknowledge it.
                        # Store only an error class: provider exceptions can
                        # include request data, credentials or response bodies.
                        safe_summary = f"Tool execution failed ({type(tool_error).__name__})"
                        try:
                            if risk_level >= 1:
                                await self._ledger.mark_unknown(context, call, safe_summary)
                            else:
                                await self._ledger.fail(context, call, safe_summary)
                        except Exception as ledger_error:
                            # A successful terminal ledger write followed by a
                            # lost database response is resolved on the next
                            # prepare. Never mask the original Tool failure.
                            logger.error(
                                "Tool Ledger failure transition failed error_type=%s",
                                type(ledger_error).__name__,
                            )
                    if claimed_approval is not None and approvals is not None:
                        try:
                            await approvals.mark_unknown(claimed_approval.approval_id)
                        except Exception as state_error:
                            # Preserve the provider failure while surfacing only
                            # the approval-state error type to safe application logs.
                            logger.error(
                                "Approval outcome quarantine failed error_type=%s",
                                type(state_error).__name__,
                            )
                    raise
                if claimed_approval is not None:
                    if approvals is None:
                        raise RuntimeError("claimed approval has no configured service")
                    await approvals.complete_execution(claimed_approval.approval_id)
            except Exception as error:
                error_type = type(error).__name__
                span.set_attribute("result", "error")
                span.set_attribute("error.type", error_type)
                await self._observe(
                    context,
                    call,
                    started=started,
                    result="error",
                    decision=decision,
                    reason_code=reason_code,
                    error_type=error_type,
                )
                raise
            span.set_attribute("result", "success")
            await self._observe(
                context,
                call,
                started=started,
                result="success",
                decision=decision,
                reason_code=reason_code,
                error_type=None,
            )
            return result

    @staticmethod
    def _authorize(
        permissions: Mapping[str, object],
        call: AgentToolCall,
    ) -> tuple[int, bool]:
        """Return the matched risk level, supporting the legacy Tool schema."""

        raw_grants = permissions.get("grants")
        if raw_grants is None:
            # The legacy allowlist predates typed capabilities and resource
            # patterns. Keep read-only Workspace and resource-bearing Tool
            # adapters usable during migration; MCP and Skill remain denied.
            if call.kind.value not in {"tool", "workspace"} or call.action != "execute":
                raise PermissionError(f"capability is not granted for this Agent: {call.name}")
            allowlist = _string_set(permissions.get("allowlist"), "tool allowlist")
            if call.name not in allowlist:
                raise PermissionError(f"tool is not allowlisted for this Agent: {call.name}")
            raw_risk_levels = _mapping(
                permissions.get("risk_levels", {}),
                "tool risk levels",
            )
            return (
                GovernedToolInvoker._risk_level(
                    raw_risk_levels.get(call.name, 0),
                    call.name,
                ),
                True,
            )

        if not isinstance(raw_grants, Sequence) or isinstance(raw_grants, (str, bytes)):
            raise ValueError("capability grants must be an array")
        for raw_grant in raw_grants:
            grant = _mapping(raw_grant, "capability grant")
            kind = grant.get("kind")
            name = grant.get("name")
            actions = _string_set(grant.get("actions"), "capability grant actions")
            resources = _string_set(grant.get("resources"), "capability grant resources")
            if not isinstance(kind, str) or not isinstance(name, str):
                raise ValueError("capability grant kind and name must be strings")
            if not actions:
                raise ValueError("capability grant actions cannot be empty")
            resource_allowed = (call.resource is None
                                and not resources) or (call.resource is not None and any(
                                    fnmatchcase(call.resource, pattern) for pattern in resources))
            if (kind == call.kind.value and name == call.name and call.action in actions
                    and resource_allowed):
                return GovernedToolInvoker._risk_level(grant.get("risk_level", 0), call.name), False
        raise PermissionError(f"capability is not granted for this Agent: {call.name}")

    @staticmethod
    def _risk_level(value: object, name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value not in range(4):
            raise ValueError(f"capability risk level must be an integer from 0 to 3: {name}")
        return value

    def _span(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AbstractContextManager[_SpanLike]:
        """Create a real or no-op Tool span without importing SDK internals."""

        if self._telemetry is None:
            return nullcontext(_NoopSpan())
        return cast(
            AbstractContextManager[_SpanLike],
            self._telemetry.start_span(
                "tool.invoke",
                attributes={
                    "tool.name": call.name,
                    "tenant.id": str(context.request.tenant.tenant_id),
                    "request.id": context.request.tenant.request_id,
                },
            ),
        )

    async def _observe(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        *,
        started: float,
        result: str,
        decision: str,
        reason_code: str,
        error_type: str | None,
    ) -> None:
        """Emit bounded Tool facts after an invocation reaches a terminal state."""

        latency_ms = (perf_counter() - started) * 1000
        if self._telemetry is not None:
            metric_tool_name = ("_not_allowlisted"
                                if reason_code == "TOOL_NOT_ALLOWLISTED" else call.name)
            self._telemetry.record_tool(
                tool_name=metric_tool_name,
                result=result,
                duration_seconds=latency_ms / 1000,
            )
            if decision != "allow":
                self._telemetry.record_governance(decision, reason_code)
        if self._audit is not None:
            await self._audit.record(
                context.request,
                context.config,
                action="agent.tool.invoke",
                decision=decision,
                reason_code=reason_code,
                latency_ms=latency_ms,
                error_type=error_type,
                tool_name=call.name,
            )

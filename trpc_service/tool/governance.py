"""Ordered tenant governance checks before any tool side effect."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from trpc_agent_sdk.tools.safety import (
    Decision as SdkDecision,
)
from trpc_agent_sdk.tools.safety import (
    ToolSafetyGuard,
    ToolScriptScanRequest,
)

from trpc_service.log.redaction import redact
from trpc_service.tenant.models import TenantConfig, TenantContext
from trpc_service.tool.confirmation import (
    ConfirmationScope,
    ConfirmationTokenService,
    arguments_hash,
)


class Decision(StrEnum):
    ALLOW = "allow"
    DENY = "deny"
    NEEDS_CONFIRMATION = "needs_confirmation"


class SafetyScanner(Protocol):
    async def scan(self, tool_name: str, arguments: dict[str, Any]) -> Decision: ...


class BudgetLedger(Protocol):
    async def reserve(
        self, tenant_id: str, *, token_units: int, cost_units: int, monthly_limit: int
    ) -> bool: ...


class GovernanceAuditSink(Protocol):
    async def record(
        self,
        *,
        context: TenantContext,
        config: TenantConfig,
        tool_name: str,
        decision: Decision,
        reason: str,
    ) -> None: ...


class InMemoryBudgetLedger:
    def __init__(self) -> None:
        self._spent: dict[str, int] = {}

    async def reserve(
        self, tenant_id: str, *, token_units: int, cost_units: int, monthly_limit: int
    ) -> bool:
        current = self._spent.get(tenant_id, 0)
        if current + cost_units > monthly_limit:
            return False
        self._spent[tenant_id] = current + cost_units
        return True


class SdkToolSafetyScanner:
    """Use SDK 1.1.x safety rules without its optional JSONL audit sink."""

    _FIELDS = (
        "script",
        "code",
        "command",
        "cmd",
        "python_code",
        "bash_code",
        "code_blocks",
    )

    def __init__(self, guard: ToolSafetyGuard | None = None) -> None:
        self._guard = guard or ToolSafetyGuard(audit_log_path=None, block_on_review=False)

    async def scan(self, tool_name: str, arguments: dict[str, Any]) -> Decision:
        result = Decision.ALLOW
        for field in self._FIELDS:
            value = arguments.get(field)
            if not value:
                continue
            values = value if isinstance(value, list) else [value]
            for script in values:
                if not isinstance(script, str):
                    continue
                language = "python" if field == "python_code" else "bash"
                report = self._guard.check(
                    ToolScriptScanRequest(
                        script=script,
                        language=language,
                        tool_name=tool_name,
                    )
                )
                if report.decision == SdkDecision.DENY:
                    return Decision.DENY
                if report.decision == SdkDecision.NEEDS_HUMAN_REVIEW:
                    result = Decision.NEEDS_CONFIRMATION
        return result


@dataclass(frozen=True, slots=True)
class GovernanceResult:
    decision: Decision
    reason: str
    audit_arguments: dict[str, Any]


class GovernancePipeline:
    """Identity -> allow-list -> safety -> confirmation -> budget -> redaction.

    Deterministic denials happen before a reservation so a rejected or
    confirmation-pending call cannot exhaust a tenant's budget.
    """

    def __init__(
        self,
        budget: BudgetLedger,
        scanner: SafetyScanner,
        confirmations: ConfirmationTokenService,
        audit: GovernanceAuditSink | None = None,
    ) -> None:
        self._budget = budget
        self._scanner = scanner
        self._confirmations = confirmations
        self._audit = audit

    async def evaluate(
        self,
        *,
        context: TenantContext,
        config: TenantConfig,
        tool_name: str,
        arguments: dict[str, Any],
        estimated_tokens: int = 0,
        estimated_cost: int = 0,
        confirmation_token: str | None = None,
    ) -> GovernanceResult:
        safe_arguments = redact(arguments)
        if context.tenant_id != config.tenant_id or context.app_id != config.app_id:
            return await self._result(
                context, config, tool_name, Decision.DENY, "identity_mismatch", safe_arguments
            )
        if tool_name not in config.tools.allow:
            return await self._result(
                context, config, tool_name, Decision.DENY, "tool_not_allowed", safe_arguments
            )
        safety = await self._scanner.scan(tool_name, arguments)
        requires_confirmation = (
            safety == Decision.NEEDS_CONFIRMATION or tool_name in config.tools.require_confirmation
        )
        if safety == Decision.DENY:
            return await self._result(
                context, config, tool_name, Decision.DENY, "safety_policy", safe_arguments
            )
        if requires_confirmation:
            scope = ConfirmationScope(
                tenant_id=context.tenant_id,
                principal_id=context.principal_id,
                session_id=context.session_id,
                tool_name=tool_name,
                arguments_hash=arguments_hash(arguments),
            )
            if not confirmation_token:
                return await self._result(
                    context,
                    config,
                    tool_name,
                    Decision.NEEDS_CONFIRMATION,
                    "confirmation_required",
                    safe_arguments,
                )
            await self._confirmations.consume(confirmation_token, scope)
        reserved = await self._budget.reserve(
            context.tenant_id,
            token_units=estimated_tokens,
            cost_units=estimated_cost,
            monthly_limit=config.budget.monthly_cost_units,
        )
        if not reserved:
            return await self._result(
                context, config, tool_name, Decision.DENY, "budget_exhausted", safe_arguments
            )
        return await self._result(
            context, config, tool_name, Decision.ALLOW, "policy_passed", safe_arguments
        )

    async def _result(
        self,
        context: TenantContext,
        config: TenantConfig,
        tool_name: str,
        decision: Decision,
        reason: str,
        audit_arguments: dict[str, Any],
    ) -> GovernanceResult:
        if self._audit is not None:
            await self._audit.record(
                context=context,
                config=config,
                tool_name=tool_name,
                decision=decision,
                reason=reason,
            )
        return GovernanceResult(decision, reason, audit_arguments)


__all__ = [
    "BudgetLedger",
    "Decision",
    "GovernanceAuditSink",
    "GovernancePipeline",
    "GovernanceResult",
    "InMemoryBudgetLedger",
    "SafetyScanner",
    "SdkToolSafetyScanner",
]

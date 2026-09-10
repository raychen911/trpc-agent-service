"""Single governance coordinator used by gateway, worker and recovery paths."""
from __future__ import annotations
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from trpc_service.governance.errors import GovernanceUnavailable, PrincipalUnauthorized, PolicyMissing
from trpc_service.governance.models import Decision, GovernanceAdmission, GovernanceContext, PolicyDecision, ToolDescriptor, ToolRiskLevel
from trpc_service.governance.content import inspect, InspectionAction


class GovernanceCoordinator:
    def __init__(self, ports: Any, *, audit: Any = None, metrics: Any = None) -> None:
        self.ports, self.audit, self.metrics = ports, audit, metrics

    async def authorize(self, *, tenant_id: str, agent_name: str, binding_id: str, principal: Any, tool: ToolDescriptor | None = None, session_id: str = "", execution_id: str = "", trace_id: str | None = None) -> GovernanceAdmission:
        policy = await self.ports.policy.get_active(tenant_id=tenant_id, agent_name=agent_name, binding_id=binding_id)
        grant = await self.ports.principal.evaluate(principal=principal, agent_name=agent_name, binding_id=binding_id, at=datetime.now(timezone.utc))
        if not grant.allowed:
            raise PrincipalUnauthorized()
        if tool is not None and tool.tool_name not in policy.document.allowed_tools:
            return GovernanceAdmission(decision=PolicyDecision(decision=Decision.DENY, policy_id=policy.policy_id, policy_version=policy.version, reason_code="tool_not_allowed", tool_name=tool.tool_name), context=None)
        if tool is not None and tool.risk_level == ToolRiskLevel.HIGH and (tool.confirmation_required is not False):
            decision = Decision.CONFIRMATION_REQUIRED
        else:
            decision = Decision.ALLOW
        context = GovernanceContext(tenant_id=tenant_id, agent_name=agent_name, binding_id=binding_id, principal_digest=principal.subject_digest, session_id=session_id, execution_id=execution_id, policy_version=policy.version, trace_id=trace_id)
        return GovernanceAdmission(decision=PolicyDecision(decision=decision, policy_id=policy.policy_id, policy_version=policy.version, reason_code="allowed" if decision == Decision.ALLOW else "confirmation_required", tool_name=getattr(tool, "tool_name", None)), context=context)

    async def inspect_content(self, text: str, *, rules: dict[str, str] | None = None):
        result = inspect(text, rules=rules)
        if result.action == InspectionAction.REJECT:
            raise PrincipalUnauthorized()
        return result

    async def reserve(self, *, tenant_id: str, execution_id: str, maximum: Any, generation: int = 0, policy: Any = None, trace_id: str = ""):
        return await self.ports.budget.reserve_maximum(tenant_id=tenant_id, execution_id=execution_id, maximum=maximum, owner_generation=generation, policy=policy, trace_id=trace_id)

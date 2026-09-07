"""tRPC-Agent Filter implementations for tenant tool governance."""

from __future__ import annotations

import logging
import time
from typing import Any

from trpc_agent_sdk.filter import BaseFilter, FilterHandleType, FilterResult

from tenant_agent.governance.policies import ConfirmationManager, GovernanceService
from tenant_agent.models import AuditRecord, TenantConfig
from tenant_agent.observability import ERRORS, TOOL_LATENCY, trace_id
from tenant_agent.storage.base import AuditRepository

logger = logging.getLogger(__name__)


class TenantToolGovernanceFilter(BaseFilter):  # type: ignore[misc]  # upstream lacks py.typed
    """Whitelist, confirmation, audit, and sanitization at the tool boundary."""

    def __init__(
        self,
        *,
        tenant: TenantConfig,
        app_id: str,
        tool_name: str,
        governance: GovernanceService,
        confirmations: ConfirmationManager,
        audit: AuditRepository,
    ) -> None:
        super().__init__()
        self.name = f"tenant_governance_{tool_name}"
        self.tenant = tenant
        self.app_id = app_id
        self.tool_name = tool_name
        self.governance = governance
        self.confirmations = confirmations
        self.audit = audit

    async def run(self, ctx: Any, req: Any, handle: FilterHandleType) -> FilterResult:
        started = time.perf_counter()
        decision = self.governance.authorize_tool(self.tenant, self.app_id, self.tool_name)
        session_id = str(ctx.get_metadata("session_id", "unknown"))
        user_id = str(ctx.get_metadata("user_id", "unknown"))
        channel = str(ctx.get_metadata("channel", "unknown"))
        result_label = "denied"
        handled_result: FilterResult | None = None
        response: Any
        if decision == "deny":
            response = {
                "error": "tool_not_permitted",
                "tool": self.tool_name,
                "message": "Tenant policy denied this tool.",
            }
        elif decision == "confirm":
            approved = False
            for token in ctx.get_metadata("confirmation_tokens", ()):
                if await self.confirmations.consume(
                    token,
                    tenant_id=self.tenant.tenant_id,
                    user_id=user_id,
                    session_id=session_id,
                    tool_name=self.tool_name,
                    args=req,
                ):
                    approved = True
                    break
            if not approved:
                token = self.confirmations.issue(
                    tenant_id=self.tenant.tenant_id,
                    user_id=user_id,
                    session_id=session_id,
                    tool_name=self.tool_name,
                    args=req,
                    ttl_seconds=self.tenant.governance.tools.confirmation_ttl_seconds,
                )
                response = {
                    "error": "confirmation_required",
                    "tool": self.tool_name,
                    "confirmation_token": token,
                    "message": f"Reply with /confirm {token} to authorize this exact call once.",
                }
            else:
                decision = "confirmed"
                handled_result = await handle()
                response = handled_result.rsp
                result_label = "error" if handled_result.error else "success"
        else:
            handled_result = await handle()
            response = handled_result.rsp
            result_label = "error" if handled_result.error else "success"

        latency_ms = (time.perf_counter() - started) * 1_000
        TOOL_LATENCY.labels(self.tenant.tenant_id, self.tool_name, result_label).observe(latency_ms / 1_000)
        tool_error = handled_result.error if handled_result is not None else None
        if self.tenant.audit.enabled:
            try:
                await self.audit.append_audit(
                    AuditRecord(
                        audit_id=f"tool-{time.time_ns()}",
                        tenant_id=self.tenant.tenant_id,
                        channel=channel,
                        user_id=user_id,
                        session_id=session_id,
                        agent_name=self.tenant.apps[self.app_id].agent_name,
                        tool_name=self.tool_name,
                        decision="tool_error" if tool_error else decision,
                        latency_ms=latency_ms,
                        error_type=(tool_error.__class__.__name__ if tool_error else None),
                        trace_id=trace_id(),
                        details={"argument_keys": sorted(req.keys()) if isinstance(req, dict) else []},
                    )
                )
            except Exception as exc:
                error_type = exc.__class__.__name__
                ERRORS.labels(
                    self.tenant.tenant_id,
                    "tool_audit",
                    error_type,
                ).inc()
                logger.warning("Tool audit failed with %s", error_type)
        if handled_result is not None:
            return handled_result
        return FilterResult(rsp=response, is_continue=False)

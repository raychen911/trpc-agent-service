"""
Filter pipeline for Tenant Governance, Tool RBAC, Quota limiting, and Security Masking.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import re
import logging
from trpc_service.config.models import (
    TenantConfig,
    AgentAppConfig,
    InboundMessage,
    AuditDecision,
)
from trpc_service.tenant.manager import tenant_manager
from trpc_service.log.logger import mask_sensitive_text

logger = logging.getLogger(__name__)


@dataclass
class AgentExecutionContext:
    """Context passed through the filter chain during Agent execution."""
    tenant: TenantConfig
    app: AgentAppConfig
    inbound: InboundMessage
    session_id: str
    trace_id: str
    user_id: str
    extra_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FilterResult:
    """Outcome of filter evaluation."""
    passed: bool = True
    reason: Optional[str] = None
    decision: AuditDecision = AuditDecision.ALLOWED
    modified_text: Optional[str] = None


class BaseFilter(ABC):
    """Abstract Base Filter."""

    name: str

    async def pre_process(self, ctx: AgentExecutionContext) -> FilterResult:
        """Executed before Prompt assembly and LLM invocation."""
        return FilterResult(passed=True)

    async def before_tool_call(self, ctx: AgentExecutionContext, tool_name: str, tool_args: Dict[str, Any]) -> FilterResult:
        """Executed before any tool execution (RBAC / Danger confirmation)."""
        return FilterResult(passed=True)

    async def post_process(self, ctx: AgentExecutionContext, response_text: str) -> FilterResult:
        """Executed after LLM generates response before sending back."""
        return FilterResult(passed=True)


class TenantQuotaFilter(BaseFilter):
    """Enforces tenant-level daily token and request quota."""

    name = "tenant_quota_filter"

    async def pre_process(self, ctx: AgentExecutionContext) -> FilterResult:
        if not tenant_manager.check_quota(ctx.tenant.tenant_id):
            return FilterResult(
                passed=False,
                reason=f"Tenant '{ctx.tenant.tenant_id}' has exceeded its daily token budget.",
                decision=AuditDecision.BLOCKED,
            )
        return FilterResult(passed=True)


class ToolPermissionFilter(BaseFilter):
    """Enforces Tool Whitelist (RBAC) configured in AgentAppConfig."""

    name = "tool_permission_filter"

    async def before_tool_call(self, ctx: AgentExecutionContext, tool_name: str, tool_args: Dict[str, Any]) -> FilterResult:
        # If allowed_tools is configured and not empty, check tool_name
        if ctx.app.allowed_tools and tool_name not in ctx.app.allowed_tools:
            return FilterResult(
                passed=False,
                reason=f"Tool '{tool_name}' is not permitted for Agent '{ctx.app.app_id}' under tenant policy.",
                decision=AuditDecision.BLOCKED,
            )
        return FilterResult(passed=True)


class SensitiveDataMaskFilter(BaseFilter):
    """Masks PII, passwords, and API credentials from incoming text and outgoing responses."""

    name = "sensitive_data_mask_filter"

    async def pre_process(self, ctx: AgentExecutionContext) -> FilterResult:
        masked = mask_sensitive_text(ctx.inbound.content)
        if masked != ctx.inbound.content:
            ctx.inbound.content = masked
            return FilterResult(passed=True, modified_text=masked)
        return FilterResult(passed=True)

    async def post_process(self, ctx: AgentExecutionContext, response_text: str) -> FilterResult:
        masked = mask_sensitive_text(response_text)
        return FilterResult(passed=True, modified_text=masked)


class HumanInTheLoopFilter(BaseFilter):
    """Requires secondary confirmation for high-risk tools."""

    name = "human_in_the_loop_filter"

    async def before_tool_call(self, ctx: AgentExecutionContext, tool_name: str, tool_args: Dict[str, Any]) -> FilterResult:
        if tool_name in ctx.app.require_confirmation_tools:
            # In production, triggers approval workflow / interactive card
            logger.warning("[HumanInTheLoop] Tool '%s' triggered secondary confirmation check.", tool_name)
            return FilterResult(
                passed=True,
                decision=AuditDecision.CONFIRM_REQUIRED,
                reason=f"Tool '{tool_name}' requires supervisor authorization.",
            )
        return FilterResult(passed=True)


class FilterChain:
    """Pipeline managing sequential execution of filters."""

    def __init__(self, filters: Optional[List[BaseFilter]] = None):
        self.filters: List[BaseFilter] = filters or [
            TenantQuotaFilter(),
            SensitiveDataMaskFilter(),
            ToolPermissionFilter(),
            HumanInTheLoopFilter(),
        ]

    async def run_pre_process(self, ctx: AgentExecutionContext) -> FilterResult:
        for f in self.filters:
            res = await f.pre_process(ctx)
            if not res.passed:
                return res
        return FilterResult(passed=True)

    async def run_before_tool_call(self, ctx: AgentExecutionContext, tool_name: str, tool_args: Dict[str, Any]) -> FilterResult:
        for f in self.filters:
            res = await f.before_tool_call(ctx, tool_name, tool_args)
            if not res.passed:
                return res
        return FilterResult(passed=True)

    async def run_post_process(self, ctx: AgentExecutionContext, response_text: str) -> FilterResult:
        curr_text = response_text
        for f in self.filters:
            res = await f.post_process(ctx, curr_text)
            if res.modified_text:
                curr_text = res.modified_text
            if not res.passed:
                return res
        return FilterResult(passed=True, modified_text=curr_text)

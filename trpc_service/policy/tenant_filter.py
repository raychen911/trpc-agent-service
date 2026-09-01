"""Tenant-scoped policy checks, redaction, and tool authorization."""

from __future__ import annotations

import re
from dataclasses import dataclass

from trpc_service.tenant.models import TenantConfig
from trpc_service.tenant.models import ChannelBinding


class PolicyDenied(PermissionError):
    pass


@dataclass(slots=True)
class TenantPolicy:
    config: TenantConfig
    agent_app_id: str | None = None

    def check_input(self, text: str) -> None:
        if len(text) > 20_000:
            raise PolicyDenied("input exceeds tenant size limit")

    @staticmethod
    def check_im_user(binding: ChannelBinding, external_user_id: str, internal_user_id: str | None = None) -> None:
        allowed = binding.allowed_user_ids
        identities = {external_user_id, internal_user_id or binding.resolve_user_id(external_user_id)}
        if allowed and not identities.intersection(allowed):
            raise PolicyDenied("IM user is outside tenant channel scope")

    def check_tool(self, tool_name: str, approval_granted: bool = False) -> None:
        app = self.config.app(self.agent_app_id or self.config.apps[0].agent_app_id)
        if tool_name in app.tool_policy.denylist:
            raise PolicyDenied(f"tool denied: {tool_name}")
        if app.tool_policy.allowlist and tool_name not in app.tool_policy.allowlist:
            raise PolicyDenied(f"tool not allowlisted: {tool_name}")
        if tool_name in app.tool_policy.approval_rules and not approval_granted:
            raise PolicyDenied(f"approval required: {tool_name}")

    def requires_tool_approval(self, tool_name: str) -> bool:
        app = self.config.app(self.agent_app_id or self.config.apps[0].agent_app_id)
        return tool_name in app.tool_policy.approval_rules

    def redact(self, value: str) -> str:
        rules = set(self.config.audit_policy.redact_rules)
        result = value
        if "email" in rules:
            result = re.sub(
                r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
                "[email-redacted]",
                result,
            )
        if "phone" in rules:
            result = re.sub(r"(?<!\d)(?:\+?86[- ]?)?1\d{10}(?!\d)", "[phone-redacted]", result)
        if "token" in rules or "authorization" in rules:
            result = re.sub(
                r"(?i)\b(?:bearer|token|authorization|api[_ -]?key)\s*[:=]\s*\S+",
                "[secret-redacted]",
                result,
            )
        return result

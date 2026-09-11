"""Tenant-level tool authorization and budget guard."""

from __future__ import annotations

from dataclasses import dataclass

from ..tenant.models import TenantConfig


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason: str
    requires_confirmation: bool = False


class TenantPolicyFilter:
    def __init__(self, config: TenantConfig) -> None:
        self.config = config

    def check_input(self, text: str) -> PolicyDecision:
        if len(text) > self.config.policy.max_input_chars:
            return PolicyDecision(False, "input_too_large")
        return PolicyDecision(True, "ok")

    def check_user(self, user_id: str, channel: str | None = None, account_id: str | None = None) -> PolicyDecision:
        if channel is not None:
            binding = self.config.channels.get(channel)
            if binding is None or (account_id is not None and binding.account_id != account_id):
                return PolicyDecision(False, "binding_not_allowed")
            restricted = binding.allowed_users
        else:
            restricted = frozenset().union(
                *(binding.allowed_users for binding in self.config.channels.values() if binding.allowed_users)
            )
        if restricted and user_id not in restricted:
            return PolicyDecision(False, "user_not_allowed")
        return PolicyDecision(True, "ok")

    def check_tool(self, tool_name: str, confirmed: bool = False) -> PolicyDecision:
        policy = self.config.policy
        if tool_name not in policy.allowed_tools:
            return PolicyDecision(False, "tool_not_allowlisted")
        if tool_name in policy.dangerous_tools and not confirmed:
            return PolicyDecision(False, "confirmation_required", requires_confirmation=True)
        return PolicyDecision(True, "ok")

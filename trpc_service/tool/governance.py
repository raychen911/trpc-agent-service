"""Budget reservation and one-time confirmation for tool side effects."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from .policy import PolicyDecision, TenantPolicyFilter


@dataclass(frozen=True)
class BudgetLease:
    tenant_id: str
    reservation_id: str
    tokens: int
    expires_at: float


class BudgetExceeded(RuntimeError):
    pass


class InMemoryBudgetLedger:
    def __init__(self) -> None:
        self.used: dict[tuple[str, date], int] = {}
        self.reserved: dict[tuple[str, date], int] = {}
        self._leases: dict[str, BudgetLease] = {}

    async def reserve(self, tenant_id: str, budget: int, tokens: int, ttl_seconds: int = 300) -> BudgetLease:
        if tokens < 0 or tokens > budget:
            raise BudgetExceeded("invalid token reservation")
        day_key = (tenant_id, datetime.now(timezone.utc).date())
        current = self.used.get(day_key, 0) + self.reserved.get(day_key, 0)
        if current + tokens > budget:
            raise BudgetExceeded("tenant token budget exceeded")
        identifier = hashlib.sha256(f"{tenant_id}:{time.time_ns()}".encode()).hexdigest()[:24]
        lease = BudgetLease(tenant_id, identifier, tokens, time.time() + ttl_seconds)
        self.reserved[day_key] = self.reserved.get(day_key, 0) + tokens
        self._leases[identifier] = lease
        return lease

    async def settle(self, lease: BudgetLease, actual_tokens: int) -> None:
        current = self._leases.pop(lease.reservation_id, None)
        if current is None:
            return
        day_key = (lease.tenant_id, datetime.now(timezone.utc).date())
        self.reserved[day_key] = max(0, self.reserved.get(day_key, 0) - current.tokens)
        self.used[day_key] = self.used.get(day_key, 0) + max(0, actual_tokens)


class ToolGovernance:
    def __init__(self, policy: TenantPolicyFilter, budget: InMemoryBudgetLedger | None = None) -> None:
        self.policy = policy
        self.budget = budget or InMemoryBudgetLedger()

    def check(self, tool_name: str, *, confirmed: bool = False) -> PolicyDecision:
        return self.policy.check_tool(tool_name, confirmed=confirmed)

    async def execute(
        self,
        tenant_id: str,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        token_budget: int,
        call: Callable[[], Awaitable[Any]],
        confirmed: bool = False,
    ) -> Any:
        decision = self.check(tool_name, confirmed=confirmed)
        if not decision.allowed:
            raise PermissionError(decision.reason)
        lease = await self.budget.reserve(tenant_id, token_budget, 0)
        try:
            return await call()
        finally:
            await self.budget.settle(lease, 0)


def confirmation_fingerprint(tenant_id: str, session_id: str, tool_name: str, arguments: dict[str, Any], key: bytes) -> str:
    canonical = json.dumps([tenant_id, session_id, tool_name, arguments], sort_keys=True, separators=(",", ":"))
    return hmac.new(key, canonical.encode(), hashlib.sha256).hexdigest()

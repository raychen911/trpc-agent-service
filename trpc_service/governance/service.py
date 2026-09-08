from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.governance.pii import PiiRedactor
from trpc_service.storage.contracts import AuditEntry, AuditStore
from trpc_service.storage.models import Tenant, TenantBudgetUsage

if TYPE_CHECKING:
    from trpc_service.gateway.contracts import AgentReply, NormalizedMessage


class GovernanceDeniedError(PermissionError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class BudgetReservation:
    tenant_id: str
    period: str
    estimated_tokens: int
    estimated_cost: Decimal


@dataclass(frozen=True, slots=True)
class GovernedMessage:
    message: NormalizedMessage
    reservation: BudgetReservation
    pii_types: tuple[str, ...]
    redact_output: bool


class GovernanceService:
    def __init__(
        self,
        factory: sessionmaker[Session],
        audit_store: AuditStore,
        redactor: PiiRedactor | None = None,
    ) -> None:
        self._factory = factory
        self._audit = audit_store
        self._redactor = redactor or PiiRedactor()

    async def authorize(self, message: NormalizedMessage) -> GovernedMessage:
        policy = await asyncio.to_thread(self._load_policy, message.tenant_id)
        governance = dict(policy.get("governance", policy))
        acl = dict(governance.get("im_acl", {}))
        allowed_channels = set(map(str, acl.get("allow_channels", [])))
        allowed_users = set(map(str, acl.get("allow_users", [])))
        denied_users = set(map(str, acl.get("deny_users", [])))
        reason = None
        if allowed_channels and message.channel not in allowed_channels:
            reason = "channel_not_allowed"
        elif message.sender_user_id in denied_users:
            reason = "user_denied"
        elif allowed_users and message.sender_user_id not in allowed_users:
            reason = "user_not_allowed"
        if reason:
            await self._record(message, "deny", reason)
            raise GovernanceDeniedError(reason)

        pii = dict(governance.get("pii", {}))
        redacted_text, pii_types = self._redactor.redact(message.text)
        governed_message = (
            replace(message, text=redacted_text) if pii.get("redact_input", True) else message
        )
        budget = dict(governance.get("budget", {}))
        estimated_tokens = max(1, len(governed_message.text) // 4) + int(
            budget.get("reserved_output_tokens", 512)
        )
        estimated_cost = Decimal(str(budget.get("reserved_cost", 0)))
        try:
            reservation = await asyncio.to_thread(
                self._reserve_budget,
                message.tenant_id,
                budget,
                estimated_tokens,
                estimated_cost,
            )
        except GovernanceDeniedError as error:
            await self._record(message, "deny", error.reason)
            raise
        await self._record(
            message,
            "allow",
            "authorized",
            {"pii_types": pii_types, "estimated_tokens": estimated_tokens},
        )
        return GovernedMessage(
            governed_message,
            reservation,
            pii_types,
            bool(pii.get("redact_output", True)),
        )

    async def settle(self, governed: GovernedMessage, reply: AgentReply) -> AgentReply:
        actual_tokens = reply.input_tokens + reply.output_tokens
        actual_cost = Decimal(str(reply.cost))
        await asyncio.to_thread(
            self._settle_budget, governed.reservation, actual_tokens, actual_cost
        )
        if not governed.redact_output:
            return reply
        text, _ = self._redactor.redact(reply.text)
        summary = self._redactor.redact(reply.summary)[0] if reply.summary else None
        return replace(reply, text=text, summary=summary)

    async def release(self, reservation: BudgetReservation) -> None:
        await asyncio.to_thread(self._settle_budget, reservation, 0, Decimal("0"))

    def _load_policy(self, tenant_id: str) -> dict[str, Any]:
        with self._factory() as session:
            tenant = session.get(Tenant, tenant_id)
            if tenant is None:
                raise GovernanceDeniedError("tenant_not_found")
            return dict(tenant.audit_policy)

    def _reserve_budget(
        self,
        tenant_id: str,
        budget: dict[str, Any],
        estimated_tokens: int,
        estimated_cost: Decimal,
    ) -> BudgetReservation:
        period = datetime.now(timezone.utc).date().isoformat()
        with self._factory.begin() as session:
            row = session.scalar(
                select(TenantBudgetUsage)
                .where(
                    TenantBudgetUsage.tenant_id == tenant_id,
                    TenantBudgetUsage.period == period,
                )
                .with_for_update()
            )
            if row is None:
                row = TenantBudgetUsage(tenant_id=tenant_id, period=period)
                session.add(row)
                session.flush()
            request_limit = int(budget.get("daily_requests", 0))
            token_limit = int(budget.get("daily_tokens", 0))
            cost_limit = Decimal(str(budget.get("daily_cost", 0)))
            if request_limit and row.request_count + 1 > request_limit:
                raise GovernanceDeniedError("daily_request_budget_exceeded")
            if token_limit and row.token_count + estimated_tokens > token_limit:
                raise GovernanceDeniedError("daily_token_budget_exceeded")
            if cost_limit and row.cost + estimated_cost > cost_limit:
                raise GovernanceDeniedError("daily_cost_budget_exceeded")
            row.request_count += 1
            row.token_count += estimated_tokens
            row.cost += estimated_cost
        return BudgetReservation(tenant_id, period, estimated_tokens, estimated_cost)

    def _settle_budget(
        self,
        reservation: BudgetReservation,
        actual_tokens: int,
        actual_cost: Decimal,
    ) -> None:
        with self._factory.begin() as session:
            row = session.scalar(
                select(TenantBudgetUsage).where(
                    TenantBudgetUsage.tenant_id == reservation.tenant_id,
                    TenantBudgetUsage.period == reservation.period,
                )
            )
            if row is not None:
                row.token_count = max(
                    0, row.token_count - reservation.estimated_tokens + actual_tokens
                )
                row.cost = max(Decimal("0"), row.cost - reservation.estimated_cost + actual_cost)

    async def _record(
        self,
        message: NormalizedMessage,
        decision: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        await self._audit.append_audit(
            AuditEntry(
                tenant_id=message.tenant_id,
                agent_app_id=message.agent_app_id,
                agent_name="gateway_governance",
                decision=decision,
                trace_id=message.trace_id,
                request_id=message.external_message_id,
                channel=message.channel,
                user_id=message.sender_user_id,
                session_id=message.session_id,
                details={"reason": reason, **(details or {})},
            )
        )

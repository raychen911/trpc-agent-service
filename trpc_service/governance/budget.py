"""SQL budget authority. No network request or expiry can implicitly release money."""

from datetime import timezone
from decimal import Decimal, ROUND_CEILING
import hashlib
import json
import re
import uuid
from zoneinfo import ZoneInfo

from sqlalchemy import select, func

from trpc_service.channels.persistence import database_now
from trpc_service.persistence.models import (TenantRow, InboundMessageRow, AuditLogRow, ModelPriceRow, BudgetPolicyRow,
                                             BudgetAccountRow, ModelAttemptRow)

QUANTUM = Decimal("0.0000000001")
MAX_MONEY = Decimal("1000000000")


class BudgetConflict(ValueError):
    pass


class BudgetDenied(PermissionError):
    pass


def money(value):
    if isinstance(value, (float, bool)):
        raise ValueError("money must be an exact decimal")
    try:
        result = Decimal(value)
        if not result.is_finite() or result < 0 or result > MAX_MONEY or result != result.quantize(QUANTUM):
            raise ValueError()
        return result.quantize(QUANTUM)
    except (ValueError, ArithmeticError, TypeError):
        raise ValueError("invalid monetary amount") from None


def token_count(value):
    if type(value) is not int or not 0 <= value <= 100_000_000:
        raise ValueError("invalid token count")
    return value


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value):
        raise ValueError("invalid identifier")
    return value


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def cost(price, input_tokens, output_tokens):
    amount = (token_count(input_tokens) * price.input_per_million +
              token_count(output_tokens) * price.output_per_million) / Decimal(1_000_000)
    # Actual bills may exceed configured policy limits; never cap or reject their
    # accounting merely because they exceed the maximum configurable allowance.
    return amount.quantize(QUANTUM, rounding=ROUND_CEILING)


def period_keys(now, zone):
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(ZoneInfo(zone))
    return sorted([f"day:{zone}:{local:%Y-%m-%d}", f"month:{zone}:{local:%Y-%m}"])


class BudgetLedger:
    """All writes lock the tenant first, then accounts in canonical order.

    This deliberately serializes the tenant's financial mutations, including first
    account/attempt creation. PostgreSQL is the deployment backend; SQLite is for
    local contract tests only. No caller-controlled HTTP reservation endpoint exists.
    """

    def __init__(self, database):
        self.database = database

    @staticmethod
    def tenant(session, tenant_id, *, active=False):
        row = session.get(TenantRow, tenant_id, with_for_update=True)
        if row is None:
            raise LookupError("tenant not found")
        if active and row.status != "active":
            raise BudgetDenied("tenant is not active")
        return row

    @staticmethod
    def audit(session, tenant, decision, *, actor="model_budget", attempt=None, metadata=None):
        fields = dict(metadata or {})
        execution_fields = {}
        if attempt is not None:
            fields.update(attempt_id=attempt.attempt_id,
                          execution_id=attempt.execution_id,
                          currency=attempt.currency,
                          price_id=attempt.price_id)
            inbound = session.scalar(
                select(InboundMessageRow).where(InboundMessageRow.tenant_id == tenant,
                                                InboundMessageRow.execution_id == attempt.execution_id))
            if inbound is not None:
                route = inbound.payload_json.get("route", {})
                execution_fields = dict(channel=inbound.payload_json.get("message", {}).get("channel"),
                                        session_id=route.get("session_id"),
                                        trace_id=inbound.trace_id,
                                        request_id=inbound.request_id,
                                        config_version=inbound.config_version)
                fields["actor_id"] = route.get("actor_id")
                fields["agent_app_id"] = route.get("agent_app_id")
        session.add(
            AuditLogRow(audit_id=uuid.uuid4().hex,
                        tenant_id=tenant,
                        user_id=actor,
                        decision=decision,
                        cost=float(attempt.actual_amount)
                        if attempt is not None and attempt.actual_amount is not None else None,
                        redacted=True,
                        **execution_fields,
                        metadata_json=fields))

    def register_price(self, tenant, price_id, model_id, currency, input_rate, output_rate, *, actor):
        identifier(price_id)
        identifier(model_id)
        if len(price_id) > 64 or not re.fullmatch("[A-Z]{3}", currency):
            raise ValueError("invalid price identity")
        values = (model_id, currency, money(input_rate), money(output_rate))
        with self.database.sessions.begin() as session:
            self.tenant(session, tenant)
            row = session.get(ModelPriceRow, (tenant, price_id))
            if row:
                if (row.model_id, row.currency, row.input_per_million, row.output_per_million) != values:
                    raise BudgetConflict("price version is immutable")
                return
            session.add(
                ModelPriceRow(tenant_id=tenant,
                              price_id=price_id,
                              model_id=model_id,
                              currency=currency,
                              input_per_million=values[2],
                              output_per_million=values[3]))
            self.audit(session,
                       tenant,
                       "model_price_registered",
                       actor=actor,
                       metadata={
                           "price_id": price_id,
                           "currency": currency
                       })

    def configure(self, tenant, currency, daily, monthly, zone, expected_revision, *, actor):
        if not re.fullmatch("[A-Z]{3}", currency):
            raise ValueError("invalid currency")
        ZoneInfo(zone)
        daily, monthly = money(daily), money(monthly)
        with self.database.sessions.begin() as session:
            self.tenant(session, tenant)
            policy = session.get(BudgetPolicyRow, (tenant, currency))
            if (policy.revision if policy else 0) != expected_revision:
                raise BudgetConflict("budget revision changed")
            if policy and policy.timezone != zone:
                raise BudgetConflict("budget timezone is immutable; offline reconciliation required")
            accounts = list(
                session.scalars(
                    select(BudgetAccountRow).where(BudgetAccountRow.tenant_id == tenant,
                                                   BudgetAccountRow.currency == currency).order_by(
                                                       BudgetAccountRow.period).with_for_update()))
            current = period_keys(database_now(session), zone)
            for account in accounts:
                if account.period in current:
                    limit = daily if account.period.startswith("day:") else monthly
                    if limit < account.reserved + account.settled:
                        raise BudgetConflict("new limit cannot hide existing obligations")
                    account.limit_amount = limit
            if policy is None:
                policy = BudgetPolicyRow(tenant_id=tenant, currency=currency, timezone=zone, revision=0)
                session.add(policy)
            policy.daily_limit, policy.monthly_limit = daily, monthly
            policy.revision += 1
            self.audit(session,
                       tenant,
                       "budget_policy_changed",
                       actor=actor,
                       metadata={
                           "currency": currency,
                           "revision": policy.revision
                       })
            return policy.revision

    def reserve(self,
                tenant,
                attempt_id,
                execution_id,
                price_id,
                model_id,
                request_hash,
                max_input_tokens,
                max_output_tokens,
                *,
                max_calls=20):
        for value in (attempt_id, execution_id, price_id, model_id):
            identifier(value)
        if any(len(value) > 64 for value in (attempt_id, execution_id, price_id)):
            raise ValueError("invalid attempt identity")
        if not re.fullmatch("[a-f0-9]{64}", request_hash) or type(max_calls) is not int or not 1 <= max_calls <= 500:
            raise ValueError("invalid request identity or call limit")
        token_count(max_input_tokens)
        token_count(max_output_tokens)
        signature = digest([execution_id, price_id, model_id, request_hash, max_input_tokens, max_output_tokens])
        with self.database.sessions.begin() as session:
            self.tenant(session, tenant, active=True)
            previous = session.get(ModelAttemptRow, (tenant, attempt_id))
            if previous:
                if previous.reservation_hash != signature:
                    raise BudgetConflict("model attempt identity changed")
                return self.view(previous)
            parent = session.scalar(
                select(InboundMessageRow).where(InboundMessageRow.tenant_id == tenant,
                                                InboundMessageRow.execution_id == execution_id))
            if parent is None:
                raise BudgetDenied("model attempt requires a trusted execution")
            price = session.get(ModelPriceRow, (tenant, price_id))
            if price is None or price.model_id != model_id:
                raise BudgetDenied("approved model price is missing")
            policy = session.get(BudgetPolicyRow, (tenant, price.currency))
            if policy is None or policy.blocked:
                raise BudgetDenied("budget is missing or blocked")
            calls = session.scalar(
                select(func.count()).select_from(ModelAttemptRow).where(ModelAttemptRow.tenant_id == tenant,
                                                                        ModelAttemptRow.execution_id == execution_id))
            if calls >= max_calls:
                raise BudgetDenied("execution model call limit reached")
            amount = cost(price, max_input_tokens, max_output_tokens)
            periods = period_keys(database_now(session), policy.timezone)
            accounts = []
            for period in periods:
                account = session.get(BudgetAccountRow, (tenant, period, price.currency), with_for_update=True)
                if account is None:
                    account = BudgetAccountRow(
                        tenant_id=tenant,
                        period=period,
                        currency=price.currency,
                        limit_amount=policy.daily_limit if period.startswith("day:") else policy.monthly_limit,
                        reserved=Decimal(0),
                        settled=Decimal(0))
                    session.add(account)
                if amount > account.limit_amount - account.reserved - account.settled:
                    raise BudgetDenied("tenant budget exhausted")
                accounts.append(account)
            for account in accounts:
                account.reserved += amount
            row = ModelAttemptRow(tenant_id=tenant,
                                  attempt_id=attempt_id,
                                  execution_id=execution_id,
                                  price_id=price_id,
                                  model_id=model_id,
                                  currency=price.currency,
                                  request_hash=request_hash,
                                  reservation_hash=signature,
                                  periods_json=periods,
                                  reserved_amount=amount,
                                  max_input_tokens=max_input_tokens,
                                  max_output_tokens=max_output_tokens,
                                  status="reserved")
            session.add(row)
            self.audit(session, tenant, "model_budget_reserved", attempt=row)
            session.flush()
            return self.view(row)

    def _attempt(self, session, tenant, attempt_id):
        self.tenant(session, tenant)
        row = session.get(ModelAttemptRow, (tenant, attempt_id), with_for_update=True)
        if row is None:
            raise LookupError("model attempt not found")
        return row

    @staticmethod
    def authorize_dispatch(session, row):
        parent = session.scalar(
            select(InboundMessageRow).where(InboundMessageRow.tenant_id == row.tenant_id,
                                            InboundMessageRow.execution_id == row.execution_id))
        if parent is None:
            raise BudgetDenied("trusted execution is unavailable")
        if "authz_epoch" in parent.payload_json:
            from trpc_service.channels.authorization import require_im_execution_in_session
            from trpc_service.tenant.routing import ResolvedRoute
            route = ResolvedRoute.model_validate(parent.payload_json["route"])
            if route.tenant_id != row.tenant_id:
                raise BudgetDenied("execution tenant mismatch")
            require_im_execution_in_session(session, route, parent.payload_json["authz_epoch"])

    @staticmethod
    def _accounts(session, row):
        accounts = [
            session.get(BudgetAccountRow, (row.tenant_id, period, row.currency), with_for_update=True)
            for period in sorted(row.periods_json)
        ]
        if any(account is None for account in accounts):
            raise BudgetConflict("reservation account is missing")
        return accounts

    def mark_sent(self, tenant, attempt_id):
        """Single-use network permit. A second caller must never repeat this request."""
        with self.database.sessions.begin() as session:
            row = self._attempt(session, tenant, attempt_id)
            self.tenant(session, tenant, active=True)
            policy = session.get(BudgetPolicyRow, (tenant, row.currency))
            if policy.blocked:
                raise BudgetDenied("budget is blocked")
            if row.status != "reserved":
                raise BudgetConflict("model attempt has already been dispatched or closed")
            self.authorize_dispatch(session, row)
            row.status, row.sent_at = "in_flight", database_now(session)
            self.audit(session, tenant, "model_request_started", attempt=row)

    def cancel_unsent(self, tenant, attempt_id):
        with self.database.sessions.begin() as session:
            row = self._attempt(session, tenant, attempt_id)
            if row.status == "cancelled":
                return self.view(row)
            if row.status != "reserved":
                raise BudgetConflict("a possibly billable request cannot release its reservation")
            for account in self._accounts(session, row):
                account.reserved -= row.reserved_amount
            row.status, row.completed_at = "cancelled", database_now(session)
            self.audit(session, tenant, "model_budget_cancelled_unsent", attempt=row)
            return self.view(row)

    def pending(self, tenant, attempt_id):
        with self.database.sessions.begin() as session:
            row = self._attempt(session, tenant, attempt_id)
            if row.status in {"settled", "pending_reconciliation"}:
                return self.view(row)
            if row.status != "in_flight":
                raise BudgetConflict("only dispatched requests require reconciliation")
            row.status = "pending_reconciliation"
            self.audit(session, tenant, "model_cost_unknown", attempt=row)
            return self.view(row)

    def record_provider_receipt(self, tenant, attempt_id, generation_id):
        if not re.fullmatch(r"gen-[A-Za-z0-9_-]{1,200}", generation_id):
            raise ValueError("invalid provider generation receipt")
        with self.database.sessions.begin() as session:
            row = self._attempt(session, tenant, attempt_id)
            if row.sent_at is None:
                raise BudgetConflict("receipt has no dispatched attempt")
            previous = list(
                session.scalars(
                    select(AuditLogRow).where(AuditLogRow.tenant_id == tenant,
                                              AuditLogRow.decision == "model_provider_receipt")))
            for receipt in previous:
                if receipt.metadata_json.get("attempt_id") == attempt_id:
                    if receipt.metadata_json.get("generation_id") != generation_id:
                        raise BudgetConflict("provider receipt changed")
                    return
                if receipt.metadata_json.get("generation_id") == generation_id:
                    raise BudgetConflict("provider receipt belongs to another attempt")
            self.audit(session,
                       tenant,
                       "model_provider_receipt",
                       attempt=row,
                       metadata={"generation_id": generation_id})

    def settle(self,
               tenant,
               attempt_id,
               input_tokens,
               output_tokens,
               evidence_hash,
               *,
               actor="model_budget",
               provider_cost=None):
        """Called with verified provider usage; no endpoint accepts caller-declared usage."""
        token_count(input_tokens)
        token_count(output_tokens)
        if not re.fullmatch("[a-f0-9]{64}", evidence_hash):
            raise ValueError("usage evidence hash required")
        if provider_cost is not None:
            provider_cost = money(provider_cost)
        with self.database.sessions.begin() as session:
            row = self._attempt(session, tenant, attempt_id)
            if row.status == "settled":
                if (row.input_tokens, row.output_tokens, row.evidence_hash) != (input_tokens, output_tokens,
                                                                                evidence_hash):
                    raise BudgetConflict("usage differs from settled evidence")
                if provider_cost is not None and row.actual_amount != provider_cost:
                    raise BudgetConflict("provider cost differs from settled evidence")
                return self.view(row)
            if row.status not in {"in_flight", "pending_reconciliation"}:
                raise BudgetConflict("request was not dispatched")
            price = session.get(ModelPriceRow, (tenant, row.price_id))
            actual = cost(price, input_tokens, output_tokens) if provider_cost is None else provider_cost
            accounts = self._accounts(session, row)
            overrun = actual > row.reserved_amount or input_tokens > row.max_input_tokens or \
                output_tokens > row.max_output_tokens
            for account in accounts:
                account.reserved -= row.reserved_amount
                account.settled += actual
                overrun = overrun or account.settled + account.reserved > account.limit_amount
            row.input_tokens, row.output_tokens, row.actual_amount = input_tokens, output_tokens, actual
            row.status, row.evidence_hash, row.overrun = "settled", evidence_hash, overrun
            row.completed_at = database_now(session)
            if overrun:
                session.get(BudgetPolicyRow, (tenant, row.currency)).blocked = True
            self.audit(session,
                       tenant,
                       "model_budget_overrun" if overrun else "model_budget_settled",
                       actor=actor,
                       attempt=row,
                       metadata={
                           "amount": str(actual),
                           "evidence_hash": evidence_hash
                       })
            return self.view(row)

    @staticmethod
    def view(row):
        return {
            "attempt_id": row.attempt_id,
            "execution_id": row.execution_id,
            "model_id": row.model_id,
            "price_id": row.price_id,
            "currency": row.currency,
            "status": row.status,
            "reserved_amount": str(row.reserved_amount),
            "actual_amount": str(row.actual_amount) if row.actual_amount is not None else None,
            "input_tokens": row.input_tokens,
            "output_tokens": row.output_tokens,
            "overrun": bool(row.overrun),
            "periods": row.periods_json,
            "created_at": row.created_at,
            "sent_at": row.sent_at,
            "completed_at": row.completed_at
        }

"""Operator-authenticated budget controls; public routes cannot dispatch or settle models."""

from decimal import Decimal
from zoneinfo import ZoneInfoNotFoundError

from .admin_auth import current_actor

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select

from trpc_service.persistence.models import BudgetPolicyRow, BudgetAccountRow, ModelAttemptRow, ModelPriceRow
from .budget import BudgetLedger, BudgetConflict, money


class BudgetUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    daily_limit: Decimal
    monthly_limit: Decimal
    timezone: str = Field(default="Asia/Shanghai", min_length=1, max_length=64)
    expected_revision: int = Field(ge=0, strict=True)

    @field_validator("daily_limit", "monthly_limit", mode="before")
    @classmethod
    def validate_money(cls, value):
        return money(value)


class PriceRegistration(BaseModel):
    model_config = ConfigDict(extra="forbid")
    price_id: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    model_id: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,128}$")
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    input_per_million: Decimal
    output_per_million: Decimal

    @field_validator("input_per_million", "output_per_million", mode="before")
    @classmethod
    def validate_money(cls, value):
        return money(value)


def budget_router(database, actor):
    # Included under management_router's operator authentication dependency.
    router = APIRouter()
    ledger = BudgetLedger(database)

    @router.put("/tenants/{tenant_id}/budgets/{currency}")
    def configure(tenant_id: str, currency: str, body: BudgetUpdate):
        try:
            revision = ledger.configure(tenant_id,
                                        currency,
                                        body.daily_limit,
                                        body.monthly_limit,
                                        body.timezone,
                                        body.expected_revision,
                                        actor=current_actor(actor))
            return {"revision": revision}
        except ZoneInfoNotFoundError:
            raise HTTPException(422, "invalid budget timezone") from None
        except LookupError:
            raise HTTPException(404, "tenant not found") from None
        except BudgetConflict:
            raise HTTPException(409, "budget revision or obligations conflict") from None
        except (ValueError, ZoneInfoNotFoundError):
            raise HTTPException(422, "invalid budget policy") from None

    @router.post("/tenants/{tenant_id}/model-prices", status_code=201)
    def register_price(tenant_id: str, body: PriceRegistration):
        try:
            ledger.register_price(tenant_id,
                                  body.price_id,
                                  body.model_id,
                                  body.currency,
                                  body.input_per_million,
                                  body.output_per_million,
                                  actor=current_actor(actor))
            return {"price_id": body.price_id}
        except LookupError:
            raise HTTPException(404, "tenant not found") from None
        except BudgetConflict:
            raise HTTPException(409, "price version is immutable") from None

    @router.get("/tenants/{tenant_id}/model-prices")
    def prices(tenant_id: str, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
        with database.sessions() as session:
            return [{
                "price_id": row.price_id,
                "model_id": row.model_id,
                "currency": row.currency,
                "input_per_million": str(row.input_per_million),
                "output_per_million": str(row.output_per_million)
            } for row in session.scalars(
                select(ModelPriceRow).where(
                    ModelPriceRow.tenant_id == tenant_id).order_by(ModelPriceRow.price_id).offset(offset).limit(limit))]

    @router.get("/tenants/{tenant_id}/budgets")
    def policies(tenant_id: str):
        with database.sessions() as session:
            return [{
                "currency": row.currency,
                "timezone": row.timezone,
                "daily_limit": str(row.daily_limit),
                "monthly_limit": str(row.monthly_limit),
                "revision": row.revision,
                "blocked": row.blocked
            } for row in session.scalars(
                select(BudgetPolicyRow).where(
                    BudgetPolicyRow.tenant_id == tenant_id).order_by(BudgetPolicyRow.currency))]

    @router.get("/tenants/{tenant_id}/budget-accounts")
    def accounts(tenant_id: str, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
        with database.sessions() as session:
            return [{
                "period": row.period,
                "currency": row.currency,
                "limit": str(row.limit_amount),
                "reserved": str(row.reserved),
                "settled": str(row.settled),
                "available": str(max(Decimal(0), row.limit_amount - row.settled - row.reserved))
            } for row in session.scalars(
                select(BudgetAccountRow).where(BudgetAccountRow.tenant_id == tenant_id).order_by(
                    BudgetAccountRow.period.desc(), BudgetAccountRow.currency).offset(offset).limit(limit))]

    @router.get("/tenants/{tenant_id}/model-attempts")
    def attempts(tenant_id: str,
                 status: str | None = None,
                 limit: int = Query(100, ge=1, le=500),
                 offset: int = Query(0, ge=0)):
        with database.sessions() as session:
            query = select(ModelAttemptRow).where(ModelAttemptRow.tenant_id == tenant_id)
            if status is not None:
                query = query.where(ModelAttemptRow.status == status)
            return [
                ledger.view(row) for row in session.scalars(
                    query.order_by(ModelAttemptRow.created_at.desc(), ModelAttemptRow.attempt_id).offset(offset).limit(
                        limit))
            ]

    @router.post("/tenants/{tenant_id}/model-attempts/{attempt_id}/reconcile")
    async def reconcile_attempt(tenant_id: str, attempt_id: str):
        from .reconciliation import reconcile
        try:
            return await reconcile(database, tenant_id, attempt_id, current_actor(actor))
        except LookupError:
            raise HTTPException(404, "model attempt not found") from None
        except BudgetConflict:
            raise HTTPException(409, "verified provider receipt unavailable; reservation retained") from None

    return router

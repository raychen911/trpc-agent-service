"""Strict tenant budget reservation and settlement."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Mapping

from trpc_service.governance.errors import BudgetExhausted, UsageExceedsReservation
from trpc_service.governance.models import BudgetReservation, BudgetReservationSet, BudgetSettlement, ReservationStatus, UsageVector


def new_reservation(tenant_id: str, execution_id: str, maximum: UsageVector) -> BudgetReservation:
    return BudgetReservation.initial(tenant_id, execution_id, maximum)


def settle(reservation: BudgetReservation, actual: UsageVector) -> BudgetReservation:
    try:
        return reservation.transition(ReservationStatus.SETTLED, actual=actual)
    except ValueError as exc:
        raise UsageExceedsReservation() from exc


def release(reservation: BudgetReservation) -> BudgetReservation:
    try:
        return reservation.transition(ReservationStatus.RELEASED)
    except ValueError as exc:
        raise ValueError("reservation cannot be released") from exc


class InMemoryBudgetRepository:
    def __init__(self, limits: Mapping[str, Decimal] | None = None) -> None:
        self.limits = {str(key): Decimal(value) for key, value in (limits or {}).items()}
        self.reserved = {key: Decimal("0") for key in self.limits}
        self.settled = {key: Decimal("0") for key in self.limits}
        self._items: dict[tuple[str, str], BudgetReservation] = {}
        self._lock = asyncio.Lock()

    async def reserve_maximum(self, *, tenant_id: str, execution_id: str, maximum: UsageVector, owner_generation: int, policy: object | None = None, trace_id: str = "") -> BudgetReservationSet:
        del owner_generation, policy, trace_id
        async with self._lock:
            key = (tenant_id, execution_id)
            if key in self._items:
                return BudgetReservationSet(tenant_id=tenant_id, execution_id=execution_id, reservation=self._items[key])
            for dimension, limit in self.limits.items():
                amount = getattr(maximum, dimension, Decimal("0"))
                if self.reserved.get(dimension, Decimal("0")) + amount + self.settled.get(dimension, Decimal("0")) > limit:
                    raise BudgetExhausted()
            for dimension in self.limits:
                self.reserved[dimension] += getattr(maximum, dimension, Decimal("0"))
            item = new_reservation(tenant_id, execution_id, maximum)
            self._items[key] = item
            return BudgetReservationSet(tenant_id=tenant_id, execution_id=execution_id, reservation=item)

    async def settle(self, *, tenant_id: str, execution_id: str, actuals: UsageVector, owner_generation: int) -> BudgetSettlement:
        del owner_generation
        async with self._lock:
            item = self._items[(tenant_id, execution_id)]
            if item.status == ReservationStatus.SETTLED:
                return BudgetSettlement(tenant_id=tenant_id, execution_id=execution_id, actual=item.actual, reservation=item)
            updated = settle(item, actuals)
            for dimension in self.limits:
                maximum = getattr(item.maximum, dimension)
                actual = getattr(actuals, dimension)
                self.reserved[dimension] -= maximum
                self.settled[dimension] += actual
            self._items[(tenant_id, execution_id)] = updated
            return BudgetSettlement(tenant_id=tenant_id, execution_id=execution_id, actual=actuals, reservation=updated)

    async def mark_execution_started(self, *, tenant_id: str, execution_id: str, owner_generation: int) -> BudgetReservation:
        del owner_generation
        async with self._lock:
            item = self._items[(tenant_id, execution_id)]
            if item.status != ReservationStatus.RESERVED:
                return item
            updated = item.model_copy(update={"execution_started": True})
            self._items[(tenant_id, execution_id)] = updated
            return updated

    async def release_before_execution(self, *, tenant_id: str, execution_id: str, owner_generation: int, reason: str) -> BudgetReservation:
        del owner_generation, reason
        async with self._lock:
            item = self._items[(tenant_id, execution_id)]
            if item.status != ReservationStatus.RESERVED:
                return item
            if item.execution_started:
                updated = item.model_copy(update={"status": ReservationStatus.REVIEW_REQUIRED})
            else:
                updated = release(item)
                for dimension in self.limits:
                    self.reserved[dimension] -= getattr(item.maximum, dimension)
            self._items[(tenant_id, execution_id)] = updated
            return updated

    async def get_by_execution(self, *, tenant_id: str, execution_id: str) -> BudgetReservation:
        async with self._lock:
            return self._items[(tenant_id, execution_id)]

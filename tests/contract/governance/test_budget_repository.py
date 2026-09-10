from __future__ import annotations

import asyncio
from decimal import Decimal

from trpc_service.governance import budget


def test_budget_repository_duplicate_reserve_is_idempotent() -> None:
    async def scenario() -> None:
        repository = budget.InMemoryBudgetRepository({"request": Decimal("2")})
        maximum = budget.UsageVector(request=Decimal("1"))
        first = await repository.reserve_maximum(tenant_id="tenant-a", execution_id="e1", maximum=maximum, owner_generation=1)
        second = await repository.reserve_maximum(tenant_id="tenant-a", execution_id="e1", maximum=maximum, owner_generation=1)
        assert first.reservation.reservation_id == second.reservation.reservation_id
        assert repository.reserved["request"] == Decimal("1")

    asyncio.run(scenario())

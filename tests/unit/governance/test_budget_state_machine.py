from __future__ import annotations

from decimal import Decimal

import pytest

from trpc_service.governance import budget


def test_actual_usage_cannot_exceed_maximum_and_settlement_is_terminal() -> None:
    maximum = budget.UsageVector(token=Decimal("10"), cost=Decimal("2"))
    reservation = budget.new_reservation("tenant-a", "exec-a", maximum)
    with pytest.raises(Exception):
        budget.settle(reservation, budget.UsageVector(token=Decimal("11"), cost=Decimal("1")))
    settled = budget.settle(reservation, budget.UsageVector(token=Decimal("5"), cost=Decimal("1")))
    assert settled.status.value == "settled"
    with pytest.raises(Exception):
        budget.release(settled)

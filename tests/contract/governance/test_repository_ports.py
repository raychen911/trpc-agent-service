from __future__ import annotations

import inspect

from trpc_service.storage import contracts


def test_governance_repository_protocols_exist_and_are_async() -> None:
    expected = (
        "GovernancePolicyRepository", "PrincipalGrantRepository", "BudgetRepository",
        "PendingConfirmationRepository", "GovernanceRecoveryRepository",
    )
    missing = [name for name in expected if not hasattr(contracts, name)]
    assert not missing, f"governance repository ports missing: {missing}"
    for name in expected:
        protocol = getattr(contracts, name)
        assert getattr(protocol, "_is_protocol", False)
        methods = [value for value in protocol.__dict__.values() if callable(value)]
        assert any(inspect.iscoroutinefunction(value) for value in methods)


def test_governance_ports_include_tenant_scope_and_fencing_arguments() -> None:
    budget = contracts.BudgetRepository
    reserve = inspect.signature(budget.reserve_maximum)
    assert {"tenant_id", "execution_id", "owner_generation"}.issubset(reserve.parameters)
    confirmation = contracts.PendingConfirmationRepository
    claim = inspect.signature(confirmation.claim)
    assert {"intent", "owner_node_id", "owner_generation"}.issubset(claim.parameters)

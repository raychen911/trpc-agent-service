from __future__ import annotations

from decimal import Decimal

import pytest

from trpc_service.governance import errors, models


def test_public_governance_model_exports_exist() -> None:
    expected = (
        "PolicyStatus", "Decision", "ToolRiskLevel", "SideEffectClass",
        "UsageDimension", "ConfirmationStatus", "ReservationStatus",
        "ChannelPrincipal", "ToolDescriptor", "UsageVector", "PolicyDecision",
        "BudgetReservation", "GovernanceContext",
    )
    missing = [name for name in expected if not hasattr(models, name)]
    assert not missing, f"governance model exports missing: {missing}"


def test_domain_models_are_immutable_and_fail_closed() -> None:
    policy = models.PolicyDocument.model_validate({"allowed_tools": ["lookup"]})
    with pytest.raises(Exception):
        policy.allowed_tools = ("other",)
    with pytest.raises(Exception):
        models.ChannelPrincipal(
            tenant_id="tenant-a", channel="feishu", binding_id="b",
            provider_subject="user", subject_digest="not-a-digest",
        )


def test_high_risk_tool_requires_confirmation_and_usage_is_bounded() -> None:
    descriptor = models.ToolDescriptor(
        tool_name="delete_record", side_effect_class="external",
        risk_level="high", usage_dimensions={"request", "tool_call", "token", "cost"},
        max_usage=models.UsageVector(request=Decimal("1"), tool_call=Decimal("1"), token=Decimal("10"), cost=Decimal("2")),
    )
    assert descriptor.confirmation_required is True
    with pytest.raises(Exception):
        models.UsageVector(token=Decimal("-1"))


def test_reservation_and_confirmation_state_transitions_are_restricted() -> None:
    reservation = models.BudgetReservation.initial("tenant-a", "execution-a")
    assert reservation.status.value == "reserved"
    with pytest.raises(Exception):
        reservation.transition("settled", actual=models.UsageVector(token=Decimal("99")))
    confirmation = models.PendingConfirmation.new(
        tenant_id="tenant-a", confirmation_id="c1", reservation_id="r1",
    )
    assert confirmation.status.value == "pending"


def test_stable_errors_do_not_include_secrets() -> None:
    error = errors.GovernanceUnavailable("internal secret=do-not-print")
    assert "do-not-print" not in str(error)
    assert "secret" not in repr(error).lower()

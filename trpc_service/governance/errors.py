"""Stable, deliberately non-disclosing governance errors."""

from __future__ import annotations


class GovernanceError(RuntimeError):
    code = "governance_error"

    def __init__(self, _detail: object | None = None) -> None:
        super().__init__(self.code)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.code!r})"


class GovernanceUnavailable(GovernanceError):
    code = "governance_unavailable"


class PolicyMissing(GovernanceError):
    code = "policy_missing"


class PolicyDisabled(GovernanceError):
    code = "policy_disabled"


class PolicyStale(GovernanceError):
    code = "policy_stale"


class PrincipalUnauthorized(GovernanceError):
    code = "principal_unauthorized"


class PrincipalInvalid(GovernanceError):
    code = "principal_invalid"


class BudgetExhausted(GovernanceError):
    code = "budget_exhausted"


class BudgetUnavailable(GovernanceError):
    code = "budget_unavailable"


class BudgetConflict(GovernanceError):
    code = "budget_conflict"


class FencingRejected(GovernanceError):
    code = "fencing_rejected"


class UsageExceedsReservation(GovernanceError):
    code = "usage_exceeds_reservation"


class ConfirmationInvalid(GovernanceError):
    code = "confirmation_invalid"


class ConfirmationExpired(GovernanceError):
    code = "confirmation_expired"


class ConfirmationConsumed(GovernanceError):
    code = "confirmation_consumed"

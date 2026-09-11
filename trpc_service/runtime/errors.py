"""Errors raised when a durable-runtime invariant cannot be satisfied."""

from __future__ import annotations


class RuntimeErrorBase(Exception):
    """Base class for errors that are safe to surface as a typed API failure."""

    code = "runtime_error"


class TenantMismatch(RuntimeErrorBase):
    code = "tenant_mismatch"


class ExecutionUnavailable(RuntimeErrorBase):
    code = "execution_unavailable"


class LeaseLost(RuntimeErrorBase):
    code = "lease_lost"


class StaleFence(LeaseLost):
    code = "stale_fence"


class SecurityRejected(RuntimeErrorBase):
    code = "security_rejected"


class BudgetExceeded(RuntimeErrorBase):
    code = "budget_exceeded"


class BudgetUnavailable(RuntimeErrorBase):
    """Hard budgets fail closed when their fact store cannot be read or written."""

    code = "budget_unavailable"


class ExecutionDivergence(RuntimeErrorBase):
    code = "execution_divergence"


class InvalidTransition(RuntimeErrorBase):
    code = "invalid_transition"


class MigrationBusy(RuntimeErrorBase):
    code = "migration_busy"


class NotFound(RuntimeErrorBase):
    code = "not_found"

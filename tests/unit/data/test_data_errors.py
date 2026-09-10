from __future__ import annotations

from trpc_service.storage import contracts


def test_data_domain_errors_have_stable_codes_and_retryability() -> None:
    names = (
        "TenantScopeInvalid", "SequenceGap", "IdempotencyConflict", "VersionConflict",
        "SummaryConflict", "ContentTooLarge", "DigestMismatch", "TenantFilterUnsupported",
        "MigrationWritePaused", "MigrationConflict", "ForwardRepairRequired",
        "AuditUnavailable", "StateBackendUnavailable", "StaleFence",
    )
    for name in names:
        error_type = getattr(contracts, name, None)
        assert error_type is not None
        error = error_type()
        assert isinstance(getattr(error, "code", None), str)
        assert isinstance(getattr(error, "retryable", None), bool)
        assert "postgres" not in str(error).lower()
        assert "redis" not in str(error).lower()

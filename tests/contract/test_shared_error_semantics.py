import pytest

from trpc_service.storage.contracts import ConfigurationUnavailable, IdempotencyConflict, OutcomeUnknown, Processing, StateBackendUnavailable, AuditIncomplete
from trpc_service.web.errors import map_platform_error


@pytest.mark.parametrize("error,status,code", [
    (ConfigurationUnavailable(), 503, "authorization_unavailable"),
    (StateBackendUnavailable(), 503, "backend_unavailable"),
    (Processing(), 202, "processing"),
    (IdempotencyConflict(), 409, "idempotency_conflict"),
    (OutcomeUnknown(), 503, "outcome_unknown"),
    (AuditIncomplete(), 503, "audit_incomplete"),
])
def test_shared_errors_have_stable_safe_http_mapping(error, status, code) -> None:
    mapped = map_platform_error(error)
    assert (mapped.status_code, mapped.code) == (status, code)
    assert "redis" not in mapped.message.lower() and "postgres" not in mapped.message.lower()

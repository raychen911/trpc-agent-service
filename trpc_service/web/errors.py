"""Stable HTTP error semantics independent of Redis/PostgreSQL exceptions."""

from dataclasses import dataclass

from trpc_service.storage.contracts import (
    AuditIncomplete, ConfigurationUnavailable, IdempotencyConflict,
    OutcomeUnknown, Processing, StateBackendUnavailable,
)


@dataclass(frozen=True, slots=True)
class HttpError:
    status_code: int
    code: str
    message: str


def map_platform_error(error: Exception) -> HttpError:
    if isinstance(error, ConfigurationUnavailable):
        return HttpError(503, "authorization_unavailable", "Authorization configuration is unavailable.")
    if isinstance(error, StateBackendUnavailable):
        return HttpError(503, "backend_unavailable", "Shared state is unavailable.")
    if isinstance(error, Processing):
        return HttpError(202, "processing", "The message is being processed.")
    if isinstance(error, IdempotencyConflict):
        return HttpError(409, "idempotency_conflict", "The message identifier conflicts with an existing request.")
    if isinstance(error, AuditIncomplete):
        return HttpError(503, "audit_incomplete", "Audit completion is unavailable.")
    if isinstance(error, OutcomeUnknown):
        return HttpError(503, "outcome_unknown", "The execution outcome is unknown.")
    return HttpError(503, "service_unavailable", "The service is unavailable.")

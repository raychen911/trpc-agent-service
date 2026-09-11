"""Shared Gateway error constants and WorkerErrorCode mapping."""

from __future__ import annotations

from trpc_service.governance.content_policy import CONTENT_INPUT_BLOCKED_TEXT
from trpc_service.transport.models import WorkerErrorCode

SAFE_ERROR_TEXT = "An internal error occurred while talking to the model."
CONFIG_ERROR_TEXT = "Service is not configured. Set TRPC_MODEL_* environment variables and restart."
TENANT_AGENT_CONFIG_ERROR_TEXT = "Tenant agent configuration is not available."
SESSION_BUSY_ERROR_TEXT = "The session is busy. Please try again shortly."
TENANT_SERVICE_UNAVAILABLE_TEXT = "Tenant service is temporarily unavailable."
MESSAGE_IN_PROGRESS_ERROR_TEXT = "This message is already being processed."
IDEMPOTENCY_CONFLICT_ERROR_TEXT = "This message identifier conflicts with a different message."
ACCESS_DENIED_TEXT = "Access is not allowed."
RATE_LIMITED_TEXT = "Too many requests. Please try again shortly."
USAGE_BUDGET_EXCEEDED_TEXT = \
    "The tenant daily usage budget has been reached. Please retry after the next UTC day."
APPROVAL_CONFLICT_TEXT = "Approval is no longer available."
APPROVAL_IN_PROGRESS_TEXT = "This approval is already being processed."
APPROVAL_STALE_TEXT = "The governance policy changed. Please send your request again."
APPROVAL_EXECUTION_TEXT = "The approved action could not be completed."


def map_worker_error(code: WorkerErrorCode) -> str:
    """Map a WorkerErrorCode to a fixed public-safe error message."""
    if code in (WorkerErrorCode.TENANT_CONFIG_MISMATCH, WorkerErrorCode.TENANT_AGENT_CONFIGURATION):
        return TENANT_AGENT_CONFIG_ERROR_TEXT
    if code == WorkerErrorCode.MODEL_CONFIGURATION:
        return CONFIG_ERROR_TEXT
    if code == WorkerErrorCode.SESSION_BUSY:
        return SESSION_BUSY_ERROR_TEXT
    if code == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE:
        return TENANT_SERVICE_UNAVAILABLE_TEXT
    if code == WorkerErrorCode.MESSAGE_IN_PROGRESS:
        return MESSAGE_IN_PROGRESS_ERROR_TEXT
    if code == WorkerErrorCode.IDEMPOTENCY_CONFLICT:
        return IDEMPOTENCY_CONFLICT_ERROR_TEXT
    if code in (WorkerErrorCode.APPROVAL_NOT_AVAILABLE, WorkerErrorCode.APPROVAL_CONFLICT):
        return APPROVAL_CONFLICT_TEXT
    if code == WorkerErrorCode.APPROVAL_IN_PROGRESS:
        return APPROVAL_IN_PROGRESS_TEXT
    if code == WorkerErrorCode.APPROVAL_CONFIG_STALE:
        return APPROVAL_STALE_TEXT
    if code == WorkerErrorCode.APPROVAL_EXECUTION_FAILED:
        return APPROVAL_EXECUTION_TEXT
    if code == WorkerErrorCode.APPROVAL_REPOSITORY_UNAVAILABLE:
        return TENANT_SERVICE_UNAVAILABLE_TEXT
    if code == WorkerErrorCode.CONTENT_INPUT_BLOCKED:
        # Fixed public text (Stage 6B2): names no category, echoes nothing.
        return CONTENT_INPUT_BLOCKED_TEXT
    if code == WorkerErrorCode.USAGE_BUDGET_EXCEEDED:
        return USAGE_BUDGET_EXCEEDED_TEXT
    return SAFE_ERROR_TEXT


__all__ = [
    "ACCESS_DENIED_TEXT",
    "RATE_LIMITED_TEXT",
    "USAGE_BUDGET_EXCEEDED_TEXT",
    "APPROVAL_CONFLICT_TEXT",
    "APPROVAL_EXECUTION_TEXT",
    "APPROVAL_IN_PROGRESS_TEXT",
    "APPROVAL_STALE_TEXT",
    "CONFIG_ERROR_TEXT",
    "IDEMPOTENCY_CONFLICT_ERROR_TEXT",
    "MESSAGE_IN_PROGRESS_ERROR_TEXT",
    "SAFE_ERROR_TEXT",
    "SESSION_BUSY_ERROR_TEXT",
    "TENANT_AGENT_CONFIG_ERROR_TEXT",
    "TENANT_SERVICE_UNAVAILABLE_TEXT",
    "map_worker_error",
]

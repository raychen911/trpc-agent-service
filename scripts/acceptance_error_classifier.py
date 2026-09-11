"""Deterministic response classification used by acceptance scripts."""

from __future__ import annotations

from enum import Enum

from trpc_service.gateway.errors import (
    CONFIG_ERROR_TEXT,
    SAFE_ERROR_TEXT,
    SESSION_BUSY_ERROR_TEXT,
    TENANT_AGENT_CONFIG_ERROR_TEXT,
    TENANT_SERVICE_UNAVAILABLE_TEXT,
)


class RetryDecision(str, Enum):
    """Whether an acceptance request succeeded, may retry, or must fail."""

    RETRY = "retry"
    FAIL = "fail"
    SUCCESS = "success"


# A public Gateway response has no machine-readable error code.  Its generic
# safe text is deliberately ambiguous, so retrying it could hide a product bug.
RETRYABLE_ERRORS = frozenset()
NON_RETRYABLE_ERRORS = frozenset({
    SAFE_ERROR_TEXT,
    SESSION_BUSY_ERROR_TEXT,
    CONFIG_ERROR_TEXT,
    TENANT_AGENT_CONFIG_ERROR_TEXT,
    TENANT_SERVICE_UNAVAILABLE_TEXT,
})


def classify_gateway_response(
    http_code: str,
    response_text: str,
    marker: str,
) -> tuple[RetryDecision, str]:
    """Classify a public Gateway response without inferring hidden causes."""
    if http_code != "200":
        return RetryDecision.FAIL, f"HTTP {http_code}"
    if response_text in NON_RETRYABLE_ERRORS:
        return RetryDecision.FAIL, f"non-retryable error: {response_text}"
    if marker in response_text:
        return RetryDecision.SUCCESS, "marker found"
    return RetryDecision.FAIL, f"unknown response: {response_text[:100]}"


def classify_worker_response(
    http_code: str,
    error_code: str | None,
    response_text: str,
    marker: str,
) -> tuple[RetryDecision, str]:
    """Classify an internal Worker response using its precise error code."""
    if http_code != "200":
        return RetryDecision.FAIL, f"HTTP {http_code}"
    if error_code == "model_runtime":
        return RetryDecision.RETRY, "model_runtime error"
    if error_code is not None:
        return RetryDecision.FAIL, f"error_code: {error_code}"
    if marker in response_text:
        return RetryDecision.SUCCESS, "marker found"
    return RetryDecision.FAIL, f"unknown response: {response_text[:100]}"


__all__ = [
    "RetryDecision",
    "classify_gateway_response",
    "classify_worker_response",
    "RETRYABLE_ERRORS",
    "NON_RETRYABLE_ERRORS",
]

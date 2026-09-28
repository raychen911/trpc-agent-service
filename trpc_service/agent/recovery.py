"""Provider-neutral failure classification and bounded retry decisions."""

from dataclasses import dataclass
from enum import StrEnum
import hashlib


class FailureDisposition(StrEnum):
    """Durable action selected after one external operation fails."""

    RETRY = "retry"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"


class RetryableOperationError(RuntimeError):
    """Explicit transient failure optionally carrying a trusted retry delay."""

    def __init__(self, message: str, *, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        if retry_after_seconds is not None and retry_after_seconds < 0:
            raise ValueError("retry-after delay cannot be negative")
        self.retry_after_seconds = retry_after_seconds


class PermanentOperationError(RuntimeError):
    """Failure that cannot succeed without a configuration or input change."""


class ProviderOutcomeUnknown(RuntimeError):
    """The provider may have accepted a side effect before its response was lost."""


@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    """Safe durable classification without retaining exception text."""

    disposition: FailureDisposition
    error_code: str
    safe_summary: str
    retry_after_seconds: float | None = None


class RecoveryPolicy:
    """Classify failures and calculate deterministic exponential backoff."""

    _PERMANENT_ERRORS = (
        PermanentOperationError,
        PermissionError,
        ValueError,
        TypeError,
        LookupError,
        NotImplementedError,
    )

    @staticmethod
    def _code(error: Exception) -> str:
        # Domain failures may provide a bounded stable code so operators can
        # distinguish configuration faults without persisting secret-bearing text.
        configured = getattr(error, "error_code", None)
        return (configured
                if isinstance(configured, str) and configured else type(error).__name__)[:100]

    def classify_agent(self, error: Exception) -> RecoveryDecision:
        """Retry infrastructure/model failures but stop invalid configuration."""

        if isinstance(error, self._PERMANENT_ERRORS):
            return RecoveryDecision(
                FailureDisposition.PERMANENT,
                self._code(error),
                "Agent task cannot be executed",
            )
        retry_after = (error.retry_after_seconds
                       if isinstance(error, RetryableOperationError) else None)
        # Unknown runtime errors remain retryable within the configured attempt
        # budget. This tolerates provider SDK errors without unbounded retries.
        return RecoveryDecision(
            FailureDisposition.RETRY,
            self._code(error),
            "Agent task execution failed",
            retry_after,
        )

    def classify_delivery(self, error: Exception) -> RecoveryDecision:
        """Never blindly retry a delivery whose provider outcome is ambiguous."""

        if isinstance(error, (ProviderOutcomeUnknown, TimeoutError)):
            return RecoveryDecision(
                FailureDisposition.UNKNOWN,
                self._code(error),
                "Channel delivery outcome requires reconciliation",
            )
        if isinstance(error, self._PERMANENT_ERRORS):
            return RecoveryDecision(
                FailureDisposition.PERMANENT,
                self._code(error),
                "Channel delivery cannot be retried",
            )
        retry_after = (error.retry_after_seconds
                       if isinstance(error, RetryableOperationError) else None)
        return RecoveryDecision(
            FailureDisposition.RETRY,
            self._code(error),
            "Channel delivery failed",
            retry_after,
        )

    @staticmethod
    def retry_delay_seconds(
        *,
        operation_key: str,
        attempt_count: int,
        base_seconds: float,
        maximum_seconds: float,
        jitter_ratio: float,
        retry_after_seconds: float | None = None,
    ) -> float:
        """Return capped exponential delay with stable cross-node jitter."""

        if attempt_count < 1 or base_seconds < 0 or maximum_seconds <= 0:
            raise ValueError("retry delay inputs are invalid")
        if not 0 <= jitter_ratio <= 0.5:
            raise ValueError("retry jitter ratio must be between zero and one half")
        # Saturating the exponent avoids huge integers during a prolonged
        # database outage while retaining the configured delay ceiling.
        exponential = min(maximum_seconds, base_seconds * (2**min(attempt_count - 1, 30)))
        if exponential == 0:
            jittered = 0.0
        else:
            digest = hashlib.sha256(f"{operation_key}:{attempt_count}".encode()).digest()
            unit = int.from_bytes(digest[:8], "big") / ((1 << 64) - 1)
            jittered = exponential * (1 - jitter_ratio + (2 * jitter_ratio * unit))
        if retry_after_seconds is not None:
            # Retry-After is the provider's earliest safe retry time. The local
            # exponential cap must never make us retry before that time.
            return max(jittered, retry_after_seconds)
        return jittered

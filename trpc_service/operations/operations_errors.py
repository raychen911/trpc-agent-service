"""Stable operations errors with safe external envelopes (data-model section 7).

The rendered message of every error is derived ONLY from its stable ``code``.
Backend detail (DSN, SQL, vendor exception text, secrets, tracebacks) may be
passed to the constructor for internal logging, but it is never echoed back:
``str(error)`` stays a fixed, leak-free envelope (DEC-002/DEC-003/DEC-004).
"""

from __future__ import annotations


class OperationsError(RuntimeError):
    """Base class for the seventeen stable operations errors."""

    code = "operations_error"
    retryable = False

    def __init__(self, detail: str | None = None) -> None:
        # ``detail`` is internal-only: it may carry backend diagnostics for
        # process-local logging and must never surface in the public envelope.
        self.internal_detail = detail
        super().__init__(self._public_message())

    def _public_message(self) -> str:
        return f"[{self.code}] {self.code.replace('_', ' ').capitalize()}."

    def safe_envelope(self) -> dict[str, object]:
        """Stable external shape: code, retryable and nothing else."""
        return {"code": self.code, "retryable": self.retryable}


class TelemetryUnavailable(OperationsError):
    """Ordinary telemetry export failure; bounded degradation, business-safe."""

    code = "telemetry_unavailable"
    retryable = True


class TelemetryDropped(OperationsError):
    """Envelope expired or overflowed; already counted, do not retry."""

    code = "telemetry_dropped"
    retryable = False


class TelemetryAdapterIncompatible(OperationsError):
    """SDK span shape no longer matches the sanitizing contract; fail closed."""

    code = "telemetry_adapter_incompatible"
    retryable = False


class DiagnosticAccessDenied(OperationsError):
    """Diagnostic query is outside the caller's verified scope."""

    code = "diagnostic_access_denied"
    retryable = False


class HealthStateUnknown(OperationsError):
    """Dependency observations expired; must not be presented as ready."""

    code = "health_state_unknown"
    retryable = False


class ReleaseNotFound(OperationsError):
    """Release does not exist within the trusted scope."""

    code = "release_not_found"
    retryable = False


class ReleaseNotAuthorized(OperationsError):
    """Operator is not authorised to publish or roll back this release."""

    code = "release_not_authorized"
    retryable = False


class ReleaseConflict(OperationsError):
    """Revision/CAS conflict; caller must re-read and retry."""

    code = "release_conflict"
    retryable = True


class StaleReleaseFence(OperationsError):
    """A fenced-out controller attempted the command; reacquire lease."""

    code = "stale_release_fence"
    retryable = True


class SnapshotInvalid(OperationsError):
    """Snapshot fields or references are incomplete."""

    code = "snapshot_invalid"
    retryable = False


class SnapshotDigestMismatch(OperationsError):
    """Canonical payload digest does not match the stored snapshot."""

    code = "snapshot_digest_mismatch"
    retryable = False


class ConfigurationIncompatible(OperationsError):
    """Node runtime contract cannot serve this snapshot."""

    code = "configuration_incompatible"
    retryable = False


class QualityGatePaused(OperationsError):
    """Quality threshold breached; rollout paused pending human decision."""

    code = "quality_gate_paused"
    retryable = False


class HardGateTriggered(OperationsError):
    """Zero-tolerance gate latched; automatic rollback is in progress."""

    code = "hard_gate_triggered"
    retryable = False
    resolution = "automatic_rollback"

    def safe_envelope(self) -> dict[str, object]:
        envelope = super().safe_envelope()
        envelope["resolution"] = self.resolution
        return envelope


class RollbackTargetUnavailable(OperationsError):
    """Last-good snapshot cannot be proven usable; route fails closed."""

    code = "rollback_target_unavailable"
    retryable = False


class ReleaseStateUnavailable(OperationsError):
    """PostgreSQL release authority unreadable; no fallback to defaults."""

    code = "release_state_unavailable"
    retryable = True


class DrainTimeout(OperationsError):
    """Drain deadline reached with in-flight work; unknown work marked."""

    code = "drain_timeout"
    retryable = False

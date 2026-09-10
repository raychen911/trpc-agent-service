"""T007 RED: stable operations errors with safe envelopes."""

from __future__ import annotations

import importlib

FORBIDDEN_MARKERS = ("postgres", "redis", "dsn", "password", "secret", "token", "select ", "traceback")


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


operations_errors = _load("trpc_service.operations.operations_errors")

EXPECTED_ERRORS: tuple[tuple[str, str, bool], ...] = (
    ("TelemetryUnavailable", "telemetry_unavailable", True),
    ("TelemetryDropped", "telemetry_dropped", False),
    ("TelemetryAdapterIncompatible", "telemetry_adapter_incompatible", False),
    ("DiagnosticAccessDenied", "diagnostic_access_denied", False),
    ("HealthStateUnknown", "health_state_unknown", False),
    ("ReleaseNotFound", "release_not_found", False),
    ("ReleaseNotAuthorized", "release_not_authorized", False),
    ("ReleaseConflict", "release_conflict", True),
    ("StaleReleaseFence", "stale_release_fence", True),
    ("SnapshotInvalid", "snapshot_invalid", False),
    ("SnapshotDigestMismatch", "snapshot_digest_mismatch", False),
    ("ConfigurationIncompatible", "configuration_incompatible", False),
    ("QualityGatePaused", "quality_gate_paused", False),
    ("HardGateTriggered", "hard_gate_triggered", False),
    ("RollbackTargetUnavailable", "rollback_target_unavailable", False),
    ("ReleaseStateUnavailable", "release_state_unavailable", True),
    ("DrainTimeout", "drain_timeout", False),
)


def test_operations_errors_module_exists() -> None:
    assert operations_errors is not None, (
        "trpc_service.operations.operations_errors is not implemented yet"
    )


def test_all_seventeen_stable_errors_exist_with_codes() -> None:
    assert operations_errors is not None
    codes: set[str] = set()
    for class_name, expected_code, _retryable in EXPECTED_ERRORS:
        error_type = getattr(operations_errors, class_name, None)
        assert error_type is not None, f"missing error class {class_name}"
        error = error_type()
        code = getattr(error, "code", None)
        assert code == expected_code, f"{class_name}.code must be {expected_code!r}"
        codes.add(code)
    assert len(codes) == len(EXPECTED_ERRORS), "stable codes must be unique"


def test_retryability_matches_operations_contract() -> None:
    assert operations_errors is not None
    for class_name, _code, retryable in EXPECTED_ERRORS:
        error_type = getattr(operations_errors, class_name, None)
        assert error_type is not None, f"missing error class {class_name}"
        error = error_type()
        assert isinstance(error.retryable, bool)
        assert error.retryable is retryable, f"{class_name}.retryable must be {retryable}"


def test_error_envelopes_do_not_leak_backend_details() -> None:
    assert operations_errors is not None
    for class_name, _code, _retryable in EXPECTED_ERRORS:
        error_type = getattr(operations_errors, class_name, None)
        assert error_type is not None, f"missing error class {class_name}"
        rendered = str(error_type("detail postgres://user:pass@host/db"))
        lowered = rendered.lower()
        for marker in FORBIDDEN_MARKERS:
            assert marker not in lowered, (
                f"{class_name} envelope leaks {marker!r}: {rendered!r}"
            )


def test_hard_gate_error_is_automatic_rollback_category() -> None:
    assert operations_errors is not None
    hard = getattr(operations_errors, "HardGateTriggered", None)
    assert hard is not None
    error = hard()
    assert error.code == "hard_gate_triggered"
    resolution = getattr(error, "resolution", None)
    assert resolution == "automatic_rollback", (
        "hard gate must be classified as automatic rollback, not human decision"
    )
